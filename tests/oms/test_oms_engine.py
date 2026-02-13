from __future__ import annotations

import asyncio
import sys
import types
from dataclasses import dataclass
from datetime import timedelta

import pytest

if "loguru" not in sys.modules:  # pragma: no cover - テスト環境用スタブ
    stub = types.ModuleType("loguru")

    class _StubLogger:
        """これは何をするクラス？→ loguru.logger の簡易スタブです。"""

        def __getattr__(self, name: str):  # pragma: no cover - 単純スタブ
            def _(*args, **kwargs):
                return None

            return _

    stub.logger = _StubLogger()
    sys.modules["loguru"] = stub

if "pydantic" not in sys.modules:  # pragma: no cover - テスト環境用スタブ
    pydantic_stub = types.ModuleType("pydantic")

    class _BaseModel:
        """これは何をするクラス？→ Pydantic BaseModel の簡易代替です。"""

        def __init__(self, **data):
            for key, value in data.items():
                setattr(self, key, value)

        def model_copy(self, *, update: dict | None = None):  # pragma: no cover - 最小互換
            values = dict(self.__dict__)
            if update:
                values.update(update)
            return self.__class__(**values)

    pydantic_stub.BaseModel = _BaseModel
    sys.modules["pydantic"] = pydantic_stub

from bot.core.time import utc_now

repo_module_stub: types.ModuleType | None = None
try:
    from bot.data.repo import Repo
except ModuleNotFoundError as exc:  # pragma: no cover - SQLAlchemy未導入環境
    if exc.name == "sqlalchemy":
        repo_module_stub = types.ModuleType("bot.data.repo")

        class Repo:  # type: ignore[no-redef]
            """これは何をするクラス？→ SQLAlchemy 非導入時の簡易Repoスタブです。"""

            def __init__(self, db_url: str = "") -> None:  # pragma: no cover - スタブ
                self.db_url = db_url
                self.order_logs: list[dict] = []
                self.trades: list[dict] = []

            async def create_all(self) -> None:  # pragma: no cover - スタブ
                return None

            async def add_order_log(self, **data):  # pragma: no cover - スタブ
                self.order_logs.append(data)
                return data

            async def add_trade(self, **data):  # pragma: no cover - スタブ
                self.trades.append(data)
                return data

            async def dispose(self) -> None:  # pragma: no cover - スタブ
                return None

        repo_module_stub.Repo = Repo
        sys.modules["bot.data.repo"] = repo_module_stub
    else:  # pragma: no cover - 想定外の欠落はそのまま再送
        raise

if repo_module_stub is not None:  # pragma: no cover - スタブ適用時
    Repo = repo_module_stub.Repo  # type: ignore[attr-defined]

from bot.exchanges.base import ExchangeGateway  # noqa: E402
from bot.exchanges.types import Order, OrderRequest  # noqa: E402
from bot.oms.engine import OmsEngine  # noqa: E402
from bot.oms.types import OmsConfig, OrderLifecycleState  # noqa: E402


# ---- モック取引所（最小実装） ----
@dataclass
class _MockOrderRec:
    order_id: str
    client_id: str
    req: OrderRequest
    status: str = "new"
    filled: float = 0.0
    avg: float | None = None


class MockExchange(ExchangeGateway):
    """これは何をするクラス？→ テスト用の最小モック取引所です。"""

    def __init__(self) -> None:
        self.placed: dict[str, _MockOrderRec] = {}
        self.canceled: set[str] = set()
        self._counter = 0
        # OmsEngine の制約チェックを通すため、最小限のスケール情報を持たせる。
        self._scale_cache = {
            "BTCUSDT": {
                "minQty_perp": 0.001,
                "qtyStep_perp": 0.001,
                "minNotional_perp": 1.0,
            },
            "ETHUSDT": {
                "minQty_perp": 0.001,
                "qtyStep_perp": 0.001,
                "minNotional_perp": 1.0,
            },
        }

    async def get_balances(self):  # pragma: no cover - 未使用
        return []

    async def get_positions(self):  # pragma: no cover - 未使用
        return []

    async def get_open_orders(self, symbol: str | None = None):  # pragma: no cover - 未使用
        return []

    async def place_order(self, req: OrderRequest) -> Order:
        """これは何をする関数？→ 発注リクエストを記録してダミー注文を返します。"""

        self._counter += 1
        order_id = f"OID-{self._counter}"
        client_id = req.client_id or order_id
        self.placed[client_id] = _MockOrderRec(order_id=order_id, client_id=client_id, req=req)
        return Order(order_id=order_id, client_id=req.client_id, status="new", filled_qty=0.0, avg_fill_price=None)

    async def cancel_order(self, order_id: str | None = None, client_id: str | None = None) -> None:
        """これは何をする関数？→ キャンセル対象の注文IDを記録します。"""

        if order_id:
            self.canceled.add(order_id)
        elif client_id and client_id in self.placed:
            self.canceled.add(self.placed[client_id].order_id)

    async def get_ticker(self, symbol: str):  # pragma: no cover - 未使用
        # 成行再送の最小notionalチェックを通すため、シンボル別の簡易価格を返す。
        if symbol == "BTCUSDT":
            return 10.0
        if symbol == "ETHUSDT":
            return 2000.0
        return 1.0

    async def get_funding_info(self, symbol: str):  # pragma: no cover - 未使用
        raise NotImplementedError

    async def subscribe_private(self, callbacks):  # pragma: no cover - 未使用
        raise NotImplementedError

    async def subscribe_public(self, symbols, callbacks):  # pragma: no cover - 未使用
        raise NotImplementedError


def test_partial_fill_then_resend_then_fill(tmp_path):
    """発注→部分約定→再送（IOC成行）→全約定 が動くこと"""

    if Repo is None:  # pragma: no cover - SQLAlchemy未導入時
        pytest.skip("SQLAlchemy is not installed")

    async def _scenario() -> None:
        repo = Repo(db_url=f"sqlite+aiosqlite:///{tmp_path / 'db' / 't.db'}")
        await repo.create_all()
        ex = MockExchange()
        oms = OmsEngine(ex, repo, OmsConfig(order_timeout_sec=5.0, max_retries=2))

        req = OrderRequest(
            symbol="BTCUSDT",
            side="buy",
            type="limit",
            qty=1.0,
            price=10.0,
            time_in_force="GTC",
            post_only=True,
        )
        created = await oms.submit(req)
        assert created.order_id
        first_cid = req.client_id

        await oms.on_execution_event(
            {
                "client_id": first_cid,
                "status": "partially_filled",
                "last_filled_qty": 0.4,
                "cum_filled_qty": 0.4,
                "avg_fill_price": 10.0,
            }
        )

        assert len(ex.placed) >= 2
        child_cids = [cid for cid in ex.placed if cid != first_cid]
        assert child_cids, "再送注文が作られていること"
        child_cid = child_cids[-1]
        child = ex.placed[child_cid]
        assert child.req.type == "market"
        assert abs(child.req.qty - 0.6) < 1e-9

        await oms.on_execution_event(
            {
                "client_id": child_cid,
                "status": "filled",
                "last_filled_qty": 0.6,
                "cum_filled_qty": 0.6,
                "avg_fill_price": 10.1,
            }
        )

        await repo.dispose()

    asyncio.run(_scenario())


def test_timeout_then_cancel_and_resend(tmp_path):
    """タイムアウト→取消→（残ありなら）成行IOCで再送が動くこと"""

    if Repo is None:  # pragma: no cover - SQLAlchemy未導入時
        pytest.skip("SQLAlchemy is not installed")

    async def _scenario() -> None:
        repo = Repo(db_url=f"sqlite+aiosqlite:///{tmp_path / 'db' / 't.db'}")
        await repo.create_all()
        ex = MockExchange()
        oms = OmsEngine(ex, repo, OmsConfig(order_timeout_sec=0.1, max_retries=1))

        req = OrderRequest(
            symbol="ETHUSDT",
            side="sell",
            type="limit",
            qty=2.0,
            price=2000.0,
            time_in_force="GTC",
            post_only=True,
        )
        created = await oms.submit(req)
        assert created.order_id

        managed = oms._orders[req.client_id]
        managed.sent_at = utc_now() - timedelta(seconds=1)

        await oms.process_timeouts()

        assert created.order_id in ex.canceled or managed.state == OrderLifecycleState.CANCELED
        assert len(ex.placed) >= 2

        await repo.dispose()

    asyncio.run(_scenario())
