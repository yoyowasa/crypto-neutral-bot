from __future__ import annotations

import pytest

from bot.data.repo import Repo
from bot.exchanges.base import ExchangeGateway
from bot.exchanges.types import Balance, Order, OrderRequest, Position
from bot.monitor.metrics import MetricsLogger


class _DummyEx(ExchangeGateway):
    async def get_balances(self):
        return [
            Balance(asset="USDT", total=10000.0, available=10000.0),
            Balance(asset="BTC", total=0.1, available=0.1),
        ]

    async def get_positions(self):
        return [Position(symbol="BTCUSDT", side="long", size=0.1, entry_price=30000.0, unrealized_pnl=0.0)]

    async def get_open_orders(self, symbol: str | None = None):  # pragma: no cover - 未使用
        return []

    async def place_order(self, req: OrderRequest) -> Order:  # pragma: no cover - 未使用
        raise NotImplementedError

    async def cancel_order(
        self, order_id: str | None = None, client_id: str | None = None
    ):  # pragma: no cover - 未使用
        raise NotImplementedError

    async def get_ticker(self, symbol: str) -> float:
        return 30000.0

    async def get_funding_info(self, symbol: str):  # pragma: no cover - 未使用
        raise NotImplementedError

    async def subscribe_private(self, callbacks):  # pragma: no cover - 未使用
        pass

    async def subscribe_public(self, symbols, callbacks):  # pragma: no cover - 未使用
        pass


@pytest.mark.asyncio
async def test_metrics_one_shot_runs(tmp_path):
    """メトリクスのone_shotが例外なく実行できること"""

    repo = Repo(db_url=f"sqlite+aiosqlite:///{tmp_path/'db'/'t.db'}")
    await repo.create_all()
    ex = _DummyEx()
    m = MetricsLogger(ex=ex, repo=repo, symbols=["BTCUSDT"])
    await m.one_shot()
