from __future__ import annotations

import asyncio
import sys
import types


if "loguru" not in sys.modules:  # pragma: no cover - テスト環境用スタブ
    stub = types.ModuleType("loguru")

    class _StubLogger:
        """loguru.logger の簡易スタブ。"""

        def __getattr__(self, name: str):  # pragma: no cover - 単純スタブ
            def _(*args, **kwargs):
                return None

            return _

    stub.logger = _StubLogger()
    sys.modules["loguru"] = stub

try:  # pragma: no cover - pydantic 未導入環境向け
    import pydantic  # type: ignore  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover - スタブを注入
    pydantic_stub = types.ModuleType("pydantic")

    class _BaseModel:
        """pydantic.BaseModel の最低限スタブ。"""

        def __init__(self, **data):
            for key, value in data.items():
                setattr(self, key, value)

        def model_dump(self, *_, **__):  # pragma: no cover - テスト用最低限API
            return dict(self.__dict__)

        def model_copy(self, *, update: dict | None = None):  # pragma: no cover - 最低限互換
            payload = dict(self.__dict__)
            if update:
                payload.update(update)
            return self.__class__(**payload)

    pydantic_stub.BaseModel = _BaseModel
    sys.modules["pydantic"] = pydantic_stub

repo_module_stub: types.ModuleType | None = None
try:  # pragma: no cover - SQLAlchemy 未導入環境向け
    from bot.data.repo import Repo
except ModuleNotFoundError as exc:  # pragma: no cover - スタブ挿入
    if exc.name == "sqlalchemy":
        repo_module_stub = types.ModuleType("bot.data.repo")

        class Repo:  # type: ignore[no-redef]
            """SQLAlchemy 非導入環境で利用するための簡易 Repo スタブ。"""

            def __init__(self, db_url: str = "") -> None:  # pragma: no cover - スタブ
                self.db_url = db_url

            async def create_all(self) -> None:  # pragma: no cover - スタブ
                return None

            async def dispose(self) -> None:  # pragma: no cover - スタブ
                return None

        repo_module_stub.Repo = Repo
        sys.modules["bot.data.repo"] = repo_module_stub
    else:
        raise

from bot.config.models import RiskConfig, StrategyFundingConfig
from bot.exchanges.types import FundingInfo
from bot.strategy.funding_basis.engine import FundingBasisStrategy
from bot.strategy.funding_basis.models import DecisionAction


class DummyOms:
    """FundingBasisStrategy のテスト用に最低限のインターフェースを提供するダミーOMS。"""

    def __init__(self) -> None:
        self.submitted = []
        self.hedges = []

    async def submit(self, req):
        """発注内容を記録するだけの疑似 submit。"""

        self.submitted.append(req)
        return None

    async def submit_hedge(self, symbol: str, delta_to_neutral: float):
        """ヘッジ要求を記録するだけの疑似 submit_hedge。"""

        self.hedges.append((symbol, delta_to_neutral))
        return None


def test_funding_basis_open_hedge_close():
    """FundingBasisStrategy が評価→発注→ヘッジ→解消まで一連の流れを辿れること。"""

    async def _scenario() -> None:
        oms = DummyOms()
        risk_cfg = RiskConfig(
            max_total_notional=100000.0,
            max_symbol_notional=60000.0,
            max_net_delta=1.0,
            max_slippage_bps=50.0,
            loss_cut_daily_jpy=100000.0,
        )
        strat_cfg = StrategyFundingConfig(
            symbols=["BTCUSDT"],
            min_expected_apr=0.05,
            pre_event_open_minutes=60,
            hold_across_events=True,
            rebalance_band_bps=5.0,
        )
        strategy = FundingBasisStrategy(
            oms=oms,
            risk_config=risk_cfg,
            strategy_config=strat_cfg,
            period_seconds=8.0 * 3600.0,
            taker_fee_bps_roundtrip=2.0,
            estimated_slippage_bps=1.0,
        )

        funding_positive = FundingInfo(symbol="BTCUSDT", current_rate=0.0, predicted_rate=0.0006, next_funding_time=None)
        decision_open = strategy.evaluate(funding=funding_positive, spot_price=30000.0, perp_price=30100.0)
        assert decision_open.action is DecisionAction.OPEN
        await strategy.execute(decision_open, spot_price=30000.0, perp_price=30100.0)

        assert len(oms.submitted) == 2
        assert oms.submitted[0].side == "sell"
        assert oms.submitted[1].side == "buy"

        holding = strategy._holdings.get("BTCUSDT")  # noqa: SLF001 - テスト用途で内部状態を参照
        assert holding is not None
        holding.spot_qty += 0.001

        decision_hedge = strategy.evaluate(funding=funding_positive, spot_price=30000.0, perp_price=30100.0)
        assert decision_hedge.action is DecisionAction.HEDGE
        assert decision_hedge.delta_to_neutral != 0
        await strategy.execute(decision_hedge, spot_price=30000.0, perp_price=30100.0)
        assert oms.hedges, "ヘッジ注文が記録されていない"

        funding_drop = FundingInfo(symbol="BTCUSDT", current_rate=0.0, predicted_rate=0.00001, next_funding_time=None)
        decision_close = strategy.evaluate(funding=funding_drop, spot_price=30000.0, perp_price=30100.0)
        assert decision_close.action is DecisionAction.CLOSE
        await strategy.execute(decision_close, spot_price=30000.0, perp_price=30100.0)

        assert not strategy._holdings.symbols()  # noqa: SLF001 - テスト用途

    asyncio.run(_scenario())
