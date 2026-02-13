from __future__ import annotations

import asyncio
import sys
import types

# loguru が無くてもテストが通るように簡易スタブを用意
if "loguru" not in sys.modules:  # pragma: no cover - テスト用スタブ
    stub = types.ModuleType("loguru")

    class _StubLogger:
        """loguru.logger の最小限スタブ。"""

        def __getattr__(self, name: str):  # pragma: no cover - 単純スタブ
            def _(*args, **kwargs):
                return None

            return _

    stub.logger = _StubLogger()
    sys.modules["loguru"] = stub


# pydantic が無くてもテストが通るようにスタブを用意
try:  # pragma: no cover - pydantic が入っている環境では本物を使う
    import pydantic  # type: ignore  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover - スタブ側
    pydantic_stub = types.ModuleType("pydantic")

    class _BaseModel:
        """pydantic.BaseModel のごく簡易な代替。"""

        def __init__(self, **data):
            for key, value in data.items():
                setattr(self, key, value)

        def model_dump(self, *_, **__):  # pragma: no cover - テスト用途の簡易API
            return dict(self.__dict__)

        def model_copy(self, *, update: dict | None = None):  # pragma: no cover - 簡易コピー
            payload = dict(self.__dict__)
            if update:
                payload.update(update)
            return self.__class__(**payload)

    pydantic_stub.BaseModel = _BaseModel
    sys.modules["pydantic"] = pydantic_stub


# SQLAlchemy が無い環境向けに Repo のスタブを用意
repo_module_stub: types.ModuleType | None = None
try:  # pragma: no cover - SQLAlchemy が入っている環境では本物を使う
    from bot.data.repo import Repo
except ModuleNotFoundError as exc:  # pragma: no cover - スタブ定義側
    if exc.name == "sqlalchemy":
        repo_module_stub = types.ModuleType("bot.data.repo")

        class Repo:  # type: ignore[no-redef]
            """SQLAlchemy 未インストール環境向けの簡易 Repo スタブ。"""

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


class DummyOms:
    """FundingBasisStrategy のテスト用に使う最小限の OMS スタブ。"""

    def __init__(self) -> None:
        self.submitted = []
        self.hedges = []
        self._seq = 0

    async def submit(self, req, *, meta=None):
        """発注内容を記録するだけの submit。"""

        self._seq += 1
        self.submitted.append((req, meta))
        return types.SimpleNamespace(order_id=f"DUMMY-{self._seq}")

    async def submit_hedge(self, symbol: str, delta_to_neutral: float, *, meta=None):
        """ヘッジ内容を記録するだけの submit_hedge。"""

        self.hedges.append((symbol, delta_to_neutral, meta))
        return None


def test_funding_basis_open_hedge_close():
    """FundingBasisStrategy が OPEN → HEDGE → CLOSE の流れを取れることを確認する。"""

    async def _scenario() -> None:
        from bot.config.models import RiskConfig, StrategyFundingConfig
        from bot.exchanges.types import FundingInfo
        from bot.strategy.funding_basis.engine import FundingBasisStrategy
        from bot.strategy.funding_basis.models import DecisionAction

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

        # 市場データ READY 判定を通すための簡易ゲートウェイ（テスト専用）
        sym = "BTCUSDT"
        px = 30000.0
        gw = types.SimpleNamespace()
        gw._scale_cache = {sym: {"priceScale": 2}}
        gw._price_state = {sym: "READY"}
        gw._last_spot_px = {sym: px}
        gw._last_index_px = {sym: px}
        gw._bbo_cache = {sym: {"bid": px * 0.999, "ask": px * 1.001, "ts": None}}
        # FundingBasisStrategy._market_data_ready / _get_bbo が OMS 経由で参照できるようにする
        oms._ex = gw

        funding_positive = FundingInfo(
            symbol="BTCUSDT",
            current_rate=0.0,
            predicted_rate=0.0006,
            next_funding_time=None,
        )
        decision_open = strategy.evaluate(funding=funding_positive, spot_price=px, perp_price=px + 100.0)
        assert decision_open.action is DecisionAction.OPEN
        await strategy.execute(decision_open, spot_price=px, perp_price=px + 100.0)

        # OPEN では perp=SELL / spot=BUY の 2 本が出る想定
        assert len(oms.submitted) == 2
        assert oms.submitted[0][0].side == "sell"
        assert oms.submitted[1][0].side == "buy"

        # テスト用途として内部 _holdings を直接いじってデルタ乖離を作る
        holding = strategy._holdings.get("BTCUSDT")  # noqa: SLF001 - テスト用に内部状態へアクセス
        assert holding is not None
        # スポットだけ少し増やして、リバランスバンドを超えるデルタを作る
        holding.spot_qty += 0.01

        decision_hedge = strategy.evaluate(funding=funding_positive, spot_price=px, perp_price=px + 100.0)
        assert decision_hedge.action is DecisionAction.HEDGE
        assert decision_hedge.delta_to_neutral != 0
        await strategy.execute(decision_hedge, spot_price=px, perp_price=px + 100.0)
        assert oms.hedges, "ヘッジ注文が記録されていない"

        # Funding が大きく低下し、マイナス側に傾いたケースを CLOSE 判定として扱う
        funding_drop = FundingInfo(
            symbol="BTCUSDT",
            current_rate=0.0,
            predicted_rate=-0.00001,
            next_funding_time=None,
        )
        decision_close = strategy.evaluate(funding=funding_drop, spot_price=px, perp_price=px + 100.0)
        assert decision_close.action is DecisionAction.CLOSE
        await strategy.execute(decision_close, spot_price=px, perp_price=px + 100.0)

        assert not strategy._holdings.symbols()  # noqa: SLF001 - 全シンボルがクローズされているはず

    asyncio.run(_scenario())
