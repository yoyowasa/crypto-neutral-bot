from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from loguru import logger

from bot.backtest.replay import FundingSchedule, ReplayDataSource
from bot.config.loader import load_config
from bot.oms.engine import OmsEngine
from bot.oms.fill_sim import PaperExchange
from bot.strategy.funding_basis.engine import FundingBasisStrategy
from bot.strategy.funding_basis.models import Decision, DecisionAction


async def main() -> None:
    # 設定読み込み（symbolsなど）
    cfg = load_config()
    symbol = cfg.strategy.symbols[0]

    # バックテスト用データソース（リアルWS不要）
    sched = (
        FundingSchedule(path=".backtest_empty_funding.csv")
        if False
        else FundingSchedule(path=".backtest_empty_funding.csv")
    )
    data_src = ReplayDataSource(schedule=sched)
    data_src.set_now(datetime.now(timezone.utc))

    # PaperExchange + OMS
    paper = PaperExchange(data_source=data_src, initial_usdt=100_000.0)
    oms = OmsEngine(ex=paper, repo=None, cfg=None)
    paper.bind_oms(oms)

    # Strategy
    strat = FundingBasisStrategy(
        oms=oms,
        risk_config=cfg.risk,
        strategy_config=cfg.strategy,
        period_seconds=8.0 * 3600.0,
    )

    # 市場データREADYを通すための最低限の擬似データ（スケール/価格状態/アンカー/BBO）
    if not hasattr(paper, "_scale_cache"):
        paper._scale_cache = {}
    if not hasattr(paper, "_price_state"):
        paper._price_state = {}
    paper._scale_cache[symbol] = {"priceScale": 2}
    paper._price_state[symbol] = "READY"
    # アンカー（spot→index）としてlastを入れる
    px = 100.0
    paper._last_spot_px = {symbol: px}
    paper._last_index_px = {symbol: px}
    # BBOの供給（_get_bbo が拾える代表的な属性）
    paper._bbo = {symbol: (px * 0.999, px * 1.001)}

    # OPEN決定を直接作って実行（evaluateは通さず、数量算出の挙動だけ確認）
    dec = Decision(
        action=DecisionAction.OPEN,
        symbol=symbol,
        reason="quick-open-test",
        notional=1000.0,
        perp_side="sell",
        spot_side="buy",
    )

    await strat._open_basis_position(decision=dec, spot_price=px, perp_price=px)
    logger.info("quick_open_test done for {}", symbol)


if __name__ == "__main__":
    asyncio.run(main())
