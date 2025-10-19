from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from bot.config.models import RiskConfig, StrategyFundingConfig
from bot.backtest.replay import BacktestRunner, CsvPriceFeed, FundingSchedule


@pytest.mark.asyncio
async def test_backtest_replay_one_day(tmp_path):
    """小さなCSVで1日リプレイが完走し、Fundingイベントが記録されること"""

    # 価格CSV（UTC 00:00～00:01:10 の間に複数ティック）
    prices_csv = tmp_path / "prices.csv"
    df = pd.DataFrame(
        [
            # ts, symbol, bid, ask, last
            ["2024-01-01T00:00:00Z", "BTCUSDT", 100.0, 100.1, 100.05],
            ["2024-01-01T00:00:10Z", "BTCUSDT", 100.0, 100.1, 100.05],
            # Funding前（1分後）に十分stepが走るようにティックを置く
            ["2024-01-01T00:00:30Z", "BTCUSDT", 100.0, 100.1, 100.05],
            ["2024-01-01T00:01:00Z", "BTCUSDT", 100.0, 100.1, 100.05],  # Funding時刻
            ["2024-01-01T00:01:10Z", "BTCUSDT", 100.0, 100.1, 100.05],
        ],
        columns=["ts", "symbol", "bid", "ask", "last"],
    )
    df.to_csv(prices_csv, index=False)

    # Funding CSV（1分後に +0.001 の支払/受取。predicted=nextとして扱う）
    funding_csv = tmp_path / "funding.csv"
    fd = pd.DataFrame(
        [
            ["2024-01-01T00:01:00Z", "BTCUSDT", 0.001],
        ],
        columns=["ts", "symbol", "rate"],
    )
    fd.to_csv(funding_csv, index=False)

    # 設定（MVP：閾値を緩くして確実にOPENする）
    strategy_cfg = StrategyFundingConfig(
        symbols=["BTCUSDT"],
        min_expected_apr=0.0,  # 必ず閾値を満たす
        pre_event_open_minutes=60,  # Fundingの1分前ならOK
        hold_across_events=True,
        rebalance_band_bps=5.0,
    )
    risk_cfg = RiskConfig(
        max_total_notional=1_000_000.0,
        max_symbol_notional=1_000_000.0,
        max_net_delta=0.01,
        max_slippage_bps=50.0,
        loss_cut_daily_jpy=1_000_000.0,
    )

    feed = CsvPriceFeed(path=str(prices_csv))
    sched = FundingSchedule(path=str(funding_csv))
    db_url = f"sqlite+aiosqlite:///{tmp_path/'db'/'bt.db'}"

    runner = BacktestRunner(
        price_feed=feed,
        funding_schedule=sched,
        strategy_cfg=strategy_cfg,
        risk_cfg=risk_cfg,
        db_url=db_url,
        step_sec=1.0,  # 1秒間隔でstepを回す
    )
    res = await runner.run_one_day(date_utc="2024-01-01")

    # Fundingイベントが1件以上、NetPnLが計算できている
    assert res.funding_events >= 1
    # +rate で perp short（想定）なら受取がプラス寄与になる
    # 厳密な金額までは検証しない（コストを入れていないため）
    assert isinstance(res.net_pnl, float)

