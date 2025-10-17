from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("loguru")

from bot.core.time import utc_now
from bot.data.repo import Repo


@pytest.mark.asyncio
async def test_create_tables_and_trade_roundtrip(tmp_path: Path):
    """スキーマ作成→TradeLogを書いて→読み戻せること"""
    dbp = tmp_path / "db" / "trading.db"
    url = f"sqlite+aiosqlite:///{dbp}"
    repo = Repo(db_url=url)

    await repo.create_all()
    assert dbp.exists(), "SQLiteファイルが作成されていること"

    # 1件追加
    t0 = await repo.add_trade(
        ts=utc_now(),
        symbol="BTCUSDT",
        side="buy",
        qty=0.001,
        price=30000.0,
        fee=0.1,
        exchange_order_id="EX-1",
        client_id="CID-1",
    )
    assert t0.id is not None

    # 読み戻し
    rows = await repo.list_trades(symbol="BTCUSDT")
    assert len(rows) >= 1
    got = rows[0]
    assert got.symbol == "BTCUSDT"
    assert got.side in ("buy", "sell")


@pytest.mark.asyncio
async def test_order_funding_daily_roundtrip(tmp_path: Path):
    """OrderLog / FundingEvent / DailyPnlも書けて読めること"""
    dbp = tmp_path / "db" / "trading.db"
    url = f"sqlite+aiosqlite:///{dbp}"
    repo = Repo(db_url=url)

    await repo.create_all()

    o0 = await repo.add_order_log(
        symbol="ETHUSDT",
        side="sell",
        type="limit",
        qty=0.01,
        price=2000.0,
        status="new",
        exchange_order_id="EX-2",
        client_id="CID-2",
    )
    assert o0.id is not None

    f0 = await repo.add_funding_event(
        symbol="ETHUSDT",
        rate=0.0001,
        notional=1000.0,
        realized_pnl=0.1,
    )
    assert f0.id is not None

    d0 = await repo.add_daily_pnl(date="2024-01-01", gross=10.0, fees=1.0, net=9.0)
    assert d0.id is not None

    oo = await repo.list_order_logs(symbol="ETHUSDT")
    ff = await repo.list_funding_events(symbol="ETHUSDT")
    dd = await repo.list_daily_pnl(date="2024-01-01")

    assert len(oo) >= 1
    assert len(ff) >= 1
    assert len(dd) >= 1
