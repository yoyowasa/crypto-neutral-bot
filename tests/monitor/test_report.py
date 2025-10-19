from __future__ import annotations

from datetime import datetime, timezone

import pytest

from bot.data.repo import Repo
from bot.monitor.report import generate_daily_report


@pytest.mark.asyncio
async def test_generate_daily_report_creates_file(tmp_path):
    """日次レポートが生成され、ファイルが存在すること"""

    repo = Repo(db_url=f"sqlite+aiosqlite:///{tmp_path/'db'/'t.db'}")
    await repo.create_all()

    # 最低限のダミーデータ（手数料1件、Funding1件）
    await repo.add_funding_event(symbol="BTCUSDT", rate=0.0001, notional=1000.0, realized_pnl=0.1)
    await repo.add_trade(
        symbol="BTCUSDT",
        side="buy",
        qty=0.001,
        price=30000.0,
        fee=0.5,
        exchange_order_id="X",
        client_id="C",
    )

    out_dir = tmp_path / "reports"
    path = await generate_daily_report(
        repo=repo, date_str=datetime.now(timezone.utc).date().isoformat(), out_dir=str(out_dir)
    )
    assert path.exists()
    text = path.read_text(encoding="utf-8")
    assert "Daily Report" in text
    assert "Funding PnL" in text
