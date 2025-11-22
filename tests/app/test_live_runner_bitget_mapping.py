from __future__ import annotations

import asyncio
from typing import Any

from bot.app.live_runner import _handle_private_execution


class _StubOms:
    """Private WS -> OMS イベント変換を検証するための簡易スタブ。"""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.touched_ts: list[Any] = []

    def touch_private_ws(self, ts: Any) -> None:
        self.touched_ts.append(ts)

    async def on_execution_event(self, event: dict[str, Any]) -> None:
        self.events.append(event)


def test_handle_private_execution_bitget_liquidity_and_fee() -> None:
    """Bitget 互換 row から liquidity / fee などが正しく event に写ることを検証する。"""

    async def _scenario() -> None:
        oms = _StubOms()

        row = {
            "orderId": "123456",
            "orderLinkId": "CID-1",
            "execQty": "0.01",
            "cumExecQty": "0.02",
            "execPrice": "30000.5",
            "symbol": "BTCUSDT",
            "side": "buy",
            # bitget.py 側の互換処理で付与される想定のフィールド
            "execFee": "-0.02",
            "feeCurrency": "USDT",
            "liquidity": "TAKER",
            "execTime": "1710000000000",
        }
        msg = {"data": [row]}

        await _handle_private_execution(msg, oms)

        assert len(oms.events) == 1
        ev = oms.events[0]

        # 数量・価格
        assert ev["client_id"] == "CID-1"
        assert ev["order_id"] == "123456"
        assert ev["last_filled_qty"] == 0.01
        assert ev["cum_filled_qty"] == 0.02
        assert ev["avg_fill_price"] == 30000.5

        # 手数料・通貨
        assert float(ev["fee"]) == -0.02
        assert ev["fee_currency"] == "USDT"

        # 流動性（execType→liquidity 互換フィールド経由）
        assert ev["liquidity"] == "TAKER"

    asyncio.run(_scenario())
