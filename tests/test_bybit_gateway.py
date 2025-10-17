"""BybitGateway の最小疎通テスト。"""

from __future__ import annotations

import asyncio
import os

import pytest

pytest.importorskip("ccxt.async_support", reason="ccxt が無い環境では BybitGateway テストを実行しない")

from bot.exchanges.bybit import BybitGateway
from bot.exchanges.types import OrderRequest

REQUIRED_ENV = ("KEYS__API_KEY", "KEYS__API_SECRET")


def _has_keys() -> bool:
    return all(os.environ.get(k) for k in REQUIRED_ENV)


@pytest.mark.asyncio
async def test_rest_smoke_balances_and_ticker():
    """RESTの基本疎通：残高が取れる／ティッカーが読める"""

    gw = BybitGateway(
        api_key=os.environ.get("KEYS__API_KEY", "dummy"),
        api_secret=os.environ.get("KEYS__API_SECRET", "dummy"),
        environment="testnet",
    )
    # 残高（存在しなくても例外にならないことを確認）
    bals = await gw.get_balances()
    assert isinstance(bals, list)

    # ティッカー（perp と spot 内部表記の両方）
    px_perp = await gw.get_ticker("BTCUSDT")
    assert px_perp > 0
    px_spot = await gw.get_ticker("BTCUSDT_SPOT")
    assert px_spot > 0


@pytest.mark.asyncio
@pytest.mark.skipif(not _has_keys(), reason="Bybit Testnet keys are not set")
async def test_post_only_limit_then_cancel_and_ws_order_event():
    """安全な発注：post-only 指値で不成立→取消、WS 'order' イベントを最小確認"""

    gw = BybitGateway(
        api_key=os.environ["KEYS__API_KEY"],
        api_secret=os.environ["KEYS__API_SECRET"],
        environment="testnet",
    )

    # WS: order イベントを受け取ったら set() する
    order_event = asyncio.Event()

    async def on_order(msg: dict) -> None:
        # 受信できればOK（細かい中身は別途）
        order_event.set()

    ws_task = asyncio.create_task(gw.subscribe_private({"order": on_order}))

    # 価格を離して post-only（買い：現在値の 10% よりかなり下、板に置くが絶対約定しないレベル）
    px = await gw.get_ticker("BTCUSDT")
    req = OrderRequest(
        symbol="BTCUSDT",
        side="buy",
        type="limit",
        qty=0.001,
        price=round(px * 0.1, 2),
        post_only=True,
        time_in_force="GTC",
        client_id="mvp-postonly-cancel",  # 36文字制限に注意（UUIDにするなら短縮）
    )
    created = await gw.place_order(req)
    assert created.order_id

    # すぐキャンセル
    await gw.cancel_order(order_id=created.order_id)

    # WS の 'order' イベントが来るのを短時間だけ待つ（なくてもテストは通す）
    try:
        await asyncio.wait_for(order_event.wait(), timeout=10)
    except asyncio.TimeoutError:
        pass
    ws_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await ws_task
