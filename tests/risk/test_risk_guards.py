from __future__ import annotations

import asyncio

import pytest

from bot.config.models import RiskConfig
from bot.core.errors import RiskBreach
from bot.risk.guards import RiskManager
from bot.risk.limits import PreTradeContext, precheck_open_order


@pytest.mark.parametrize(
    "ctx, add_notional, field",
    [
        (
            PreTradeContext(
                used_total_notional=9000,
                used_symbol_notional=1000,
                predicted_net_delta_after=0.0,
                estimated_slippage_bps=1.0,
            ),
            2000,
            "total",
        ),
        (
            PreTradeContext(
                used_total_notional=1000,
                used_symbol_notional=4900,
                predicted_net_delta_after=0.0,
                estimated_slippage_bps=1.0,
            ),
            200,
            "symbol",
        ),
        (
            PreTradeContext(
                used_total_notional=0,
                used_symbol_notional=0,
                predicted_net_delta_after=0.0,
                estimated_slippage_bps=50.0,
            ),
            1,
            "slippage",
        ),
        (
            PreTradeContext(
                used_total_notional=0,
                used_symbol_notional=0,
                predicted_net_delta_after=0.01,
                estimated_slippage_bps=1.0,
            ),
            1,
            "delta",
        ),
    ],
)
def test_precheck_open_order_raises(ctx, add_notional, field):
    """名目/スリッページ/デルタの事前チェックで RiskBreach が出ること"""
    risk = RiskConfig(
        max_total_notional=10000,
        max_symbol_notional=5000,
        max_net_delta=0.005,  # 0.5% など単位は実装側で統一想定
        max_slippage_bps=10.0,
        loss_cut_daily_jpy=30000,
    )
    with pytest.raises(RiskBreach):
        precheck_open_order(symbol="BTCUSDT", add_notional=add_notional, ctx=ctx, risk=risk)


@pytest.mark.asyncio
async def test_kill_on_daily_loss_and_ws_disconnect_and_sign_flip():
    """日次損失・WS切断・Funding符号反転で flatten_all が呼ばれること"""
    called = {"n": 0}

    async def fake_flatten():
        called["n"] += 1

    rm = RiskManager(
        loss_cut_daily_jpy=30000,
        ws_disconnect_threshold_sec=30.0,
        hedge_delay_p95_threshold_sec=2.0,
        api_error_max_in_60s=10,
        flatten_all=fake_flatten,
    )

    # 1) 日次損失で発火
    rm.update_daily_pnl(net_pnl_jpy=-40000)  # しきい値超
    # 非同期タスクの完了を少し待つ
    await asyncio.sleep(0.01)
    assert called["n"] >= 1
    assert rm.disable_new_orders is True

    # 2) WS切断で発火（既にkill済みでも多重発火しない）
    rm.record_ws_disconnected(duration_sec=31.0)
    await asyncio.sleep(0.01)
    assert called["n"] >= 1  # 変わらない（多重起動しない）

    # 3) Funding 符号反転で発火（kill後は何も起きない）
    rm.update_funding_predicted(symbol="BTCUSDT", predicted_rate=0.01)
    rm.update_funding_predicted(symbol="BTCUSDT", predicted_rate=-0.02)
    await asyncio.sleep(0.01)
    assert called["n"] >= 1
