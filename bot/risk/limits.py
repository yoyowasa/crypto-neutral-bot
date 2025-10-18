# これは「建てる前の安全確認（名目、スリッページ、ネットデルタ）」を行うユーティリティです。
from __future__ import annotations

from dataclasses import dataclass

from bot.core.errors import RiskBreach
from bot.config.models import RiskConfig


@dataclass(frozen=True)
class PreTradeContext:
    """これは何を表す型？
    → 事前チェックに必要な現在の状況（MVP）。
      - used_total_notional: すでに使っている総名目
      - used_symbol_notional: その銘柄で使っている名目
      - predicted_net_delta_after: この発注を入れた「あと」の想定ネットデルタ（ベース数量 or 比率は実装側で統一）
      - estimated_slippage_bps: 今回発注の推定スリッページ[bps]
    """

    used_total_notional: float
    used_symbol_notional: float
    predicted_net_delta_after: float
    estimated_slippage_bps: float


def precheck_open_order(
    *,
    symbol: str,
    add_notional: float,
    ctx: PreTradeContext,
    risk: RiskConfig,
) -> None:
    """これは何をする関数？
    → 「この注文（名目 add_notional）を追加で建てても良いか？」をチェックします。
      1) 総名目上限
      2) 銘柄別名目上限
      3) スリッページ上限（estimated_slippage_bps <= max_slippage_bps）
      4) ネットデルタ上限（predicted_net_delta_after の絶対値）
    NG の場合は RiskBreach を投げ、発注を止めます。
    """
    # 1) 総名目上限
    if ctx.used_total_notional + add_notional > risk.max_total_notional:
        raise RiskBreach(
            f"total notional limit: used={ctx.used_total_notional} + add={add_notional} > max={risk.max_total_notional}"
        )

    # 2) 銘柄別名目上限
    if ctx.used_symbol_notional + add_notional > risk.max_symbol_notional:
        raise RiskBreach(
            f"symbol notional limit({symbol}): used={ctx.used_symbol_notional} + add={add_notional} > max={risk.max_symbol_notional}"
        )

    # 3) 推定スリッページ上限
    if ctx.estimated_slippage_bps > risk.max_slippage_bps:
        raise RiskBreach(
            f"slippage limit: estimated={ctx.estimated_slippage_bps}bps > max={risk.max_slippage_bps}bps"
        )

    # 4) ネットデルタ上限（単位の統一は上位で保証する想定 / TODO: 要仕様統一）
    if abs(ctx.predicted_net_delta_after) > risk.max_net_delta:
        raise RiskBreach(
            f"net delta limit: predicted_after={ctx.predicted_net_delta_after} > max={risk.max_net_delta}"
        )
