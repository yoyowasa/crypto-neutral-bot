"""Funding/Basis 戦略で利用する基礎モデル群とユーティリティ関数。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from bot.config.models import RiskConfig


class DecisionAction(str, Enum):
    """戦略が取り得る代表的なアクション。"""

    SKIP = "skip"
    OPEN = "open"
    CLOSE = "close"
    HEDGE = "hedge"


@dataclass(slots=True)
class Decision:
    """評価ステップで得られた結果を保持するデータ構造。"""

    action: DecisionAction
    symbol: str
    reason: str
    predicted_apr: float | None = None
    notional: float = 0.0
    perp_side: str | None = None
    spot_side: str | None = None
    delta_to_neutral: float = 0.0


def annualize_rate(rate_per_period: float | None, *, period_seconds: float) -> float | None:
    """Fundingレート（期間基準）を単純年率換算する。"""

    if rate_per_period is None:
        return None
    if period_seconds <= 0:
        raise ValueError("period_seconds must be positive")
    periods_per_year = (365.0 * 24.0 * 3600.0) / period_seconds
    return rate_per_period * periods_per_year


def notional_candidate(
    *,
    risk: RiskConfig,
    used_total_notional: float,
    used_symbol_notional: float,
) -> float:
    """現在の利用状況から追加で使える名目額を計算する。"""

    remaining_total = max(risk.max_total_notional - used_total_notional, 0.0)
    remaining_symbol = max(risk.max_symbol_notional - used_symbol_notional, 0.0)
    return max(min(remaining_total, remaining_symbol), 0.0)


def net_delta_base(spot_qty: float, perp_qty: float) -> float:
    """スポットとパーペの建玉からネットデルタ（ベース数量）を計算する。"""

    return float(spot_qty) + float(perp_qty)
