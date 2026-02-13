from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Side = Literal["buy", "sell"]
Venue = Literal["spot", "perp"]


@dataclass
class CostModel:
    """手数料・スリッページ・スプレッド補正を一元管理する簡易モデル。"""

    spot_taker_fee_bps: float = 6.0
    perp_taker_fee_bps: float = 6.0
    slippage_bps: float = 3.0
    extra_spread_bps: float = 1.0

    def _is_spot(self, symbol: str) -> bool:
        return symbol.endswith("_SPOT")

    def taker_fee(self, *, symbol: str, qty: float, price: float) -> float:
        """名目に応じたテイカー手数料を返す。"""
        notional = abs(qty * price)
        bps = self.spot_taker_fee_bps if self._is_spot(symbol) else self.perp_taker_fee_bps
        return notional * bps / 10_000.0

    def notional_to_qty(self, *, notional_quote: float, px: float) -> float:
        """名目(クオート)からベース数量へ変換する。"""
        if px <= 0:
            return 0.0
        return abs(float(notional_quote)) / float(px)

    def fee_quote(self, *, notional_quote: float, venue: Venue) -> float:
        """名目に対する手数料（クオート建て）を返す。"""
        bps = self.spot_taker_fee_bps if venue == "spot" else self.perp_taker_fee_bps
        return abs(float(notional_quote)) * float(bps) / 10_000.0

    def slippage_px(self, *, px: float, side: Side) -> float:
        """価格に対してスリッページ/補正を乗せた擬似約定価格を返す（BBOが無い場合の近似）。"""
        slip = (self.slippage_bps + self.extra_spread_bps) / 10_000.0
        if side.lower() == "buy":
            return float(px) * (1.0 + slip)
        return float(px) * (1.0 - slip)

    def market_fill_price(
        self, *, bid: float | None, ask: float | None, side: Side, fallback: float | None = None
    ) -> float:
        """BBOにスリッページ/スプレッド補正を乗せた約定価格を返す。"""
        slip = (self.slippage_bps + self.extra_spread_bps) / 10_000.0
        if side.lower() == "buy":
            if ask is not None:
                return float(ask) * (1.0 + slip)
            if bid is not None:
                return float(bid) * (1.0 + slip)
        else:
            if bid is not None:
                return float(bid) * (1.0 - slip)
            if ask is not None:
                return float(ask) * (1.0 - slip)
        base = fallback if fallback is not None else 0.0
        return float(base)

    def slippage_cost(self, *, notional: float) -> float:
        """名目に対するスリッページコスト推定値。"""
        return abs(notional) * (self.slippage_bps + self.extra_spread_bps) / 10_000.0

    def roundtrip_cost_quote(self, *, notional_quote: float) -> float:
        """Funding/Basis（2レッグ）を想定した往復コスト概算（entry+exitの合計）。"""
        n = abs(float(notional_quote))
        # 2レッグ（spot+perp）× entry/exit = 合計4約定を想定
        fee_bps_total = (self.spot_taker_fee_bps + self.perp_taker_fee_bps) * 2.0
        slip_bps_total = (self.slippage_bps + self.extra_spread_bps) * 4.0
        return n * (fee_bps_total + slip_bps_total) / 10_000.0
