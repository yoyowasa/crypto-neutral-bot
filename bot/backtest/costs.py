from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CostModel:
    """これは何を表す型？
    → コスト見積りに使うパラメータ（bps単位）。1 bps = 0.01%。
      - taker_fee_bps: テイカー手数料
      - maker_fee_bps: メイカー手数料
      - min_slippage_bps: 最低スリッページ（板が厚くてもゼロにはならない前提）
      - depth_qty_base: 板1%内に概ね存在するベース数量（疑似）。サイズ比でスリッページを近似するための目安
    """

    taker_fee_bps: float = 5.0
    maker_fee_bps: float = 2.0
    min_slippage_bps: float = 1.0
    depth_qty_base: float = 1.0  # 例：BTCで1枚相当


def taker_fee(notional: float, *, model: CostModel) -> float:
    """これは何をする関数？
    → テイカー手数料（USDT想定）を返します：notional × bps / 1e4
    """

    return float(notional) * (model.taker_fee_bps / 1e4)


def maker_fee(notional: float, *, model: CostModel) -> float:
    """これは何をする関数？
    → メイカー手数料（USDT想定）を返します：notional × bps / 1e4
    """

    return float(notional) * (model.maker_fee_bps / 1e4)


def estimate_slippage_bps(qty_base: float, *, model: CostModel) -> float:
    """これは何をする関数？
    → 約定サイズ（ベース数量）と仮想的な板厚（depth_qty_base）から、スリッページの概算bpsを返します。
      - qty_base が depth_qty_base と同程度→ 数bps 程度
      - 比例で増やすが、最低 model.min_slippage_bps は下回らない
    """

    if model.depth_qty_base <= 0:
        return model.min_slippage_bps
    ratio = max(0.0, float(qty_base)) / float(model.depth_qty_base)
    return max(model.min_slippage_bps, model.min_slippage_bps * (1.0 + ratio))

