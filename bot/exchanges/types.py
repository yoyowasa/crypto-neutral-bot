# これは「取引所に依らない型（残高/ポジション/注文/Funding）」を定義するファイルです。
from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel


class Balance(BaseModel):
    """口座内の資産残高を表す型"""

    asset: str
    total: float
    available: float


class Position(BaseModel):
    """建玉を表す型（デルタ計算で使用）"""

    symbol: str
    side: str  # "long" / "short"
    size: float  # ベース数量（例：BTC数量）
    entry_price: float
    unrealized_pnl: float


class OrderRequest(BaseModel):
    """新規注文リクエストの型"""

    symbol: str  # 内部表記（例：BTCUSDT / BTCUSDT_SPOT）
    side: str  # "buy" / "sell"
    type: str  # "limit" / "market"
    qty: float
    price: float | None = None
    time_in_force: str = "GTC"  # "GTC"/"IOC"/"FOK"
    reduce_only: bool = False
    post_only: bool = False
    client_id: str | None = None  # Bybit: orderLinkId（要API確認）


class Order(BaseModel):
    """発注後に返る注文の要約"""

    order_id: str
    client_id: str | None
    status: str  # "new","partially_filled","filled","canceled","rejected"
    filled_qty: float
    avg_fill_price: float | None


class FundingInfo(BaseModel):
    """Funding に関する最小情報"""

    symbol: str
    current_rate: float | None  # 直近（ティッカー等から）
    predicted_rate: float | None  # 予想（取得元は要API確認）
    next_funding_time: datetime | None
