from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class Balance(BaseModel):
    asset: str
    total: float
    available: float


class Position(BaseModel):
    symbol: str
    side: str  # "long" / "short"
    size: float  # base quantity
    entry_price: float
    unrealized_pnl: float


class OrderRequest(BaseModel):
    symbol: str
    side: str  # "buy" / "sell"
    type: str  # "limit" / "market"
    qty: float
    price: float | None = None
    time_in_force: str = "GTC"  # "GTC"/"IOC"/"FOK"
    reduce_only: bool = False
    post_only: bool = False
    client_id: str | None = None  # legacy client id used by OMS
    client_order_id: str | None = None  # exchange client order id (Bybit: orderLinkId)


class Order(BaseModel):
    symbol: str | None = None
    order_id: str
    client_id: str | None
    client_order_id: str | None = None  # exchange client order id (Bybit: orderLinkId)
    status: str  # "new","partially_filled","filled","canceled","rejected"
    filled_qty: float
    avg_fill_price: float | None


class FundingInfo(BaseModel):
    symbol: str
    current_rate: float | None  # current (ticker-based)
    predicted_rate: float | None  # predicted (for next settlement)
    next_funding_time: datetime | None
    funding_interval_hours: int | None = None  # Bybit v5 ticker 'fundingIntervalHour' if present
