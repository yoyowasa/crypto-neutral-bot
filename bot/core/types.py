from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


@dataclass(frozen=True)
class BBO:
    """最小の板情報。Funding/Basis戦略で基準価格やスリッページ見積りに使う。"""

    symbol: str
    bid_px: Optional[float]
    ask_px: Optional[float]
    bid_sz: Optional[float] = None
    ask_sz: Optional[float] = None
    ts: Optional[int] = None  # ms/秒など。プロジェクト内で統一推奨。

    @property
    def mid(self) -> Optional[float]:
        if self.bid_px is None or self.ask_px is None:
            return None
        return (self.bid_px + self.ask_px) / 2.0


@dataclass(frozen=True)
class FundingInfo:
    """次回Fundingの予測/時刻（取引所差分を吸収するための共通型）。"""

    symbol: str
    next_rate: Optional[float]
    interval_sec: int
    next_funding_ts: Optional[int] = None
    ts: Optional[int] = None


@dataclass(frozen=True)
class ExecutionEvent:
    """冪等性を保つための最小約定イベント型（WSでも紙でも使用可）。"""

    exec_id: str
    symbol: str
    order_id: Optional[str]
    client_order_id: Optional[str]
    side: Side
    qty: float
    price: float
    fee_quote: float = 0.0
    ts: Optional[int] = None
    tag: str = ""
