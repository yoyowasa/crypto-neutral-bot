# これは「OMSが扱う注文状態の型と、OMS用の設定」を定義するファイルです。
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from pydantic import BaseModel

from bot.exchanges.types import OrderRequest


class OrderLifecycleState(str, Enum):
    """注文ライフサイクルの標準状態"""

    NEW = "NEW"
    SENT = "SENT"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"


class OmsConfig(BaseModel):
    """OMSの動作設定（MVP）
    - order_timeout_sec: 発注後この秒数を越えたらタイムアウト→再送/取消を検討
    - max_retries: 再送できる最大回数
    """

    order_timeout_sec: float = 5.0
    max_retries: int = 2


@dataclass
class ManagedOrder:
    """OMSが内部で追跡する注文の状態"""

    req: OrderRequest
    state: OrderLifecycleState = OrderLifecycleState.NEW
    sent_at: datetime | None = None
    order_id: str | None = None
    filled_qty: float = 0.0
    avg_price: float | None = None
    retries: int = 0  # 再送回数（新規発注時は0）
