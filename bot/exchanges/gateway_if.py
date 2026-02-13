from __future__ import annotations

from typing import Any, AsyncIterator, Protocol, runtime_checkable

from bot.core.types import BBO, ExecutionEvent, FundingInfo
from bot.exchanges.types import Order, OrderRequest


@runtime_checkable
class ExchangeGatewayProto(Protocol):
    """薄いProtocol版のGateway IF（既存 ExchangeGateway に影響を与えない追加用）。"""

    # --- Market data ---
    async def get_ticker(self, symbol: str) -> float: ...
    async def get_bbo(self, symbol: str) -> BBO: ...
    async def get_funding_info(self, symbol: str) -> FundingInfo: ...

    # --- Account ---
    async def get_positions(self) -> list[Any]: ...

    # --- Trading ---
    async def place_order(self, req: OrderRequest) -> Order: ...
    async def cancel_order(self, symbol: str, order_id: str | None = None, client_order_id: str | None = None) -> None: ...

    # --- Optional: constraints normalization ---
    async def normalize_qty(self, symbol: str, qty: float) -> float: ...
    async def normalize_price(self, symbol: str, price: float) -> float: ...
    async def min_trade_notional(self, symbol: str) -> float: ...


@runtime_checkable
class ExchangeGatewayStreamingProto(ExchangeGatewayProto, Protocol):
    """execution ストリームが取れる場合にだけ実装するオプションIF。"""

    async def stream_executions(self) -> AsyncIterator[ExecutionEvent]: ...
