# これは「取引所の違いを意識せず使える抽象ゲートウェイ」を定義するファイルです。
from __future__ import annotations

from typing import Awaitable, Callable

from .types import Balance, FundingInfo, Order, OrderRequest, Position


class ExchangeGateway:
    """取引所アクセスの最小インターフェース"""

    async def get_balances(self) -> list[Balance]:
        """これは何をする関数？→ 残高の一覧を返す"""
        raise NotImplementedError

    async def get_positions(self) -> list[Position]:
        """これは何をする関数？→ 現在の建玉一覧を返す"""
        raise NotImplementedError

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """これは何をする関数？→ 未約定注文（オープンオーダー）を返す"""
        raise NotImplementedError

    async def place_order(self, req: OrderRequest) -> Order:
        """これは何をする関数？→ 注文を発注し、作成された注文情報を返す"""
        raise NotImplementedError

    async def cancel_order(self, order_id: str | None = None, client_id: str | None = None) -> None:
        """これは何をする関数？→ 指定した注文を取り消す（order_id または client_id を指定）"""
        raise NotImplementedError

    async def get_ticker(self, symbol: str) -> float:
        """これは何をする関数？→ 指定シンボルの現在価格（近似）を返す"""
        raise NotImplementedError

    async def get_funding_info(self, symbol: str) -> FundingInfo:
        """これは何をする関数？→ Funding レート等の最小情報を返す"""
        raise NotImplementedError

    async def subscribe_private(self, callbacks: dict[str, Callable[[dict], Awaitable[None]]]) -> None:
        """これは何をする関数？→ Private WS（order/execution/position 等）に購読し、コールバックへ通知する"""
        raise NotImplementedError

    async def subscribe_public(
        self, symbols: list[str], callbacks: dict[str, Callable[[dict], Awaitable[None]]]
    ) -> None:
        """これは何をする関数？→ Public WS（trade/orderbook 等）に購読し、コールバックへ通知する"""
        raise NotImplementedError
