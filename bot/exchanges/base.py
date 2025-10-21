from __future__ import annotations

from typing import Awaitable, Callable

from .types import Balance, FundingInfo, Order, OrderRequest, Position


class ExchangeGateway:
    """最小アクセスの抽象インターフェース"""

    async def get_balances(self) -> list[Balance]:
        """口座残高一覧を返す"""
        raise NotImplementedError

    async def get_positions(self) -> list[Position]:
        """現在の建玉一覧を返す"""
        raise NotImplementedError

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """未約定注文一覧を返す"""
        raise NotImplementedError

    async def place_order(self, req: OrderRequest) -> Order:
        """注文を発注し、作成された注文を返す"""
        raise NotImplementedError

    async def cancel_order(self, symbol: str, order_id: str | None = None, client_order_id: str | None = None) -> None:
        """取引所の注文を取り消す。orderId が無ければ client_order_id（orderLinkId）で取消可能にする。"""
        raise NotImplementedError

    async def get_ticker(self, symbol: str) -> float:
        """指定シンボルの現在価格（近似）を返す"""
        raise NotImplementedError

    async def get_funding_info(self, symbol: str) -> FundingInfo:
        """Funding レートの最小情報を返す"""
        raise NotImplementedError

    async def subscribe_private(self, callbacks: dict[str, Callable[[dict], Awaitable[None]]]) -> None:
        """Private WSに購読し、コールバックへ通知する"""
        raise NotImplementedError

    async def subscribe_public(
        self, symbols: list[str], callbacks: dict[str, Callable[[dict], Awaitable[None]]]
    ) -> None:
        """Public WSに購読し、コールバックへ通知する"""
        raise NotImplementedError

    async def amend_order(
        self,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
        new_price: object | None = None,
        new_qty: object | None = None,
        side: str | None = None,
        post_only: bool | None = None,
        time_in_force: str | None = None,
    ) -> None:  # 既存注文の価格/数量/TIFを安全に修正する入口。orderIdかclient_order_idのどちらかで特定する。
        raise NotImplementedError
