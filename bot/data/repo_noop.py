from __future__ import annotations

# A no-op repository: keeps the same async interface as Repo but does not persist.
# Useful when you want to avoid DB writes entirely.
from typing import Any, List


class NoopRepo:
    async def create_all(self) -> None:  # pragma: no cover - trivial
        return None

    # ----- TradeLog -----
    async def add_trade(
        self,
        *,
        ts=None,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        fee: float,
        exchange_order_id: str,
        client_id: str | None = None,
    ) -> Any:
        return None

    async def list_trades(self, *, symbol: str | None = None) -> List[Any]:
        return []

    # ----- OrderLog -----
    async def add_order_log(
        self,
        *,
        ts=None,
        symbol: str,
        side: str,
        type: str,
        qty: float,
        price: float | None,
        status: str,
        exchange_order_id: str,
        client_id: str | None = None,
    ) -> Any:
        return None

    async def list_order_logs(self, *, symbol: str | None = None) -> List[Any]:
        return []

    # ----- PositionSnap -----
    async def add_position_snap(
        self,
        *,
        ts=None,
        symbol: str,
        side: str,
        size: float,
        entry_price: float,
        upnl: float,
    ) -> Any:
        return None

    async def list_position_snaps(self, *, symbol: str | None = None) -> List[Any]:
        return []

    # ----- FundingEvent -----
    async def add_funding_event(
        self,
        *,
        ts=None,
        symbol: str,
        rate: float,
        notional: float,
        realized_pnl: float,
    ) -> Any:
        return None

    async def list_funding_events(self, *, symbol: str | None = None) -> List[Any]:
        return []

    # ----- Misc -----
    async def dispose(self) -> None:  # pragma: no cover - trivial
        return None
