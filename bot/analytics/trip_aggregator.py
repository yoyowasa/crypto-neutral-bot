from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Set

from bot.core.time import parse_exchange_ts
from bot.tools.jsonl_sink import append_jsonl


@dataclass
class _RoundState:
    # Open leg state
    open_sign: int = 0  # +1 long, -1 short, 0 flat
    open_qty: float = 0.0
    entry_ts_iso: str | None = None
    fees_open: float = 0.0
    entry_client_ids: Set[str] = field(default_factory=set)

    # Close leg accumulators (for current round)
    closed_qty: float = 0.0
    entry_value_closed: float = 0.0  # sum(entry_avg_at_close * close_qty)
    exit_value_closed: float = 0.0  # sum(close_price * close_qty)
    fees_close: float = 0.0
    exit_ts_iso: str | None = None
    exit_client_ids: Set[str] = field(default_factory=set)

    # Running weighted average of entry for remaining open position
    # We keep avg only for remaining open leg
    entry_avg_open: float = 0.0


class RoundTripAggregator:
    """
    Build round-trip records (entry -> exit) from trade fills and
    append to JSONL for analysis. Works with weighted-average cost.

    Generated record schema (one line per completed round):
      {
        "event": "round_trip",
        "symbol": str,
        "direction": "long"|"short",
        "qty": float,                  # base amount closed
        "entry_ts": iso8601,
        "exit_ts": iso8601,
        "hold_secs": float,
        "entry_avg_px": float,
        "exit_avg_px": float,
        "gross_pnl": float,           # sign-consistent
        "fees_open": float,
        "fees_close": float,
        "net_pnl": float,
        "entry_client_ids": list[str],
        "exit_client_ids": list[str],
      }
    """

    def __init__(self, out_jsonl_path: str) -> None:
        self._out = out_jsonl_path
        self._state: Dict[str, _RoundState] = {}

    def on_fill(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        fee: float,
        ts_iso: str,
        exchange_order_id: str | None,
        client_id: str | None,
    ) -> None:
        if qty <= 0 or price is None:
            return

        s = self._state.get(symbol)
        if s is None:
            s = _RoundState()
            self._state[symbol] = s

        side_l = (side or "").lower()
        signed_qty = qty if side_l == "buy" else (-qty if side_l == "sell" else 0.0)
        if signed_qty == 0.0:
            return

        # Open new position if flat
        if s.open_sign == 0:
            s.open_sign = 1 if signed_qty > 0 else -1
            s.open_qty = abs(signed_qty)
            s.entry_ts_iso = ts_iso
            s.entry_avg_open = price
            s.fees_open = max(0.0, float(fee or 0.0))
            if client_id:
                s.entry_client_ids.add(client_id)
            return

        # Same direction -> increase open
        if (signed_qty > 0 and s.open_sign > 0) or (signed_qty < 0 and s.open_sign < 0):
            new_open_qty = s.open_qty + abs(signed_qty)
            if new_open_qty > 0:
                s.entry_avg_open = (s.entry_avg_open * s.open_qty + abs(signed_qty) * price) / new_open_qty
            s.open_qty = new_open_qty
            s.fees_open += max(0.0, float(fee or 0.0))
            if client_id:
                s.entry_client_ids.add(client_id)
            return

        # Opposite direction -> closing some or all
        close_qty = min(abs(signed_qty), s.open_qty)
        if close_qty > 0:
            # Accumulate realized leg values
            s.closed_qty += close_qty
            s.entry_value_closed += s.entry_avg_open * close_qty
            s.exit_value_closed += price * close_qty
            s.fees_close += max(0.0, float(fee or 0.0))
            s.exit_ts_iso = ts_iso
            if client_id:
                s.exit_client_ids.add(client_id)

            # Reduce open leg
            s.open_qty -= close_qty

        # If round completed (flat now), emit record
        if s.open_qty <= 1e-12:
            try:
                entry_dt = parse_exchange_ts(s.entry_ts_iso)
                exit_dt = parse_exchange_ts(s.exit_ts_iso)
                hold_secs = max(0.0, (exit_dt - entry_dt).total_seconds()) if (entry_dt and exit_dt) else 0.0
            except Exception:
                hold_secs = 0.0

            qty_rt = s.closed_qty
            entry_avg_px = (s.entry_value_closed / qty_rt) if qty_rt > 0 else 0.0
            exit_avg_px = (s.exit_value_closed / qty_rt) if qty_rt > 0 else 0.0
            sign = 1.0 if s.open_sign > 0 else -1.0
            gross_pnl = sign * (s.exit_value_closed - s.entry_value_closed)
            net_pnl = gross_pnl - (s.fees_open + s.fees_close)

            try:
                append_jsonl(
                    self._out,
                    {
                        "event": "round_trip",
                        "symbol": symbol,
                        "direction": "long" if s.open_sign > 0 else "short",
                        "qty": qty_rt,
                        "entry_ts": s.entry_ts_iso,
                        "exit_ts": s.exit_ts_iso,
                        "hold_secs": hold_secs,
                        "entry_avg_px": entry_avg_px,
                        "exit_avg_px": exit_avg_px,
                        "gross_pnl": gross_pnl,
                        "fees_open": s.fees_open,
                        "fees_close": s.fees_close,
                        "net_pnl": net_pnl,
                        "entry_client_ids": sorted(s.entry_client_ids),
                        "exit_client_ids": sorted(s.exit_client_ids),
                    },
                )
            finally:
                # Reset state (round completed)
                self._state[symbol] = _RoundState()

        # If there is remainder beyond the close that flips direction, open new
        remainder = abs(signed_qty) - close_qty
        if remainder > 1e-12:
            # This remainder opens opposite direction as current signed_qty
            new_sign = 1 if signed_qty > 0 else -1
            s_new = self._state.get(symbol) or _RoundState()
            s_new.open_sign = new_sign
            s_new.open_qty = remainder
            s_new.entry_ts_iso = ts_iso
            s_new.entry_avg_open = price
            s_new.fees_open = 0.0  # fee already counted on closing leg above
            s_new.entry_client_ids = set()
            if client_id:
                s_new.entry_client_ids.add(client_id)
            # Reset close accumulators for new round
            s_new.closed_qty = 0.0
            s_new.entry_value_closed = 0.0
            s_new.exit_value_closed = 0.0
            s_new.fees_close = 0.0
            s_new.exit_ts_iso = None
            s_new.exit_client_ids = set()
            self._state[symbol] = s_new
