from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import asyncio
import time as _time


def utc_now() -> datetime:
    "UTC の timezone-aware datetime を返す"
    return datetime.now(timezone.utc)


def to_tz(dt: datetime, tz: str) -> datetime:
    "任意の IANA TZ へ変換（例: 'UTC', 'Asia/Tokyo'）"
    return dt.astimezone(ZoneInfo(tz))


async def sleep_until(dt_utc: datetime) -> None:
    "UTC の時刻まで非同期 sleep。過去なら即 return。"
    now = utc_now()
    secs = (dt_utc - now).total_seconds()
    if secs > 0:
        await asyncio.sleep(secs)


def monotonic_ms() -> int:
    "モノトニック時計（ms）"
    return int(_time.monotonic() * 1000)


__all__ = ["utc_now", "to_tz", "sleep_until", "monotonic_ms"]
