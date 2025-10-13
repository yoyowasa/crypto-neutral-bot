from __future__ import annotations

import asyncio
import time as _time
from datetime import UTC, datetime
from zoneinfo import ZoneInfo


def utc_now() -> datetime:
    "UTC の timezone-aware datetime を返す"
    return datetime.now(UTC)


def to_tz(dt: datetime, tz: str) -> datetime:
    "任意の IANA TZ へ変換(例: 'UTC', 'Asia/Tokyo')"
    return dt.astimezone(ZoneInfo(tz))


async def sleep_until(dt_utc: datetime) -> None:
    "UTC の時刻まで非同期 sleep。過去なら即 return。"
    now = utc_now()
    secs = (dt_utc - now).total_seconds()
    if secs > 0:
        await asyncio.sleep(secs)


def monotonic_ms() -> int:
    "モノトニック時計(ms)"
    return int(_time.monotonic() * 1000)


__all__ = ["monotonic_ms", "sleep_until", "to_tz", "utc_now"]
