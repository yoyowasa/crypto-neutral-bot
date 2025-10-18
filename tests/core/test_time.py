from __future__ import annotations

from datetime import timezone

import pytest

pytest.importorskip("loguru")


def test_parse_ts_int_ms() -> None:
    """ミリ秒エポック→UTC datetime に変換できること"""
    from bot.core.time import parse_exchange_ts, utc_now

    now = utc_now()
    ms = int(now.timestamp() * 1000)
    parsed = parse_exchange_ts(ms)
    assert abs((parsed - now).total_seconds()) < 1.0


@pytest.mark.asyncio
async def test_sleep_until_now_returns_quickly() -> None:
    """現在時刻を指定した sleep_until がすぐ返ること"""
    from bot.core.time import sleep_until, utc_now

    start = utc_now()
    await sleep_until(start)
    end = utc_now()
    assert (end - start).total_seconds() < 0.5


def test_parse_iso_z() -> None:
    """ISO8601のZ付き文字列がパースできること"""
    from bot.core.time import parse_exchange_ts

    s = "2024-01-02T03:04:05Z"
    dt = parse_exchange_ts(s)
    assert dt.tzinfo is not None
    assert dt.tzinfo == timezone.utc
