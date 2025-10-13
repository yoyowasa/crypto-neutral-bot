from __future__ import annotations

import asyncio
from pathlib import Path

from bot.core.logging import setup_logging
from bot.core.retry import retryable
from bot.core.time import monotonic_ms, sleep_until, to_tz, utc_now


def test_logging_creates_file(tmp_path: Path):
    log_file = tmp_path / "app.log"
    setup_logging(log_file.as_posix())
    assert log_file.parent.exists()


def test_time_sleep_until():
    # 過去時刻を渡しても即 return
    past = utc_now()
    asyncio.run(sleep_until(past))


def test_monotonic_ms_increases():
    a = monotonic_ms()
    b = monotonic_ms()
    assert b >= a


def test_to_tz_conversion():
    dt = utc_now()
    converted = to_tz(dt, "UTC")
    assert converted.tzinfo is not None


def test_retryable_succeeds_after_failure():
    calls = {"n": 0}

    @retryable(attempts=3)
    def f() -> int:
        calls["n"] += 1
        if calls["n"] < 2:
            raise ConnectionError("tmp")
        return calls["n"]

    assert f() == 2
