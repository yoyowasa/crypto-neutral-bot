from __future__ import annotations

import asyncio
from pathlib import Path

from bot.core.logging import setup_logging
from bot.core.retry import retryable
from bot.core.time import monotonic_ms, sleep_until, to_tz, utc_now


def test_logging_creates_file(tmp_path: Path) -> None:
    log_file = tmp_path / "app.log"
    setup_logging(log_file.as_posix())
    assert log_file.parent.exists()


def test_time_sleep_until() -> None:
    past = utc_now()
    asyncio.run(sleep_until(past))
    tokyo = to_tz(past, "Asia/Tokyo")
    assert tokyo.tzinfo is not None


def test_monotonic_ms_increases() -> None:
    first = monotonic_ms()
    second = monotonic_ms()
    assert second >= first


def test_retryable_succeeds_after_failure() -> None:
    calls = {"n": 0}

    @retryable(attempts=3)
    def func() -> int:
        calls["n"] += 1
        if calls["n"] < 2:
            raise ConnectionError("tmp")
        return calls["n"]

    assert func() == 2
