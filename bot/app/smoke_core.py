from __future__ import annotations

import asyncio

from bot.core.logging import logger, setup_logging
from bot.core.retry import retryable
from bot.core.time import monotonic_ms, sleep_until, utc_now


@retryable(attempts=3)
def flaky_once(counter={"n": 0}) -> int:  # noqa: B006
    counter["n"] += 1
    if counter["n"] < 2:
        raise ConnectionError("temporary")
    return counter["n"]


async def main() -> None:
    setup_logging("logs/app.log")
    logger.info("hello core utils")
    ms = monotonic_ms()
    logger.info("monotonic start: {}", ms)
    await sleep_until(utc_now())
    value = flaky_once()
    logger.info("flaky_once ok: {}", value)


if __name__ == "__main__":
    asyncio.run(main())
