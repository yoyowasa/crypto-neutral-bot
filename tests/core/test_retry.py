from __future__ import annotations

import pytest

pytest.importorskip("loguru")

from bot.core.errors import ExchangeError
from bot.core.retry import retryable


def test_retry_sync_succeeds_after_retries() -> None:
    """同期関数：2回失敗→3回目成功の再試行を確認"""
    calls = {"n": 0}

    @retryable(tries=3)
    def flakey() -> int:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ExchangeError("boom")
        return 42

    assert flakey() == 42
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_retry_async_succeeds_after_retries() -> None:
    """非同期関数：1回失敗→2回目成功の再試行を確認"""
    calls = {"n": 0}

    @retryable(tries=2)
    async def flakey_async() -> str:
        calls["n"] += 1
        if calls["n"] < 2:
            raise ExchangeError("boom-async")
        return "ok"

    assert await flakey_async() == "ok"
    assert calls["n"] == 2
