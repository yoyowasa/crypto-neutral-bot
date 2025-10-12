from __future__ import annotations

from collections.abc import Iterable

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

try:  # pragma: no cover - httpx が無い環境でも壊れないようにする
    import httpx  # type: ignore

    _HTTPX_ERRORS: tuple[type[BaseException], ...] = (
        httpx.ConnectError,
        httpx.ReadTimeout,
    )
except Exception:  # pragma: no cover
    _HTTPX_ERRORS = ()

from .errors import RateLimitError, WsDisconnected


def retryable(
    *,
    attempts: int = 5,
    min_wait: float = 0.2,
    max_wait: float = 2.5,
    extra_exceptions: Iterable[type[BaseException]] = (),
):
    """ネットワーク系の一時エラーに対する共通リトライデコレータ。"""

    base_exceptions: tuple[type[BaseException], ...] = (
        *_HTTPX_ERRORS,
        RateLimitError,
        WsDisconnected,
    )
    retry_types: tuple[type[BaseException], ...] = tuple(
        set(base_exceptions) | set(extra_exceptions)
    )

    return retry(
        reraise=True,
        stop=stop_after_attempt(attempts),
        wait=wait_exponential_jitter(initial=min_wait, max=max_wait),
        retry=retry_if_exception_type(retry_types),
    )


__all__ = ["retryable"]
