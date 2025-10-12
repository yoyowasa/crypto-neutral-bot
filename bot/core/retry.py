from __future__ import annotations

from collections.abc import Callable, Iterable
from importlib import import_module
from typing import Any, TypeVar, cast

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from .errors import RateLimitError, WsDisconnected

try:  # pragma: no cover - httpx が無い環境でも壊れないようにする
    _httpx = cast(Any, import_module("httpx"))
except Exception:  # pragma: no cover
    _HTTPX_ERRORS: tuple[type[BaseException], ...] = ()
else:
    _HTTPX_ERRORS = (
        _httpx.ConnectError,
        _httpx.ReadTimeout,
    )


FuncT = TypeVar("FuncT", bound=Callable[..., Any])


def retryable(
    *,
    attempts: int = 5,
    min_wait: float = 0.2,
    max_wait: float = 2.5,
    extra_exceptions: Iterable[type[BaseException]] = (),
) -> Callable[[FuncT], FuncT]:
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
