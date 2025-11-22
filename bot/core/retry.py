# これは「同期/非同期関数どちらにも使える指数バックオフ再試行デコレータ」を提供するファイルです。
from __future__ import annotations

import inspect
from typing import Any, Callable

from loguru import logger
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random_exponential,
)

from .errors import ExchangeError, RateLimitError, WsDisconnected


def _log_before_sleep(retry_state: RetryCallState) -> None:
    """これは何をする関数？
    → 次の再試行まで眠る直前に、関数名・試行回数・例外・待機秒数を警告ログに出します。
    """
    fn_name = getattr(retry_state.fn, "__name__", str(retry_state.fn))
    attempt = retry_state.attempt_number
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    wait_s = getattr(retry_state.next_action, "sleep", None)
    # 認証系の既知メッセージに簡易ヒントを付与
    hint = None
    try:
        msg = str(exc)
        if "API key is invalid" in msg or 'retCode":10003' in msg:
            hint = "(Bybit認証エラー: キー不正/環境(testnet/mainnet)不一致の可能性)"
    except Exception:
        pass
    logger.warning(
        "retryable: {fn} attempt={attempt} error={exc} {hint} next_wait={wait}s",
        fn=fn_name,
        attempt=attempt,
        exc=exc,
        hint=hint or "",
        wait=wait_s,
    )


def retryable(
    *,
    tries: int = 5,
    wait_initial: float = 0.5,
    wait_max: float = 8.0,
    jitter: bool = True,
    retry_on: tuple[type[BaseException], ...] = (ExchangeError, RateLimitError, WsDisconnected, TimeoutError),
    reraise: bool = True,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """これは何をする関数（デコレータ）？
    → 対象関数を「失敗したら指数バックオフで再試行」する関数に包みます。
      - tries: 最大試行回数
      - wait_initial: 最初の待機秒
      - wait_max: 待機の上限秒
      - jitter: ランダムゆらぎ（スパイク回避）
      - retry_on: 再試行対象の例外タプル
      - reraise: 上限到達で例外をそのまま送出するか
    """
    wait_policy = (
        wait_random_exponential(multiplier=wait_initial, max=wait_max)
        if jitter
        else wait_exponential(multiplier=wait_initial, max=wait_max)
    )

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if inspect.iscoroutinefunction(func):

            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(tries),
                    wait=wait_policy,
                    retry=retry_if_exception_type(retry_on),
                    reraise=reraise,
                    before_sleep=_log_before_sleep,
                ):
                    with attempt:
                        return await func(*args, **kwargs)

            return async_wrapper

        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in Retrying(
                stop=stop_after_attempt(tries),
                wait=wait_policy,
                retry=retry_if_exception_type(retry_on),
                reraise=reraise,
                before_sleep=_log_before_sleep,
            ):
                with attempt:
                    return func(*args, **kwargs)

        return sync_wrapper

    return decorator
