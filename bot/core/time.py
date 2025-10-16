# これは「UTC基準の時刻、TSパース、指定時刻までの待機、遅延警告」を提供するファイルです。
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from loguru import logger


def utc_now() -> datetime:
    """これは何をする関数？
    → タイムゾーン付きの現在UTC時刻を返します。
    """
    return datetime.now(timezone.utc)


def parse_exchange_ts(x: Any) -> datetime:
    """これは何をする関数？
    → 取引所から来る様々な型のタイムスタンプ（ms/秒/ISO文字列等）をUTCのdatetimeに正規化します。
      - int/float: 10桁なら秒、13桁ならミリ秒として解釈
      - str: ISO8601を想定（末尾'Z'は+00:00として扱う）。数字のみなら数値扱い
      - datetime: タイムゾーン未設定ならUTCとみなす
    """
    if isinstance(x, datetime):
        return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
    if isinstance(x, (int, float)):
        ts = float(x)
        if ts > 1e12:  # 13桁（ミリ秒）
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    if isinstance(x, str):
        s = x.strip()
        if s.isdigit():
            return parse_exchange_ts(int(s))
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(s).astimezone(timezone.utc)
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"unsupported timestamp format: {x}") from e
    raise TypeError(f"unsupported timestamp type: {type(x)}")


async def sleep_until(when: datetime) -> None:
    """これは何をする関数？
    → 与えられたUTC時刻（naiveの場合はUTCとみなす）まで非同期で待機します。
    """
    target = when if when.tzinfo else when.replace(tzinfo=timezone.utc)
    delay = (target - utc_now()).total_seconds()
    await asyncio.sleep(max(0.0, delay))


def age_ms(ts: datetime) -> float:
    """これは何をする関数？
    → ts（UTC）から今（UTC）までの経過ミリ秒を返します（負のときは0）。
    """
    delta = (utc_now() - (ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc))).total_seconds()
    return max(0.0, delta * 1000.0)


def warn_if_event_delay(event_ts: datetime | int | float | str, *, threshold_ms: int = 2000) -> None:
    """これは何をする関数？
    → イベントの時刻（取引所タイムスタンプ）と現在時刻の差を測り、しきい値を超えたら警告ログを出します。
      NTP 同期はOS任せ。ここでは「遅延が大きい」を検知して知らせる役割だけを持ちます。
    """
    ts = parse_exchange_ts(event_ts)
    delay = age_ms(ts)
    if delay > threshold_ms:
        logger.warning("clock/latency warning: event_delay_ms={} (threshold_ms={})", int(delay), threshold_ms)
