# これは「運用中に使う共通の例外クラス」を定義するファイルです。
from __future__ import annotations


class ExchangeError(Exception):
    """取引所まわりの一般的な失敗（HTTP/WSエラー等）の基底例外。"""


class RateLimitError(ExchangeError):
    """APIレート制限に到達したときの例外。再試行対象。"""


class WsDisconnected(ExchangeError):
    """WebSocketが切断/ハートビート欠落のときに投げる例外。"""


class RiskBreach(Exception):
    """リスクガードに違反したときに投げる例外（キルスイッチ発火）。"""


class ConfigError(Exception):
    """設定ファイルや環境変数の不備があるときの例外。"""


class DataError(Exception):
    """受信/読込データの形式不正・値異常を表す例外。"""


class RetryGiveup(Exception):
    """再試行の上限に達してギブアップしたことを示す例外。"""
