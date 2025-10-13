from __future__ import annotations


class ExchangeError(Exception):
    "取引所レイヤの基底例外"


class RateLimitError(ExchangeError):
    "HTTP 429 などのレート制限"


class WsDisconnected(ExchangeError):
    "WebSocket 切断"


class RiskBreach(Exception):
    "リスク上限違反"
