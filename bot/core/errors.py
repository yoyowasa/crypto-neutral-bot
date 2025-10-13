from __future__ import annotations


class ExchangeError(Exception):
    "取引所レイヤの基底例外"
    pass


class RateLimitError(ExchangeError):
    "HTTP 429 などのレート制限"
    pass


class WsDisconnected(ExchangeError):
    "WebSocket 切断"
    pass


class RiskBreach(Exception):
    "リスク上限違反"
    pass
