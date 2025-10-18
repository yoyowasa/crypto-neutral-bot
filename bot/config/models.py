# これは「設定の型（箱の設計図）」を定義するファイルです。
# Pydantic の BaseModel を使い、型安全に設定を扱えるようにします。

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

DEFAULT_STRATEGY_SYMBOLS: tuple[str, str] = ("BTCUSDT", "ETHUSDT")


class ExchangeKeys(BaseModel):
    """取引所APIの鍵（平文は .env にのみ置く）"""

    api_key: str
    api_secret: str


class ExchangeConfig(BaseModel):
    """取引所接続の基本設定（環境・WS URL等。URLは最終的に公式ドキュメントで要確認）"""

    name: str = "bybit"
    environment: str = "testnet"  # "mainnet" も許容
    recv_window_ms: int = 5000
    ws_public_url: str | None = None  # TODO: 要API確認
    ws_private_url: str | None = None  # TODO: 要API確認


class RiskConfig(BaseModel):
    """リスク上限と安全装置のしきい値"""

    max_total_notional: float
    max_symbol_notional: float
    max_net_delta: float
    max_slippage_bps: float
    loss_cut_daily_jpy: float


class StrategyFundingConfig(BaseModel):
    """Funding/ベーシス戦略のパラメータ（MVP）"""

    symbols: list[str]
    min_expected_apr: float = 0.05
    pre_event_open_minutes: int = 60
    hold_across_events: bool = True
    rebalance_band_bps: float = 5.0  # ネットデルタ再ヘッジ帯

    def __init__(self, **data: Any) -> None:
        """これは何をする関数？→ 初期化時にシンボルの既定値を安全に設定します"""
        if "symbols" not in data:
            data["symbols"] = list(DEFAULT_STRATEGY_SYMBOLS)
        super().__init__(**data)


class AppConfig(BaseModel):
    """アプリ全体の設定のルート（.env / YAML を統合してここに流し込む）"""

    keys: ExchangeKeys
    exchange: ExchangeConfig = ExchangeConfig()
    risk: RiskConfig
    strategy: StrategyFundingConfig = StrategyFundingConfig()
    db_url: str = "sqlite+aiosqlite:///./db/trading.db"
    timezone: str = "UTC"

    # Pydantic v2 では model_config、v1 では Config を参照するため両方定義しておく
    model_config = {
        "extra": "ignore",
    }

    class Config:  # type: ignore[override]
        extra = "ignore"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AppConfig":
        """これは何をする関数？→ 辞書から AppConfig を生成し、ネストした辞書も適切なモデルに変換します"""

        payload = dict(data)
        if "keys" in payload and not isinstance(payload["keys"], ExchangeKeys):
            payload["keys"] = ExchangeKeys(**payload["keys"])
        if "exchange" in payload and not isinstance(payload["exchange"], ExchangeConfig):
            payload["exchange"] = ExchangeConfig(**payload["exchange"])
        if "risk" in payload and not isinstance(payload["risk"], RiskConfig):
            payload["risk"] = RiskConfig(**payload["risk"])
        if "strategy" in payload and not isinstance(payload["strategy"], StrategyFundingConfig):
            payload["strategy"] = StrategyFundingConfig(**payload["strategy"])
        return cls(**payload)

    def to_dict(self) -> dict[str, Any]:
        """これは何をする関数？→ AppConfig を辞書に変換し、ログ用などに再利用します"""

        return self.model_dump(mode="python")
