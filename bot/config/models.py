# これは「設定の型（箱の設計図）」を定義するファイルです。
# Pydantic の BaseModel / BaseSettings を使い、型安全に設定を扱えるようにします。
from __future__ import annotations

from pydantic import BaseModel
try:
    from pydantic import Field  # Fieldはミュータブル型の安全なデフォルト（default_factory）に使う
except ImportError:  # Pydanticのバージョン差異でFieldの場所が異なる場合に備える
    from pydantic.fields import Field  # type: ignore[attr-defined]
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    # ミュータブル（list）は Field(default_factory=...) で安全にデフォルト化する
    symbols: list[str] = Field(default_factory=lambda: ["BTCUSDT", "ETHUSDT"])
    min_expected_apr: float = 0.05
    pre_event_open_minutes: int = 60
    hold_across_events: bool = True
    rebalance_band_bps: float = 5.0  # ネットデルタ再ヘッジ帯


class AppConfig(BaseSettings):
    """アプリ全体の設定のルート（.env / YAML を統合してここに流し込む）"""

    keys: ExchangeKeys
    exchange: ExchangeConfig = ExchangeConfig()
    risk: RiskConfig
    strategy: StrategyFundingConfig = StrategyFundingConfig()
    db_url: str = "sqlite+aiosqlite:///./db/trading.db"
    timezone: str = "UTC"

    # 将来、環境変数だけで読みたい場合のために __ 区切りを有効化しておく
    model_config = SettingsConfigDict(
        env_nested_delimiter="__",
        extra="ignore",
    )
