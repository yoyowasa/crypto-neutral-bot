"""設定モデル定義（Pydantic v2）。
このファイルは「設定の入れ物（型）」を定義する。環境変数とYAMLから読み込んだ値を安全に保持する。
"""

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ExchangeKeys(BaseModel):
    """取引所APIキーを保持するモデル（何をするクラスか：鍵の入れ物）"""

    api_key: str = Field(..., description="Bybit API Key")
    api_secret: str = Field(..., description="Bybit API Secret")


class ExchangeConfig(BaseModel):
    """取引所周りの固定設定（何をするクラスか：環境/WS URLなどの設定入れ物）"""

    name: str = "bybit"
    environment: str = "testnet"  # "mainnet" も許容
    recv_window_ms: int = 5000
    ws_public_url: str | None = None  # TODO: 要API確認（v5 Public WS URL）
    ws_private_url: str | None = None  # TODO: 要API確認（v5 Private WS URL）


class RiskConfig(BaseModel):
    """リスク上限（何をするクラスか：名目/スリッページ/デルタ/日次損失の上限を保持）"""

    max_total_notional: float
    max_symbol_notional: float
    max_net_delta: float
    max_slippage_bps: float
    loss_cut_daily_jpy: float


class StrategyFundingConfig(BaseModel):
    """戦略設定（何をするクラスか：対象銘柄や閾値・再ヘッジ帯を保持）"""

    symbols: list[str] = ["BTCUSDT", "ETHUSDT"]
    min_expected_apr: float = 0.05
    pre_event_open_minutes: int = 60
    hold_across_events: bool = True
    rebalance_band_bps: float = 5.0  # ネットデルタ再ヘッジ帯（bps）


class AppConfig(BaseSettings):
    """アプリ全体の設定（何をするクラスか：各設定をまとめて保持し、環境変数も受け入れる）"""

    # BaseSettings の挙動：KEYS__API_KEY のように "__" でネスト解釈する
    model_config = SettingsConfigDict(
        env_nested_delimiter="__",
        extra="ignore",
    )

    keys: ExchangeKeys
    exchange: ExchangeConfig = ExchangeConfig()
    risk: RiskConfig
    strategy: StrategyFundingConfig
    db_url: str = "sqlite+aiosqlite:///./db/trading.db"
    timezone: str = "UTC"
