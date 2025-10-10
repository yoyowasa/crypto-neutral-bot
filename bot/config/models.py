from __future__ import annotations
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ExchangeKeys(BaseModel):
    api_key: str = Field(default="")
    api_secret: str = Field(default="")


class ExchangeConfig(BaseModel):
    name: str = "bybit"
    environment: str = "testnet"  # "mainnet" も許容
    recv_window_ms: int = 5000
    ws_public_url: str | None = None  # TODO: 必要なら設定
    ws_private_url: str | None = None


class RiskConfig(BaseModel):
    # いずれも USDT（クォート）換算
    max_total_notional_usdt: float
    max_symbol_notional_usdt: float
    max_net_delta_bps: float  # ネットデルタの許容帯（bps）
    max_slippage_bps: float  # 片道スリッページ上限（bps）
    loss_cut_daily_usdt: float  # 日次損失の停止閾値


class StrategyFundingConfig(BaseModel):
    symbols: list[str] = ["BTCUSDT", "ETHUSDT"]
    min_expected_apr: float = 0.05
    pre_event_open_minutes: int = 60
    hold_across_events: bool = True
    rebalance_band_bps: float = 5.0


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter="__",
        extra="ignore",
    )

    keys: ExchangeKeys
    exchange: ExchangeConfig = ExchangeConfig()
    risk: RiskConfig
    strategy: StrategyFundingConfig = StrategyFundingConfig()

    db_url: str = "sqlite+aiosqlite:///./db/trading.db"
    timezone: str = "UTC"
