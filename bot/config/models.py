# これは「設定の型（箱の設計図）」を定義するファイルです。
# Pydantic の BaseModel / BaseSettings を使い、型安全に設定を扱えるようにします。
from __future__ import annotations

from pydantic import BaseModel

try:
    from pydantic_settings import BaseSettings  # type: ignore[attr-defined]

    _HAS_PYDANTIC_SETTINGS = True
except Exception:  # noqa: BLE001 - import 互換性のため広めに捕捉
    from pydantic import BaseSettings  # type: ignore[no-redef]

    _HAS_PYDANTIC_SETTINGS = False


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

    # Pydantic は BaseModel 初期化時にコピーを作るため、リストのデフォルトも安全に扱える
    symbols: list[str] = ["BTCUSDT", "ETHUSDT"]
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
    if _HAS_PYDANTIC_SETTINGS:
        model_config = {
            "env_nested_delimiter": "__",
            "extra": "ignore",
        }
    else:  # Pydantic v1 向けの後方互換

        class Config:  # type: ignore[override,misc]
            env_nested_delimiter = "__"
            extra = "ignore"
