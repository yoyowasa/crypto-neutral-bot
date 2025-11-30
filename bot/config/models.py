from __future__ import annotations

# アプリ設定用の Pydantic モデル群（v2 対応）。
from typing import Any

from pydantic import BaseModel

DEFAULT_STRATEGY_SYMBOLS: tuple[str, str] = ("BTCUSDT", "ETHUSDT")


class ExchangeKeys(BaseModel):
    """取引所 API キー（.env で定義する想定）。"""

    api_key: str
    api_secret: str
    passphrase: str | None = None


class ExchangeConfig(BaseModel):
    """取引所接続の共通設定（詳細な WS URL はデフォルト値を利用）。"""

    # 既定は Bitget。必要なら config/app*.yaml 側で name を上書きする。
    name: str = "bitget"
    # Bitget では environment はメモ用途（"mainnet" 固定でもよい）。
    environment: str = "testnet"
    recv_window_ms: int = 5000
    ws_public_url: str | None = None
    ws_private_url: str | None = None
    use_uta: bool = False  # Bitget UTA (unified) を利用する。Classic口座なら False
    bbo_max_age_ms: int = 3000  # BBO の鮮度ガード（ms）。古い場合は REST で補完。
    rest_max_concurrency: int = 4  # REST 同時実行数（Semaphore）。
    rest_cb_fail_threshold: int = 5  # REST サーキットブレーカーを開く失敗回数しきい値。
    rest_cb_open_seconds: int = 3  # サーキットブレーカーのクールダウン秒数。
    instrument_info_ttl_s: int = 300  # instruments-info 等のメタ情報 TTL（秒）。
    allow_live: bool = False  # 本番口座での発注を許可するかどうか（セーフティ）。


class RiskConfig(BaseModel):
    """リスク上限・スリッページ・日次損失などの閾値。"""

    max_total_notional: float
    max_symbol_notional: float
    max_net_delta: float
    max_slippage_bps: float
    loss_cut_daily_jpy: float
    price_dev_bps_limit: int | None = 50  # BBO からの乖離許容 [bps]。None で無効。
    ws_stale_block_ms: int = 10_000  # Private WS が止まったとみなすまでの許容 ms。
    # prefer_post_only: bool = False  # 将来のためのフラグ（未使用）。
    reject_burst_threshold: int = 3  # REJECT がこの回数を超えたらクールダウン開始。
    reject_burst_window_s: int = 30  # 上記を数える時間窓（秒）。
    symbol_cooldown_s: int = 120  # クールダウン中に新規を止める時間（秒）。


class StrategyFundingConfig(BaseModel):
    """Funding/Basis 戦略のパラメータ（MVP）。"""

    symbols: list[str]
    min_expected_apr: float = 0.13  # 新規エントリーに必要な APR（年率）。
    holdover_min_expected_apr: float = 0.10  # 継続保有を許容する下限 APR。
    pre_event_open_minutes: int = 15
    hold_across_events: bool = False
    rebalance_band_bps: float = 20.0
    min_hold_periods: float = 6.0  # 期待収益計算で想定する最低Funding回数（例:6回=約2日）
    taker_fee_bps_roundtrip: float = 6.0  # 往復テイカー手数料[bps]
    estimated_slippage_bps: float = 5.0  # 想定スリッページ[bps]
    # PostOnly チェイス（注文価格の追従）設定
    chase_enabled: bool = True
    chase_min_reprice_bps: int = 4
    chase_min_reprice_ticks: int = 1
    chase_interval_ms: int = 2500
    chase_max_amends_per_min: int = 6

    def __init__(self, **data: Any) -> None:  # type: ignore[override]
        """シンボル未指定時はデフォルト銘柄を使う。"""

        if "symbols" not in data:
            data["symbols"] = list(DEFAULT_STRATEGY_SYMBOLS)
        super().__init__(**data)


class AppConfig(BaseModel):
    """アプリ全体の設定ルート（.env / YAML をマージして生成）。"""

    keys: ExchangeKeys
    exchange: ExchangeConfig = ExchangeConfig()
    risk: RiskConfig
    strategy: StrategyFundingConfig = StrategyFundingConfig()
    db_url: str = "sqlite+aiosqlite:///./db/trading.db"
    timezone: str = "UTC"

    # Pydantic v2 用設定
    model_config = {
        "extra": "ignore",
    }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AppConfig":
        """生 dict から AppConfig を構築し、サブモデルも必要に応じて型付けする。"""

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
        """AppConfig をロギング等で扱いやすい dict 形式に変換する。"""

        return self.model_dump(mode="python")
