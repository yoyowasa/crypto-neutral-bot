from __future__ import annotations

# アプリ設定用のPydanticモデル群（v2対応）。
from typing import Any

from pydantic import BaseModel

DEFAULT_STRATEGY_SYMBOLS: tuple[str, str] = ("BTCUSDT", "ETHUSDT")


class ExchangeKeys(BaseModel):
    """取引所APIの鍵（.env にて供給）。"""

    api_key: str
    api_secret: str


class ExchangeConfig(BaseModel):
    """取引所接続の基本設定（環境やWS URLはデフォルト任せ）。"""

    name: str = "bybit"
    environment: str = "testnet"  # "mainnet" など
    recv_window_ms: int = 5000
    ws_public_url: str | None = None
    ws_private_url: str | None = None
    bbo_max_age_ms: int = 3000  # BBO鮮度ガード（ミリ秒）。古い板はRESTで補う（STEP28）
    rest_max_concurrency: int = 4  # REST同時実行の上限（セマフォ、STEP29）
    rest_cb_fail_threshold: int = 5  # RESTサーキットブレーカの連続失敗回数（STEP31）
    rest_cb_open_seconds: int = 3  # RESTサーキットのクールダウン秒（STEP31）


class RiskConfig(BaseModel):
    """リスク枠と損切りレベルなどの値"""

    max_total_notional: float
    max_symbol_notional: float
    max_net_delta: float
    max_slippage_bps: float
    loss_cut_daily_jpy: float
    price_dev_bps_limit: int | None = 50  # BBO中値からの最大乖離[bps]。Noneで無効（STEP34）
    ws_stale_block_ms: int = 10_000  # Private WSが古いとき新規発注を止める閾値（ミリ秒、STEP32）
    # （将来用）prefer_post_only: bool = False  # OPEN系をメイカー優先にしたい場合のフラグ
    reject_burst_threshold: int = 3  # この回数だけREJECTEDが
    reject_burst_window_s: int = 30  # この秒数以内に起きたら
    symbol_cooldown_s: int = 120  # その銘柄の新規発注をこの秒数だけ停止（クールダウン）


class StrategyFundingConfig(BaseModel):
    """Funding/ベーシス戦略のパラメータ（MVP）"""

    symbols: list[str]
    min_expected_apr: float = 0.05
    pre_event_open_minutes: int = 60
    hold_across_events: bool = True
    rebalance_band_bps: float = 5.0
    # メイカーチェイス（PostOnly指値の安全な価格追従）の設定（控えめなデフォルト）
    chase_enabled: bool = True  # PostOnly注文の価格追従（アメンド）を有効化するか
    chase_min_reprice_bps: int = 2  # 中値からのズレがこのbps以上なら価格を修正する
    chase_min_reprice_ticks: int = 1  # 最低でもこのtick以上ズレていないと修正しない（将来拡張用）
    chase_interval_ms: int = 1500  # 同じ注文を連続で修正しないための最短間隔（ミリ秒）
    chase_max_amends_per_min: int = 12  # 1注文あたり1分間の最大修正回数（やりすぎ防止）

    def __init__(self, **data: Any) -> None:  # type: ignore[override]
        """初期化時にシンボルの既定値を設定する"""

        if "symbols" not in data:
            data["symbols"] = list(DEFAULT_STRATEGY_SYMBOLS)
        super().__init__(**data)


class AppConfig(BaseModel):
    """アプリ全体の設定ルート（.env / YAML をマージして構築）"""

    keys: ExchangeKeys
    exchange: ExchangeConfig = ExchangeConfig()
    risk: RiskConfig
    strategy: StrategyFundingConfig = StrategyFundingConfig()
    db_url: str = "sqlite+aiosqlite:///./db/trading.db"
    timezone: str = "UTC"

    # Pydantic v2 用の設定
    model_config = {
        "extra": "ignore",
    }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AppConfig":
        """任意の辞書から AppConfig を生成し、必要ならサブモデルへ正規化する"""

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
        """AppConfig を辞書に変換（ログなどに利用）"""

        return self.model_dump(mode="python")
