"""これは「Bybit v5 を CCXT(REST)＋WebSocket(v5) で実装するゲートウェイ」です。"""

from __future__ import annotations

import asyncio  # REST同時実行をセマフォで制御して429を予防するために使う
import hmac
import json
import logging  # _try_prime_scale の成否をログ出力するために使用
import math  # tickSize から桁数を推定するために使用
import os  # PRICE_SCALE_MAX_WAIT_SEC を環境変数から読むために使用
import time
from contextlib import suppress  # これは何をするimport？→ close時の例外を握りつぶして穏当に終了する
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import ROUND_DOWN, Decimal  # 価格・数量を取引所ステップに正確丸めするために使う
from hashlib import sha256
from typing import (  # 認証チェックの戻り値型ヒントに使用  # 型ヒント（内部状態の可読性向上）
    Any,
    Awaitable,
    Callable,
    Dict,
    Optional,
    Tuple,
)

import ccxt as ccxt_sync  # 認証プレフライトで同期版を一時的に利用
import ccxt.async_support as ccxt  # CCXT の async 版を使う（REST はこれで十分）
import websockets

# --- 価格正規化ガードのしきい値（あとで設定化しやすいように定数化） ---
ANCHOR_MAX_AGE_SEC = 5  # spot/index アンカーを「新鮮」とする最大年齢(秒)
PERP_SPOT_RATIO_LOW = 0.7  # perp/spot 比の下限（これ未満は桁落ち等を疑う）
PERP_SPOT_RATIO_HIGH = 1.3  # perp/spot 比の上限（これ超過は桁跳ね等を疑う）
FREEZE_STALE_MAX_SEC = 120  # 凍結状態から復帰させる最大待ち秒数
PRICE_SCALE_MAX_WAIT_SEC = int(os.getenv("PRICE_SCALE_MAX_WAIT_SEC", "15"))  # priceScale待ちの最大秒数

logger = logging.getLogger(__name__)  # このモジュール用のロガー


def _format_guard_context(_ldict: dict) -> dict:
    """
# ruff: noqa: E402
guard.skip 用の要約コンテキストを生成して安全に返す。"""

    keys = (
        "ready_count",
        "ready_required",
        "cooldown_until",
        "next_ready_at",
        "wait_ms",
        "since_prime_ms",
        "since_tick_ms",
    )
    ctx = {}
    for key in keys:
        value = _ldict.get(key)
        if value is not None:
            ctx[key] = value
    return ctx


from bot.core.errors import DataError, ExchangeError, RateLimitError, RiskBreach, WsDisconnected
from bot.core.retry import retryable  # REST呼び出しに指数バックオフ再試行を付与するデコレータ
from bot.core.time import (
    parse_exchange_ts,
    utc_now,  # BBOの鮮度判定に使用
)
from .base import ExchangeGateway
from .types import Balance, FundingInfo, Order, OrderRequest, Position


@dataclass
class _Auth:
    api_key: str
    api_secret: str
    testnet: bool


class _RestWrapper:
    """Callable + closable RESTラッパ
    - セマフォで同時実行を制御するために __call__ を提供
    - 互換性のために close() を実装して ccxt 側へ委譲
    - 未定義の属性は ccxt インスタンスへフォワード
    """

    def __init__(self, owner: "BybitGateway", semaphore: asyncio.Semaphore, ccxt_exchange: Any) -> None:
        self._owner = owner
        self._sem = semaphore
        self._ccxt = ccxt_exchange

    async def __call__(self, func, params: dict) -> dict:
        # Circuit breaker: if cooling down, surface to @retryable
        cb_until = getattr(self._owner, "_rest_cb_open_until", None)
        if cb_until is not None:
            if utc_now() < cb_until:
                try:
                    self._owner._log.warning(
                        "rest.circuit_open seconds=%d",
                        getattr(self._owner, "_rest_cb_open_seconds", 0),
                    )
                except Exception:
                    pass
                raise RateLimitError("REST circuit open (cooling down)")
            # cooldown finished; clear
            self._owner._rest_cb_open_until = None

        async with self._sem:
            try:
                res = await func(params)
                # success: reset failure counter
                self._owner._rest_cb_failures = 0
                return res
            except Exception:
                # failure path: increment and possibly open breaker
                self._owner._rest_cb_failures += 1
                if self._owner._rest_cb_failures >= getattr(self._owner, "_rest_cb_fail_threshold", 5):
                    try:
                        self._owner._log.warning(
                            "rest.circuit_trip threshold=%d open_seconds=%d",
                            getattr(self._owner, "_rest_cb_fail_threshold", 0),
                            getattr(self._owner, "_rest_cb_open_seconds", 0),
                        )
                    except Exception:
                        pass
                    seconds = getattr(self._owner, "_rest_cb_open_seconds", 3)
                    self._owner._rest_cb_open_until = utc_now() + timedelta(seconds=seconds)
                    self._owner._rest_cb_failures = 0
                raise

    async def close(self) -> None:
        close = getattr(self._ccxt, "close", None)
        if callable(close):
            try:
                await close()
            except TypeError:
                close()

    def __getattr__(self, name: str):  # fallback: expose ccxt attributes if accessed
        return getattr(self._ccxt, name)


class BybitGateway(ExchangeGateway):
    """Bybit v5 の REST/WS をまとめたゲートウェイ実装（MVP）"""

    def __init__(self, *, api_key: str, api_secret: str, environment: str = "testnet") -> None:
        """これは何をする関数？
        → 認証情報と環境を受け取り、RESTクライアントとWS接続情報を初期化します。
        """

        # environment を正規化（前後空白/大文字小文字の揺れ対策）
        env_norm = (environment or "testnet").strip().lower()
        self._auth = _Auth(api_key=api_key, api_secret=api_secret, testnet=(env_norm != "mainnet"))
        self._log = logging.getLogger(__name__)  # このゲートウェイの監査ログ出力口

        # --- REST (CCXT) 初期化 ---
        self._ccxt = ccxt.bybit(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
                "options": {
                    # デリバティブ（USDT perp）を既定にする
                    "defaultType": "swap",
                },
            }
        )
        # テストネット切替（Bybit は CCXT の sandbox mode で testnet に切り替わる）
        # 参考: CCXT manual set_sandbox_mode
        if self._auth.testnet:
            self._ccxt.set_sandbox_mode(True)
        # これは何をする初期化？→ ccxtの非同期クライアントを1つだけ生成し、全REST呼び出しで共有する
        # 既存の _rest インスタンスを _ccxt としても参照し、明示 close() の対象とする
        # self._ccxt は上で作成済み
        self._symbol_category_cache: dict[str, str] = (
            {}
        )  # {symbol: {"info": {...}, "ts": datetime}} に拡張（TTL付きキャッシュ）
        self._instrument_info_cache: dict[str, dict] = (
            {}
        )  # {symbol: {"info": {...}, "ts": datetime}} に拡張（TTL付きキャッシュ）
        self._instrument_info_ttl_s: int = 300  # instruments-info のキャッシュ有効期限（秒）。過ぎたら自動で再取得する
        self._bbo_cache: dict[str, dict] = (
            {}
        )  # 公開WS由来の最良気配(bid/ask/ts)をキャッシュしてPostOnly調整等で即時利用
        self._price_scale: dict[str, float] = {}
        self._bbo_max_age_ms: int = 3000  # BBO freshness threshold (ms)
        self._scale_recent: dict[str, float] = {}
        self._price_scale_ready_count: dict[str, int] = {}
        self._price_scale_wait_start_ms: dict[str, int] = {}
        self._scale_probe_ts: Dict[str, float] = (
            {}
        )  # 何をする？→ スケール未準備時にRESTで取得を“試みた最後の時刻”を記録して連打を防ぐ

        # 価格スケールと価格ガードの内部状態（シンボルごとに保持する）
        self._scale_cache: Dict[str, Dict[str, float]] = (
            {}
        )  # {sym: {"priceScale": int, "tickSize": float, "multiplier": float}}
        self._last_good_perp_px: Dict[str, float] = {}  # {sym: float} 最後に信頼できた先物価格
        self._anchor_last_ts: Dict[str, float] = {}  # {sym: float} アンカー(spot/index)更新のUNIX時刻
        self._price_state: Dict[str, str] = {}  # {sym: "NO_ANCHOR"|"READY"|"FROZEN"}
        self._frozen_since: Dict[str, float] = {}  # {sym: float} 凍結開始時刻
        self._last_spot_px: Dict[str, float] = {}  # {sym: float} 直近のspotアンカー
        self._last_index_px: Dict[str, float] = {}  # {sym: float} 直近のindexアンカー
        self._price_dev_bps_limit: int | None = (
            50  # BBO中値からの最大乖離[bps]。Noneならガード無効（設定化は後続STEPで）
        )

        self._rest_max_concurrency: int = 4  # RESTの同時実行上限。環境に応じて調整可能（429予防）
        self._rest_semaphore = asyncio.Semaphore(self._rest_max_concurrency)  # 上限を守るセマフォ
        # RESTサーキットブレーカ状態
        self._rest_cb_failures: int = 0  # 連続失敗回数（成功したら0に戻す）
        self._rest_cb_fail_threshold: int = 5  # この回数だけ連続で失敗したら休憩に入る
        self._rest_cb_open_seconds: int = 3  # 休憩する秒数（短く小さく、再開はすぐ）
        self._rest_cb_open_until: datetime | None = None  # 休憩を終える時刻（UTC）。過ぎたら自動で開通
        # _rest を callable + closable なラッパにして互換性を保つ
        self._rest = _RestWrapper(self, self._rest_semaphore, self._ccxt)

        # --- WS エンドポイント（Bybit v5 公式・要確認 & 必要に応じて linear/inverse/spot を選択） ---
        # Public:
        #   Mainnet:
        #     Spot: wss://stream.bybit.com/v5/public/spot
        #     Linear: wss://stream.bybit.com/v5/public/linear
        #     Inverse: wss://stream.bybit.com/v5/public/inverse
        #   Testnet:
        #     Spot:   wss://stream-testnet.bybit.com/v5/public/spot
        #     Linear: wss://stream-testnet.bybit.com/v5/public/linear
        #     Inverse:wss://stream-testnet.bybit.com/v5/public/inverse
        # Private:
        #   Mainnet: wss://stream.bybit.com/v5/private
        #   Testnet: wss://stream-testnet.bybit.com/v5/private
        self._ws_public_linear = (
            "wss://stream-testnet.bybit.com/v5/public/linear"
            if self._auth.testnet
            else "wss://stream.bybit.com/v5/public/linear"
        )
        self._ws_private = (
            "wss://stream-testnet.bybit.com/v5/private" if self._auth.testnet else "wss://stream.bybit.com/v5/private"
        )

        # ping 間隔（推奨は20秒おきに ping）
        self._ws_ping_interval = 20.0

    # ---------- 内部ユーティリティ（シンボル正規化ほか） ----------

    @staticmethod
    def _to_ccxt_symbol(internal: str) -> tuple[str, str]:
        """これは何をする関数？
        → 内部表記（例：'BTCUSDT' / 'BTCUSDT_SPOT'）を CCXT の統一シンボルに変換します。
          - perp: 'BTCUSDT' → 'BTC/USDT:USDT'
          - spot: 'BTCUSDT_SPOT' → 'BTC/USDT'
        戻り値：(ccxt_symbol, market_kind)  # market_kind: "swap" or "spot"
        """

        if internal.endswith("_SPOT"):
            core = internal[:-5]
            base, quote = core[:-4], core[-4:]
            return f"{base}/{quote}", "spot"
        base, quote = internal[:-4], internal[-4:]
        return f"{base}/{quote}:{quote}", "swap"

    @staticmethod
    def _order_status_from_ccxt(s: str) -> str:
        """これは何をする関数？
        → CCXT の status を内部標準に正規化します。
        """

        mapping = {
            "open": "new",
            "closed": "filled",
            "canceled": "canceled",
            "rejected": "rejected",
            "expired": "canceled",
        }
        return mapping.get(s, s)

    @staticmethod
    def _safe_float(value: object | None) -> float | None:
        if value in (None, "", "0E-8"):
            return None
        try:
            return float(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            try:
                return float(str(value))
            except Exception:
                return None

    def _apply_price_scale(self, symbol: str, value: object | None) -> float | None:
        val = self._safe_float(value)
        if val is None:
            return None
        scale = self._price_scale.get(symbol, 1.0)
        return val * scale

    # ---------- REST 実装（CCXT） ----------

    async def get_balances(self) -> list[Balance]:
        """これは何をする関数？→ 残高の一覧を返す（0は省く）"""

        try:
            bal = await self._ccxt.fetch_balance()
            out: list[Balance] = []
            for asset, info in bal.get("total", {}).items():
                total = float(info)
                if total == 0:
                    continue
                available = float(bal.get("free", {}).get(asset, 0))
                out.append(Balance(asset=asset, total=total, available=available))
            return out
        except Exception as e:  # noqa: BLE001
            msg = str(e).lower()
            if "api key" in msg or "authentication" in msg or "auth" in msg:
                return []
            raise ExchangeError(f"get_balances failed: {e}") from e

    async def get_positions(self) -> list[Position]:
        """これは何をする関数？→ デリバティブの建玉を返す（非ゼロのみ）"""

        try:
            # Bybit は symbol 指定なしでも返る。なければ空配列。
            pos_list = await self._ccxt.fetch_positions()
            out: list[Position] = []
            for p in pos_list or []:
                size = float(p.get("contracts") or p.get("info", {}).get("size") or 0.0)
                if size == 0:
                    continue
                side = "long" if (p.get("side", "").lower() == "long" or size > 0) else "short"
                entry = float(p.get("entryPrice") or 0.0)
                upnl = float(p.get("unrealizedPnl") or p.get("unrealizedPnl", 0.0) or 0.0)
                sym = p.get("symbol") or ""
                out.append(
                    Position(
                        symbol=sym.replace("/", "").replace(":USDT", ""),
                        side=side,
                        size=abs(size),
                        entry_price=entry,
                        unrealized_pnl=upnl,
                    )
                )
            return out
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"get_positions failed: {e}") from e

    @retryable()  # 再接続直後のopen注文取り直しで一過性の失敗にも強くする
    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """これは何をする関数？→ 指定（または全て）の未約定注文を返す"""

        try:
            if symbol:
                ccxt_sym, _kind = self._to_ccxt_symbol(symbol)
                oo = await self._ccxt.fetch_open_orders(symbol=ccxt_sym)
            else:
                oo = await self._ccxt.fetch_open_orders()
            out: list[Order] = []
            for o in oo or []:
                # convert ccxt symbol to internal format like 'BTCUSDT'
                internal_symbol = None
                try:
                    cs = o.get("symbol")
                    if isinstance(cs, str):
                        internal_symbol = cs.replace("/", "").replace(":USDT", "").replace(":USDC", "")
                except Exception:
                    internal_symbol = None
                out.append(
                    Order(
                        symbol=internal_symbol,
                        order_id=str(o.get("id") or o.get("orderId")),
                        client_id=o.get("clientOrderId"),
                        status=self._order_status_from_ccxt(o.get("status") or ""),
                        filled_qty=float(o.get("filled", 0.0)),
                        avg_fill_price=float(o.get("average", 0.0)) if o.get("average") is not None else None,
                    )
                )
            return out
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"get_open_orders failed: {e}") from e

    async def get_open_orders_detailed(self, symbol: str) -> list[dict]:
        """Bybit v5 /v5/order/realtime で詳細フィールド付きの open 注文一覧を返す。
        - timeInForce / orderType / orderStatus / side / price などを含む辞書のリスト
        - maintain_postonly_orders などの精緻なフィルタリングに利用
        """

        try:
            category = await self._resolve_category(symbol)
            api_symbol = symbol[:-5] if symbol.endswith("_SPOT") else symbol
            res = await self._rest(
                self._ccxt.privateGetV5OrderRealtime,
                {"category": category, "symbol": api_symbol},
            )
            return ((res or {}).get("result", {}) or {}).get("list", []) or []
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"get_open_orders_detailed failed: {e}") from e

    def check_auth(self) -> Tuple[bool, str]:
        """Bybitの署名付きエンドポイントを1回だけ叩いて認証の成否と理由を返す関数。
        - イベントループ外でも扱えるよう、ccxtの同期版を一時的に使用する。
        - 成功: (True, "OK")
        - 失敗: (False, "retCode=... retMsg=...") もしくは例外内容
        """

        ex = None
        try:
            # ccxt同期版の一時クライアントを使用（署名付GET /v5/account/info）
            ex = ccxt_sync.bybit(
                {
                    "apiKey": self._auth.api_key,
                    "secret": self._auth.api_secret,
                    "enableRateLimit": True,
                }
            )
            ex.set_sandbox_mode(self._auth.testnet)

            # Bybit標準のレスポンス: retCode==0 が成功
            resp = ex.privateGetV5AccountInfo({})
            ret_code = (resp or {}).get("retCode")
            ret_msg = (resp or {}).get("retMsg", "")

            ok = False
            try:
                ok = int(ret_code) == 0
            except Exception:
                ok = str(ret_code) == "0"

            if ok:
                logging.getLogger(__name__).info("auth.preflight.ok: exchange=bybit")
                return True, "OK"

            logging.getLogger(__name__).warning(
                "auth.preflight.ng: exchange=bybit retCode=%s retMsg=%s",
                ret_code,
                ret_msg,
            )
            return False, f"retCode={ret_code} retMsg={ret_msg}"

        except Exception as e:  # 例外はネットワーク/署名/時刻ずれ等の可能性
            logging.getLogger(__name__).warning("auth.preflight.error: exchange=bybit error=%s", e)
            return False, f"exception={type(e).__name__}: {e}"
        finally:
            try:
                if ex is not None:
                    ex.close()
            except Exception:
                pass

    @retryable()  # 発注の一過性失敗（429/接続断など）に対して自動で指数バックオフ再試行する
    async def place_order(self, req: OrderRequest) -> Order:
        """これは何をする関数？→ 注文を発注し、作成された注文情報を返す"""

        try:
            ccxt_sym, kind = self._to_ccxt_symbol(req.symbol)
            params: dict[str, Any] = {}

            # CCXT 統一パラメータ
            # PostOnly が優先（Bybit v5 は timeInForce="PostOnly" を使用）
            if getattr(req, "post_only", False):
                params["timeInForce"] = "PostOnly"
            elif getattr(req, "time_in_force", None):
                params["timeInForce"] = req.time_in_force

            # reduceOnly は線形/逆数カテゴリのみ有効
            try:
                _cat = await self._resolve_category(req.symbol)
            except Exception:
                _cat = None
            if _cat in ("linear", "inverse") and getattr(req, "reduce_only", False):
                params["reduceOnly"] = True
            coid = getattr(req, "client_order_id", None) or req.client_id
            if coid:
                params["clientOrderId"] = coid
            # Normalize price/qty by instruments-info to avoid invalid step errors
            price_str, qty_str = await self._normalize_price_qty(
                symbol=req.symbol,
                side=req.side,
                price=(req.price if req.type == "limit" else None),
                qty=req.qty,
                order_type=req.type,
            )  # BybitのtickSize/qtyStepに合わせて安全に丸める
            if qty_str is not None:
                params["qty"] = qty_str  # 取引所許容ステップの数量（文字列）
            if price_str is not None:
                params["price"] = price_str  # 取引所許容ステップの価格（文字列：Limitのみ）
            else:
                params.pop("price", None)
            # PostOnly: BBO基準で非クロスに最終調整
            is_post_only = (params.get("timeInForce") == "PostOnly") or bool(getattr(req, "post_only", None))
            if is_post_only and (req.type or "").lower().startswith("limit") and ("price" in params):
                try:
                    params["price"] = await self._ensure_post_only_price(req.symbol, req.side, params["price"])
                except Exception:
                    pass
            if (
                (req.type or "").lower().startswith("limit")
                and ("price" in params)
                and (self._price_dev_bps_limit is not None)
            ):
                await self._guard_price_deviation(req.symbol, req.side, params["price"])  # BBO中値からの乖離を安全確認
            price = float(params["price"]) if ("price" in params and params["price"] is not None) else None
            try:
                created = await self._ccxt.create_order(
                    symbol=ccxt_sym,
                    type=req.type,
                    side=req.side,
                    amount=float(qty_str),
                    price=price,
                    params=params,
                )
            except Exception as e:
                # 冪等フォールバック: duplicate orderLinkId やネット不明時
                coid = params.get("clientOrderId") or getattr(req, "client_order_id", None)
                found = await self._find_existing_order(req.symbol, coid)
                if found:
                    # 既に同一注文が存在。found から Order を組み立てて返す（状態更新はWSが運ぶ）
                    oid = str(found.get("orderId") or found.get("orderID") or "")
                    status_raw = (found.get("orderStatus") or "").lower()
                    if status_raw in {"new", "created"}:
                        status = "open"
                    elif status_raw in {"partiallyfilled", "partialfilled", "partially_filled", "partial"}:
                        status = "open"
                    elif status_raw in {"filled", "closed", "done"}:
                        status = "closed"
                    elif status_raw in {"canceled", "cancelled", "expired"}:
                        status = "canceled"
                    elif status_raw == "rejected":
                        status = "rejected"
                    else:
                        status = "open"
                    avg_p = found.get("avgPrice")
                    return Order(
                        symbol=req.symbol,
                        order_id=oid or (coid or ""),
                        client_id=coid,
                        client_order_id=coid,
                        status=status,
                        filled_qty=float(found.get("cumExecQty") or 0.0),
                        avg_fill_price=(float(str(avg_p)) if avg_p not in (None, "") else None),
                    )
                # 見つからなければリトライ機構へ委譲
                raise e
            return Order(
                symbol=req.symbol,
                order_id=str(created.get("id") or created.get("orderId")),
                client_id=created.get("clientOrderId"),
                client_order_id=created.get("clientOrderId"),
                status=self._order_status_from_ccxt(created.get("status") or "open"),
                filled_qty=float(created.get("filled", 0.0)),
                avg_fill_price=float(created.get("average", 0.0)) if created.get("average") is not None else None,
            )
        except ccxt.RateLimitExceeded as e:
            raise RateLimitError(str(e)) from e
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"place_order failed: {e}") from e

    async def cancel_order_old(self, order_id: str | None = None, client_id: str | None = None) -> None:
        """これは何をする関数？→ order_id か client_id の指定で注文を取り消す"""

        try:
            params: dict[str, Any] = {}
            if client_id:
                params["clientOrderId"] = client_id  # Bybitの orderLinkId にマップされる想定（要API確認）
            # CCXT は symbol が必要な場合があるため、未指定なら全キャンセル系を検討するが、MVPは注文ID系を優先
            if order_id:
                await self._ccxt.cancel_order(id=order_id, symbol=None, params=params)
            elif client_id:
                # 一部CCXT実装では symbol 必須のことがあるため、注文一覧から検索→cancel でも良い
                await self._ccxt.cancel_order(id=None, symbol=None, params=params)
            else:
                raise ExchangeError("cancel_order requires order_id or client_id")
        except ccxt.RateLimitExceeded as e:
            raise RateLimitError(str(e)) from e
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"cancel_order failed: {e}") from e

    async def get_ticker(self, symbol: str) -> float:
        """Bybitのティッカー価格を取得し、異常なスケールを補正する。"""

        try:
            logger.info("bybit.get_ticker.call sym=%s", symbol)  # ティッカー取得開始の可視化
            ccxt_sym, kind = self._to_ccxt_symbol(symbol)
            ticker = await self._ccxt.fetch_ticker(ccxt_sym)
            anchor_spot = None
            if kind == "swap" and (not symbol.endswith("_SPOT")):
                try:
                    # CCXT spot ティッカーと揃える（metrics が参照する px_spot に合わせる）
                    spot_ccxt_sym, _ = self._to_ccxt_symbol(f"{symbol}_SPOT")
                    spot_tk = await self._ccxt.fetch_ticker(spot_ccxt_sym)

                    def _sf(x):
                        try:
                            return float(x) if x not in (None, "") else None
                        except Exception:
                            return None

                    sl = _sf(spot_tk.get("last"))
                    sb = _sf(spot_tk.get("bid")) or _sf((spot_tk.get("info") or {}).get("bid1Price"))
                    sa = _sf(spot_tk.get("ask")) or _sf((spot_tk.get("info") or {}).get("ask1Price"))
                    si = _sf((spot_tk.get("info") or {}).get("indexPrice"))
                    if sb is not None and sa is not None:
                        anchor_spot = (sb + sa) / 2.0
                    elif sl is not None:
                        anchor_spot = sl
                    elif si is not None:
                        anchor_spot = si
                    elif sb is not None:
                        anchor_spot = sb
                    elif sa is not None:
                        anchor_spot = sa
                except Exception:
                    anchor_spot = None
            # アンカー/スケールの内部キャッシュを更新
            try:
                if anchor_spot is not None:
                    self._last_spot_px[symbol] = float(anchor_spot)  # 直近spotアンカー
                idx_px = None
                try:
                    idx_px = (
                        float((ticker.get("info") or {}).get("indexPrice"))
                        if (ticker.get("info") or {}).get("indexPrice") not in (None, "")
                        else None
                    )
                except Exception:
                    idx_px = None
                if idx_px is not None:
                    self._last_index_px[symbol] = float(idx_px)  # 直近indexアンカー
                self._update_scale_cache(symbol, ticker.get("info") or {})  # スケール情報のキャッシュ
            except Exception:
                pass

            # perp はガード付き正規化、spot は従来処理
            raw_last = self._safe_float(ticker.get("last")) or 0.0
            if kind == "swap" and (not symbol.endswith("_SPOT")):
                logger.info("bybit.get_ticker.ok sym=%s", symbol)  # ティッカー取得成功の可視化
                return self._normalize_ticker_price_guarded(symbol, float(raw_last))
            logger.info("bybit.get_ticker.ok sym=%s", symbol)  # ティッカー取得成功の可視化
            return self._normalize_ticker_price(symbol, ticker, anchor_spot)
        except Exception as e:  # noqa: BLE001
            msg = str(e).lower()
            if "api key" in msg or "authentication" in msg or "auth" in msg:
                pub = ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "swap"}})
                try:
                    if self._auth.testnet:
                        pub.set_sandbox_mode(True)
                    ticker = await pub.fetch_ticker(ccxt_sym)
                    logger.info("bybit.get_ticker.ok sym=%s", symbol)  # ティッカー取得成功の可視化
                    return self._normalize_ticker_price(symbol, ticker, None)
                except Exception:
                    pass
                finally:
                    with suppress(Exception):
                        await pub.close()
            raise ExchangeError(f"get_ticker failed: {e}") from e

    def _normalize_ticker_price(self, symbol: str, ticker: dict, anchor_spot: float | None = None) -> float:
        info = ticker.get("info") or {}
        # 何をする？→ tickerのinfoから得られるlot/刻み情報を即時にキャッシュ（RESTプライムのフォールバック）
        try:
            self._update_scale_cache(symbol, info)
            cat = "spot" if symbol.endswith("_SPOT") else "linear"
            self._update_scale_cache_extended(symbol, info, cat)
        except Exception:
            pass
        existing_scale = self._price_scale.get(symbol, 1.0)
        raw_last = self._safe_float(ticker.get("last"))
        index_price = self._safe_float(info.get("indexPrice"))
        mark_price = self._safe_float(info.get("markPrice"))
        raw_bid = self._safe_float(ticker.get("bid")) or self._safe_float(info.get("bid1Price"))
        raw_ask = self._safe_float(ticker.get("ask")) or self._safe_float(info.get("ask1Price"))

        scale = existing_scale
        price = None

        if raw_last is not None and index_price is not None and index_price > 0:
            ratio = raw_last / index_price if index_price else None
            if ratio is not None and (ratio >= 4.0 or ratio <= 0.25):
                scale = index_price / raw_last if raw_last else 1.0
                price = index_price
            else:
                scale = 1.0
                price = raw_last
        elif raw_last is not None:
            price = raw_last * scale
        elif index_price is not None:
            scale = 1.0
            price = index_price
        elif mark_price is not None:
            price = mark_price * scale

        scaled_bid = raw_bid * scale if raw_bid is not None else None
        scaled_ask = raw_ask * scale if raw_ask is not None else None

        if price is None:
            if scaled_bid is not None and scaled_ask is not None:
                price = (scaled_bid + scaled_ask) / 2.0
            elif scaled_bid is not None:
                price = scaled_bid
            elif scaled_ask is not None:
                price = scaled_ask

        if price is None:
            raise ExchangeError(f"ticker has no price fields: {ticker}")

        if (not symbol.endswith("_SPOT")) and (anchor_spot is not None):
            try:
                ratio = None
                if price and anchor_spot:
                    ratio = float(price) / float(anchor_spot)
                if ratio is None or (ratio > 2.0 or ratio < 0.5):
                    ref_raw = None
                    if raw_last is not None:
                        ref_raw = raw_last
                    elif raw_bid is not None and raw_ask is not None:
                        ref_raw = (raw_bid + raw_ask) / 2.0
                    elif raw_bid is not None:
                        ref_raw = raw_bid
                    elif raw_ask is not None:
                        ref_raw = raw_ask
                    if ref_raw and ref_raw > 0:
                        scale = float(anchor_spot) / float(ref_raw)
                        price = float(anchor_spot)
            except Exception:
                pass

        if abs(scale - 1.0) > 1e-9:
            self._price_scale[symbol] = scale
        else:
            self._price_scale.pop(symbol, None)

        # --- scale 安定性の連続カウントを更新（ready 判定用）---
        try:
            prev = self._scale_recent.get(symbol)
            now_ms = int(utc_now().timestamp() * 1000)
            if prev is None:
                # 初回観測
                self._scale_recent[symbol] = float(scale)
                self._price_scale_ready_count[symbol] = 1
                self._price_scale_wait_start_ms[symbol] = now_ms
            else:
                if abs(float(scale) - float(prev)) <= 1e-9:
                    # 変動なし -> 連続回数を加算
                    self._price_scale_ready_count[symbol] = self._price_scale_ready_count.get(symbol, 0) + 1
                else:
                    # 変動あり -> 連続カウントをリセットし直近スケールと開始時刻を更新
                    self._scale_recent[symbol] = float(scale)
                    self._price_scale_ready_count[symbol] = 1
                    self._price_scale_wait_start_ms[symbol] = now_ms
        except Exception:
            # カウント更新の失敗は取引フローをブロックしない
            pass

        return float(price)

    def is_price_scale_ready(self, symbol: str, required: int = 2) -> bool:
        """Return True if price scale appears stabilized (consecutive checks).

        環境変数で制御可能:
          - PRICE_SCALE_READY_REQUIRED: 連続安定回数の要求値（デフォルト: 引数の値、通常2）
          - PRICE_SCALE_MAX_WAIT_SEC: 最大待機秒数（経過で ready 扱い, デフォルト: 未設定）
        """
        try:
            # Env ノブの適用
            try:
                required_env = os.environ.get("PRICE_SCALE_READY_REQUIRED")
                if required_env not in (None, ""):
                    required = int(required_env)
            except Exception:
                pass

            # デフォルトは定数（環境変数が未設定でも 15 秒で解放）
            max_wait_sec: int | None = PRICE_SCALE_MAX_WAIT_SEC

            cnt = int(getattr(self, "_price_scale_ready_count", {}).get(symbol, 0))
            if cnt >= int(required):
                return True

            if max_wait_sec is not None and max_wait_sec >= 0:
                start_map = getattr(self, "_price_scale_wait_start_ms", {})
                start_ms = start_map.get(symbol)
                now_ms = int(utc_now().timestamp() * 1000)
                if not start_ms:
                    start_map[symbol] = now_ms
                elif (now_ms - int(start_ms)) >= max_wait_sec * 1000:
                    return True
            return False
        except Exception:
            return False

    def _update_scale_cache(self, symbol: str, meta: dict) -> None:
        """Bybitの銘柄メタ（tickSize/priceScale/契約倍率）を読み取りキャッシュする関数。"""
        price_filter = (meta or {}).get("priceFilter") or {}
        tick_size = price_filter.get("tickSize", (meta or {}).get("tickSize"))
        scale = (meta or {}).get("priceScale")
        # 例: tickSize=0.01 → scale=2 のように小数桁数を推定する
        try:
            if scale in (None, "") and isinstance(tick_size, (int, float)) and float(tick_size) > 0:
                scale = max(0, -int(round(math.log10(float(tick_size)))))
        except Exception:
            pass
        multiplier = (meta or {}).get("multiplier") or (meta or {}).get("contractSize") or 1.0

        # スケール情報をキャッシュ
        info = self._scale_cache.get(symbol, {}).copy()
        if scale not in (None, ""):
            info["priceScale"] = int(scale)
        if tick_size not in (None, ""):
            info["tickSize"] = float(tick_size)
        if multiplier not in (None, ""):
            info["multiplier"] = float(multiplier)
        if "multiplier" not in info:
            info["multiplier"] = 1.0
        self._scale_cache[symbol] = info

    def _is_perp_price_plausible(self, spot_px: Optional[float], perp_px: Optional[float]) -> bool:
        """perp/spot の比率で“値の常識性”を判定する関数。"""
        if not spot_px or not perp_px:
            return False
        try:
            ratio = float(perp_px) / float(spot_px)
        except Exception:
            return False
        return PERP_SPOT_RATIO_LOW <= ratio <= PERP_SPOT_RATIO_HIGH

    def _normalize_ticker_price_guarded(self, symbol: str, raw_last: float) -> float:
        """perpの生価格(raw_last)を、アンカーとスケールを用いて正しい桁へ正規化する関数。
        - アンカー/スケールが未準備・不合理な値のときは更新を凍結（freeze）して自己復旧を待つ。
        """
        log = logging.getLogger(__name__)
        now = time.time()
        ready_count = int(getattr(self, "_price_scale_ready_count", {}).get(symbol, 0))
        try:
            ready_required = int(os.environ.get("PRICE_SCALE_READY_REQUIRED", "2"))
        except Exception:
            ready_required = 2
        wait_ms = None
        start_ms = getattr(self, "_price_scale_wait_start_ms", {}).get(symbol)
        if start_ms is not None:
            try:
                wait_ms = max(0, int(now * 1000 - int(start_ms)))
            except Exception:
                wait_ms = None
        cooldown_until = (
            getattr(self, "_price_scale_cooldown_until", {}).get(symbol)
            if hasattr(self, "_price_scale_cooldown_until")
            else None
        )
        next_ready_at = (
            getattr(self, "_price_scale_next_ready", {}).get(symbol)
            if hasattr(self, "_price_scale_next_ready")
            else None
        )
        since_prime_ms = None
        last_prime = getattr(self, "_scale_probe_ts", {}).get(symbol)
        if last_prime is not None:
            try:
                since_prime_ms = max(0, int((now - float(last_prime)) * 1000))
            except Exception:
                since_prime_ms = None
        since_tick_ms = None
        last_tick = getattr(self, "_scale_recent_ts", {}).get(symbol)
        if last_tick is not None:
            try:
                since_tick_ms = max(0, int((now - float(last_tick)) * 1000))
            except Exception:
                since_tick_ms = None

        # スケール準備の確認（未準備なら更新せず、最後の良値か生値を返す）
        scale_info = self._scale_cache.get(symbol)
        if not scale_info or scale_info.get("priceScale") is None:
            # 何をする？→ スケール未準備なら、1分に1回まで REST で自動取得を試みる（成功すればそのまま続行）
            try:
                self._try_prime_scale(symbol)
                scale_info = self._scale_cache.get(symbol)
            except Exception:
                scale_info = self._scale_cache.get(symbol)
            if not scale_info or scale_info.get("priceScale") is None:
                self._price_state[symbol] = "NO_ANCHOR"
                reason = "price_scale_not_ready"
                log.info("guard.skip sym=%s reason=%s ctx=%s", symbol, reason, _format_guard_context(locals()))
                return float(self._last_good_perp_px.get(symbol, float(raw_last)))

        # アンカー取得（存在すれば鮮度も更新）
        spot_px = getattr(self, "_last_spot_px", {}).get(symbol) if hasattr(self, "_last_spot_px") else None
        index_px = getattr(self, "_last_index_px", {}).get(symbol) if hasattr(self, "_last_index_px") else None
        anchor_px = spot_px or index_px
        now = time.time()
        if anchor_px:
            self._anchor_last_ts[symbol] = now
        else:
            anchor_ts = self._anchor_last_ts.get(symbol)
            if (anchor_ts is None) or ((now - anchor_ts) > ANCHOR_MAX_AGE_SEC):
                self._price_state[symbol] = "NO_ANCHOR"
                reason = "anchor_stale"
                log.info("guard.skip sym=%s reason=%s ctx=%s", symbol, reason, _format_guard_context(locals()))
                return float(self._last_good_perp_px.get(symbol, float(raw_last)))

        # スケール丸め
        try:
            candidate = float(raw_last)
        except Exception:
            log.warning("normalize.error: non-numeric raw_last sym=%s raw_last=%r", symbol, raw_last)
            return float(self._last_good_perp_px.get(symbol, float(raw_last)))
        scale = int(scale_info.get("priceScale") or 0)
        if scale >= 0:
            candidate = round(candidate, scale)

        # 候補値の常識性チェック（アンカー比率 or 直近良値の±30%）
        plausible = self._is_perp_price_plausible(anchor_px, candidate) if anchor_px else False
        if not plausible:
            last_good = self._last_good_perp_px.get(symbol)
            if last_good:
                lower = last_good * 0.7
                upper = last_good * 1.3
                plausible = lower <= candidate <= upper

        if not plausible:
            prev_state = self._price_state.get(symbol)
            self._price_state[symbol] = "FROZEN"
            self._frozen_since.setdefault(symbol, now)
            log.warning(
                "guard.freeze: perp price out-of-band sym=%s candidate=%s anchor=%s state_from=%s",
                symbol,
                candidate,
                anchor_px,
                prev_state,
            )
            frozen_at = self._frozen_since.get(symbol)
            if frozen_at and (now - frozen_at) > FREEZE_STALE_MAX_SEC:
                self._price_state[symbol] = "NO_ANCHOR"
            return float(self._last_good_perp_px.get(symbol, candidate))

        # 正常値採用（凍結→復旧ログ）
        if self._price_state.get(symbol) == "FROZEN":
            log.info("guard.unfreeze: perp price recovered sym=%s candidate=%s anchor=%s", symbol, candidate, anchor_px)
        self._price_state[symbol] = "READY"
        self._frozen_since.pop(symbol, None)
        self._last_good_perp_px[symbol] = candidate
        return float(candidate)

    def _update_scale_cache_extended(self, symbol: str, meta: dict, category: str = "") -> None:
        """何をする関数？→ 価格スケールに加えて数量刻み/最小数量/名目額をカテゴリ別にキャッシュする。"""
        info = self._scale_cache.get(symbol, {}).copy()
        # price scale
        price_filter = (meta or {}).get("priceFilter") or {}
        tick_size = price_filter.get("tickSize", (meta or {}).get("tickSize"))
        scale = (meta or {}).get("priceScale")
        try:
            if scale in (None, "") and isinstance(tick_size, (int, float)) and float(tick_size) > 0:
                scale = max(0, -int(round(math.log10(float(tick_size)))))
        except Exception:
            pass
        if info.get("priceScale") is None and (category == "linear" or not category) and scale not in (None, ""):
            info["priceScale"] = int(scale)
        if tick_size not in (None, ""):
            info["tickSize"] = float(tick_size)

        # 数量刻み/最小制約
        lot = (meta or {}).get("lotSizeFilter") or {}

        def _to_f(x):
            try:
                return float(x)
            except Exception:
                return None

        qty_step = _to_f(lot.get("qtyStep") or lot.get("basePrecision") or lot.get("minOrderQty"))
        min_qty = _to_f(lot.get("minOrderQty"))
        min_notional = _to_f(lot.get("minOrderAmt") or lot.get("minNotional"))
        if category == "linear":
            if qty_step is not None:
                info["qtyStep_perp"] = qty_step
            if min_qty is not None:
                info["minQty_perp"] = min_qty
            if min_notional is not None:
                info["minNotional_perp"] = min_notional
        elif category == "spot":
            if qty_step is not None:
                info["qtyStep_spot"] = qty_step
            if min_qty is not None:
                info["minQty_spot"] = min_qty
            if min_notional is not None:
                info["minNotional_spot"] = min_notional

        # 契約倍率（参考）
        multiplier = (meta or {}).get("multiplier") or (meta or {}).get("contractSize") or 1.0
        info["multiplier"] = float(multiplier) if multiplier not in (None, "") else 1.0

        self._scale_cache[symbol] = info

    def _common_qty_step(self, symbol: str) -> Optional[float]:
        """何をする関数？→ spot と perp の刻みの“最小公倍刻み(LCM)”を返す。両足で割り切れる安全な共通刻み。"""
        info = self._scale_cache.get(symbol) or {}
        q_spot = info.get("qtyStep_spot")
        q_perp = info.get("qtyStep_perp")
        if q_spot is None and q_perp is None:
            return None
        if q_spot is None:
            return float(q_perp)
        if q_perp is None:
            return float(q_spot)
        try:
            d1 = Decimal(str(q_spot)).normalize()
            d2 = Decimal(str(q_perp)).normalize()
            s1 = max(0, -d1.as_tuple().exponent)
            s2 = max(0, -d2.as_tuple().exponent)
            scale10 = 10 ** max(s1, s2)
            i1 = int(Decimal(str(q_spot)) * scale10)
            i2 = int(Decimal(str(q_perp)) * scale10)
            if i1 <= 0 or i2 <= 0:
                return float(max(q_spot, q_perp))
            lcm_int = (i1 * i2) // math.gcd(i1, i2)
            return float(Decimal(lcm_int) / Decimal(scale10))
        except Exception:
            return float(max(q_spot, q_perp))

    def _try_prime_scale(self, symbol: str) -> bool:
        """スケール未準備のシンボルに対し、Bybit RESTでメタを取得しpriceScale/tickSizeをキャッシュする。
        """

        now = time.time()
        last = self._scale_probe_ts.get(symbol, 0.0)
        if (now - last) < 60.0:
            return False
        self._scale_probe_ts[symbol] = now

        ex = None
        try:
            # 同期ccxtで軽量に問い合わせ（asyncにせず、呼び出し元の同期処理でも使えるようにする）
            ex = ccxt_sync.bybit({"enableRateLimit": True, "options": {"defaultType": "swap"}})
            if self._auth.testnet:
                ex.set_sandbox_mode(True)
            api_symbol = symbol[:-5] if symbol.endswith("_SPOT") else symbol
            for category in ("linear", "spot"):
                try:
                    res = ex.publicGetV5MarketInstrumentsInfo({"category": category, "symbol": api_symbol})
                    items = ((res or {}).get("result", {}) or {}).get("list", []) or []
                    if items:
                        meta = items[0]
                        self._update_scale_cache_extended(
                            symbol, meta, category
                        )  # 数量刻み/最小制約もカテゴリ別にキャッシュ
                        self._update_scale_cache(symbol, meta)  # ここで priceScale/tickSize をキャッシュ
                        price_scale = self._scale_cache.get(symbol, {}).get("priceScale")
                        logger.info(
                            "scale.prime.ok sym=%s priceScale=%s", symbol, price_scale
                        )  # 何をする: プライミング成功時に取得値を可視化
                        return True
                except Exception as e:
                    logger.exception(
                        "scale.prime.error sym=%s reason=%s", symbol, e
                    )  # 何をする: プライミング失敗の理由を可視化
        except Exception as e:
            logger.exception("scale.prime.error sym=%s reason=%s", symbol, e)
        finally:
            try:
                if ex is not None:
                    ex.close()
            except Exception:
                pass
        return False  # どちらのカテゴリでも情報が得られなかった

    async def get_funding_info_old(self, symbol: str) -> FundingInfo:
        """これは何をする関数？
        → Funding レートをまとめて返す（predicted は取得元により空もあり）。
           - CCXT fetchFundingRate を優先（fundingRate / nextFundingRate / nextFundingTimestamp）
           - 不足分はティッカーの 'fundingRate' / 'nextFundingTime' 等で補う（要API確認）
        """

        try:
            ccxt_sym, _kind = self._to_ccxt_symbol(symbol)
            current_rate = None
            predicted_rate = None
            next_time = None

            try:
                fr = await self._ccxt.fetch_funding_rate(ccxt_sym)
                current_rate = float(fr.get("fundingRate")) if fr.get("fundingRate") is not None else None
                predicted_rate = float(fr.get("nextFundingRate")) if fr.get("nextFundingRate") is not None else None
                nft = fr.get("nextFundingTimestamp")
                if nft is not None:
                    next_time = parse_exchange_ts(nft)
            except Exception:
                # フォールバック：ティッカー側
                t = await self._ccxt.fetch_ticker(ccxt_sym)
                info = t.get("info", {})
                if "fundingRate" in info:
                    current_rate = float(info.get("fundingRate"))
                # predictedFundingRate が取れるなら使う（未提供なら None）
                if "predictedFundingRate" in info:
                    predicted_rate = float(info.get("predictedFundingRate"))
                nft = info.get("nextFundingTime")
                if nft is not None:
                    next_time = parse_exchange_ts(nft)

            logger.info("bybit.get_funding.ok sym=%s", symbol)  # Funding取得成功の可視化
            return FundingInfo(
                symbol=symbol,
                current_rate=current_rate,
                predicted_rate=predicted_rate,
                next_funding_time=next_time,
            )
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"get_funding_info failed: {e}") from e

    # ---------- WebSocket 実装（Bybit v5） ----------

    async def subscribe_private(self, callbacks: dict[str, Callable[[dict], Awaitable[None]]]) -> None:
        """これは何をする関数？
        → Private WS に接続し、auth → {order, execution, position} を購読してメッセージを callback に流す。
        callback のキー例：
          - "order": async def _(msg: dict) -> None: ...
          - "execution": async def _(msg: dict) -> None: ...
          - "position": async def _(msg: dict) -> None: ...
        """

        url = self._ws_private
        try:
            async with websockets.connect(url, ping_interval=None) as ws:
                # --- 認証（docsのサンプルに基づく。署名は "GET/realtime" + expires(ms)） ---
                # ref: Bybit v5 Connect docs
                expires = int((time.time() + 60) * 1000)  # 60秒猶予
                payload = f"GET/realtime{expires}"
                signature = hmac.new(self._auth.api_secret.encode(), payload.encode(), sha256).hexdigest()
                auth = {"op": "auth", "args": [self._auth.api_key, expires, signature]}
                await ws.send(json.dumps(auth))
                auth_resp = json.loads(await ws.recv())
                if not (auth_resp.get("success") or auth_resp.get("op") == "auth"):
                    raise WsDisconnected(f"auth failed: {auth_resp}")

                # --- 購読 ---
                topics: list[str] = []
                for k in ("order", "execution", "position"):
                    if k in callbacks:
                        topics.append(k)
                if topics:
                    sub = {"op": "subscribe", "args": topics}
                    await ws.send(json.dumps(sub))

                # --- ping ループ & 受信ループをまとめて管理 ---
                async def _ping_loop() -> None:
                    while True:
                        try:
                            await asyncio.sleep(self._ws_ping_interval)
                            await ws.send(json.dumps({"op": "ping"}))
                        except Exception:
                            break

                async def _recv_loop() -> None:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        topic = msg.get("topic") or ""
                        cb = callbacks.get(topic)
                        if cb:
                            await cb(msg)

                try:
                    # これは何をする処理？→ ping と recv を並行実行し続ける
                    await asyncio.gather(_ping_loop(), _recv_loop())
                finally:
                    # これは何をする処理？→ タスクキャンセルや例外時でも必ずWSを閉じる
                    try:
                        await ws.close()
                    except Exception:
                        pass
        except Exception as e:  # noqa: BLE001
            raise WsDisconnected(f"private ws error: {e}") from e

    async def subscribe_public(
        self, symbols: list[str], callbacks: dict[str, Callable[[dict], Awaitable[None]]]
    ) -> None:
        """これは何をする関数？
        → Public WS（linear）に接続し、指定シンボルの publicTrade / orderbook.1 を購読します。
        callback のキー例：
          - "publicTrade": async def _(msg: dict) -> None: ...
          - "orderbook": async def _(msg: dict) -> None: ...
        """

        url = self._ws_public_linear
        args: list[str] = []
        for s in symbols:
            core = s.replace("_SPOT", "")
            args.append(f"publicTrade.{core}")
            # 必要に応じて板も
            if "orderbook" in callbacks:
                args.append(f"orderbook.1.{core}")

        try:
            async with websockets.connect(url, ping_interval=None) as ws:
                sub = {"op": "subscribe", "args": args}
                await ws.send(json.dumps(sub))

                async def _ping_loop() -> None:
                    while True:
                        try:
                            await asyncio.sleep(self._ws_ping_interval)
                            await ws.send(json.dumps({"op": "ping"}))
                        except Exception:
                            break

                async def _recv_loop() -> None:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        topic = msg.get("topic") or ""
                        if topic.startswith("publicTrade."):
                            cb = callbacks.get("publicTrade")
                            if cb:
                                await cb(msg)
                        elif topic.startswith("orderbook."):
                            cb = callbacks.get("orderbook")
                            if cb:
                                await cb(msg)

                await asyncio.gather(_ping_loop(), _recv_loop())
        except Exception as e:  # noqa: BLE001
            raise WsDisconnected(f"public ws error: {e}") from e

    async def close(self) -> None:
        """これは何をする関数？
        → ccxtの非同期クライアントや保持中のWebSocketをクローズし、接続リーク警告を防ぎます。
        """

        # ccxt async exchange（推定される属性名を広めに走査）
        for name in ("_ccxt", "ccxt", "_rest", "rest", "exchange", "_exchange"):
            ex = getattr(self, name, None)
            if ex is not None:
                close = getattr(ex, "close", None)
                if callable(close):
                    try:
                        await close()
                    except TypeError:
                        # 同期closeの可能性
                        close()
                    except Exception:
                        pass

        # WebSocketクライアントがあれば穏当に閉じる
        for name in ("_ws_public", "_ws_private", "ws_public", "ws_private"):
            ws = getattr(self, name, None)
            if ws is not None:
                with suppress(Exception):
                    await ws.close()

    @retryable()  # Funding取得時の一過性エラーに備える（数回だけ指数バックオフ再試行）
    async def get_funding_info(self, symbol: str) -> FundingInfo:
        """Bybit v5 から資金調達レートと次回時刻を取ってくる関数。
        logger.info("bybit.get_funding.call sym=%s", symbol)  # Funding取得開始の可視化
        まずは Ticker（早い・軽い）で取り、足りなければ履歴+仕様でやさしく補完する。
        """

        category = await self._resolve_category(
            symbol
        )  # {symbol: {"info": {...}, "ts": datetime}} に拡張（TTL付きキャッシュ）
        try:
            res = await self._rest(
                self._ccxt.publicGetV5MarketTickers, {"category": category, "symbol": symbol}
            )  # RESTは必ず共通ラッパ経由
            items = (res or {}).get("result", {}).get("list", []) or []
            data = items[0] if items else None
        except Exception:
            data = None

        predicted_rate: float | None = None
        next_dt: datetime | None = None
        interval_hours: int | None = None

        if data:
            raw_rate = data.get("fundingRate")
            raw_next = data.get("nextFundingTime")
            raw_interval_h = data.get("fundingIntervalHour")
            if raw_rate not in (None, ""):
                try:
                    predicted_rate = float(raw_rate)
                except Exception:
                    predicted_rate = None
            if raw_next not in (None, ""):
                try:
                    next_dt = parse_exchange_ts(raw_next)
                except Exception:
                    next_dt = None
            if raw_interval_h not in (None, ""):
                try:
                    interval_hours = int(raw_interval_h)
                except Exception:
                    interval_hours = None

        if (predicted_rate is None) or (next_dt is None) or (interval_hours is None):
            try:
                hist = await self._rest(
                    self._ccxt.publicGetV5MarketFundingHistory,
                    {"category": category, "symbol": symbol, "limit": 1},
                )  # セマフォで同時実行制御
                hlist = (hist or {}).get("result", {}).get("list", []) or []
                last = hlist[0] if hlist else None
            except Exception:
                last = None

            interval_minutes: int | None = None
            if interval_hours is None:
                try:
                    ins = await self._rest(
                        self._ccxt.publicGetV5MarketInstrumentsInfo,
                        {"category": category, "symbol": symbol},
                    )  # REST共通ラッパ
                    ilist = (ins or {}).get("result", {}).get("list", []) or []
                    info = ilist[0] if ilist else None
                    if info:
                        raw_interval_min = info.get("fundingInterval")
                        if raw_interval_min not in (None, ""):
                            interval_minutes = int(raw_interval_min)
                except Exception:
                    interval_minutes = None

                if (interval_minutes is None) and (interval_hours is not None):
                    interval_minutes = interval_hours * 60
            else:
                interval_minutes = interval_hours * 60

        if (predicted_rate is None) and last:
            lr = last.get("fundingRate")
            if lr not in (None, ""):
                try:
                    predicted_rate = float(lr)
                except Exception:
                    predicted_rate = None

            if (next_dt is None) and last and interval_minutes:
                ts_ms = last.get("fundingRateTimestamp")
                try:
                    settled_dt = parse_exchange_ts(ts_ms)
                    next_dt = settled_dt + timedelta(minutes=interval_minutes)
                except Exception:
                    next_dt = None

        # --- 次回時刻の最終確定（APIが欠損/過去でも必ず“未来”のスロットへ） ---
        final_interval_min = None
        if interval_hours is not None:
            final_interval_min = interval_hours * 60
        elif "interval_minutes" in locals() and interval_minutes:
            final_interval_min = interval_minutes
        else:
            final_interval_min = 480  # 取れない場合は8時間を既定に

        next_dt = self._compute_next_funding_time(final_interval_min, next_dt)  # 未来の次回Funding時刻を確定

        if (predicted_rate is None) or (next_dt is None):
            raise DataError(f"Bybit funding info missing: symbol={symbol}, rate={predicted_rate}, next={next_dt}")

        logger.info("bybit.get_funding.ok sym=%s", symbol)  # Funding取得成功の可視化
        return FundingInfo(
            symbol=symbol,
            current_rate=None,
            predicted_rate=predicted_rate,
            next_funding_time=next_dt,
            funding_interval_hours=interval_hours,
        )

    @retryable()  # 取消の一過性失敗に対して自動再試行（安全に諦めず数回だけ挑戦）
    async def cancel_order_legacy(
        self, symbol: str, order_id: str | None = None, client_order_id: str | None = None
    ) -> None:
        """Bybit v5 の注文取消。
        - orderId があればそれを使う
        - なければ client_order_id（= orderLinkId）で確実に取り消す
        """

        category = await self._resolve_category(
            symbol
        )  # {symbol: {"info": {...}, "ts": datetime}} に拡張（TTL付きキャッシュ）
        try:
            params: dict[str, Any] = {"category": category, "symbol": symbol}
            if order_id:
                params["orderId"] = order_id
            elif client_order_id:
                params["orderLinkId"] = client_order_id
            else:
                raise DataError(f"cancel_order requires order_id or client_order_id: symbol={symbol}")

            await self._rest(self._ccxt.privatePostV5OrderCancel, params)  # 取消も共通ラッパ経由
        except ccxt.RateLimitExceeded as e:
            raise RateLimitError(str(e)) from e
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"cancel_order failed: {e}") from e

    async def _resolve_category(self, symbol: str) -> str:
        """Bybit v5 instruments-infoを用いて spot/linear/inverse を判定しキャッシュする。"""

        cached = self._symbol_category_cache.get(symbol)
        if cached:
            return cached

        # internal spot symbol may end with _SPOT; Bybit v5 APIs expect core symbol like 'BTCUSDT'
        _api_symbol = symbol[:-5] if symbol.endswith("_SPOT") else symbol

        for cat_try in ("linear", "inverse", "spot"):
            try:
                res = await self._rest(
                    self._ccxt.publicGetV5MarketInstrumentsInfo,
                    {"category": cat_try, "symbol": _api_symbol},
                )
                items = (res or {}).get("result", {}).get("list", []) or []
                if items:
                    self._symbol_category_cache[symbol] = cat_try
                    return cat_try
            except Exception:
                continue

        fallback = "linear" if _api_symbol.endswith(("USDT", "USDC")) else "inverse"
        self._symbol_category_cache[symbol] = fallback
        return fallback

    @staticmethod
    def _dec_to_str(d: Decimal) -> str:
        """Decimalを指数表記なしの文字列にする（RESTに渡しやすくする）"""
        return format(d.normalize(), "f")

    @staticmethod
    def _quantize_step(value: Decimal, step: Decimal) -> Decimal:
        """valueをstepの倍数に下方向（安全側）で丸める。取引所ステップを超えないようにする。"""
        if step <= 0:
            return value
        return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

    async def _get_instrument_info(self, symbol: str) -> dict:
        """Bybit v5 instruments-infoから、この銘柄のtickSize/qtyStep/最小最大を取得しキャッシュして返す。"""

        cache = self._instrument_info_cache.get(symbol)
        now = utc_now()
        if cache:
            ts = cache.get("ts")
            if isinstance(ts, datetime):
                age = (now - ts).total_seconds()
                if age <= getattr(self, "_instrument_info_ttl_s", 300):
                    return cache.get("info") or cache

        category = await self._resolve_category(symbol)
        try:
            api_symbol = symbol[:-5] if symbol.endswith("_SPOT") else symbol
            res = await self._rest(
                self._ccxt.publicGetV5MarketInstrumentsInfo,
                {"category": category, "symbol": api_symbol},
            )
            items = (res or {}).get("result", {}).get("list", []) or []
            if not items:
                raise DataError(f"instruments-info not found: symbol={symbol}")
            info_raw = items[0]
            price_filter = info_raw.get("priceFilter", {}) or {}
            lot_filter = info_raw.get("lotSizeFilter", {}) or {}
            info = {
                "tick_size": price_filter.get("tickSize"),
                "min_price": price_filter.get("minPrice"),
                "max_price": price_filter.get("maxPrice"),
                "qty_step": lot_filter.get("qtyStep"),
                "min_order_qty": lot_filter.get("minOrderQty"),
                "max_order_qty": lot_filter.get("maxOrderQty"),
            }
            self._instrument_info_cache[symbol] = {"info": info, "ts": now}
            return info
        except Exception:
            if cache:
                return cache.get("info") or cache
            raise

    async def _normalize_price_qty(
        self,
        symbol: str,
        side: str,
        price: object | None,
        qty: object,
        order_type: str,
    ) -> tuple[str | None, str]:
        """価格と数量をBybitのtickSize/qtyStepに合わせて丸め、範囲外は例外にする。
        戻り値はRESTに渡す文字列（指数表記なし）。priceはLimit時のみ返る。
        """

        info = await self._get_instrument_info(symbol)

        # 数量
        dqty = Decimal(str(qty))
        step = Decimal(str(info["qty_step"])) if info.get("qty_step") not in (None, "") else None
        if step is not None:
            dqty = self._quantize_step(dqty, step)
        min_q = Decimal(str(info["min_order_qty"])) if info.get("min_order_qty") not in (None, "") else None
        max_q = Decimal(str(info["max_order_qty"])) if info.get("max_order_qty") not in (None, "") else None
        if min_q and dqty < min_q:
            raise DataError(f"qty below minOrderQty: {dqty} < {min_q} ({symbol})")
        if max_q and dqty > max_q:
            dqty = self._quantize_step(max_q, step or Decimal("1"))
        if dqty <= 0:
            raise DataError(f"qty rounded to zero for {symbol}")
        qty_str = self._dec_to_str(dqty)

        # 価格（Limit時のみ）
        price_str: str | None = None
        if (order_type or "").lower().startswith("limit") and price is not None:
            dpx = Decimal(str(price))
            tick = Decimal(str(info["tick_size"])) if info.get("tick_size") not in (None, "") else None
            if tick is not None:
                dpx = self._quantize_step(dpx, tick)
            min_p = Decimal(str(info["min_price"])) if info.get("min_price") not in (None, "") else None
            max_p = Decimal(str(info["max_price"])) if info.get("max_price") not in (None, "") else None
            if min_p and dpx < min_p:
                dpx = min_p
                if tick is not None:
                    dpx = self._quantize_step(dpx, tick)
            if max_p and dpx > max_p:
                dpx = self._quantize_step(max_p, tick or Decimal("1"))
            price_str = self._dec_to_str(dpx)

        return price_str, qty_str

    async def _ensure_post_only_price(self, symbol: str, side: str, price_str: str) -> str:
        """PostOnly時に価格を非クロスへ微調整する。
        - BUY: best_ask - 1tick 以下へ
        - SELL: best_bid + 1tick 以上へ
        その後、tick に合わせて（安全側へ）丸め直して返す。
        取得失敗などの場合は入力価格をそのまま返す。
        """
        orig_px = Decimal(str(price_str))  # ログのため、補正前の価格を覚える
        # WSキャッシュのBBO優先、なければREST
        bid_str, ask_str = await self._get_bbo_if_fresh(symbol)  # 鮮度ガード付きBBO取得（古ければRESTで補う）
        if not bid_str and not ask_str:
            return price_str  # BBOが無ければ調整しない

        try:
            info = await self._get_instrument_info(symbol)
            tick = info.get("tick_size")
        except Exception:
            tick = None
        if not tick:
            return price_str

        tick_d = Decimal(str(tick))
        px = Decimal(str(price_str))
        side_u = (side or "").upper()

        if side_u == "BUY" and ask_str:
            try:
                ask = Decimal(str(ask_str))
                target = ask - tick_d
                if px > target:
                    px = target
            except Exception:
                pass
        elif side_u == "SELL" and bid_str:
            try:
                bid = Decimal(str(bid_str))
                target = bid + tick_d
                if px < target:
                    px = target
            except Exception:
                pass

        px = self._quantize_step(px, tick_d)
        try:
            changed = orig_px != px
        except Exception:
            changed = False
        if changed:
            try:
                self._log.debug(
                    "postonly.adjust symbol=%s side=%s from=%s to=%s",
                    symbol,
                    side,
                    self._dec_to_str(orig_px),
                    self._dec_to_str(px),
                )
            except Exception:
                pass
        return self._dec_to_str(px)

    async def _guard_price_deviation(self, symbol: str, side: str, price_str: str) -> None:
        """BBO中値からの乖離[bps]が上限を超えたら RiskBreach を投げて送信を止める（安全のための見張り）"""
        bid_str, ask_str = await self._get_bbo_if_fresh(symbol)
        if not bid_str or not ask_str:
            return  # BBOが無ければスキップ
        try:
            mid = (Decimal(str(bid_str)) + Decimal(str(ask_str))) / Decimal("2")
            if mid <= 0:
                return
            px = Decimal(str(price_str))
            bps = (abs(px - mid) / mid) * Decimal("10000")
            limit = Decimal(str(self._price_dev_bps_limit)) if self._price_dev_bps_limit is not None else None
            if (limit is not None) and (bps > limit):
                try:
                    self._log.warning(
                        "guard.price_deviation symbol=%s side=%s bps=%s limit=%s price=%s",
                        symbol,
                        side,
                        str(bps),
                        str(limit),
                        price_str,
                    )
                except Exception:
                    pass
                raise RiskBreach(f"price deviation {bps}bps exceeds {limit}bps (symbol={symbol}, side={side})")
        except Exception:
            # 価格計算に失敗しても送信は阻害しない（安全側にスキップ）
            return

    def update_bbo(
        self, symbol: str, bid: str | float | None, ask: str | float | None, ts: int | str | None = None
    ) -> None:
        """公開WSから届いた最良気配をキャッシュする（価格は文字列でも数値でもOK）。"""
        bid_val = self._apply_price_scale(symbol, bid)
        ask_val = self._apply_price_scale(symbol, ask)
        self._bbo_cache[symbol] = {"bid": bid_val, "ask": ask_val, "ts": ts}

    async def get_bbo(self, symbol: str) -> tuple[str | None, str | None]:
        """キャッシュ済みBBOを返し、無ければRESTのTickerで補う（bid, ask）。"""
        cached = self._bbo_cache.get(symbol)
        if cached and (cached.get("bid") is not None or cached.get("ask") is not None):
            bid = cached.get("bid")
            ask = cached.get("ask")
            return (str(bid) if bid is not None else None, str(ask) if ask is not None else None)

        # フォールバック：RESTのTicker
        category = await self._resolve_category(symbol)
        api_symbol = symbol[:-5] if symbol.endswith("_SPOT") else symbol
        try:
            res = await self._rest(
                self._ccxt.publicGetV5MarketTickers, {"category": category, "symbol": api_symbol}
            )  # BBOのRESTフォールバックも統一
            item = ((res or {}).get("result", {}) or {}).get("list", [])[:1]
            row = item[0] if item else {}
            bid = self._apply_price_scale(symbol, row.get("bid1Price"))
            ask = self._apply_price_scale(symbol, row.get("ask1Price"))
            if bid is not None or ask is not None:
                self._bbo_cache[symbol] = {"bid": bid, "ask": ask, "ts": row.get("ts")}
            return (str(bid) if bid is not None else None, str(ask) if ask is not None else None)
        except Exception:
            return (None, None)

    def _compute_next_funding_time(self, interval_minutes: int | None, api_next: object | None):
        """次回Funding時刻を安全に確定する関数。
        1) APIのnextFundingTimeが「未来」ならそれを採用
        2) 欠損や「過去」の場合は、UTCの00:00を起点に interval_minutes ごとのスロットへ切り上げ
        """

        now = utc_now()  # いまのUTC
        # API値をdatetimeへ（ms/秒/ISO/naiveの揺れはparse_exchange_tsが吸収）
        dt_api = None
        if api_next not in (None, ""):
            try:
                dt_api = api_next if isinstance(api_next, datetime) else parse_exchange_ts(api_next)
            except Exception:
                dt_api = None

        # 1) 未来のAPI値はそのまま使う
        if dt_api and dt_api > now:
            return dt_api

        # 2) 欠損/過去ならスロット切り上げ（既定=480分=8h）
        iv = int(interval_minutes or 480)
        # その日のUTC 00:00 をアンカーに、iv分の倍数の“次の”スロットへ
        anchor = datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=timezone.utc)
        elapsed_s = (now - anchor).total_seconds()
        slot_s = iv * 60
        k = int(elapsed_s // slot_s) + 1  # “次の”スロット（境界ちょうどの時も次へ送る）
        return anchor + timedelta(seconds=slot_s * k)

    async def _get_bbo_if_fresh(self, symbol: str) -> tuple[str | None, str | None]:
        """WSキャッシュのBBOが新しいときだけ返し、古いときはRESTのTickerで補う。"""
        cached = self._bbo_cache.get(symbol)
        if cached:
            ts_raw = cached.get("ts")
            try:
                ts_dt = parse_exchange_ts(ts_raw)
                age_ms = int((utc_now() - ts_dt).total_seconds() * 1000)
            except Exception:
                age_ms = None

            if (age_ms is not None) and (age_ms <= self._bbo_max_age_ms):
                bid = cached.get("bid")
                ask = cached.get("ask")
                return (str(bid) if bid is not None else None, str(ask) if ask is not None else None)

        # 古い/時刻不明ならRESTで補う
        category = await self._resolve_category(symbol)
        api_symbol = symbol[:-5] if symbol.endswith("_SPOT") else symbol
        try:
            res = await self._rest(
                self._ccxt.publicGetV5MarketTickers, {"category": category, "symbol": api_symbol}
            )  # 鮮度ガードのREST補完も共通
            item = ((res or {}).get("result", {}) or {}).get("list", [])[:1]
            row = item[0] if item else {}
            bid = self._apply_price_scale(symbol, row.get("bid1Price"))
            ask = self._apply_price_scale(symbol, row.get("ask1Price"))
            if bid is not None or ask is not None:
                self._bbo_cache[symbol] = {"bid": bid, "ask": ask, "ts": row.get("ts")}
            return (str(bid) if bid is not None else None, str(ask) if ask is not None else None)
        except Exception:
            return (None, None)

    async def _find_existing_order(self, symbol: str, client_order_id: str | None) -> dict | None:
        """orderLinkIdで既存の注文をBybit v5 /v5/order/realtimeから一件だけ探す。無ければNone。
        ネット揺れやduplicate orderLinkIdエラー時の冪等フォールバックに使用。
        """
        if not client_order_id:
            return None
        category = await self._resolve_category(symbol)
        api_symbol = symbol[:-5] if symbol.endswith("_SPOT") else symbol
        try:
            res = await self._rest(
                self._ccxt.privateGetV5OrderRealtime,
                {
                    "category": category,
                    "symbol": api_symbol,
                    "orderLinkId": client_order_id,
                },
            )  # 既存照会もラッパ経由
            items = (res or {}).get("result", {}).get("list", []) or []
            if items:
                try:
                    self._log.info(
                        "idempotent.create.found symbol=%s orderLinkId=%s orderId=%s",
                        symbol,
                        client_order_id,
                        items[0].get("orderId"),
                    )
                except Exception:
                    pass
            return items[0] if items else None
        except Exception:
            return None

    async def _is_order_still_open(self, symbol: str, order_id: str | None, client_order_id: str | None) -> bool:
        """Bybit v5 /v5/order/realtime で、対象注文がまだ open かどうかを確認する。
        - orderId があればそれを最優先で検索、なければ orderLinkId（client_order_id）で検索
        - 0 件なら open ではない（＝既に Filled/Cancelled 等）
        - 失敗時は安全側に True（まだ open かも）
        """
        category = await self._resolve_category(symbol)
        api_symbol = symbol[:-5] if symbol.endswith("_SPOT") else symbol
        params: dict[str, object] = {"category": category, "symbol": api_symbol}
        if order_id:
            params["orderId"] = order_id
        elif client_order_id:
            params["orderLinkId"] = client_order_id
        else:
            # 識別子が無ければ open ではない扱い（冪等化のため）
            return False
        try:
            res = await self._rest(self._ccxt.privateGetV5OrderRealtime, params)  # open判定も共通ラッパ
            items = (res or {}).get("result", {}).get("list", []) or []
            return bool(items)
        except Exception:
            return True

    @retryable()
    async def cancel_order(self, symbol: str, order_id: str | None = None, client_order_id: str | None = None) -> None:
        """Bybit v5 取消の冪等化対応。
        - 通常は /v5/order/cancel を呼ぶ
        - 失敗時（not found/duplicate/ネット断など）は /v5/order/realtime で open を確認し、残っていなければ成功扱い
        """

        category = await self._resolve_category(symbol)
        params: dict[str, Any] = {"category": category, "symbol": symbol}
        if order_id:
            params["orderId"] = order_id
        elif client_order_id:
            params["orderLinkId"] = client_order_id
        else:
            raise DataError(f"cancel_order requires order_id or client_order_id: symbol={symbol}")

        try:
            await self._rest(self._ccxt.privatePostV5OrderCancel, params)  # 取消も共通ラッパ経由
            return
        except Exception as e:  # noqa: BLE001
            try:
                still_open = await self._is_order_still_open(symbol, order_id, client_order_id)
            except Exception:
                still_open = True
            if not still_open:
                try:
                    self._log.info(
                        "idempotent.cancel.already_closed symbol=%s order_id=%s client_order_id=%s",
                        symbol,
                        order_id,
                        client_order_id,
                    )
                except Exception:
                    pass
                return
            if isinstance(e, ccxt.RateLimitExceeded):
                raise RateLimitError(str(e)) from e
            raise ExchangeError(f"cancel_order failed: {e}") from e

    @retryable()  # 一過性エラーに強くする（STEP19）
    async def amend_order(
        self,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
        new_price: object | None = None,
        new_qty: object | None = None,
        side: str | None = None,
        post_only: bool | None = None,
        time_in_force: str | None = None,
    ) -> None:
        """Bybit v5 /v5/order/amend で既存注文を安全に修正する。
        - price/qtyはtickSize/qtyStepへ丸め（STEP21）
        - PostOnlyならBBO基準で非クロスに微調整（STEP24/25/28）
        - IDはorderId優先、無ければorderLinkId（=client_order_id）
        """

        if not any([new_price is not None, new_qty is not None, post_only is not None, time_in_force]):
            return  # 変更点なしなら何もしない

        category = await self._resolve_category(symbol)  # 正しいカテゴリを特定（STEP20）

        # sideが無い時は既存注文から補う（可能なら）
        if side is None:
            existing = None
            if client_order_id:
                existing = await self._find_existing_order(symbol, client_order_id)  # STEP26のヘルパー
            if (existing is None) and order_id:
                try:
                    res = await self._rest(
                        self._ccxt.privateGetV5OrderRealtime,
                        {"category": category, "symbol": symbol, "orderId": order_id},
                    )
                    items = (res or {}).get("result", {}).get("list", []) or []
                    existing = items[0] if items else None
                except Exception:
                    existing = None
            if existing:
                side = existing.get("side")

        params: dict[str, Any] = {"category": category, "symbol": symbol}
        if order_id:
            params["orderId"] = order_id
        if client_order_id:
            params["orderLinkId"] = client_order_id

        # instruments-info（tick/step/min/max）を取得（STEP21）
        info = await self._get_instrument_info(symbol)

        # --- 数量の修正（ある時だけ） ---
        if new_qty is not None:
            dqty = Decimal(str(new_qty))
            step = Decimal(str(info["qty_step"])) if info.get("qty_step") not in (None, "") else None
            if step is not None:
                dqty = self._quantize_step(dqty, step)
            min_q = Decimal(str(info["min_order_qty"])) if info.get("min_order_qty") not in (None, "") else None
            max_q = Decimal(str(info["max_order_qty"])) if info.get("max_order_qty") not in (None, "") else None
            if min_q and dqty < min_q:
                raise DataError(f"amend qty below minOrderQty: {dqty} < {min_q} ({symbol})")
            if max_q and dqty > max_q:
                dqty = self._quantize_step(max_q, step or Decimal("1"))
            if dqty <= 0:
                raise DataError(f"amend qty rounded to zero for {symbol}")
            params["qty"] = self._dec_to_str(dqty)

        # --- 価格の修正（ある時だけ） ---
        if new_price is not None:
            dpx = Decimal(str(new_price))
            tick = Decimal(str(info["tick_size"])) if info.get("tick_size") not in (None, "") else None
            if tick is not None:
                dpx = self._quantize_step(dpx, tick)
            min_p = Decimal(str(info["min_price"])) if info.get("min_price") not in (None, "") else None
            max_p = Decimal(str(info["max_price"])) if info.get("max_price") not in (None, "") else None
            if min_p and dpx < min_p:
                dpx = self._quantize_step(min_p, tick or Decimal("1"))
            if max_p and dpx > max_p:
                dpx = self._quantize_step(max_p, tick or Decimal("1"))
            price_str = self._dec_to_str(dpx)

            # PostOnlyなら非クロスに微調整（BBOキャッシュ優先、鮮度ガード付き）
            is_post_only = bool(post_only) or (time_in_force == "PostOnly")
            if is_post_only and (side is not None):
                price_str = await self._ensure_post_only_price(symbol, side, price_str)
            params["price"] = price_str

        # --- TIF/PostOnly の適用（任意） ---
        if post_only:
            params["timeInForce"] = "PostOnly"
        elif time_in_force:
            params["timeInForce"] = time_in_force

        # 価格乖離ガード（BBO中値からの最大乖離[bps]を超える場合はブロック）
        if params.get("price") and (self._price_dev_bps_limit is not None):
            await self._guard_price_deviation(symbol, side or "", params["price"])  # 修正価格の乖離を安全確認

        # 実行（RESTは統一ラッパ経由・セマフォ/サーキット付き：STEP29/31）
        await self._rest(self._ccxt.privatePostV5OrderAmend, params)
