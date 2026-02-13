# これは「注文のライフサイクルを安全に管理するOMSエンジン」を実装するファイルです。
from __future__ import annotations

import asyncio  # ドレイン中に小さく待つためのスリープで使う
import logging  # 構造化ログ（運用監査）用にloggerを使う
import math  # 数量のステップ丸め（安全側の切り下げ）に使う
import os
import random
import time
from collections import deque
from decimal import Decimal  # 価格のズレをbpsで計算するために使用
from typing import (
    Any,
    Dict,  # クールダウン解除(出口)ログ用の状態フラグ型
    Optional,
    cast,
)

from loguru import logger

from bot.analytics.trip_aggregator import RoundTripAggregator
from bot.core.errors import (
    ExchangeError,
    RiskBreach,  # 二重発注を止めるための制御用エラー（リスク違反として扱う）
    WsDisconnected,  # WSが古いときに新規発注をブロックするための例外
)
from bot.core.time import (
    parse_exchange_ts,  # 取引所/WSの時刻表現をUTC awareに直すため（順序判定で使う）
    utc_now,
)
from bot.data.repo import Repo
from bot.exchanges.base import ExchangeGateway
from bot.exchanges.types import Order, OrderRequest
from bot.tools.jsonl_sink import append_jsonl_daily

from .types import ManagedOrder, OmsConfig, OrderLifecycleState

_USD_EQUIV_QUOTES = {"USDT", "USDC", "USD"}


def _guess_base_quote(symbol: str) -> tuple[str, str]:
    """Best-effort split of symbols like BTCUSDT or ETHUSDC into (base, quote)."""

    sym = symbol.replace("_SPOT", "").upper()
    for quote in ("USDT", "USDC", "USDD", "USD", "BTC", "ETH"):
        if sym.endswith(quote):
            return sym[: -len(quote)], quote
    mid = max(3, len(sym) // 2)
    return sym[:mid], sym[mid:]


def _fee_to_usdt(
    *,
    fee: object | None,
    fee_currency: object | None,
    symbol: str,
    price: float,
) -> float | None:
    """Convert fee to USDT if possible using trade price."""

    if fee in (None, "", 0, "0"):
        return None
    try:
        fee_val = float(str(fee))
    except Exception:
        return None
    cur = str(fee_currency or "").upper()
    if not cur:
        return None

    base, quote = _guess_base_quote(symbol)
    if cur in _USD_EQUIV_QUOTES:
        return fee_val
    if cur == quote:
        return fee_val
    if cur == base and price:
        return fee_val * float(price)
    return None


class OmsEngine:
    """注文管理（状態機械）を提供するエンジン。
    - submit(): 新規発注
    - cancel(): 取消
    - submit_hedge(): ネットデルタを埋める成行IOC
    - on_execution_event(): 約定/状態更新の取り込み
    - process_timeouts(): タイムアウト監視（テストから明示呼び出し）
    """

    def __init__(self, ex: ExchangeGateway, repo: Repo | None, cfg: OmsConfig | None = None) -> None:
        """これは何をする関数？
        → ExchangeGateway/Repo/設定を受け取り、注文追跡テーブルを用意します。
        """

        self._ex = ex
        self._repo = repo
        self._cfg = cfg or OmsConfig()
        self._log = logging.getLogger(__name__)  # このOMSの監査ログ出力口
        self._run_id = os.environ.get("RUN_ID")
        self._strategy_name = os.environ.get("STRATEGY_NAME")
        self._orders: dict[str, ManagedOrder] = {}  # key: client_id
        # WS ライブネスのメモとブロックしきい値（デフォルト値は安全側）
        self._ws_private_last_ms: int = 0
        self._ws_stale_block_ms: int = 10_000
        # REJECT連発→シンボル別クールダウン（デフォルト値、runnerで上書き）
        self._reject_burst_threshold: int = 3
        self._reject_window_ms: int = 30_000
        self._symbol_cooldown_ms: int = 120_000
        self._reject_window: dict[str, tuple[int, int]] = {}
        self._symbol_cooldown_until: dict[str, int] = {}
        self._cooldown_active: Dict[str, bool] = (
            {}
        )  # 何をする？→ 現在クールダウン中かのフラグ（exitを一度だけ出すため）
        # PostOnly追従（アメンド）の頻度制御メモ
        self._last_amend_ms: dict[str, int] = {}
        self._amend_count_minute: dict[str, tuple[int, int]] = {}
        self._orders_jsonl: str = "logs/orders.jsonl"
        self._trades_jsonl: str = "logs/trades.jsonl"
        self._round_trips_jsonl: str = "logs/round_trips.jsonl"
        self._trip_agg: RoundTripAggregator = RoundTripAggregator(self._round_trips_jsonl)
        # 制約取得不全などの危険状態に入ったら新規発注を止める（CLOSE系は許容して退避を試みる）
        self._safe_mode: bool = False
        self._safe_mode_reason: str = ""
        # 制約（minQty/minNotional/qtyStep）の取得を連打しないためのバックオフ（シンボル単位）
        self._constraints_prime_backoff_ms: int = 30_000
        self._constraints_prime_next_ms: dict[str, int] = {}
        # ===== メトリクス（集計用） =====
        self._metrics_chase_amend_total: dict[str, int] = {}
        self._metrics_cooldown_enter_total: dict[str, int] = {}
        self._metrics_chase_amend_period: dict[str, int] = {}
        self._metrics_cooldown_enter_period: dict[str, int] = {}

        # 起動時に1回だけBitget認証を確認し、結果を保持する（監視のみ運転の判定に使う）
        self.auth_ok: bool = True
        self.auth_message: str = ""
        try:
            check = getattr(self._ex, "check_auth", None)
            if callable(check):
                ok, msg = check()
                self.auth_ok = bool(ok)
                self.auth_message = str(msg or "")
                if not self.auth_ok:
                    logging.getLogger(__name__).warning("auth.preflight.failed: %s", self.auth_message)
        except Exception as e:  # 例外はネットワーク/署名/時刻ずれ等を含む
            self.auth_ok = False
            self.auth_message = f"exception={type(e).__name__}: {e}"
            logging.getLogger(__name__).warning("auth.preflight.error: %s", self.auth_message)
        self._last_event_ms: dict[str, int] = (
            {}
        )  # 注文ごとの最終更新時刻（ms）を覚えて、古いWSイベントを無視するためのメモ
        self._inflight_client_ids: set[str] = set()  # 送信済みのclient注文IDを記録して二重発注を防ぐメモ帳
        # execution 冪等性メモ（WSで同一約定が二重に飛ぶケースの対策）
        self._processed_exec_ids: set[str] = set()
        self._processed_exec_ids_fifo: deque[str] = deque()
        self._processed_exec_ids_max: int = 50_000

    # ---------- 内部: client_id 生成 ----------

    def _gen_client_id(self, prefix: str = "oms") -> str:
        """これは何をする関数？
        → 時刻(ms)＋ナンスから、衝突しにくい client_id を生成します。
        """

        ms = int(time.time() * 1000)
        nonce = random.randint(1000, 9999)
        return f"{prefix}-{ms}-{nonce}"

    def _enter_safe_mode(self, reason: str) -> None:
        """これは何をする関数？
        → 危険状態に入ったため、新規発注を止めるためのフラグを立てます（CLOSE系は可能なら継続）。
        """

        if self._safe_mode:
            return
        self._safe_mode = True
        self._safe_mode_reason = str(reason or "")
        logging.getLogger(__name__).error("oms.safe_mode.enter reason=%s", self._safe_mode_reason)

    async def _prime_scale_cache_if_possible(self, symbol: str, *, force: bool = False) -> bool:
        """これは何をする関数？
        → 取引所メタ（minQty/minNotional/qtyStep など）が未取得なら、取得できる範囲で温めます。
        """

        core = symbol.replace("_SPOT", "")
        now_ms = int(utc_now().timestamp() * 1000)
        next_ms = self._constraints_prime_next_ms.get(core)
        if (not force) and next_ms and now_ms < next_ms:
            return False
        # 候補: 直接のゲートウェイ / PaperExchange の下にある data_source
        candidates: list[Any] = [self._ex]
        data_ex = getattr(self._ex, "_data", None)
        if data_ex is not None:
            candidates.append(data_ex)

        ok = False
        for ex in candidates:
            prime = getattr(ex, "_prime_scale_from_markets", None)
            if not callable(prime):
                continue
            try:
                res = prime(core)
                if asyncio.iscoroutine(res):
                    await res
                ok = True
            except Exception:
                continue
        if not ok:
            # 取得に失敗した場合は、短時間の連打を避ける（ただし CLOSE系は force=True で上書き可能）
            self._constraints_prime_next_ms[core] = now_ms + self._constraints_prime_backoff_ms
        else:
            # 成功したらバックオフを解除
            self._constraints_prime_next_ms.pop(core, None)
        return ok

    async def warmup_order_constraints(self, symbols: list[str]) -> None:
        """これは何をする関数？
        → 起動時に対象シンボルの制約（minQty/minNotional/qtyStep）を温め、constraints_missing を減らします。
        """

        cores = sorted({str(s).replace("_SPOT", "") for s in (symbols or []) if s})
        if not cores:
            return
        logger.info("oms.constraints.warmup.start cores={}", cores)
        for core in cores:
            await self._prime_scale_cache_if_possible(core, force=True)
        logger.info("oms.constraints.warmup.done cores={}", cores)

    async def _get_order_constraints(self, symbol: str) -> dict[str, float] | None:
        """これは何をする関数？
        → 発注サイズの正規化/最小判定に必要な制約（minQty/minNotional/qtyStep）を返します。
        - *_SPOT は spot 側の制約を優先します
        - キャッシュが空なら、可能な範囲で prime を試みます
        """

        core = symbol.replace("_SPOT", "")
        is_spot = symbol.endswith("_SPOT")

        def _lookup(ex: Any) -> dict[str, float] | None:
            cache = getattr(ex, "_scale_cache", None)
            if not isinstance(cache, dict):
                return None
            info = cache.get(core) or cache.get(symbol)
            if not isinstance(info, dict):
                return None
            out: dict[str, float] = {}
            try:
                if is_spot:
                    out["min_qty"] = float(info.get("minQty_spot") or 0.0)
                    out["qty_step"] = float(info.get("qtyStep_spot") or 0.0)
                    out["min_notional"] = float(info.get("minNotional_spot") or 0.0)
                else:
                    out["min_qty"] = float(info.get("minQty_perp") or 0.0)
                    out["qty_step"] = float(info.get("qtyStep_perp") or 0.0)
                    out["min_notional"] = float(info.get("minNotional_perp") or 0.0)
                # qty_step が無い場合は min_qty をステップとして使う（最小刻みが minQty と一致する取引所が多い）
                if out.get("qty_step", 0.0) <= 0.0 and out.get("min_qty", 0.0) > 0.0:
                    out["qty_step"] = float(out["min_qty"])
            except Exception:
                return None
            return out

        info = _lookup(self._ex)
        if info is None:
            data_ex = getattr(self._ex, "_data", None)
            if data_ex is not None:
                info = _lookup(data_ex)

        if info is None:
            await self._prime_scale_cache_if_possible(symbol, force=False)
            info = _lookup(self._ex)
            if info is None:
                data_ex = getattr(self._ex, "_data", None)
                if data_ex is not None:
                    info = _lookup(data_ex)

        if not info:
            return None

        # どれも取れない場合は「制約不明」として None（fail-closed でブロック）
        if float(info.get("min_qty", 0.0)) <= 0.0 and float(info.get("min_notional", 0.0)) <= 0.0:
            return None

        return info

    def touch_private_ws(self, ts: object | None = None) -> None:
        """Private WSを受け取った合図。取引所形式のtsでも、そのままNoneでもOK。内部でUTC msに正規化して保存する。"""
        try:
            dt = utc_now() if ts in (None, "") else parse_exchange_ts(ts)  # 取引所/WSの時刻表現をUTC awareに変換
            self._ws_private_last_ms = int(dt.timestamp() * 1000)
        except Exception:
            # 時刻が読めなくても、最低限「いま来た」ことは記録する
            self._ws_private_last_ms = int(utc_now().timestamp() * 1000)

    # ---------- 発注/取消API ----------

    async def submit(self, req: OrderRequest, *, meta: dict[str, Any] | None = None) -> Optional[Order]:
        """これは何をする関数？
        → 注文を取引所へ発注し、OMSの追跡に登録します（idempotencyのためclient_id必須）。
        """

        # Bitget の client order id（orderLinkId）を未指定ならここで採番
        try:
            import uuid

            if getattr(req, "client_order_id", None) in (None, ""):
                req.client_order_id = f"bot-{uuid.uuid4().hex}"
        except Exception:
            pass

        # --- 構造化トレードログ（fillsを1行で記録） ---

        coid = req.client_order_id or self._gen_client_id("bot")
        req.client_order_id = coid
        meta_payload: dict[str, Any] = dict(meta or {})
        # meta 未指定でも最低限の intent を推定してログへ残す（open/close の切り分け用）
        meta_payload.setdefault("intent", "close" if getattr(req, "reduce_only", False) else "open")
        meta_payload.setdefault("source", "strategy")
        intent = str(meta_payload.get("intent") or "")
        is_close_like = intent in {"close", "unwind", "flatten"} or bool(getattr(req, "reduce_only", False))
        # SAFEモード中は新規建て/リバランスをブロック（CLOSE系は許容して退避を試みる）
        if self._safe_mode and not is_close_like:
            try:
                rec = {
                    "event": "order_skip",
                    "ts": utc_now().isoformat(),
                    "symbol": req.symbol,
                    "side": req.side,
                    "type": req.type,
                    "qty": float(req.qty or 0.0),
                    "price": req.price,
                    "status": "skipped",
                    "client_id": coid,
                    "reason": "safe_mode",
                    "safe_mode_reason": self._safe_mode_reason,
                }
                for k, v in meta_payload.items():
                    if v is None or k in rec:
                        continue
                    rec[k] = v
                append_jsonl_daily("logs", "orders", rec)
            except Exception:
                pass
            logger.warning("order.skip: safe_mode sym={} intent={} cid={}", req.symbol, intent, coid)
            return None
        if coid in self._inflight_client_ids:
            raise RiskBreach(f"duplicate client_order_id (idempotent submit): {coid}")  # 同じIDでの二重発注をブロック
        # Private WS ライブネス・ガード：直近の受信が古すぎれば新規発注はブロック
        now_ms = int(utc_now().timestamp() * 1000)
        if getattr(self, "_ws_private_last_ms", 0) and (now_ms - self._ws_private_last_ms) > getattr(
            self, "_ws_stale_block_ms", 10_000
        ):
            raise WsDisconnected(
                "private WS stale: "
                f"{(now_ms - self._ws_private_last_ms)}ms > "
                f"{getattr(self, '_ws_stale_block_ms', 10000)}ms"
            )
        # シンボル別クールダウン（REJECTEDの連発があった直後は新規発注を停止）
        cool_until = self._symbol_cooldown_until.get(req.symbol)
        # CLOSE系は「閉じられない」のが危険なので、クールダウン中でも通す（新規建て/増し玉だけ止める）
        if cool_until and now_ms < cool_until and (not is_close_like):
            remaining = cool_until - now_ms
            raise RiskBreach(f"symbol cooldown active for {req.symbol}: {remaining}ms remaining")
        elif cool_until and now_ms >= cool_until:
            # 期限満了でブロック解除される瞬間に 'cooldown.exit' を記録（解除処理の直前）
            self._log_cooldown_exit(req.symbol, "timeout")
            self._symbol_cooldown_until.pop(req.symbol, None)
        # client_id も client_order_id(coid) と同じ値にしておくことで、
        # 約定イベント側が client_id / clientOrderId / orderLinkId のどれで返してきても、
        # OMS のキー(self._orders)と一致させて追跡できるようにする
        # OMSの追跡キー(client_id)を、取引所が返す clientOrderId/orderLinkId と同じ値(coid)に揃える（照合ズレ防止）
        req.client_id = coid

        logger.info(
            "OMS submit: symbol={} side={} type={} qty={} price={} cid={}",
            req.symbol,
            req.side,
            req.type,
            req.qty,
            req.price,
            req.client_id,
        )

        # 認証NGなら「監視のみ」。発注系APIは一切呼ばず、安全に戻る
        if not getattr(self, "auth_ok", False):
            logging.getLogger(__name__).info("order.skip: auth=false reason=%s", getattr(self, "auth_message", ""))
            return None

        # --- サイズガード（取引所制約の正規化/最小判定） ---
        # Funding/Basis は薄利なので、極小qty（dust）や最小名目未満の注文は構造上「出さない」(fail-closed)
        orig_qty = float(req.qty or 0.0)
        constraints = await self._get_order_constraints(req.symbol)
        # CLOSE/UNWIND は「閉じられない」のが危険なので、制約不明なら即時に温め直して再試行する
        if constraints is None and is_close_like:
            for attempt in range(3):
                await self._prime_scale_cache_if_possible(req.symbol, force=True)
                constraints = await self._get_order_constraints(req.symbol)
                if constraints is not None:
                    break
                # 取りに行けない状態が続くなら、少しだけ間隔を空ける（毎tickで連打しない）
                await asyncio.sleep(0.2 * float(attempt + 1))
        if constraints is None:
            if is_close_like:
                self._enter_safe_mode(f"constraints_missing_close sym={req.symbol}")
            try:
                rec = {
                    "event": "order_skip",
                    "ts": utc_now().isoformat(),
                    "symbol": req.symbol,
                    "side": req.side,
                    "type": req.type,
                    "qty": orig_qty,
                    "price": req.price,
                    "status": "skipped",
                    "client_id": req.client_id,
                    "reason": "constraints_missing_close" if is_close_like else "constraints_missing",
                }
                for k, v in meta_payload.items():
                    if v is None or k in rec:
                        continue
                    rec[k] = v
                append_jsonl_daily(
                    "logs",
                    "orders",
                    rec,
                )
            except Exception:
                pass
            logger.warning("order.skip: constraints_missing sym={} qty={} cid={}", req.symbol, orig_qty, req.client_id)
            return None

        min_qty = float(constraints.get("min_qty") or 0.0)
        qty_step = float(constraints.get("qty_step") or 0.0)
        min_notional = float(constraints.get("min_notional") or 0.0)

        norm_qty = orig_qty
        if qty_step > 0.0:
            norm_qty = math.floor(float(orig_qty) / float(qty_step)) * float(qty_step)
            norm_qty = round(float(norm_qty), 8)

        if norm_qty <= 0.0:
            try:
                rec = {
                    "event": "order_skip",
                    "ts": utc_now().isoformat(),
                    "symbol": req.symbol,
                    "side": req.side,
                    "type": req.type,
                    "qty": orig_qty,
                    "normalized_qty": norm_qty,
                    "status": "skipped",
                    "client_id": req.client_id,
                    "reason": "qty_non_positive_after_norm",
                    "min_qty": min_qty,
                    "qty_step": qty_step,
                    "min_notional": min_notional,
                }
                for k, v in meta_payload.items():
                    if v is None or k in rec:
                        continue
                    rec[k] = v
                append_jsonl_daily(
                    "logs",
                    "orders",
                    rec,
                )
            except Exception:
                pass
            logger.warning(
                "order.skip: qty_non_positive_after_norm sym={} qty={} step={} cid={}",
                req.symbol,
                orig_qty,
                qty_step,
                req.client_id,
            )
            return None

        if min_qty > 0.0 and norm_qty < min_qty:
            try:
                rec = {
                    "event": "order_skip",
                    "ts": utc_now().isoformat(),
                    "symbol": req.symbol,
                    "side": req.side,
                    "type": req.type,
                    "qty": orig_qty,
                    "normalized_qty": norm_qty,
                    "status": "skipped",
                    "client_id": req.client_id,
                    "reason": "qty_below_min",
                    "min_qty": min_qty,
                    "qty_step": qty_step,
                    "min_notional": min_notional,
                }
                for k, v in meta_payload.items():
                    if v is None or k in rec:
                        continue
                    rec[k] = v
                append_jsonl_daily(
                    "logs",
                    "orders",
                    rec,
                )
            except Exception:
                pass
            logger.warning(
                "order.skip: qty_below_min sym={} qty={} norm={} min_qty={} cid={}",
                req.symbol,
                orig_qty,
                norm_qty,
                min_qty,
                req.client_id,
            )
            return None

        approx_px = 0.0
        if min_notional > 0.0:
            try:
                if req.price is not None and (req.type or "").lower().startswith("limit"):
                    approx_px = float(req.price)
                else:
                    approx_px = float(await self._ex.get_ticker(req.symbol) or 0.0)
            except Exception:
                approx_px = 0.0
            notional = float(norm_qty) * float(approx_px)
            # min_notional は境界付近で弾かれやすいので、5%だけ安全側にバッファを取る
            if approx_px <= 0.0 or notional < (min_notional * 1.05):
                try:
                    rec = {
                        "event": "order_skip",
                        "ts": utc_now().isoformat(),
                        "symbol": req.symbol,
                        "side": req.side,
                        "type": req.type,
                        "qty": orig_qty,
                        "normalized_qty": norm_qty,
                        "price": req.price,
                        "approx_px": approx_px,
                        "notional": notional,
                        "status": "skipped",
                        "client_id": req.client_id,
                        "reason": "notional_below_min",
                        "min_qty": min_qty,
                        "qty_step": qty_step,
                        "min_notional": min_notional,
                    }
                    for k, v in meta_payload.items():
                        if v is None or k in rec:
                            continue
                        rec[k] = v
                    append_jsonl_daily(
                        "logs",
                        "orders",
                        rec,
                    )
                except Exception:
                    pass
                logger.warning(
                    "order.skip: notional_below_min sym={} qty={} norm={} px={} notional={} min_notional={} cid={}",
                    req.symbol,
                    orig_qty,
                    norm_qty,
                    approx_px,
                    round(notional, 8),
                    min_notional,
                    req.client_id,
                )
                return None

        # 正規化結果を req へ反映（取引所ルールに寄せる）
        if norm_qty != orig_qty:
            req.qty = norm_qty

        # ここから先で「送信済み」として扱う（idempotency）
        self._inflight_client_ids.add(coid)  # 今回送るIDをメモして二重送信を防止

        # place_order() 内で execution イベントが先に来ても拾えるよう、送信前にManagedOrderを登録しておく
        managed = ManagedOrder(
            req=req,
            state=OrderLifecycleState.SENT,
            sent_at=utc_now(),
            order_id="",
            filled_qty=0.0,
            avg_price=0.0,
            retries=0,
        )
        self._orders[req.client_id] = managed

        try:
            created = await self._ex.place_order(req)
        except Exception:
            # 発注失敗時は追跡登録を戻す（unknown client_id連発や再発注不能を防ぐ）
            self._orders.pop(req.client_id, None)
            self._inflight_client_ids.discard(coid)
            raise

        # 返却値で不足情報だけ反映（execution側で先に更新されていても上書きで壊さない）
        managed.order_id = getattr(created, "order_id", "") or managed.order_id
        managed.filled_qty = max(managed.filled_qty, float(getattr(created, "filled_qty", 0.0) or 0.0))
        created_avg = float(getattr(created, "avg_fill_price", 0.0) or 0.0)
        if created_avg > 0.0:
            managed.avg_price = created_avg

        if self._repo is not None:
            await self._repo.add_order_log(
                symbol=req.symbol,
                side=req.side,
                type=req.type,
                qty=req.qty,
                price=req.price,
                status="new",
                exchange_order_id=created.order_id,
                client_id=req.client_id,
            )
        try:
            rec = {
                "event": "order_new",
                "ts": utc_now().isoformat(),
                "symbol": req.symbol,
                "side": req.side,
                "type": req.type,
                "qty": req.qty,
                "price": req.price,
                "status": "new",
                "exchange_order_id": created.order_id,
                "client_id": req.client_id,
            }
            for k, v in meta_payload.items():
                if v is None or k in rec:
                    continue
                rec[k] = v
            append_jsonl_daily(
                "logs",
                "orders",
                rec,
            )
        except Exception:
            pass
        return created

    async def cancel(self, order_id: str | None = None, client_id: str | None = None) -> None:
        """これは何をする関数？
        → 指定注文を取消します（order_id または client_id を指定）。
        """

        cid = client_id
        if not cid and order_id:
            for key, candidate in self._orders.items():
                if candidate.order_id == order_id:
                    cid = key
                    break

        managed: ManagedOrder | None = self._orders.get(cid) if cid else None

        if managed:
            symbol = managed.req.symbol
            client_oid = getattr(managed.req, "client_order_id", None)
            ex_order_id = order_id or managed.order_id
            try:
                await self._ex.cancel_order(symbol=symbol, order_id=ex_order_id, client_order_id=client_oid)
            except TypeError:
                # legacy signature fallback: cancel_order(order_id=?, client_id=?)
                await cast(Any, self._ex).cancel_order(order_id=ex_order_id, client_id=client_oid)
        else:
            raise ExchangeError("cancel requires known symbol (managed order not found)")
        if managed:
            managed.state = OrderLifecycleState.CANCELED

        exchange_order_id = order_id or ""
        if not exchange_order_id and managed and managed.order_id:
            exchange_order_id = managed.order_id

        if self._repo is not None:
            await self._repo.add_order_log(
                symbol=managed.req.symbol if managed else "",
                side=managed.req.side if managed else "",
                type=managed.req.type if managed else "",
                qty=managed.req.qty if managed else 0.0,
                price=managed.req.price if managed else None,
                status="canceled",
                exchange_order_id=exchange_order_id,
                client_id=cid,
            )
        try:
            append_jsonl_daily(
                "logs",
                "orders",
                {
                    "event": "order_canceled",
                    "ts": utc_now().isoformat(),
                    "symbol": managed.req.symbol if managed else "",
                    "side": managed.req.side if managed else "",
                    "type": managed.req.type if managed else "",
                    "qty": managed.req.qty if managed else 0.0,
                    "price": managed.req.price if managed else None,
                    "status": "canceled",
                    "exchange_order_id": exchange_order_id,
                    "client_id": cid,
                },
            )
        except Exception:
            pass

    async def submit_hedge(
        self, symbol: str, delta_to_neutral: float, *, meta: dict[str, Any] | None = None
    ) -> Optional[Order]:
        """これは何をする関数？
        → ネットデルタをゼロに近づける成行IOCを出します。
        """

        if delta_to_neutral == 0:
            logger.info("OMS hedge: delta already neutral")
            return None

        side = "buy" if delta_to_neutral > 0 else "sell"
        qty = abs(delta_to_neutral)
        req = OrderRequest(
            symbol=symbol,
            side=side,
            type="market",
            qty=qty,
            time_in_force="IOC",
            reduce_only=False,
            post_only=False,
            client_id=self._gen_client_id("hedge"),
        )
        meta_payload: dict[str, Any] = dict(meta or {})
        meta_payload.setdefault("intent", "rebalance")
        meta_payload.setdefault("source", "strategy")
        return await self.submit(req, meta=meta_payload)

    async def amend(
        self,
        order,
        new_price: object | None = None,
        new_qty: object | None = None,
        post_only: bool | None = None,
        time_in_force: str | None = None,
    ) -> None:
        """手元のOrderを修正する入口。IDを取り出してゲートウェイへ渡すだけ（実処理はExchange側）。"""
        cid = getattr(order, "client_order_id", None)
        ex_order_id = getattr(order, "order_id", None) or getattr(order, "exchange_order_id", None)
        side = getattr(order, "side", None)
        await self._ex.amend_order(
            symbol=order.symbol,
            order_id=ex_order_id,
            client_order_id=cid,
            new_price=new_price,
            new_qty=new_qty,
            side=side,
            post_only=post_only,
            time_in_force=time_in_force,
        )

    # ---------- イベント取り込み ----------

    async def on_execution_event(self, event: dict[str, Any]) -> None:
        """これは何をする関数？
        → 取引所からの注文・約定イベントを受け取り、OMS内部状態を更新します。
        """

        # --- WSイベント順序ガード：古い更新は捨てる ---
        cid_e = (
            event.get("client_order_id") if isinstance(event, dict) else getattr(event, "client_order_id", None)
        )  # 注文を特定するID
        oid = (
            event.get("order_id") if isinstance(event, dict) else getattr(event, "order_id", None)
        )  # 取引所注文ID（補助）
        eid = cid_e or oid  # まずはclient_order_idを優先。無ければorder_idで代用。
        ts_raw = (
            event.get("updated_at") if isinstance(event, dict) else getattr(event, "updated_at", None)
        )  # このイベントの更新時刻（WS由来）
        event_ms = None
        if ts_raw not in (None, ""):
            try:
                event_ms = int(parse_exchange_ts(ts_raw).timestamp() * 1000)  # UTC awareに正規化→msへ
            except Exception:
                event_ms = None  # 解析できない時は判定不能として通す

        if eid and (event_ms is not None):
            last_ms = self._last_event_ms.get(eid)
            if (last_ms is not None) and (event_ms < last_ms):
                self._log.debug(
                    "guard.drop_old_event id=%s event_ms=%s last_ms=%s",
                    eid,
                    event_ms,
                    last_ms,
                )  # 古いWSイベントを捨てた
                return  # すでにより新しい状態を処理済み。古いイベントは静かに無視する

        cid = event.get("client_id") or event.get("clientOrderId") or event.get("orderLinkId") or event.get("clientId")
        # executionイベントがOMSまで届いているか・どのキーが来ているか・repoが設定されているかを確認する（切り分け用ログ）
        logger.info("OMS on_execution_event: recv cid={} keys={} repo_set={}", cid, sorted(event.keys()), self._repo is not None)
        if not cid or cid not in self._orders:
            logger.debug("OMS on_execution_event: unknown client_id, event={}", event)
            return

        exec_id = (
            event.get("exec_id")
            or event.get("execId")
            or event.get("execution_id")
            or event.get("executionId")
            or None
        )
        if exec_id:
            exec_id_str = str(exec_id)
            # 既処理の約定は捨てる（冪等性）
            if exec_id_str in self._processed_exec_ids:
                self._log.debug("oms.exec.skip_duplicate exec_id=%s cid=%s", exec_id_str, cid)
                return
            if len(self._processed_exec_ids_fifo) >= self._processed_exec_ids_max:
                old = self._processed_exec_ids_fifo.popleft()
                self._processed_exec_ids.discard(old)
            self._processed_exec_ids.add(exec_id_str)
            self._processed_exec_ids_fifo.append(exec_id_str)

        managed = self._orders[cid]

        prev_filled_qty = float(managed.filled_qty)  # 直前までの累積約定数量（増分計算に使う）

        # last_filled は取引所/シミュレータでキー名が違うことがあるため、複数キーから拾う
        last_filled = float(
            event.get("last_filled_qty")
            or event.get("last_fill_qty")
            or event.get("lastFillQty")
            or event.get("fillSz")
            or event.get("fillQty")
            or event.get("fill_qty")
            or event.get("last_exec_qty")
            or event.get("lastQty")
            or 0.0
        )

        # cum_filled も同様に複数キー対応。無ければ prev + last で推定する
        cum_filled = float(
            event.get("cum_filled_qty")
            or event.get("cum_fill_qty")
            or event.get("cumExecQty")
            or event.get("accFillSz")
            or event.get("cumFillQty")
            or event.get("filled_qty")
            or event.get("filledQty")
            or (prev_filled_qty + last_filled)
        )

        # 累積は減らさない（重複イベントが来ても安全）
        managed.filled_qty = max(prev_filled_qty, cum_filled)

        # last_filled が取れないイベントでも、cum の増分を last_filled として扱い trade_log を落とさない
        filled_delta = max(0.0, managed.filled_qty - prev_filled_qty)
        if last_filled <= 0.0 and filled_delta > 0.0:
            last_filled = filled_delta

        price_val = event.get("avg_fill_price") or event.get("last_fill_price") or event.get("fillPrice")
        if price_val is not None:
            managed.avg_price = float(price_val)

        raw_status = (event.get("status") or "").lower()
        if raw_status in {"new", "open"}:
            managed.state = OrderLifecycleState.SENT
        elif raw_status in {"partially_filled", "partial"}:
            managed.state = OrderLifecycleState.PARTIALLY_FILLED
        elif raw_status in {"filled", "done", "closed"}:
            managed.state = OrderLifecycleState.FILLED
        elif raw_status in {"canceled", "cancelled"}:
            managed.state = OrderLifecycleState.CANCELED
        elif raw_status == "rejected":
            managed.state = OrderLifecycleState.REJECTED

        # add_trade に進めない原因が「last_filled が 0」なのかを確実に確認するための切り分けログ
        logger.info(
            "OMS on_execution_event: pre_add_trade_check cid={} last_filled={} cum_filled={} managed_filled={} raw_last={} raw_cum={} status={}",
            cid,
            last_filled,
            cum_filled,
            managed.filled_qty,
            event.get("last_filled_qty"),
            event.get("cum_filled_qty"),
            event.get("status"),
        )
        if last_filled > 0:
            ts_raw = event.get("updated_at") if isinstance(event, dict) else getattr(event, "updated_at", None)
            ts_dt = parse_exchange_ts(ts_raw) if ts_raw not in (None, "") else utc_now()
            if self._repo is not None:
                self._log.info(
                    "oms.on_execution_event.before_add_trade order_id=%s trade_sym=%s side=%s qty=%s price=%s repo_obj_id=%s repo_type=%s",
                    event.get("order_id") if isinstance(event, dict) else getattr(event, "order_id", None),
                    managed.req.symbol,
                    managed.req.side,
                    last_filled,
                    float(price_val) if price_val is not None else 0.0,
                    hex(id(self._repo)),
                    type(self._repo),
                )  # 約定イベントをどのRepoインスタンスに渡そうとしているかを記録するデバッグログ
                # add_trade が実際に呼ばれているか（呼び出し前）を確認するログ
                logger.info(
                    "oms.on_execution_event: add_trade.call cid={} sym={} side={} last_filled={} cum_filled={} price_val={} avg_fill_price={} status={}",
                    cid,
                    managed.req.symbol,
                    managed.req.side,
                    last_filled,
                    cum_filled,
                    float(price_val) if price_val is not None else 0.0,
                    float(event.get("avg_fill_price") or 0.0),
                    event.get("status"),
                )
                fee_val = event.get("fee") or event.get("fee_quote") or event.get("fee_usdt") or event.get("feeUsd")
                fee = float(fee_val) if fee_val not in (None, "") else 0.0
                await self._repo.add_trade(
                    ts=ts_dt,
                    symbol=managed.req.symbol,
                    side=managed.req.side,
                    qty=last_filled,
                    price=float(price_val) if price_val is not None else 0.0,
                    fee=fee,
                    exchange_order_id=managed.order_id or "",
                    client_id=cid,
                )

                # add_trade が実際に呼ばれているか（呼び出し後）を確認するログ
                logger.info(
                    "oms.on_execution_event: add_trade.done cid={} managed_filled_qty={} managed_state={}",
                    cid,
                    managed.filled_qty,
                    managed.state,
                )
                self._log.info(
                    "oms.on_execution_event.after_add_trade order_id=%s trade_sym=%s repo_obj_id=%s",
                    event.get("order_id") if isinstance(event, dict) else getattr(event, "order_id", None),
                    managed.req.symbol,
                    hex(id(self._repo)),
                )  # Repo.add_trade呼び出し後に、このRepoインスタンスにトレードが追加されたと想定していることを記録するデバッグログ
            try:
                ts_iso = ts_dt.astimezone().isoformat()

                # enrich fields
                ev_symbol = event.get("symbol") if isinstance(event, dict) else getattr(event, "symbol", None)
                ev_side = event.get("side") if isinstance(event, dict) else getattr(event, "side", None)
                ev_fee = event.get("fee") if isinstance(event, dict) else getattr(event, "fee", None)
                ev_fee_ccy = (
                    event.get("fee_currency") if isinstance(event, dict) else getattr(event, "fee_currency", None)
                )
                ev_order_id = event.get("order_id") if isinstance(event, dict) else getattr(event, "order_id", None)
                ev_coid = (
                    event.get("client_order_id") if isinstance(event, dict) else getattr(event, "client_order_id", None)
                )
                ev_exec_id = event.get("exec_id") if isinstance(event, dict) else getattr(event, "exec_id", None)
                ev_liq = event.get("liquidity") if isinstance(event, dict) else getattr(event, "liquidity", None)
                ev_source = event.get("source") if isinstance(event, dict) else getattr(event, "source", None)

                # best bid/ask and slippage
                bbo_bid = None
                bbo_ask = None
                try:
                    if hasattr(self._ex, "_get_bbo_if_fresh"):
                        bbo_bid, bbo_ask = await self._ex._get_bbo_if_fresh(managed.req.symbol)
                    else:
                        bbo_bid, bbo_ask = await cast(Any, self._ex).get_bbo(managed.req.symbol)
                except Exception:
                    pass
                px = float(price_val) if price_val is not None else 0.0
                fee_usdt = _fee_to_usdt(
                    fee=ev_fee,
                    fee_currency=ev_fee_ccy,
                    symbol=ev_symbol or managed.req.symbol,
                    price=px,
                )
                slippage_bps = None
                try:
                    if ev_side and px and (bbo_bid is not None or bbo_ask is not None):
                        if str(ev_side).lower() == "buy" and bbo_ask not in (None, ""):
                            base = float(str(bbo_ask))
                            slippage_bps = (px - base) / base * 1e4
                        elif str(ev_side).lower() == "sell" and bbo_bid not in (None, ""):
                            base = float(str(bbo_bid))
                            slippage_bps = (base - px) / base * 1e4
                except Exception:
                    slippage_bps = None

                # order flags
                flags = {
                    "post_only": bool(getattr(managed.req, "post_only", False)),
                    "reduce_only": bool(getattr(managed.req, "reduce_only", False)),
                    "time_in_force": getattr(managed.req, "time_in_force", None),
                    "order_type": getattr(managed.req, "type", None),
                }

                # run context (optional via env)
                run_id = getattr(self, "_run_id", None)
                strat_name = getattr(self, "_strategy_name", None)

                def _to_float(val: object | None) -> float | None:
                    if val in (None, ""):
                        return None
                    try:
                        return float(str(val))
                    except Exception:
                        return None

                rec = {
                    "event": "trade_fill",
                    "ts": ts_iso,
                    "time_utc": ts_dt.astimezone().isoformat(),
                    "symbol": ev_symbol or managed.req.symbol,
                    "side": ev_side or managed.req.side,
                    "qty": last_filled,
                    "price": px,
                    "notional": round(last_filled * px, 12),
                    "order_id": ev_order_id or (managed.order_id or ""),
                    "client_order_id": ev_coid,
                    "exec_id": ev_exec_id,
                    "fee": float(ev_fee) if ev_fee is not None else 0.0,
                    "fee_currency": ev_fee_ccy,
                    "fee_usdt": fee_usdt,
                    "liquidity": ev_liq,
                    "exchange_order_id": managed.order_id or "",
                    "client_id": cid,
                    "bbo_bid": _to_float(bbo_bid),
                    "bbo_ask": _to_float(bbo_ask),
                    "slippage_bps": slippage_bps,
                    "order_flags": flags,
                    "raw_order_flags": (
                        event.get("raw_order_flags")
                        if isinstance(event, dict)
                        else getattr(event, "raw_order_flags", None)
                    ),
                    "source": ev_source or "ws.execution",
                }
                if strat_name:
                    rec["strategy_name"] = strat_name
                if run_id:
                    rec["run_id"] = run_id

                # tag heuristic
                try:
                    if flags.get("reduce_only"):
                        rec["tag"] = "CLOSE"
                    elif str(cid).startswith("hedge-"):
                        rec["tag"] = "HEDGE"
                    else:
                        rec["tag"] = "OPEN"
                except Exception:
                    pass

                append_jsonl_daily("logs", "trades", rec)
                # feed round-trip aggregator for entry/exit/PNL logging
                try:
                    self._trip_agg.on_fill(
                        symbol=managed.req.symbol,
                        side=managed.req.side,
                        qty=float(last_filled),
                        price=float(price_val) if price_val is not None else 0.0,
                        fee=0.0,
                        ts_iso=ts_iso,
                        exchange_order_id=managed.order_id or "",
                        client_id=cid,
                    )
                except Exception:
                    pass
            except Exception:
                pass

        remaining = managed.req.qty - managed.filled_qty
        if managed.state == OrderLifecycleState.PARTIALLY_FILLED and remaining > 1e-12:
            if managed.retries < self._cfg.max_retries:
                managed.retries += 1
                logger.info(
                    "OMS resend (partial): cid={} remaining={} retry={}",
                    cid,
                    remaining,
                    managed.retries,
                )
                child_req = OrderRequest(
                    symbol=managed.req.symbol,
                    side=managed.req.side,
                    type="market",
                    qty=remaining,
                    time_in_force="IOC",
                    reduce_only=managed.req.reduce_only,
                    post_only=False,
                    client_id=self._gen_client_id(f"{cid}-r{managed.retries}"),
                )
                await self.submit(child_req)
            else:
                logger.warning(
                    "OMS give up resend (partial): cid={} remaining={} retries={}",
                    cid,
                    remaining,
                    managed.retries,
                )

        # idempotent submit cleanup: 終端ステータスでclient_order_idをメモ帳から削除
        coid = event.get("client_order_id") if isinstance(event, dict) else getattr(event, "client_order_id", None)
        st = ((event.get("status") if isinstance(event, dict) else getattr(event, "status", None)) or "").upper()
        if st in ("FILLED", "CANCELED", "CANCELLED", "REJECTED"):
            if coid:
                self._inflight_client_ids.discard(coid)

        # --- 最終時刻の更新（今回のイベントを最新として記録） ---
        if eid and (event_ms is not None):
            self._last_event_ms[eid] = event_ms  # 今回のイベントを「最新」として記録する（次回の順序判定に使う）

        # --- REJECTEDが来たら、この銘柄のクールダウン判定を更新 ---
        try:
            sym = event.get("symbol") if isinstance(event, dict) else getattr(event, "symbol", None)
            st_ev = event.get("status") if isinstance(event, dict) else getattr(event, "status", None)
            if (not sym) and "cid" in locals():
                try:
                    mo = self._orders.get(cid)
                    if mo and getattr(mo, "req", None):
                        sym = getattr(mo.req, "symbol", None)
                except Exception:
                    sym = sym
            if (sym and st_ev) and (str(st_ev).upper() == "REJECTED"):
                self._note_rejection(str(sym))
        except Exception:
            pass

    async def drain_and_flatten(self, symbols: list[str], strategy, timeout_s: int = 20) -> None:
        """安全終了用のドレイン関数。
        1) 新規発注を止める → 2) 未完了注文を取り消す → 3) 戦略の全クローズを試みる、を
        タイムアウトまで数回ゆるく繰り返す（例外は飲み込んで継続）。"""
        try:
            self._disable_new_orders = True
            rm = getattr(strategy, "_risk_manager", None)
            if rm is not None:
                rm.disable_new_orders = True
        except Exception:
            pass

        start_ms = int(utc_now().timestamp() * 1000)
        deadline_ms = start_ms + timeout_s * 1000
        self._log.info("shutdown.drain start timeout_s=%d", timeout_s)

        total_canceled = 0
        loops = 0

        while True:
            loops += 1
            any_open = False
            loop_canceled = 0

            for sym in symbols:
                try:
                    open_orders = await self._ex.get_open_orders(sym)
                except Exception:
                    open_orders = []
                if open_orders:
                    any_open = True
                for o in open_orders:
                    order_id = o.get("orderId") if isinstance(o, dict) else getattr(o, "order_id", None)
                    client_oid = o.get("orderLinkId") if isinstance(o, dict) else getattr(o, "client_order_id", None)
                    try:
                        try:
                            await self._ex.cancel_order(symbol=sym, order_id=order_id, client_order_id=client_oid)
                        except TypeError:
                            await cast(Any, self._ex).cancel_order(order_id=order_id, client_id=client_oid)
                        loop_canceled += 1
                    except Exception:
                        continue

            try:
                await strategy.flatten_all()
            except Exception:
                pass

            now_ms = int(utc_now().timestamp() * 1000)
            total_canceled += loop_canceled
            try:
                self._log.info("shutdown.drain loop canceled=%d any_open=%s", loop_canceled, str(any_open))
            except Exception:
                pass

            if (not any_open) or (now_ms >= deadline_ms):
                break

            await asyncio.sleep(0.5)

        elapsed_ms = int(utc_now().timestamp() * 1000) - start_ms
        self._log.info(
            "shutdown.drain done canceled_total=%d loops=%d elapsed_ms=%d",
            total_canceled,
            loops,
            elapsed_ms,
        )

    async def reconcile_inflight_open_orders(self, symbols: list[str]) -> None:
        """取引所に残る open 注文の client_order_id を復元して二重発注を防ぐ。

        - Bitget v5 では /v5/order/realtime の各要素に orderLinkId が含まれる。
        - 互換のため、Order オブジェクトの場合は client_order_id もしくは client_id を参照する。
        """

        for sym in symbols:
            try:
                get_det = getattr(self._ex, "get_open_orders_detailed", None)
                if callable(get_det):
                    open_orders = await get_det(sym)
                else:
                    open_orders = await self._ex.get_open_orders(sym)
            except Exception:
                open_orders = []

            for o in open_orders or []:
                if isinstance(o, dict):
                    cid = o.get("orderLinkId") or o.get("clientOrderId") or o.get("clOrdId")
                else:
                    cid = getattr(o, "client_order_id", None) or getattr(o, "client_id", None)
                if cid:
                    self._inflight_client_ids.add(str(cid))
            self._log.info(
                "reconcile.inflight_restored symbol=%s count=%d", sym, len(open_orders)
            )  # 取引所側open注文からinflightを復元

    def _note_rejection(self, symbol: str) -> None:
        """この銘柄でREJECTEDが発生したことを記録し、短時間に連発したらクールダウンに入れる。"""

        now_ms = int(utc_now().timestamp() * 1000)
        rec = self._reject_window.get(symbol)
        if rec:
            win_start, cnt = rec
            if (now_ms - win_start) <= self._reject_window_ms:
                cnt += 1
                self._reject_window[symbol] = (win_start, cnt)
            else:
                self._reject_window[symbol] = (now_ms, 1)
                cnt = 1
        else:
            self._reject_window[symbol] = (now_ms, 1)
            cnt = 1

        if cnt >= self._reject_burst_threshold:
            self._log.warning(
                "cooldown.enter symbol=%s window_ms=%d threshold=%d cooldown_ms=%d",
                symbol,
                self._reject_window_ms,
                self._reject_burst_threshold,
                self._symbol_cooldown_ms,
            )  # REJECT連発→クールダウン突入
            self._symbol_cooldown_until[symbol] = now_ms + self._symbol_cooldown_ms
            self._cooldown_active[symbol] = (
                True  # 何をする？→ 突入時に“現在クールダウン中”フラグを立てる（出口で一度だけログするため）
            )
            # メトリクス更新
            self._metrics_cooldown_enter_total[symbol] = self._metrics_cooldown_enter_total.get(symbol, 0) + 1
            self._metrics_cooldown_enter_period[symbol] = self._metrics_cooldown_enter_period.get(symbol, 0) + 1
            # 次の連発を独立に数える
            self._reject_window[symbol] = (now_ms, 0)

    def _log_cooldown_exit(self, symbol: str, reason: str) -> None:
        """何をする関数？→ クールダウン解除の瞬間に一度だけ 'cooldown.exit' をINFOで記録する。"""
        if self._cooldown_active.get(symbol):
            logging.getLogger(__name__).info("cooldown.exit symbol=%s reason=%s", symbol, reason)
            self._cooldown_active[symbol] = False  # これで同じ解除イベントの重複ログを防止する

    async def maintain_postonly_orders(self, symbols: list[str], strat_cfg) -> None:
        """PostOnlyの未約定指値をBBOにあわせて安全に少しだけ寄せる。
        - BBOはExchangeのget_bbo（WSキャッシュ優先）を利用
        - ズレが小さい時は何もしない（chase_min_reprice_bps）
        - 連続修正の最短間隔＆1分あたりの回数上限でやりすぎ防止
        - 実際の非クロス補正・tick丸め・価格逸脱ガードはExchange側で安全に実行
        """

        if not getattr(strat_cfg, "chase_enabled", False):
            return

        thr_bps = Decimal(str(getattr(strat_cfg, "chase_min_reprice_bps", 2)))
        interval_ms = int(getattr(strat_cfg, "chase_interval_ms", 1500))
        max_per_min = int(getattr(strat_cfg, "chase_max_amends_per_min", 12))

        get_bbo = getattr(self._ex, "get_bbo", None)
        if not callable(get_bbo):
            return

        now_ms = int(utc_now().timestamp() * 1000)

        for sym in symbols:
            try:
                open_orders = await self._ex.get_open_orders(sym)
            except Exception:
                open_orders = []

            for o in open_orders or []:
                if not isinstance(o, dict):
                    continue
                tif = (str(o.get("timeInForce") or "")).lower()
                otype = (str(o.get("orderType") or "")).lower()
                status = (str(o.get("orderStatus") or o.get("status") or "")).upper()
                if (otype != "limit") or (tif != "postonly"):
                    continue
                if status in {"FILLED", "CANCELED", "CANCELLED", "REJECTED"}:
                    continue

                side = str(o.get("side") or "")
                price_str = o.get("price")
                if not price_str or not side:
                    continue

                bid, ask = await get_bbo(sym)
                if not bid or not ask:
                    continue
                try:
                    mid = (Decimal(str(bid)) + Decimal(str(ask))) / Decimal("2")
                except Exception:
                    continue
                if mid <= 0:
                    continue

                try:
                    px = Decimal(str(price_str))
                    bps = (abs(px - mid) / mid) * Decimal("10000")
                except Exception:
                    continue
                if bps < thr_bps:
                    continue

                eid = o.get("orderLinkId") or o.get("orderId") or f"{sym}:{side}:{price_str}"
                last = self._last_amend_ms.get(eid, 0)
                if (now_ms - last) < interval_ms:
                    continue
                minute = now_ms // 60000
                rec = self._amend_count_minute.get(eid)
                if rec and rec[0] == minute and rec[1] >= max_per_min:
                    continue

                desired = ask if side.upper() == "BUY" else bid

                try:
                    await self._ex.amend_order(
                        symbol=sym,
                        order_id=o.get("orderId"),
                        client_order_id=o.get("orderLinkId"),
                        new_price=desired,
                        side=side,
                        post_only=True,
                    )
                    # ログ（控えめ）：成功したアメンドのみbpsと価格を記録
                    try:
                        logger.info(
                            "chaser amend: sym={} eid={} side={} bps={} px_old={} px_new={}",
                            sym,
                            eid,
                            side,
                            round(float(bps), 2),
                            price_str,
                            desired,
                        )
                        self._log.info(
                            "chase.amend symbol=%s side=%s eid=%s desired=%s bps=%s",
                            sym,
                            side,
                            eid,
                            desired,
                            str(bps),
                        )  # チェイスで価格をやさしく寄せた
                    except Exception:
                        pass
                    self._last_amend_ms[eid] = now_ms
                    if rec and rec[0] == minute:
                        self._amend_count_minute[eid] = (minute, rec[1] + 1)
                    else:
                        self._amend_count_minute[eid] = (minute, 1)
                    # メトリクス更新
                    self._metrics_chase_amend_total[sym] = self._metrics_chase_amend_total.get(sym, 0) + 1
                    self._metrics_chase_amend_period[sym] = self._metrics_chase_amend_period.get(sym, 0) + 1
                except Exception:
                    continue

    # ===== メトリクス提供（MetricsLogger から呼び出し） =====
    def get_and_reset_guard_metrics(self, symbols: list[str]) -> dict[str, dict[str, int]]:
        """追従（チェイサ）・クールダウンの集計を取り出す（期間分はリセット）。
        返却: {sym: {"chase_period": n, "cooldown_period": m, "chase_total": x, "cooldown_total": y}}
        """

        out: dict[str, dict[str, int]] = {}
        for sym in symbols:
            chase_p = int(self._metrics_chase_amend_period.get(sym, 0))
            cool_p = int(self._metrics_cooldown_enter_period.get(sym, 0))
            chase_t = int(self._metrics_chase_amend_total.get(sym, 0))
            cool_t = int(self._metrics_cooldown_enter_total.get(sym, 0))
            out[sym] = {
                "chase_period": chase_p,
                "cooldown_period": cool_p,
                "chase_total": chase_t,
                "cooldown_total": cool_t,
            }
            # 期間カウンタはリセット
            if chase_p:
                self._metrics_chase_amend_period[sym] = 0
            if cool_p:
                self._metrics_cooldown_enter_period[sym] = 0
        return out

    # ---------- タイムアウト監視 ----------

    async def process_timeouts(self) -> None:
        """これは何をする関数？
        → 未約定注文を監視し、タイムアウトした場合は取消と再送を行います。
        """

        now = utc_now()
        for cid, managed in list(self._orders.items()):
            if managed.state in {
                OrderLifecycleState.FILLED,
                OrderLifecycleState.CANCELED,
                OrderLifecycleState.REJECTED,
            }:
                continue
            if not managed.sent_at:
                continue

            elapsed = (now - managed.sent_at).total_seconds()
            if elapsed < self._cfg.order_timeout_sec:
                continue

            remaining = managed.req.qty - managed.filled_qty
            logger.warning(
                "OMS timeout: cid={} elapsed={}s remaining={}",
                cid,
                round(elapsed, 3),
                remaining,
            )

            try:
                try:
                    await self._ex.cancel_order(
                        symbol=managed.req.symbol,
                        order_id=managed.order_id,
                        client_order_id=getattr(managed.req, "client_order_id", None),
                    )
                except TypeError:
                    await cast(Any, self._ex).cancel_order(
                        order_id=managed.order_id,
                        client_id=getattr(managed.req, "client_order_id", None),
                    )
            except ExchangeError as exc:
                logger.warning("OMS timeout cancel failed: cid={} err={}", cid, exc)
            except Exception as exc:  # noqa: BLE001
                logger.warning("OMS timeout cancel failed: cid={} err={}", cid, exc)

            managed.state = OrderLifecycleState.CANCELED

            if remaining > 1e-12 and managed.retries < self._cfg.max_retries:
                managed.retries += 1
                child_req = OrderRequest(
                    symbol=managed.req.symbol,
                    side=managed.req.side,
                    type="market",
                    qty=remaining,
                    time_in_force="IOC",
                    reduce_only=managed.req.reduce_only,
                    post_only=False,
                    client_id=self._gen_client_id(f"{cid}-r{managed.retries}"),
                )
                await self.submit(child_req)
