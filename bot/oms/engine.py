# これは「注文のライフサイクルを安全に管理するOMSエンジン」を実装するファイルです。
from __future__ import annotations

import random
import time
from decimal import Decimal  # 価格のズレをbpsで計算するために使用
from typing import Any, cast

from loguru import logger

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

from .types import ManagedOrder, OmsConfig, OrderLifecycleState


class OmsEngine:
    """注文管理（状態機械）を提供するエンジン。
    - submit(): 新規発注
    - cancel(): 取消
    - submit_hedge(): ネットデルタを埋める成行IOC
    - on_execution_event(): 約定/状態更新の取り込み
    - process_timeouts(): タイムアウト監視（テストから明示呼び出し）
    """

    def __init__(self, ex: ExchangeGateway, repo: Repo, cfg: OmsConfig | None = None) -> None:
        """これは何をする関数？
        → ExchangeGateway/Repo/設定を受け取り、注文追跡テーブルを用意します。
        """

        self._ex = ex
        self._repo = repo
        self._cfg = cfg or OmsConfig()
        self._orders: dict[str, ManagedOrder] = {}  # key: client_id
        # WS ライブネスのメモとブロックしきい値（デフォルト値は安全側）
        self._ws_private_last_ms: int = 0
        self._ws_stale_block_ms: int = 10_000
        # PostOnly追従（アメンド）の頻度制御メモ
        self._last_amend_ms: dict[str, int] = {}
        self._amend_count_minute: dict[str, tuple[int, int]] = {}
        self._last_event_ms: dict[str, int] = (
            {}
        )  # 注文ごとの最終更新時刻（ms）を覚えて、古いWSイベントを無視するためのメモ
        self._inflight_client_ids: set[str] = set()  # 送信済みのclient注文IDを記録して二重発注を防ぐメモ帳

    # ---------- 内部: client_id 生成 ----------

    def _gen_client_id(self, prefix: str = "oms") -> str:
        """これは何をする関数？
        → 時刻(ms)＋ナンスから、衝突しにくい client_id を生成します。
        """

        ms = int(time.time() * 1000)
        nonce = random.randint(1000, 9999)
        return f"{prefix}-{ms}-{nonce}"

    def touch_private_ws(self, ts: object | None = None) -> None:
        """Private WSを受け取った合図。取引所形式のtsでも、そのままNoneでもOK。内部でUTC msに正規化して保存する。"""
        try:
            dt = utc_now() if ts in (None, "") else parse_exchange_ts(ts)  # 取引所/WSの時刻表現をUTC awareに変換
            self._ws_private_last_ms = int(dt.timestamp() * 1000)
        except Exception:
            # 時刻が読めなくても、最低限「いま来た」ことは記録する
            self._ws_private_last_ms = int(utc_now().timestamp() * 1000)

    # ---------- 発注/取消API ----------

    async def submit(self, req: OrderRequest) -> Order:
        """これは何をする関数？
        → 注文を取引所へ発注し、OMSの追跡に登録します（idempotencyのためclient_id必須）。
        """

        # Bybit の client order id（orderLinkId）を未指定ならここで採番
        try:
            import uuid

            if getattr(req, "client_order_id", None) in (None, ""):
                req.client_order_id = f"bot-{uuid.uuid4().hex}"
        except Exception:
            pass

        coid = req.client_order_id or self._gen_client_id("bot")
        req.client_order_id = coid
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
        self._inflight_client_ids.add(coid)  # 今回送るIDをメモして二重送信を防止

        if not req.client_id:
            req.client_id = self._gen_client_id("ord")

        logger.info(
            "OMS submit: symbol={} side={} type={} qty={} price={} cid={}",
            req.symbol,
            req.side,
            req.type,
            req.qty,
            req.price,
            req.client_id,
        )

        created = await self._ex.place_order(req)

        managed = ManagedOrder(
            req=req,
            state=OrderLifecycleState.SENT,
            sent_at=utc_now(),
            order_id=created.order_id,
            filled_qty=created.filled_qty,
            avg_price=created.avg_fill_price,
            retries=0,
        )
        self._orders[req.client_id] = managed

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

    async def submit_hedge(self, symbol: str, delta_to_neutral: float) -> None:
        """これは何をする関数？
        → ネットデルタをゼロに近づける成行IOCを出します。
        """

        if delta_to_neutral == 0:
            logger.info("OMS hedge: delta already neutral")
            return

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
        await self.submit(req)

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
                return  # すでにより新しい状態を処理済み。古いイベントは静かに無視する

        cid = event.get("client_id") or event.get("clientOrderId") or event.get("orderLinkId") or event.get("clientId")
        if not cid or cid not in self._orders:
            logger.debug("OMS on_execution_event: unknown client_id, event={}", event)
            return

        managed = self._orders[cid]

        last_filled = float(event.get("last_filled_qty") or event.get("lastFillQty") or 0.0)
        cum_filled = float(event.get("cum_filled_qty") or event.get("cumExecQty") or (managed.filled_qty + last_filled))
        managed.filled_qty = max(managed.filled_qty, cum_filled)

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

        if last_filled > 0:
            await self._repo.add_trade(
                symbol=managed.req.symbol,
                side=managed.req.side,
                qty=last_filled,
                price=float(price_val) if price_val is not None else 0.0,
                fee=0.0,
                exchange_order_id=managed.order_id or "",
                client_id=cid,
            )

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

    async def reconcile_inflight_open_orders(self, symbols: list[str]) -> None:
        """取引所に残る open 注文の client_order_id を復元して二重発注を防ぐ。

        - Bybit v5 では /v5/order/realtime の各要素に orderLinkId が含まれる。
        - 互換のため、Order オブジェクトの場合は client_order_id もしくは client_id を参照する。
        """

        for sym in symbols:
            try:
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
                    self._last_amend_ms[eid] = now_ms
                    if rec and rec[0] == minute:
                        self._amend_count_minute[eid] = (minute, rec[1] + 1)
                    else:
                        self._amend_count_minute[eid] = (minute, 1)
                except Exception:
                    continue

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
