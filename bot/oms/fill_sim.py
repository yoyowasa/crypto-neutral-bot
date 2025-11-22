from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from loguru import logger

from bot.exchanges.base import ExchangeGateway
from bot.exchanges.types import Balance, FundingInfo, Order, OrderRequest, Position


@dataclass
class _PaperOrder:
    """これは何を表す型？
    → Paper環境で管理するローカル注文（成行/指値）と、進捗の最小情報。
    """

    order_id: str
    client_id: str
    req: OrderRequest
    status: str = "new"  # "new"/"filled"/"canceled"
    filled_qty: float = 0.0
    avg_price: float | None = None


class PaperExchange(ExchangeGateway):
    """ベストBid/Askを使って疑似約定する ExchangeGateway（REST発注なし）"""

    def __init__(self, *, data_source: ExchangeGateway, initial_usdt: float = 100_000.0) -> None:
        """これは何をする関数？
        → 市場データ/補助情報（Funding/ティッカー）を返す data_source と、初期USDT残高を受け取ります。
        """

        self._data = data_source  # BitgetGateway 等（REST/WSデータ専用、発注はしない）
        self._oms = None  # 後から bind_oms() でセット
        self._id_seq = 0

        # BBO/トレードのスナップショット（perp主体。spotは無ければperpで代用）
        self._bbo: dict[str, tuple[float | None, float | None]] = {}  # symbol -> (bid, ask)
        self._last_price: dict[str, float] = {}  # symbol -> last trade/mid
        # BitgetGateway 互換の価格スケール/価格ガード/価格キャッシュを簡易に持つ（バックテスト用）
        self._scale_cache: dict[str, dict[str, float]] = {}
        self._price_state: dict[str, str] = {}
        self._last_spot_px: dict[str, float] = {}
        self._last_index_px: dict[str, float] = {}

        # ローカル注文・状態
        self._orders: dict[str, _PaperOrder] = {}  # client_id -> order
        self._order_by_id: dict[str, _PaperOrder] = {}

        # 現物バランス（USDT と各ベース資産）。available=totalとして扱うMVP
        self._balances: dict[str, Balance] = {
            "USDT": Balance(asset="USDT", total=float(initial_usdt), available=float(initial_usdt))
        }

        # デリバティブ建玉（ロング/ショートを別エントリで保持）
        self._positions: list[Position] = []

        # 排他制御
        self._lock = asyncio.Lock()

    # ---------- 配線：OMSを後から結びつける ----------

    def bind_oms(self, oms: Any) -> None:
        """これは何をする関数？
        → 約定/取消時に呼び出す OmsEngine を後からバインドします。
        """

        self._oms = oms

    # ---------- ExchangeGateway: 情報系 ----------

    async def get_balances(self) -> list[Balance]:
        """これは何をする関数？→ 現在の疑似現物残高一覧を返します。"""

        async with self._lock:
            return list(self._balances.values())

    async def get_positions(self) -> list[Position]:
        """これは何をする関数？→ 現在の疑似デリバティブ建玉一覧を返します。"""

        async with self._lock:
            return list(self._positions)

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """これは何をする関数？→ 現在のローカル未約定注文を返します。"""

        async with self._lock:
            out: list[Order] = []
            for po in self._orders.values():
                if symbol and po.req.symbol != symbol:
                    continue
                out.append(
                    Order(
                        symbol=po.req.symbol,
                        order_id=po.order_id,
                        client_id=po.client_id,
                        status=po.status,
                        filled_qty=po.filled_qty,
                        avg_fill_price=po.avg_price,
                    )
                )
            return out

    async def get_ticker(self, symbol: str) -> float:
        """これは何をする関数？
        → 近似価格を返します。優先順位：mid(BBO) > last > data_sourceのticker。
        """

        bid, ask = self._bbo.get(symbol, (None, None))
        if bid is not None and ask is not None:
            return (bid + ask) / 2.0
        if symbol in self._last_price:
            return self._last_price[symbol]
        # フォールバック：data_source（REST）
        try:
            return await self._data.get_ticker(symbol)
        except Exception:
            return 0.0

    async def get_funding_info(self, symbol: str) -> FundingInfo:
        """これは何をする関数？
        → Funding 情報は data_source（BitgetGateway/CCXT）に委譲します（実発注はしない）。
        """

        return await self._data.get_funding_info(symbol)

    # ---------- ExchangeGateway: 発注/取消（疑似） ----------

    async def place_order(self, req: OrderRequest) -> Order:
        """これは何をする関数？
        → ローカル注文を作り、Marketは即時にBid/Askで約定。Limitは板内に入れば約定。
        """

        async with self._lock:
            self._id_seq += 1
            oid = f"PAPER-{self._id_seq}"
            cid = req.client_id or oid
            po = _PaperOrder(order_id=oid, client_id=cid, req=req)
            self._orders[cid] = po
            self._order_by_id[oid] = po

        # Market: 即時約定
        if req.type.lower() == "market":
            price = await self._price_for_market(req.symbol, req.side)
            await self._fill_now(po, fill_qty=req.qty, price=price, final_status="filled")
            return Order(
                symbol=req.symbol,
                order_id=po.order_id,
                client_id=po.client_id,
                status="filled",
                filled_qty=req.qty,
                avg_fill_price=price,
            )

        # Limit: いまのBBOで板内なら即時約定、そうでなければ保留
        if req.type.lower() == "limit":
            if await self._is_limit_crossing(req):
                price = await self._price_for_limit_fill(req.symbol, req.side)
                await self._fill_now(po, fill_qty=req.qty, price=price, final_status="filled")
                return Order(
                    symbol=req.symbol,
                    order_id=po.order_id,
                    client_id=po.client_id,
                    status="filled",
                    filled_qty=req.qty,
                    avg_fill_price=price,
                )
            # 未約定のまま残す
            return Order(
                symbol=req.symbol,
                order_id=po.order_id,
                client_id=po.client_id,
                status="new",
                filled_qty=0.0,
                avg_fill_price=None,
            )

        # 未対応タイプはエラーにしない（MVPではnewにして残す）
        logger.warning("paper: unsupported order type={}, leave as new", req.type)
        return Order(
            symbol=po.req.symbol,
            order_id=po.order_id,
            client_id=po.client_id,
            status="new",
            filled_qty=0.0,
            avg_fill_price=None,
        )

    async def cancel_order(self, symbol: str, order_id: str | None = None, client_order_id: str | None = None) -> None:
        """これは何をする関数？→ ローカル注文を取消し、OMSへ 'canceled' を通知します。"""

        async with self._lock:
            po = None
            cid = client_order_id
            if cid and cid in self._orders:
                po = self._orders[cid]
            elif order_id and order_id in self._order_by_id:
                po = self._order_by_id[order_id]
            if not po or po.status in {"filled", "canceled"}:
                return
            po.status = "canceled"

        if self._oms:
            await self._oms.on_execution_event(
                {
                    "client_id": po.client_id,
                    "status": "canceled",
                    "last_filled_qty": 0.0,
                    "cum_filled_qty": po.filled_qty,
                    "avg_fill_price": po.avg_price,
                }
            )

    # ---------- WS（Public）の受信ハンドラ ----------

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
    ) -> None:  # Paperでは何もしない（型と呼び出し整合のためのスタブ）
        return

    async def handle_public_msg(self, msg: dict) -> None:
        """これは何をする関数？
        → Bitget v5 Public WS からの raw メッセージ（orderbook/publicTrade）を受け取り、BBO/last を更新し、
          指値の「板内判定→約定」を試みます。
        """

        topic = msg.get("topic") or ""
        if topic.startswith("orderbook."):
            symbol = topic.split(".")[-1]
            data_obj = msg.get("data")
            d = None
            if isinstance(data_obj, list):
                d = data_obj[0] if data_obj else None
            elif isinstance(data_obj, dict):
                d = data_obj

            if d:
                bid = None
                ask = None

                # 標準形：b/a は [[price, size], ...] の配列
                b = d.get("b")
                a = d.get("a")
                if isinstance(b, list) and b:
                    try:
                        bid = float(b[0][0])
                    except Exception:
                        bid = None
                if isinstance(a, list) and a:
                    try:
                        ask = float(a[0][0])
                    except Exception:
                        ask = None

                # 代替キー（bp/ap や bid1Price/ask1Price 等）にも対応
                for k in ("bp", "bid1Price", "bestBidPrice"):
                    if bid is None and d.get(k) is not None:
                        try:
                            bid = float(d[k])
                        except Exception:
                            pass
                for k in ("ap", "ask1Price", "bestAskPrice"):
                    if ask is None and d.get(k) is not None:
                        try:
                            ask = float(d[k])
                        except Exception:
                            pass

                # Prime price-scale once using REST ticker (normalize testnet scale)
                try:
                    data_gateway = getattr(self, "_data", None)
                    price_scale = getattr(data_gateway, "_price_scale", {}) if data_gateway else {}
                    needs_prime = False
                    if isinstance(price_scale, dict):
                        needs_prime = price_scale.get(symbol) in (None, 1.0)
                    if needs_prime and data_gateway is not None:
                        # Use spot as anchor and current raw mid to derive scale
                        anchor_spot = None
                        try:
                            anchor_spot = await self.get_ticker(f"{symbol}_SPOT")
                        except Exception:
                            anchor_spot = None
                        ref_raw = None
                        try:
                            if bid is not None and ask is not None:
                                ref_raw = (float(bid) + float(ask)) / 2.0
                            elif bid is not None:
                                ref_raw = float(bid)
                            elif ask is not None:
                                ref_raw = float(ask)
                        except Exception:
                            ref_raw = None
                        if anchor_spot and ref_raw and ref_raw > 0:
                            ratio = ref_raw / anchor_spot
                            if ratio > 2.0 or ratio < 0.5:
                                try:
                                    if hasattr(data_gateway, "_price_scale") and isinstance(
                                        data_gateway._price_scale, dict
                                    ):
                                        data_gateway._price_scale[symbol] = float(anchor_spot) / float(ref_raw)
                                except Exception:
                                    pass
                        # Also call gateway.get_ticker twice to update readiness counters
                        if hasattr(data_gateway, "get_ticker"):
                            await data_gateway.get_ticker(symbol)
                            await data_gateway.get_ticker(symbol)
                except Exception:
                    pass

                # Apply scale to WS prices if available
                try:
                    data_gateway = getattr(self, "_data", None)
                    scale = 1.0
                    if data_gateway and hasattr(data_gateway, "_price_scale"):
                        sc = getattr(data_gateway, "_price_scale", {}).get(symbol, 1.0)
                        try:
                            scale = float(sc)
                        except Exception:
                            scale = 1.0
                    if bid is not None:
                        bid = float(bid) * scale
                    if ask is not None:
                        ask = float(ask) * scale
                except Exception:
                    pass

                # Apply Bitget price scale adjustment from gateway
                scale = 1.0
                data_gateway = getattr(self, "_data", None)
                try:
                    if data_gateway and hasattr(data_gateway, "_price_scale"):
                        scale = getattr(data_gateway, "_price_scale", {}).get(symbol, 1.0)
                    scale = float(scale)
                except Exception:
                    scale = 1.0
                if bid is not None:
                    try:
                        bid = float(bid) * scale
                    except Exception:
                        pass
                if ask is not None:
                    try:
                        ask = float(ask) * scale
                    except Exception:
                        pass
                # BBOを更新（どちらか一方だけ得られた場合は既存値を維持）
                async with self._lock:
                    if bid is not None or ask is not None:
                        prev_bid, prev_ask = self._bbo.get(symbol, (None, None))
                        self._bbo[symbol] = (
                            bid if bid is not None else prev_bid,
                            ask if ask is not None else prev_ask,
                        )

            # 指値の板内チェック（BBOが未更新でも安全に呼べる）
            await self._try_fill_limits(symbol)

        elif topic.startswith("publicTrade."):
            symbol = topic.split(".")[-1]
            trades = msg.get("data") or []
            if trades:
                price = float(trades[-1]["p"])
                scale = 1.0
                data_gateway = getattr(self, "_data", None)
                try:
                    if data_gateway and hasattr(data_gateway, "_price_scale"):
                        scale = getattr(data_gateway, "_price_scale", {}).get(symbol, 1.0)
                    scale = float(scale)
                except Exception:
                    scale = 1.0
                price *= scale
                async with self._lock:
                    self._last_price[symbol] = price

        # Bitget Public WS の場合（topic が空で arg.channel が使われる）
        if not topic:
            try:
                arg = msg.get("arg") or {}
                channel = (arg.get("channel") or "").lower()
                inst_id = arg.get("instId") or arg.get("instID")
                # order book: books/books1/books5/books15
                if channel in {"books", "books1", "books5", "books15"} and inst_id:
                    data_obj = msg.get("data") or []
                    item = None
                    if isinstance(data_obj, list):
                        item = data_obj[0] if data_obj else None
                    elif isinstance(data_obj, dict):
                        item = data_obj
                    if item:
                        bids = item.get("bids") or []
                        asks = item.get("asks") or []
                        bid = None
                        ask = None
                        if isinstance(bids, list) and bids:
                            try:
                                bid = float(bids[0][0])
                            except Exception:
                                bid = None
                        if isinstance(asks, list) and asks:
                            try:
                                ask = float(asks[0][0])
                            except Exception:
                                ask = None
                        async with self._lock:
                            prev_bid, prev_ask = self._bbo.get(inst_id, (None, None))
                            self._bbo[inst_id] = (
                                bid if bid is not None else prev_bid,
                                ask if ask is not None else prev_ask,
                            )
                        # Bitget でも同様に、orderbook 更新をトリガに Limit の約定判定を行う
                        await self._try_fill_limits(inst_id)
                # trade: data は [ [ts, px, sz, side], ... ]
                elif channel == "trade" and inst_id:
                    trades = msg.get("data") or []
                    if trades:
                        px = None
                        last = trades[-1]
                        if isinstance(last, list) and len(last) >= 2:
                            try:
                                px = float(last[1])
                            except Exception:
                                px = None
                        elif isinstance(last, dict):
                            val = last.get("price")
                            if val is None:
                                val = last.get("px")
                            if val is not None:
                                try:
                                    px = float(val)
                                except Exception:
                                    px = None
                        if px is not None:
                            async with self._lock:
                                self._last_price[inst_id] = px
            except Exception:
                pass

    # ---------- 内部：約定・ポジション/残高反映 ----------

    async def _is_limit_crossing(self, req: OrderRequest) -> bool:
        """これは何をする関数？
        → 指値が板内に入っているかを判定します（Buy: price>=ask / Sell: price<=bid）。
        """

        bid, ask = self._bbo.get(req.symbol, (None, None))
        if bid is None or ask is None or req.price is None:
            return False
        if req.side.lower() == "buy":
            return req.price >= ask
        return req.price <= bid

    async def _price_for_market(self, symbol: str, side: str) -> float:
        """これは何をする関数？→ Market の約定価格（Buy→Ask / Sell→Bid / 無ければmid/last）。"""

        bid, ask = self._bbo.get(symbol, (None, None))
        if side.lower() == "buy":
            if ask is not None:
                return ask
        else:
            if bid is not None:
                return bid
        if bid is not None and ask is not None:
            return (bid + ask) / 2.0
        return self._last_price.get(symbol, 0.0)

    async def _price_for_limit_fill(self, symbol: str, side: str) -> float:
        """これは何をする関数？→ Limit 成立時の約定価格（Buy→Ask / Sell→Bid）。"""

        bid, ask = self._bbo.get(symbol, (None, None))
        if side.lower() == "buy":
            return ask if ask is not None else self._last_price.get(symbol, 0.0)
        return bid if bid is not None else self._last_price.get(symbol, 0.0)

    async def _try_fill_limits(self, symbol: str) -> None:
        """これは何をする関数？
        → 指定シンボルの未約定指値を走査し、板内に入っていれば即時に全部約定させます（MVP）。
        """

        async with self._lock:
            candidates = [
                po
                for po in self._orders.values()
                if po.req.symbol == symbol and po.status == "new" and po.req.type.lower() == "limit"
            ]

        for po in candidates:
            if await self._is_limit_crossing(po.req):
                price = await self._price_for_limit_fill(po.req.symbol, po.req.side)
                await self._fill_now(po, fill_qty=po.req.qty, price=price, final_status="filled")

    async def _fill_now(self, po: _PaperOrder, *, fill_qty: float, price: float, final_status: str) -> None:
        """これは何をする関数？
        → ローカル注文を指定数量・価格で即時に約定させ、ポジション/残高を更新し、OMSへイベント通知します。
        """

        async with self._lock:
            po.filled_qty += float(fill_qty)
            po.avg_price = float(price) if po.avg_price is None else (po.avg_price + float(price)) / 2.0
            po.status = final_status

        # ポジション・残高へ反映
        await self._apply_fill_effects(symbol=po.req.symbol, side=po.req.side, qty=fill_qty, price=price)

        # OMS へ 約定イベントを通知
        if self._oms:
            await self._oms.on_execution_event(
                {
                    "client_id": po.client_id,
                    "status": "filled",
                    "last_filled_qty": float(fill_qty),
                    "cum_filled_qty": float(po.filled_qty),
                    "avg_fill_price": float(price),
                }
            )

    async def _apply_fill_effects(self, *, symbol: str, side: str, qty: float, price: float) -> None:
        """これは何をする関数？
        → 約定結果を現物バランス/先物ポジションへ反映します。
           - *_SPOT: USDT と ベース資産を増減
           - perp  : long/short ポジションの size / entry_price を更新（UPnLはMVPでは0のまま）
        """

        # 現物と先物を分岐
        if symbol.endswith("_SPOT"):
            core = symbol[:-5]  # "BTCUSDT"
            base = core[:-4]
            usdt_delta = -qty * price if side.lower() == "buy" else qty * price
            base_delta = qty if side.lower() == "buy" else -qty

            async with self._lock:
                # USDT
                us = self._balances.get("USDT")
                if not us:
                    us = Balance(asset="USDT", total=0.0, available=0.0)
                    self._balances["USDT"] = us
                us.total += usdt_delta
                us.available += usdt_delta

                # BASE
                bs = self._balances.get(base.upper())
                if not bs:
                    bs = Balance(asset=base.upper(), total=0.0, available=0.0)
                    self._balances[base.upper()] = bs
                bs.total += base_delta
                bs.available += base_delta
            return

        # perp（線形USDT想定）
        side_norm = "long" if side.lower() == "buy" else "short"
        async with self._lock:
            # 既存 side のポジションを探す（なければ作る）
            pos = None
            for p in self._positions:
                if p.symbol.replace("/", "").replace(":USDT", "") == symbol and p.side == side_norm:
                    pos = p
                    break
            if not pos:
                pos = Position(symbol=symbol, side=side_norm, size=0.0, entry_price=0.0, unrealized_pnl=0.0)
                self._positions.append(pos)

            # 加重平均で entry_price を更新
            new_size = pos.size + float(qty)
            if new_size > 0:
                pos.entry_price = (
                    (pos.entry_price * pos.size + float(qty) * float(price)) / new_size
                    if pos.size > 0
                    else float(price)
                )
            pos.size = new_size
            pos.unrealized_pnl = 0.0  # MVPでは常に0とする
