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

        self._data = data_source  # BybitGateway 等（REST/WSデータ専用、発注はしない）
        self._oms = None  # 後から bind_oms() でセット
        self._id_seq = 0

        # BBO/トレードのスナップショット（perp主体。spotは無ければperpで代用）
        self._bbo: dict[str, tuple[float | None, float | None]] = {}  # symbol -> (bid, ask)
        self._last_price: dict[str, float] = {}  # symbol -> last trade/mid

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
        → Funding 情報は data_source（BybitGateway/CCXT）に委譲します（実発注はしない）。
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
                    order_id=po.order_id,
                    client_id=po.client_id,
                    status="filled",
                    filled_qty=req.qty,
                    avg_fill_price=price,
                )
            # 未約定のまま残す
            return Order(
                order_id=po.order_id,
                client_id=po.client_id,
                status="new",
                filled_qty=0.0,
                avg_fill_price=None,
            )

        # 未対応タイプはエラーにしない（MVPではnewにして残す）
        logger.warning("paper: unsupported order type={}, leave as new", req.type)
        return Order(order_id=po.order_id, client_id=po.client_id, status="new", filled_qty=0.0, avg_fill_price=None)

    async def cancel_order(self, order_id: str | None = None, client_id: str | None = None) -> None:
        """これは何をする関数？→ ローカル注文を取消し、OMSへ 'canceled' を通知します。"""

        async with self._lock:
            po = None
            if client_id and client_id in self._orders:
                po = self._orders[client_id]
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

    async def handle_public_msg(self, msg: dict) -> None:
        """これは何をする関数？
        → Bybit v5 Public WS からの raw メッセージ（orderbook/publicTrade）を受け取り、BBO/last を更新し、
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
                async with self._lock:
                    self._last_price[symbol] = price

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
