"""これは「Bybit v5 を CCXT(REST)＋WebSocket(v5) で実装するゲートウェイ」です。"""

from __future__ import annotations

import asyncio
import hmac
import json
import time
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Awaitable, Callable

import ccxt.async_support as ccxt  # CCXT の async 版を使う（REST はこれで十分）
import websockets

from bot.core.errors import ExchangeError, RateLimitError, WsDisconnected
from bot.core.time import parse_exchange_ts

from .base import ExchangeGateway
from .types import Balance, FundingInfo, Order, OrderRequest, Position


@dataclass
class _Auth:
    api_key: str
    api_secret: str
    testnet: bool


class BybitGateway(ExchangeGateway):
    """Bybit v5 の REST/WS をまとめたゲートウェイ実装（MVP）"""

    def __init__(self, *, api_key: str, api_secret: str, environment: str = "testnet") -> None:
        """これは何をする関数？
        → 認証情報と環境を受け取り、RESTクライアントとWS接続情報を初期化します。
        """

        self._auth = _Auth(api_key=api_key, api_secret=api_secret, testnet=(environment != "mainnet"))

        # --- REST (CCXT) 初期化 ---
        self._rest = ccxt.bybit(
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
            self._rest.set_sandbox_mode(True)

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

    # ---------- REST 実装（CCXT） ----------

    async def get_balances(self) -> list[Balance]:
        """これは何をする関数？→ 残高の一覧を返す（0は省く）"""

        try:
            bal = await self._rest.fetch_balance()
            out: list[Balance] = []
            for asset, info in bal.get("total", {}).items():
                total = float(info)
                if total == 0:
                    continue
                available = float(bal.get("free", {}).get(asset, 0))
                out.append(Balance(asset=asset, total=total, available=available))
            return out
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"get_balances failed: {e}") from e

    async def get_positions(self) -> list[Position]:
        """これは何をする関数？→ デリバティブの建玉を返す（非ゼロのみ）"""

        try:
            # Bybit は symbol 指定なしでも返る。なければ空配列。
            pos_list = await self._rest.fetch_positions()
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

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """これは何をする関数？→ 指定（または全て）の未約定注文を返す"""

        try:
            if symbol:
                ccxt_sym, _kind = self._to_ccxt_symbol(symbol)
                oo = await self._rest.fetch_open_orders(symbol=ccxt_sym)
            else:
                oo = await self._rest.fetch_open_orders()
            out: list[Order] = []
            for o in oo or []:
                out.append(
                    Order(
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

    async def place_order(self, req: OrderRequest) -> Order:
        """これは何をする関数？→ 注文を発注し、作成された注文情報を返す"""

        try:
            ccxt_sym, kind = self._to_ccxt_symbol(req.symbol)
            params: dict[str, Any] = {}

            # CCXT 統一パラメータ
            if req.time_in_force:
                params["timeInForce"] = req.time_in_force
            if req.reduce_only:
                params["reduceOnly"] = True
            if req.post_only:
                params["postOnly"] = True
            if req.client_id:
                # CCXT の unified: clientOrderId（Bybit は orderLinkId にマップされる想定）
                params["clientOrderId"] = req.client_id  # TODO: 要API確認

            price = req.price if req.type == "limit" else None
            created = await self._rest.create_order(
                symbol=ccxt_sym,
                type=req.type,
                side=req.side,
                amount=req.qty,
                price=price,
                params=params,
            )
            return Order(
                order_id=str(created.get("id") or created.get("orderId")),
                client_id=created.get("clientOrderId"),
                status=self._order_status_from_ccxt(created.get("status") or "open"),
                filled_qty=float(created.get("filled", 0.0)),
                avg_fill_price=float(created.get("average", 0.0)) if created.get("average") is not None else None,
            )
        except ccxt.RateLimitExceeded as e:
            raise RateLimitError(str(e)) from e
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"place_order failed: {e}") from e

    async def cancel_order(self, order_id: str | None = None, client_id: str | None = None) -> None:
        """これは何をする関数？→ order_id か client_id の指定で注文を取り消す"""

        try:
            params: dict[str, Any] = {}
            if client_id:
                params["clientOrderId"] = client_id  # Bybitの orderLinkId にマップされる想定（要API確認）
            # CCXT は symbol が必要な場合があるため、未指定なら全キャンセル系を検討するが、MVPは注文ID系を優先
            if order_id:
                await self._rest.cancel_order(id=order_id, symbol=None, params=params)
            elif client_id:
                # 一部CCXT実装では symbol 必須のことがあるため、注文一覧から検索→cancel でも良い
                await self._rest.cancel_order(id=None, symbol=None, params=params)
            else:
                raise ExchangeError("cancel_order requires order_id or client_id")
        except ccxt.RateLimitExceeded as e:
            raise RateLimitError(str(e)) from e
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"cancel_order failed: {e}") from e

    async def get_ticker(self, symbol: str) -> float:
        """これは何をする関数？→ シンボルの近似価格（last or mark）を返す"""

        try:
            ccxt_sym, _kind = self._to_ccxt_symbol(symbol)
            t = await self._rest.fetch_ticker(ccxt_sym)
            # last を優先、なければ bid/ask の中間、さらにだめなら mark
            last = t.get("last")
            if last is not None:
                return float(last)
            bid, ask = t.get("bid"), t.get("ask")
            if bid is not None and ask is not None:
                return (float(bid) + float(ask)) / 2.0
            mark = t.get("info", {}).get("markPrice")
            if mark is not None:
                return float(mark)
            raise ExchangeError(f"ticker has no price fields: {t}")
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"get_ticker failed: {e}") from e

    async def get_funding_info(self, symbol: str) -> FundingInfo:
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
                fr = await self._rest.fetch_funding_rate(ccxt_sym)
                current_rate = float(fr.get("fundingRate")) if fr.get("fundingRate") is not None else None
                predicted_rate = float(fr.get("nextFundingRate")) if fr.get("nextFundingRate") is not None else None
                nft = fr.get("nextFundingTimestamp")
                if nft is not None:
                    next_time = parse_exchange_ts(nft)
            except Exception:
                # フォールバック：ティッカー側
                t = await self._rest.fetch_ticker(ccxt_sym)
                info = t.get("info", {})
                if "fundingRate" in info:
                    current_rate = float(info.get("fundingRate"))
                # predictedFundingRate が取れるなら使う（未提供なら None）
                if "predictedFundingRate" in info:
                    predicted_rate = float(info.get("predictedFundingRate"))
                nft = info.get("nextFundingTime")
                if nft is not None:
                    next_time = parse_exchange_ts(nft)

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

                await asyncio.gather(_ping_loop(), _recv_loop())
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
