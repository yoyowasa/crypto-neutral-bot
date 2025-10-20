"""これは「Bybit v5 を CCXT(REST)＋WebSocket(v5) で実装するゲートウェイ」です。"""

from __future__ import annotations

import asyncio
import hmac
import json
import time
from contextlib import suppress  # これは何をするimport？→ close時の例外を握りつぶして穏当に終了する
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_DOWN, Decimal  # 価格・数量を取引所ステップに正確丸めするために使う
from hashlib import sha256
from typing import Any, Awaitable, Callable

import ccxt.async_support as ccxt  # CCXT の async 版を使う（REST はこれで十分）
import websockets

from bot.core.errors import DataError, ExchangeError, RateLimitError, WsDisconnected
from bot.core.retry import retryable  # REST呼び出しに指数バックオフ再試行を付与するデコレータ
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
        # これは何をする初期化？→ ccxtの非同期クライアントを1つだけ生成し、全REST呼び出しで共有する
        # 既存の _rest インスタンスを _ccxt としても参照し、明示 close() の対象とする
        self._ccxt = self._rest
        self._symbol_category_cache: dict[str, str] = (
            {}
        )  # instruments-infoで判定したカテゴリ(spot/linear/inverse)の簡易キャッシュ
        self._instrument_info_cache: dict[str, dict] = (
            {}
        )  # instruments-infoのtickSize/qtyStep等をキャッシュしてREST前の丸めに使う
        self._bbo_cache: dict[str, dict] = (
            {}
        )  # 公開WS由来の最良気配(bid/ask/ts)をキャッシュしてPostOnly調整等で即時利用

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
            msg = str(e).lower()
            if "api key" in msg or "authentication" in msg or "auth" in msg:
                return []
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

    @retryable()  # 再接続直後のopen注文取り直しで一過性の失敗にも強くする
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
            price = float(params["price"]) if ("price" in params and params["price"] is not None) else None
            created = await self._rest.create_order(
                symbol=ccxt_sym,
                type=req.type,
                side=req.side,
                amount=float(qty_str),
                price=price,
                params=params,
            )
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
            msg = str(e).lower()
            if "api key" in msg or "authentication" in msg or "auth" in msg:
                # Fallback to a public-only CCXT client for ticker
                pub = ccxt.bybit({"enableRateLimit": True, "options": {"defaultType": "swap"}})
                try:
                    if self._auth.testnet:
                        pub.set_sandbox_mode(True)
                    t = await pub.fetch_ticker(ccxt_sym)
                    last = t.get("last")
                    if last is not None:
                        return float(last)
                    bid, ask = t.get("bid"), t.get("ask")
                    if bid is not None and ask is not None:
                        return (float(bid) + float(ask)) / 2.0
                    mark = t.get("info", {}).get("markPrice")
                    if mark is not None:
                        return float(mark)
                except Exception:
                    pass
                finally:
                    try:
                        await pub.close()
                    except Exception:
                        pass
            raise ExchangeError(f"get_ticker failed: {e}") from e

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
        まずは Ticker（早い・軽い）で取り、足りなければ履歴+仕様でやさしく補完する。
        """

        category = await self._resolve_category(symbol)  # instruments-infoで正しいカテゴリを特定してからAPIを呼ぶ
        try:
            res = await self._ccxt.publicGetV5MarketTickers({"category": category, "symbol": symbol})
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
                hist = await self._ccxt.publicGetV5MarketFundingHistory(
                    {"category": category, "symbol": symbol, "limit": 1}
                )
                hlist = (hist or {}).get("result", {}).get("list", []) or []
                last = hlist[0] if hlist else None
            except Exception:
                last = None

            interval_minutes: int | None = None
            if interval_hours is None:
                try:
                    ins = await self._ccxt.publicGetV5MarketInstrumentsInfo({"category": category, "symbol": symbol})
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

        if (predicted_rate is None) or (next_dt is None):
            raise DataError(f"Bybit funding info missing: symbol={symbol}, rate={predicted_rate}, next={next_dt}")

        return FundingInfo(
            symbol=symbol,
            current_rate=None,
            predicted_rate=predicted_rate,
            next_funding_time=next_dt,
            funding_interval_hours=interval_hours,
        )

    @retryable()  # 取消の一過性失敗に対して自動再試行（安全に諦めず数回だけ挑戦）
    async def cancel_order(self, symbol: str, order_id: str | None = None, client_order_id: str | None = None) -> None:
        """Bybit v5 の注文取消。
        - orderId があればそれを使う
        - なければ client_order_id（= orderLinkId）で確実に取り消す
        """

        category = await self._resolve_category(symbol)  # instruments-infoで正しいカテゴリを特定してからAPIを呼ぶ
        try:
            params: dict[str, Any] = {"category": category, "symbol": symbol}
            if order_id:
                params["orderId"] = order_id
            elif client_order_id:
                params["orderLinkId"] = client_order_id
            else:
                raise DataError(f"cancel_order requires order_id or client_order_id: symbol={symbol}")

            await self._ccxt.privatePostV5OrderCancel(params)
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
                res = await self._ccxt.publicGetV5MarketInstrumentsInfo({"category": cat_try, "symbol": _api_symbol})
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

        cached = self._instrument_info_cache.get(symbol)
        if cached:
            return cached

        category = await self._resolve_category(symbol)
        api_symbol = symbol[:-5] if symbol.endswith("_SPOT") else symbol
        res = await self._ccxt.publicGetV5MarketInstrumentsInfo({"category": category, "symbol": api_symbol})
        items = (res or {}).get("result", {}).get("list", []) or []
        if not items:
            raise DataError(f"instruments-info not found: symbol={symbol}")
        info = items[0]
        price_filter = info.get("priceFilter", {}) or {}
        lot_filter = info.get("lotSizeFilter", {}) or {}
        out = {
            "tick_size": price_filter.get("tickSize"),
            "min_price": price_filter.get("minPrice"),
            "max_price": price_filter.get("maxPrice"),
            "qty_step": lot_filter.get("qtyStep"),
            "min_order_qty": lot_filter.get("minOrderQty"),
            "max_order_qty": lot_filter.get("maxOrderQty"),
        }
        self._instrument_info_cache[symbol] = out
        return out

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

        # まずWSキャッシュのBBOを使い、無ければRESTで補う
        bid_str, ask_str = await self.get_bbo(symbol)
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
        return self._dec_to_str(px)

    def update_bbo(
        self, symbol: str, bid: str | float | None, ask: str | float | None, ts: int | str | None = None
    ) -> None:
        """公開WSから届いた最良気配をキャッシュする（価格は文字列でも数値でもOK）。"""
        self._bbo_cache[symbol] = {"bid": bid, "ask": ask, "ts": ts}

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
            res = await self._ccxt.publicGetV5MarketTickers({"category": category, "symbol": api_symbol})
            item = ((res or {}).get("result", {}) or {}).get("list", [])[:1]
            row = item[0] if item else {}
            bid = row.get("bid1Price")
            ask = row.get("ask1Price")
            return (str(bid) if bid is not None else None, str(ask) if ask is not None else None)
        except Exception:
            return (None, None)
