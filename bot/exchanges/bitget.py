"""Bitget UTA 向けの最小ゲートウェイ実装。

- REST（ccxt）を中心に、残高/ポジション/注文/BBO/Funding を提供する。
- WS は現状未実装（Private/ Public とも）。Bitget 用の WS ロジックとは分離して扱う。
"""

from __future__ import annotations

import asyncio
import base64
import hmac
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Awaitable, Callable, Dict

import ccxt.async_support as ccxt  # type: ignore[import-untyped]
import websockets
from loguru import logger as loguru_logger

from bot.core.errors import ExchangeError, WsDisconnected
from bot.core.retry import retryable
from bot.core.time import parse_exchange_ts, utc_now

from .base import ExchangeGateway
from .types import Balance, FundingInfo, Order, OrderRequest, Position

logger = logging.getLogger(__name__)


def _safe_float(val: Any) -> float | None:
    try:
        return float(val) if val is not None else None
    except Exception:
        return None


def _ws_inst_type(symbol: str) -> str:
    """Bitget v2 WS instType を内部シンボルから決める。"""

    # perp（USDT建て）のみサポート想定。現物は _SPOT 接尾辞を利用。
    if symbol.endswith("_SPOT"):
        return "SPOT"
    return "USDT-FUTURES"


async def _bitget_subscribe_private_impl(
    gw: "BitgetGateway",
    callbacks: dict[str, Callable[[dict], Awaitable[None]]],
) -> None:
    """Bitget Private WS に接続し、orders/positions を購読してコールバックを呼び出す内部実装。"""

    url = gw._ws_private_url
    idle_timeout = getattr(gw, "_ws_idle_timeout", 60.0)
    api_key = gw._auth.api_key
    api_secret = gw._auth.api_secret
    passphrase = getattr(gw, "_ws_passphrase", None) or os.getenv("KEYS__PASSPHRASE") or ""

    if not (api_key and api_secret and passphrase):
        raise WsDisconnected("Bitget private WS requires api_key, api_secret and passphrase")

    args: list[dict[str, str]] = []
    if any(k in callbacks for k in ("order", "execution")):
        args.append({"instType": "UMCBL", "channel": "orders", "instId": "default"})
    if "position" in callbacks:
        args.append({"instType": "UMCBL", "channel": "positions", "instId": "default"})

    channels_summary = ",".join(f"{a.get('channel')}:{a.get('instId')}" for a in args if a.get("channel"))
    connect_started = time.monotonic()
    last_recv: float = time.monotonic()
    loguru_logger.info(
        "bitget.ws.private.connect start url={} channels={} ping_interval={} timeout={} idle_timeout={}",
        url,
        channels_summary or "orders/execution",
        gw._ws_ping_interval,
        gw._ws_ping_timeout,
        idle_timeout,
    )

    try:
        async with websockets.connect(
            url,
            ping_interval=gw._ws_ping_interval,
            ping_timeout=gw._ws_ping_timeout,
            close_timeout=5,
        ) as ws:
            last_recv = time.monotonic()
            # --- login ---
            # docs: timestamp (sec) + "GET" + "/user/verify" を HMAC-SHA256 → Base64
            ts = str(int(time.time()))
            content = ts + "GET" + "/user/verify"
            sign_bytes = hmac.new(api_secret.encode("utf-8"), content.encode("utf-8"), digestmod="sha256").digest()
            sign = base64.b64encode(sign_bytes).decode("utf-8")

            login_payload = {
                "op": "login",
                "args": [
                    {
                        "apiKey": api_key,
                        "passphrase": passphrase,
                        "timestamp": ts,
                        "sign": sign,
                    }
                ],
            }
            await ws.send(json.dumps(login_payload))
            raw = await ws.recv()
            try:
                resp = json.loads(raw)
            except Exception as e:  # noqa: BLE001
                raise WsDisconnected(f"bitget login decode failed: {e}, raw={raw!r}") from e

            if not (resp.get("event") == "login" and str(resp.get("code", "0")) in {"0", ""}):
                raise WsDisconnected(f"bitget login failed: {resp}")

            # --- subscribe ---
            if args:
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                loguru_logger.info("bitget.ws.private.subscribed channels={}", channels_summary or "orders/execution")

            async def _ping_loop() -> None:
                while True:
                    try:
                        await asyncio.sleep(gw._ws_ping_interval)
                        # Bitget WS は明示的な 'ping' / 'pong' での疎通確認
                        await ws.send("ping")
                    except Exception:
                        break

            async def _watchdog_loop() -> None:
                while True:
                    try:
                        await asyncio.sleep(max(1.0, idle_timeout / 2))
                        if (time.monotonic() - last_recv) > idle_timeout:
                            raise WsDisconnected(f"bitget private ws idle {time.monotonic() - last_recv:.1f}s")
                    except Exception:
                        break

            async def _recv_loop() -> None:
                nonlocal last_recv
                async for raw_msg in ws:
                    last_recv = time.monotonic()
                    if isinstance(raw_msg, str) and raw_msg.lower() == "pong":
                        continue
                    try:
                        msg = json.loads(raw_msg)
                    except Exception:
                        continue
                    arg = msg.get("arg") or {}
                    channel = (arg.get("channel") or "").lower()
                    if channel == "orders":
                        data_list = msg.get("data") or []
                        rows_compat: list[dict[str, Any]] = []
                        for r in data_list:
                            if not isinstance(r, dict):
                                continue
                            compat = {
                                "orderId": r.get("orderId") or r.get("ordId"),
                                "orderStatus": r.get("orderStatus") or r.get("status"),
                                "orderLinkId": r.get("orderLinkId") or r.get("clOrdId"),
                                "clientOrderId": r.get("clientOrderId") or r.get("clientOid") or r.get("clOrdId"),
                                "cumExecQty": r.get("cumExecQty") or r.get("accFillSz"),
                                "cumFilledQty": r.get("cumFilledQty") or r.get("accFillSz"),
                                "avgPrice": r.get("avgPrice") or r.get("avgPx"),
                                "execQty": r.get("execQty") or r.get("fillSz"),
                                "execPrice": r.get("execPrice") or r.get("fillPx"),
                                # Bitget 特有の手数料・流動性情報を OMS 互換フィールドへマッピング
                                "execFee": r.get("execFee") or r.get("fillFee"),
                                "feeCurrency": r.get("feeCurrency") or r.get("fillFeeCcy"),
                                # execType: "T"=TAKER, "M"=MAKER
                                "liquidity": (
                                    "TAKER"
                                    if str(r.get("execType") or "").upper() == "T"
                                    else "MAKER" if str(r.get("execType") or "").upper() == "M" else None
                                ),
                                "updatedTime": r.get("updatedTime") or r.get("uTime") or r.get("cTime"),
                            }
                            row_out: dict[str, Any] = dict(r)
                            row_out.update(compat)
                            rows_compat.append(row_out)
                        msg_compat: dict[str, Any] = dict(msg)
                        if rows_compat:
                            msg_compat["data"] = rows_compat
                        # Private WS の orders push は OMS 連携の要なので、検証用フラグが有効なときだけ詳細ログを出す
                        if getattr(gw, "_debug_private_ws", False):
                            try:
                                for row in rows_compat:
                                    loguru_logger.info(
                                        (
                                            "bitget.ws.private.orders raw: "
                                            "oid={} cfq={} avg_px={} fee={} fee_ccy={} exec_type={} liq={}"
                                        ),
                                        row.get("orderId"),
                                        row.get("cumExecQty"),
                                        row.get("avgPrice"),
                                        row.get("execFee"),
                                        row.get("feeCurrency"),
                                        row.get("execType"),
                                        row.get("liquidity"),
                                    )
                            except Exception:
                                pass
                        cb = callbacks.get("order")
                        if cb is not None:
                            await cb(msg_compat)
                        exec_cb = callbacks.get("execution")
                        if exec_cb is not None:
                            await exec_cb(msg_compat)
                    elif channel == "positions":
                        pos_cb = callbacks.get("position")
                        if pos_cb is not None:
                            await pos_cb(msg)

            await asyncio.gather(_ping_loop(), _recv_loop(), _watchdog_loop())
    except Exception as e:  # noqa: BLE001
        elapsed = time.monotonic() - connect_started
        idle_gap = time.monotonic() - last_recv
        detail = f"uptime={elapsed:.1f}s channels={channels_summary or 'orders/execution'}"
        if idle_gap is not None:
            detail += f" idle_gap={idle_gap:.1f}s"
        detail += f" ping={gw._ws_ping_interval}s timeout={gw._ws_ping_timeout}s idle_timeout={idle_timeout}s"
        raise WsDisconnected(f"bitget private ws error: {e}; {detail}") from e


async def _bitget_subscribe_public_impl(
    gw: "BitgetGateway",
    symbols: list[str],
    callbacks: dict[str, Callable[[dict], Awaitable[None]]],
) -> None:
    """Bitget Public WS に接続し、ticker/books1/trade を購読してコールバックを呼び出します。"""

    url = gw._ws_public_url
    idle_timeout = getattr(gw, "_ws_idle_timeout", 60.0)

    args: list[dict[str, str]] = []
    for sym in symbols:
        inst_id = sym.replace("_SPOT", "")
        inst_type = _ws_inst_type(sym)
        if "ticker" in callbacks:
            args.append({"instType": inst_type, "channel": "ticker", "instId": inst_id})
        if "orderbook" in callbacks:
            args.append({"instType": inst_type, "channel": "books", "instId": inst_id})
        if "trade" in callbacks:
            args.append({"instType": inst_type, "channel": "trade", "instId": inst_id})

    if not args:
        return

    channels_summary = ",".join(f"{a.get('channel')}:{a.get('instId')}" for a in args if a.get("channel"))
    connect_started = time.monotonic()
    last_recv: float = time.monotonic()
    loguru_logger.info(
        "bitget.ws.public.connect start url={} channels={} ping_interval={} timeout={} idle_timeout={}",
        url,
        channels_summary,
        gw._ws_ping_interval,
        gw._ws_ping_timeout,
        idle_timeout,
    )

    try:
        async with websockets.connect(
            url,
            ping_interval=gw._ws_ping_interval,
            ping_timeout=gw._ws_ping_timeout,
            close_timeout=5,
        ) as ws:
            last_recv = time.monotonic()
            sub = {"op": "subscribe", "args": args}
            await ws.send(json.dumps(sub))
            loguru_logger.info("bitget.ws.public.subscribed channels={}", channels_summary)

            async def _ping_loop() -> None:
                while True:
                    try:
                        await asyncio.sleep(gw._ws_ping_interval)
                        await ws.send("ping")
                    except Exception:
                        break

            async def _watchdog_loop() -> None:
                while True:
                    try:
                        await asyncio.sleep(max(1.0, idle_timeout / 2))
                        if (time.monotonic() - last_recv) > idle_timeout:
                            raise WsDisconnected(f"bitget public ws idle {time.monotonic() - last_recv:.1f}s")
                    except Exception:
                        break

            async def _recv_loop() -> None:
                nonlocal last_recv
                async for raw_msg in ws:
                    last_recv = time.monotonic()
                    if isinstance(raw_msg, str) and raw_msg.lower() == "pong":
                        continue
                    try:
                        msg = json.loads(raw_msg)
                    except Exception:
                        continue
                    arg = msg.get("arg") or {}
                    channel = (arg.get("channel") or "").lower()

                    if channel == "ticker":
                        cb = callbacks.get("ticker")
                        if cb is not None:
                            await cb(msg)
                    elif channel in {"books", "books1", "books5", "books15"}:
                        cb = callbacks.get("orderbook")
                        if cb is not None:
                            await cb(msg)
                    elif channel == "trade":
                        cb = callbacks.get("trade")
                        if cb is not None:
                            await cb(msg)

            await asyncio.gather(_ping_loop(), _recv_loop(), _watchdog_loop())
    except Exception as e:  # noqa: BLE001
        elapsed = time.monotonic() - connect_started
        idle_gap = time.monotonic() - last_recv
        detail = f"uptime={elapsed:.1f}s channels={channels_summary}"
        if idle_gap is not None:
            detail += f" idle_gap={idle_gap:.1f}s"
        detail += f" ping={gw._ws_ping_interval}s timeout={gw._ws_ping_timeout}s idle_timeout={idle_timeout}s"
        raise WsDisconnected(f"bitget public ws error: {e}; {detail}") from e


@dataclass
class _Auth:
    """API キーなどの認証情報を保持するだけの単純な構造体。"""

    api_key: str
    api_secret: str
    environment: str  # "mainnet" など。Bitget では現状メモ用途。


class BitgetGateway(ExchangeGateway):
    """Bitget (UTA) 用 ExchangeGateway 実装（REST 中心の MVP）。"""

    _ws_public_url: str = "wss://ws.bitget.com/v2/ws/public"
    _ws_private_url: str = "wss://ws.bitget.com/mix/v1/stream"
    _ws_ping_interval: float = 20.0
    _ws_ping_timeout: float = 10.0
    _ws_idle_timeout: float = 60.0

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        environment: str = "mainnet",
        passphrase: str | None = None,
        use_uta: bool = False,
    ) -> None:
        """環境・API キーを受け取り、ccxt クライアントなどを初期化する。"""

        self._auth = _Auth(api_key=api_key, api_secret=api_secret, environment=environment)
        # Private WS ログイン用パスフレーズ（Bitget API 作成時に設定する）
        self._ws_passphrase: str | None = passphrase or os.getenv("KEYS__PASSPHRASE")
        self._log = logging.getLogger(__name__)
        self._debug_private_ws: bool = False
        self._ws_public_url = os.getenv("WS_PUBLIC_URL") or self._ws_public_url
        self._ws_private_url = os.getenv("WS_PRIVATE_URL") or self._ws_private_url
        self._ws_ping_interval = _safe_float(os.getenv("WS_PING_INTERVAL")) or self._ws_ping_interval
        self._ws_ping_timeout = _safe_float(os.getenv("WS_PING_TIMEOUT")) or self._ws_ping_timeout
        self._ws_idle_timeout = _safe_float(os.getenv("WS_IDLE_TIMEOUT")) or self._ws_idle_timeout

        # --- REST (ccxt) ---
        # Bitget は defaultType="swap", options["uta"]=True で UTA を使う前提。
        self._ccxt = ccxt.bitget(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "password": self._ws_passphrase or "",
                "enableRateLimit": True,
                "options": {
                    "defaultType": "swap",
                    # UTA（Unified Trading Account）を前提とする。
                    "uta": bool(use_uta),
                },
            }
        )

        # REST 並列実行と簡易サーキットブレーカー（必要に応じて拡張）
        self._rest_max_concurrency: int = 4
        self._rest_semaphore = asyncio.Semaphore(self._rest_max_concurrency)

        # マーケット定義・スケール情報のキャッシュ
        self._markets: dict[str, dict] = {}
        self._markets_loaded_at: datetime | None = None

        # {internal_symbol: {...}} 形式のスケールキャッシュ
        # internal_symbol は "BTCUSDT" / "BTCUSDT_SPOT" のような表記を想定。
        self._scale_cache: Dict[str, Dict[str, float]] = {}

        # BBO / 価格状態の簡易キャッシュ（Bitget 実装に寄せた形）
        self._bbo_cache: Dict[str, dict] = {}
        self._bbo_max_age_ms: int = 3000
        self._price_state: Dict[str, str] = {}  # {symbol: "READY" | "UNKNOWN" など}

    # ---------- 内部ユーティリティ ----------

    @staticmethod
    def _to_ccxt_symbol(internal: str) -> tuple[str, str]:
        """内部表記（BTCUSDT / BTCUSDT_SPOT）を ccxt の symbol に変換する。

        戻り値: (ccxt_symbol, kind)  # kind: "swap" / "spot"
        """

        if internal.endswith("_SPOT"):
            core = internal[:-5]
            base, quote = core[:-4], core[-4:]
            return f"{base}/{quote}", "spot"
        base, quote = internal[:-4], internal[-4:]
        # Bitget perp は "BTC/USDT:USDT" のような表記
        return f"{base}/{quote}:{quote}", "swap"

    async def _rest_call(self, func, *args, **kwargs) -> Any:
        """ccxt の REST 呼び出しをセマフォで保護する薄いラッパー。"""

        async with self._rest_semaphore:
            return await func(*args, **kwargs)

    async def _ensure_markets(self) -> None:
        """load_markets を 1 度だけ実行しキャッシュする。"""

        if self._markets:
            return
        self._markets = await self._rest_call(self._ccxt.load_markets)
        self._markets_loaded_at = utc_now()

    async def _prime_scale_from_markets(self, symbol: str) -> None:
        """ccxt のマーケット定義から priceScale/qtyStep/minNotional を取り出して _scale_cache に反映する。"""

        await self._ensure_markets()

        core = symbol.replace("_SPOT", "")
        base, quote = core[:-4], core[-4:]

        perp_key = f"{base}/{quote}:{quote}"
        spot_key = f"{base}/{quote}"

        perp = self._markets.get(perp_key) or {}
        spot = self._markets.get(spot_key) or {}

        info: Dict[str, float] = dict(self._scale_cache.get(core) or {})

        # perp 側
        if perp:
            prec = perp.get("precision") or {}
            limits = perp.get("limits") or {}
            # 価格スケール
            price_step_raw = prec.get("price")
            price_step = _safe_float(price_step_raw)
            if price_step is not None and price_step != 0:
                try:
                    scale = max(0, -int(round(Decimal(str(price_step)).log10())))
                except Exception:
                    scale = None
                if scale is not None:
                    info["priceScale"] = float(scale)
                    info["tickSize"] = float(price_step)
            # 量スケール
            qty_step_raw = prec.get("amount") or (limits.get("amount") or {}).get("min")
            qty_step = _safe_float(qty_step_raw)
            if qty_step is not None and qty_step != 0:
                info["qtyStep_perp"] = float(qty_step)
                info.setdefault("minQty_perp", float(qty_step))
            min_notional_raw = (limits.get("cost") or {}).get("min")
            min_notional = _safe_float(min_notional_raw)
            if min_notional is not None and min_notional != 0:
                info["minNotional_perp"] = float(min_notional)

        # spot 側
        if spot:
            prec = spot.get("precision") or {}
            limits = spot.get("limits") or {}
            price_step_raw = prec.get("price")
            price_step = _safe_float(price_step_raw)
            if price_step is not None and price_step != 0:
                try:
                    scale = max(0, -int(round(Decimal(str(price_step)).log10())))
                except Exception:
                    scale = None
                if scale is not None and "priceScale" not in info:
                    info["priceScale"] = float(scale)
                    info["tickSize"] = float(price_step)
            qty_step_raw = prec.get("amount") or (limits.get("amount") or {}).get("min")
            qty_step = _safe_float(qty_step_raw)
            if qty_step is not None and qty_step != 0:
                info["qtyStep_spot"] = float(qty_step)
                info.setdefault("minQty_spot", float(qty_step))
            min_notional_raw = (limits.get("cost") or {}).get("min")
            min_notional = _safe_float(min_notional_raw)
            if min_notional is not None and min_notional != 0:
                info["minNotional_spot"] = float(min_notional)

        if info:
            self._scale_cache[core] = info
            try:
                loguru_logger.info(
                    "bitget.scale.prime.ok sym={} core={} scale={} keys={}",
                    symbol,
                    core,
                    info.get("priceScale"),
                    sorted(self._scale_cache.keys()),
                )
            except Exception:
                pass

    @staticmethod
    def _order_status_from_ccxt(s: str) -> str:
        """ccxt の status を内部表記に寄せる。"""

        s = (s or "").lower()
        mapping = {
            "open": "new",
            "closed": "filled",
            "canceled": "canceled",
            "cancelled": "canceled",
            "rejected": "rejected",
            "expired": "canceled",
        }
        return mapping.get(s, s)

    # ---------- ExchangeGateway: 情報系 ----------

    @retryable()
    async def get_balances(self) -> list[Balance]:
        """残高一覧を返す（0 の資産は除外）。"""

        try:
            bal = await self._rest_call(self._ccxt.fetch_balance)
            out: list[Balance] = []
            total_map = bal.get("total") or {}
            free_map = bal.get("free") or {}
            for asset, total in total_map.items():
                try:
                    total_f = float(total)
                except Exception:
                    continue
                if total_f == 0:
                    continue
                available = float(free_map.get(asset, 0) or 0)
                out.append(Balance(asset=asset, total=total_f, available=available))
            return out
        except Exception as e:  # noqa: BLE001
            msg = str(e).lower()
            # 認証失敗系は「残高ゼロ」とみなしてスルー（ops-check の事前確認に任せる）
            if "api key" in msg or "authentication" in msg or "auth" in msg:
                return []
            raise ExchangeError(f"bitget.get_balances failed: {e}") from e

    @retryable()
    async def get_positions(self) -> list[Position]:
        """現在のポジション一覧を返す（サイズ 0 は除外）。"""

        try:
            # Bitget は symbol 指定なしで全ポジション取得可能。
            pos_list = await self._rest_call(self._ccxt.fetch_positions)
            out: list[Position] = []
            for p in pos_list or []:
                size = float(p.get("contracts") or p.get("info", {}).get("openTotalPos") or 0.0)
                if size == 0:
                    continue
                side_raw = (p.get("side") or "").lower()
                side = "long" if (side_raw == "long" or size > 0) else "short"
                entry = float(p.get("entryPrice") or 0.0)
                upnl = float(p.get("unrealizedPnl") or p.get("info", {}).get("unrealizedPL") or 0.0)
                sym = p.get("symbol") or ""
                internal_symbol = sym.replace("/", "").replace(":USDT", "").replace(":USDC", "")
                out.append(
                    Position(
                        symbol=internal_symbol,
                        side=side,
                        size=abs(size),
                        entry_price=entry,
                        unrealized_pnl=upnl,
                    )
                )
            return out
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"bitget.get_positions failed: {e}") from e

    @retryable()
    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """開いている注文一覧を返す（シンボル指定時はその銘柄のみ）。"""

        try:
            if symbol:
                ccxt_sym, _kind = self._to_ccxt_symbol(symbol)
                oo = await self._rest_call(self._ccxt.fetch_open_orders, ccxt_sym)
            else:
                oo = await self._rest_call(self._ccxt.fetch_open_orders)
            out: list[Order] = []
            for o in oo or []:
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
                        order_id=str(o.get("id") or o.get("orderId") or ""),
                        client_id=o.get("clientOrderId") or o.get("clientOid"),
                        client_order_id=o.get("clientOrderId") or o.get("clientOid"),
                        status=self._order_status_from_ccxt(o.get("status") or ""),
                        filled_qty=float(o.get("filled", 0.0)),
                        avg_fill_price=float(o.get("average", 0.0)) if o.get("average") is not None else None,
                    )
                )
            return out
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"bitget.get_open_orders failed: {e}") from e

    @retryable()
    async def get_ticker(self, symbol: str) -> float:
        """銘柄の現在価格（mid 優先）を返す。"""

        # まず BBO キャッシュ / REST orderbook から mid を試みる。
        bid_s, ask_s = await self.get_bbo(symbol)
        try:
            bid = float(bid_s) if bid_s is not None else None
            ask = float(ask_s) if ask_s is not None else None
        except Exception:
            bid = ask = None
        if bid is not None and ask is not None and bid > 0 and ask > 0:
            return (bid + ask) / 2.0

        # フォールバック: REST ticker の last
        try:
            ccxt_sym, _kind = self._to_ccxt_symbol(symbol)
            t = await self._rest_call(self._ccxt.fetch_ticker, ccxt_sym)
        except Exception:
            return 0.0
        last = t.get("last")
        try:
            return float(last) if last is not None else 0.0
        except Exception:
            return 0.0

    # ---------- BBO / Funding ----------

    @retryable()
    async def get_bbo(self, symbol: str) -> tuple[str | None, str | None]:
        """Bitget の BBO を REST orderbook から取得して返す。"""

        now_ms = int(utc_now().timestamp() * 1000)
        cached = self._bbo_cache.get(symbol)
        if cached:
            ts = int(cached.get("ts") or 0)
            if (now_ms - ts) <= self._bbo_max_age_ms:
                bid = cached.get("bid")
                ask = cached.get("ask")
                return (str(bid) if bid is not None else None, str(ask) if ask is not None else None)

        await self._prime_scale_from_markets(symbol)

        try:
            ccxt_sym, _kind = self._to_ccxt_symbol(symbol)
            ob = await self._rest_call(self._ccxt.fetch_order_book, ccxt_sym, 1)
        except Exception:
            return (None, None)

        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        bid = float(bids[0][0]) if bids else None
        ask = float(asks[0][0]) if asks else None

        self._bbo_cache[symbol] = {"bid": bid, "ask": ask, "ts": now_ms}
        if bid is not None or ask is not None:
            self._price_state[symbol] = "READY"
        else:
            self._price_state.setdefault(symbol, "UNKNOWN")

        return (str(bid) if bid is not None else None, str(ask) if ask is not None else None)

    def update_bbo(
        self,
        symbol: str,
        bid: str | float | None,
        ask: str | float | None,
        ts: int | str | None = None,
    ) -> None:
        """Public WS から取得した BBO をキャッシュするためのヘルパー。"""

        try:
            bid_val = float(bid) if bid is not None else None
        except Exception:
            bid_val = None
        try:
            ask_val = float(ask) if ask is not None else None
        except Exception:
            ask_val = None

        self._bbo_cache[symbol] = {"bid": bid_val, "ask": ask_val, "ts": ts}
        if bid_val is not None or ask_val is not None:
            self._price_state[symbol] = "READY"

    @retryable()
    async def get_funding_info(self, symbol: str) -> FundingInfo:
        """Bitget の Funding 情報（現在レート・次回時刻）を取得する。"""

        logger.info("bitget.get_funding.call sym=%s", symbol)

        ccxt_sym, _kind = self._to_ccxt_symbol(symbol)
        try:
            fr = await self._rest_call(self._ccxt.fetch_funding_rate, ccxt_sym)
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"bitget.get_funding failed: {e}") from e

        current_rate: float | None = None
        predicted_rate: float | None = None
        next_dt: datetime | None = None
        interval_hours: int | None = None

        try:
            if fr.get("fundingRate") is not None:
                current_rate = float(fr["fundingRate"])
                # Bitget は「次回」レートを別フィールドで返さないため、ひとまず current を predicted として扱う。
                predicted_rate = current_rate
            ts = fr.get("fundingTimestamp") or fr.get("nextFundingTimestamp")
            if ts is not None:
                next_dt = parse_exchange_ts(ts)
            interval = fr.get("interval")
            if isinstance(interval, str) and interval.endswith("h"):
                try:
                    interval_hours = int(interval[:-1])
                except Exception:
                    interval_hours = None
        except Exception:
            # パース失敗時は None を許容（戦略側で guard する）
            pass

        return FundingInfo(
            symbol=symbol,
            current_rate=current_rate,
            predicted_rate=predicted_rate,
            next_funding_time=next_dt,
            funding_interval_hours=interval_hours,
        )

    # ---------- ExchangeGateway: 発注/取消 ----------

    @retryable()
    async def place_order(self, req: OrderRequest) -> Order:
        """注文を発注し、作成された注文情報を返す。"""

        try:
            ccxt_sym, _kind = self._to_ccxt_symbol(req.symbol)
            params: dict[str, Any] = {}

            # timeInForce / postOnly / reduceOnly を ccxt 標準パラメータにマッピング
            if getattr(req, "post_only", False):
                params["postOnly"] = True
            tif = getattr(req, "time_in_force", None)
            if tif:
                params["timeInForce"] = tif
            if getattr(req, "reduce_only", False):
                params["reduceOnly"] = True

            coid = getattr(req, "client_order_id", None) or req.client_id
            if coid:
                params["clientOrderId"] = coid

            amount = float(req.qty)
            price = float(req.price) if (req.type or "").lower().startswith("limit") and req.price is not None else None

            created = await self._rest_call(
                self._ccxt.create_order,
                ccxt_sym,
                req.type,
                req.side,
                amount,
                price,
                params,
            )

            order_id = str(created.get("id") or created.get("orderId") or coid or "")
            status = self._order_status_from_ccxt(created.get("status") or "")
            filled = float(created.get("filled", 0.0))
            avg_price = created.get("average")
            avg_price_f = float(avg_price) if avg_price is not None else None

            return Order(
                symbol=req.symbol,
                order_id=order_id,
                client_id=coid,
                client_order_id=coid,
                status=status,
                filled_qty=filled,
                avg_fill_price=avg_price_f,
            )
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"bitget.place_order failed: {e}") from e

    @retryable()
    async def cancel_order(
        self,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> None:
        """注文をキャンセルする（order_id 優先、なければ client_order_id から検索）。"""

        ccxt_sym, _kind = self._to_ccxt_symbol(symbol)

        try:
            if order_id:
                await self._rest_call(self._ccxt.cancel_order, order_id, ccxt_sym)
                return

            if not client_order_id:
                return

            # clientOrderId から open orders を走査して id を特定する。
            oo = await self._rest_call(self._ccxt.fetch_open_orders, ccxt_sym)
            target_id: str | None = None
            for o in oo or []:
                coid = o.get("clientOrderId") or o.get("clientOid")
                if coid and str(coid) == str(client_order_id):
                    target_id = str(o.get("id") or "")
                    break
            if target_id:
                await self._rest_call(self._ccxt.cancel_order, target_id, ccxt_sym)
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"bitget.cancel_order failed: {e}") from e

    @retryable()
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
        """既存注文の価格/数量を編集する（Bitget editOrder をラップ）。"""

        ccxt_sym, _kind = self._to_ccxt_symbol(symbol)

        target_id = order_id
        if not target_id and client_order_id:
            # clientOrderId から open orders を走査して id を特定
            try:
                oo = await self._rest_call(self._ccxt.fetch_open_orders, ccxt_sym)
            except Exception:
                oo = []
            for o in oo or []:
                coid = o.get("clientOrderId") or o.get("clientOid")
                if coid and str(coid) == str(client_order_id):
                    target_id = str(o.get("id") or "")
                    break

        if not target_id:
            return

        params: dict[str, Any] = {}
        if post_only is not None:
            params["postOnly"] = bool(post_only)
        if time_in_force:
            params["timeInForce"] = time_in_force

        amount = None
        if new_qty is not None and isinstance(new_qty, (int, float, str)):
            try:
                amount = float(new_qty)
            except Exception:
                amount = None
        price = None
        if new_price is not None and isinstance(new_price, (int, float, str)):
            try:
                price = float(new_price)
            except Exception:
                price = None

        order_side = (side or "buy").lower()
        order_type = "limit"  # 価格変更の想定なので limit 固定

        try:
            await self._rest_call(
                self._ccxt.edit_order,
                target_id,
                ccxt_sym,
                order_type,
                order_side,
                amount,
                price,
                params,
            )
        except Exception as e:  # noqa: BLE001
            raise ExchangeError(f"bitget.amend_order failed: {e}") from e

    # ---------- WebSocket 互換インターフェース（MVP: 未実装） ----------

    async def subscribe_private(self, callbacks: dict[str, Callable[[dict], Awaitable[None]]]) -> None:
        """Bitget Private WS は現時点では未実装。

        - Live runner 側で Bitget のみ WS を使うようガードする。
        - 将来的に orders/account/positions チャネルをここで実装する想定。
        """

        # 実装は _subscribe_private_impl に切り出し、retryable 側から WsDisconnected を拾いやすくする
        return await _bitget_subscribe_private_impl(self, callbacks)

    async def subscribe_public(
        self,
        symbols: list[str],
        callbacks: dict[str, Callable[[dict], Awaitable[None]]],
    ) -> None:
        """Bitget Public WS は現時点では未実装。

        - Paper runner は Bitget 前提のまま利用する想定。
        - 将来的に ticker/orderbook/trades をここで実装する想定。
        """

        # 実装は _subscribe_public_impl に切り出し、retryable 側から WsDisconnected を拾いやすくする
        return await _bitget_subscribe_public_impl(self, symbols, callbacks)

    # ---------- クローズ処理 ----------

    async def close(self) -> None:
        """ccxt async クライアントを明示的にクローズする。"""

        close = getattr(self._ccxt, "close", None)
        if callable(close):
            try:
                await close()  # type: ignore[misc]
            except Exception:
                # 後始末での例外は握りつぶす
                pass
