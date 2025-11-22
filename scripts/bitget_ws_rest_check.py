from __future__ import annotations

"""Bitget Private WS の結果と REST の状態を簡易クロスチェックするスクリプト。

使い方の一例:

    python -m scripts.bitget_ws_rest_check --symbol BTCUSDT --order-id 123456789

- config は通常どおり `.env` + `config/app.yaml` から `load_config()` で読み込み。
- `order_id` / `client_order_id` に一致する REST 側の注文情報と、
  `logs/trades-*.jsonl` 内の trade_fill レコードを並べてログ出力する。
"""
# ruff: noqa: E402


import argparse
import asyncio
import json
from pathlib import Path
from typing import Any, Iterable

from loguru import logger

from bot.config.loader import load_config
from bot.exchanges.bitget import BitgetGateway


def _load_trade_records(
    *, order_id: str | None, client_order_id: str | None, logs_dir: Path = Path("logs")
) -> list[dict[str, Any]]:
    """logs/trades-*.jsonl から対象 order の trade_fill レコードを抽出する。"""

    out: list[dict[str, Any]] = []
    if not logs_dir.exists():
        return out

    # trades-YYYY-MM-DD.jsonl を新しい順に読み、該当するレコードを集める
    files: Iterable[Path] = sorted(logs_dir.glob("trades-*.jsonl"), reverse=True)
    for path in files:
        try:
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    if rec.get("event") != "trade_fill":
                        continue
                    if order_id and rec.get("order_id") == order_id:
                        out.append(rec)
                    elif client_order_id and rec.get("client_order_id") == client_order_id:
                        out.append(rec)
        except Exception:
            continue
    return out


def _summarize_trades(trades: list[dict[str, Any]]) -> dict[str, Any]:
    """trade_fill レコード群から累計サイズや平均約定価格などを集計する。"""

    if not trades:
        return {}

    total_qty = 0.0
    notional = 0.0
    total_fee = 0.0
    fee_ccy: str | None = None
    liquidity_set: set[str] = set()

    for rec in trades:
        try:
            q = float(rec.get("qty") or 0.0)
            px = float(rec.get("price") or 0.0)
            fee = float(rec.get("fee") or 0.0)
        except Exception:
            continue
        total_qty += q
        notional += q * px
        total_fee += fee
        if rec.get("fee_currency"):
            fee_ccy = str(rec.get("fee_currency"))
        if rec.get("liquidity"):
            liquidity_set.add(str(rec.get("liquidity")))

    avg_px = notional / total_qty if total_qty > 0 else 0.0

    return {
        "fills": len(trades),
        "cum_qty": total_qty,
        "avg_price": avg_px,
        "total_fee": total_fee,
        "fee_currency": fee_ccy,
        "liquidity_set": sorted(liquidity_set),
    }


async def _main_async() -> None:
    parser = argparse.ArgumentParser(description="Bitget WS/REST cross-check helper")
    parser.add_argument("--config", type=str, default=None, help="config/app.yaml のパス（省略可）")
    parser.add_argument("--symbol", required=True, help="内部シンボル（例: BTCUSDT）")
    parser.add_argument("--order-id", required=True, help="Bitget の orderId")
    parser.add_argument(
        "--client-order-id",
        type=str,
        default=None,
        help="clientOrderId / orderLinkId（任意、logs との突き合わせ用）",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)

    ex = BitgetGateway(
        api_key=cfg.keys.api_key,
        api_secret=cfg.keys.api_secret,
        environment=cfg.exchange.environment,
    )

    # --- REST 側の注文状態（fetch_order） ---
    ccxt_sym, _kind = ex._to_ccxt_symbol(args.symbol)
    rest_order: dict[str, Any] | None = None
    try:
        # 型ヒント上は private だが、検証用スクリプトなので直接利用する
        rest_order = await ex._rest_call(ex._ccxt.fetch_order, args.order_id, ccxt_sym)
    except Exception as e:  # noqa: BLE001
        logger.error("fetch_order failed: symbol={} order_id={} err={}", args.symbol, args.order_id, e)

    if rest_order is not None:
        logger.info(
            "REST.fetch_order symbol={} order_id={} status={} filled={} avg={} fee={} fee_ccy={}",
            args.symbol,
            args.order_id,
            rest_order.get("status"),
            rest_order.get("filled"),
            rest_order.get("average"),
            rest_order.get("fee", {}).get("cost") if isinstance(rest_order.get("fee"), dict) else None,
            rest_order.get("fee", {}).get("currency") if isinstance(rest_order.get("fee"), dict) else None,
        )

    # --- OMS 側 trade_fill ログの集計 ---
    trades = _load_trade_records(order_id=args.order_id, client_order_id=args.client_order_id)
    summary = _summarize_trades(trades)

    logger.info("trades.matched count={} summary={}", len(trades), summary)

    # --- open orders からの補助情報 ---
    try:
        open_orders = await ex.get_open_orders(args.symbol)
    except Exception as e:  # noqa: BLE001
        logger.warning("get_open_orders failed: symbol={} err={}", args.symbol, e)
        open_orders = []

    matched_open = [
        o
        for o in open_orders
        if str(o.order_id) == str(args.order_id)
        or (args.client_order_id and str(o.client_order_id) == str(args.client_order_id))
    ]

    for o in matched_open:
        logger.info(
            "REST.get_open_orders match symbol={} order_id={} client_order_id={} status={} filled={} avg_px={}",
            o.symbol,
            o.order_id,
            o.client_order_id,
            o.status,
            o.filled_qty,
            o.avg_fill_price,
        )

    await getattr(ex, "close", lambda: asyncio.sleep(0))()


def main() -> None:
    try:
        asyncio.run(_main_async())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
