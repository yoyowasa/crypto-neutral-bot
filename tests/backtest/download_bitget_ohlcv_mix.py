"""Bitget先物のOHLCVを直近N年分まとめてCSVに保存するユーティリティ。"""

from __future__ import annotations

import argparse
import csv
import datetime
import time
from typing import Any, Iterable

import requests

BASE_URL = "https://api.bitget.com"  # Bitget APIのベースURL


def fetch_bitget_ohlcv_mix(
    *,
    symbol: str,
    product_type: str,
    granularity: str,
    years: int = 3,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """
    Bitget /api/v2/mix/market/history-candles を90日刻みでさかのぼり、直近N年分のOHLCVを取得する。
    granularity例: "1H", "4H", "1D"。product_type例: "USDT-FUTURES"。
    """
    now = datetime.datetime.utcnow()
    cutoff_dt = now - datetime.timedelta(days=365 * years)
    cutoff_ms = int(cutoff_dt.timestamp() * 1000)

    end_ms = int(now.timestamp() * 1000)  # 現在時刻を終端にして過去方向へ取得
    max_span_ms = 90 * 24 * 60 * 60 * 1000  # 90日分のミリ秒（API最大レンジ）

    all_rows: list[dict[str, Any]] = []
    sess = session or requests.Session()

    while end_ms > cutoff_ms:
        start_ms = max(cutoff_ms, end_ms - max_span_ms)

        params = {
            "symbol": symbol,
            "productType": product_type,
            "granularity": granularity,
            "startTime": str(start_ms),
            "endTime": str(end_ms),
            "limit": "200",  # 1回200本まで
        }
        url = f"{BASE_URL}/api/v2/mix/market/history-candles"

        resp = sess.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != "00000":
            raise RuntimeError(f"Bitget API error: {data}")

        candles = data.get("data", []) or []
        if not candles:
            end_ms = start_ms - 1  # このレンジは空だったので、さらに過去へ
            if end_ms <= cutoff_ms:
                break
            time.sleep(0.2)
            continue

        earliest_ts_ms: int | None = None
        for c in candles:
            # レスポンス形式: [0]=timestamp(ms), [1]=open, [2]=high, [3]=low, [4]=close, [5]=base量, [6]=quote量
            ts_ms = int(c[0])
            if ts_ms < cutoff_ms:
                # 取得範囲より古いものはスキップ
                continue

            ts_iso = datetime.datetime.utcfromtimestamp(ts_ms / 1000).isoformat()
            all_rows.append(
                {
                    "timestamp_ms": ts_ms,
                    "timestamp_iso": ts_iso,
                    "open": c[1],
                    "high": c[2],
                    "low": c[3],
                    "close": c[4],
                    "volume_base": c[5],
                    "volume_quote": c[6],
                }
            )

            if earliest_ts_ms is None or ts_ms < earliest_ts_ms:
                earliest_ts_ms = ts_ms

        if earliest_ts_ms is None:
            break  # 3年以内のデータが無かった

        # 次は今回の最古足より1ms前までさかのぼる
        end_ms = earliest_ts_ms - 1
        time.sleep(0.2)  # レート制限に少し配慮

    # タイムスタンプ昇順に並べ、重複を除去
    all_rows.sort(key=lambda r: r["timestamp_ms"])
    deduped: list[dict[str, Any]] = []
    seen_ts: set[int] = set()
    for r in all_rows:
        ts = int(r["timestamp_ms"])
        if ts in seen_ts:
            continue
        seen_ts.add(ts)
        deduped.append(r)

    return deduped


def save_ohlcv_to_csv(
    rows: Iterable[dict[str, Any]],
    *,
    symbol: str,
    product_type: str,
    granularity: str,
    years: int = 3,
    output: str | None = None,
) -> str:
    """取得したOHLCVをCSVに書き出す。"""
    safe_product = product_type.replace("-", "")
    filename = output or f"{symbol}_{safe_product}_{granularity}_{years}y_ohlcv_bitget.csv"

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "timestamp_ms",
                "timestamp_iso",
                "open",
                "high",
                "low",
                "close",
                "volume_base",
                "volume_quote",
            ]
        )
        for r in rows:
            writer.writerow(
                [
                    r["timestamp_ms"],
                    r["timestamp_iso"],
                    r["open"],
                    r["high"],
                    r["low"],
                    r["close"],
                    r["volume_base"],
                    r["volume_quote"],
                ]
            )

    return filename


def main() -> None:
    """CLIエントリーポイント。デフォルトはBTCUSDT/USDT-FUTURESの4Hを3年分取得。"""
    parser = argparse.ArgumentParser(description="Bitget先物OHLCVを直近N年分取得してCSV保存するスクリプト")
    parser.add_argument("--symbol", default="BTCUSDT", help="例: BTCUSDT, ETHUSDT")
    parser.add_argument("--product-type", default="USDT-FUTURES", help="Bitget mix productType (例: USDT-FUTURES)")
    parser.add_argument("--granularity", default="4H", help="足種 (例: 1H, 4H, 1D)")
    parser.add_argument("--years", type=int, default=3, help="遡る年数 (直近N年)")
    parser.add_argument("--output", help="出力CSVパス（未指定ならデフォルト命名）")
    args = parser.parse_args()

    rows = fetch_bitget_ohlcv_mix(
        symbol=args.symbol,
        product_type=args.product_type,
        granularity=args.granularity,
        years=args.years,
    )
    path = save_ohlcv_to_csv(
        rows,
        symbol=args.symbol,
        product_type=args.product_type,
        granularity=args.granularity,
        years=args.years,
        output=args.output,
    )
    print(f"Saved {len(rows)} candles to {path}")


if __name__ == "__main__":
    main()
