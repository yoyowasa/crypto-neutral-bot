"""BitgetのFunding履歴を直近N年分まとめてCSVに落とすユーティリティ。"""

from __future__ import annotations

import argparse
import csv
import datetime
import time
from typing import Any, Iterable, Iterator

import requests

BASE_URL = "https://api.bitget.com"  # Bitget APIのベースURL


def _iter_funding_history(
    *,
    symbol: str,
    category: str,
    years: int,
    limit: int = 100,
    max_pages: int = 100,
    sleep_seconds: float = 0.2,
    session: requests.Session | None = None,
) -> Iterator[dict[str, Any]]:
    """Bitget v3 Funding履歴APIをページングしながらyieldする。"""
    cutoff_ts_ms = int((datetime.datetime.utcnow() - datetime.timedelta(days=365 * years)).timestamp() * 1000)
    cursor = 1  # ドキュメント上は1始まり
    sess = session or requests.Session()

    while True:
        params = {
            "category": category,  # 例: "USDT-FUTURES"
            "symbol": symbol,  # 例: "BTCUSDT"
            "limit": str(limit),
            "cursor": str(cursor),
        }
        url = f"{BASE_URL}/api/v3/market/history-fund-rate"
        resp = sess.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != "00000":
            raise RuntimeError(f"Bitget API error: {data}")

        result_list: list[dict[str, Any]] = data.get("data", {}).get("resultList", []) or []
        if not result_list:
            break  # これ以上データが無い

        for row in result_list:
            ts_ms = int(row["fundingRateTimestamp"])
            if ts_ms < cutoff_ts_ms:
                return  # 3年より古いので終了
            yield row

        cursor += 1
        if cursor > max_pages:
            break  # ドキュメント上cursor最大は100ページ
        time.sleep(sleep_seconds)  # 軽いウェイトでレート制限に配慮


def fetch_bitget_funding_history(*, symbol: str, category: str, years: int = 3) -> list[dict[str, Any]]:
    """指定シンボル/カテゴリのFunding履歴を直近N年分まとめて返す。"""
    return list(_iter_funding_history(symbol=symbol, category=category, years=years))


def save_funding_to_csv(rows: Iterable[dict[str, Any]], *, symbol: str, years: int, output: str | None = None) -> str:
    """Funding履歴をCSVに書き出す。"""
    filename = output or f"{symbol}_funding_{years}y_bitget.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["symbol", "fundingRate", "fundingRateTimestamp_ms", "fundingRateTimestamp_iso"])
        for row in rows:
            ts_ms = int(row["fundingRateTimestamp"])
            ts_iso = datetime.datetime.utcfromtimestamp(ts_ms / 1000).isoformat()
            writer.writerow([row.get("symbol"), row.get("fundingRate"), ts_ms, ts_iso])
    return filename


def main() -> None:
    """CLIエントリーポイント。"""
    parser = argparse.ArgumentParser(description="Bitget Funding履歴をCSVに保存するスクリプト")
    parser.add_argument("--symbol", default="BTCUSDT", help="例: BTCUSDT, ETHUSDT")
    parser.add_argument("--category", default="USDT-FUTURES", help="Bitget v3のカテゴリ名 (例: USDT-FUTURES)")
    parser.add_argument("--years", type=int, default=3, help="取得する年数 (直近N年)")
    parser.add_argument("--output", help="出力CSVパス（未指定なら <symbol>_funding_<years>y_bitget.csv）")
    args = parser.parse_args()

    rows = fetch_bitget_funding_history(symbol=args.symbol, category=args.category, years=args.years)
    path = save_funding_to_csv(rows, symbol=args.symbol, years=args.years, output=args.output)
    print(f"Saved {len(rows)} rows to {path}")


if __name__ == "__main__":
    main()
