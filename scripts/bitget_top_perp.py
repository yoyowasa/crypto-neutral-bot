#!/usr/bin/env python
"""
Bitget USDT-FUTURES の24h出来高上位シンボルを取得し、表示＋CSV保存するスクリプト。
依存: requests
"""
import argparse
import csv
from pathlib import Path

import requests

PRODUCT_TYPE = "USDT-FUTURES"
TOPK = 10
DEFAULT_CSV = Path("config/symbols.csv")


def main() -> None:
    parser = argparse.ArgumentParser(description="Bitget上位USDT-FUTURESのシンボルをCSVに書き出す")
    parser.add_argument("--topk", type=int, default=TOPK, help="上位何件を出力するか")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="出力先CSVパス")
    args = parser.parse_args()

    url = "https://api.bitget.com/api/v2/mix/market/tickers"
    resp = requests.get(url, params={"productType": PRODUCT_TYPE}, timeout=10)
    resp.raise_for_status()
    data = resp.json().get("data", [])

    rows = sorted(data, key=lambda r: float(r.get("quoteVolume", 0)), reverse=True)[: args.topk]
    symbols: list[str] = []
    for i, r in enumerate(rows, 1):
        sym = r.get("symbol", "")
        vol = r.get("quoteVolume")
        funding = r.get("fundingRate")
        symbols.append(sym)
        print(f"{i:02d} {sym:12s} vol_quote={vol} funding={funding}")

    # CSVにシンボルだけを書き出す
    args.csv.parent.mkdir(parents=True, exist_ok=True)
    with args.csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for sym in symbols:
            writer.writerow([sym])
    print(f"saved symbols -> {args.csv} ({len(symbols)} symbols)")


if __name__ == "__main__":
    main()
