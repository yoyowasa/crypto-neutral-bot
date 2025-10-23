from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable

from loguru import logger

from bot.config.loader import load_config


def _now_tag() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _connect_sqlite(db_url: str) -> sqlite3.Connection:
    if not db_url.startswith("sqlite"):
        raise ValueError(f"Only sqlite db_url is supported for export: {db_url}")
    # sqlite+aiosqlite:///./db/trading.db -> ./db/trading.db
    path = db_url.split("///", 1)[-1]
    return sqlite3.connect(path)


def _fetch_all(conn: sqlite3.Connection, table: str, limit: int | None = None) -> list[dict]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    sql = f"SELECT * FROM {table} ORDER BY id ASC"
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    rows = [dict(r) for r in cur.fetchall()]
    return rows


def _ensure_out_dir(path: str | os.PathLike[str]) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _write_csv(path: Path, rows: list[dict]) -> None:
    import csv

    fieldnames: list[str] = list(rows[0].keys()) if rows else []
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if fieldnames:
            w.writeheader()
        for r in rows:
            w.writerow(r)


def _write_jsonl(path: Path, rows: Iterable[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")


def _write_parquet(path: Path, rows: list[dict]) -> None:
    # Use pandas (already in dependencies) for convenience
    import pandas as pd

    df = pd.DataFrame(rows)
    df.to_parquet(path, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export SQLite tables to CSV/JSONL/Parquet")
    parser.add_argument(
        "--tables",
        nargs="*",
        default=["trade_log", "order_log", "funding_event", "position_snap", "daily_pnl"],
        help="Tables to export (default: all known)",
    )
    parser.add_argument("--fmt", choices=["csv", "jsonl", "parquet"], default="csv", help="export format")
    parser.add_argument("--out", type=str, default="reports/exports", help="output directory")
    parser.add_argument("--limit", type=int, default=None, help="max rows per table (optional)")
    parser.add_argument("--db-url", type=str, default=None, help="override db_url (default: from config)")
    args = parser.parse_args()

    cfg = load_config(None)
    db_url = args.db_url or cfg.db_url

    out_dir = _ensure_out_dir(args.out)
    tag = _now_tag()

    try:
        conn = _connect_sqlite(db_url)
    except Exception as e:  # noqa: BLE001
        logger.error("export: failed to connect to DB ({}): {}", db_url, e)
        raise

    with conn:
        for t in args.tables:
            try:
                rows = _fetch_all(conn, t, args.limit)
            except Exception as e:  # noqa: BLE001
                logger.warning("export: skip table={} err={}", t, e)
                continue

            if not rows:
                logger.info("export: table={} empty", t)
                continue

            out_path = out_dir / f"{t}-{tag}.{args.fmt}"
            if args.fmt == "csv":
                _write_csv(out_path, rows)
            elif args.fmt == "jsonl":
                _write_jsonl(out_path, rows)
            else:
                _write_parquet(out_path, rows)
            logger.info("export: wrote {} rows to {}", len(rows), out_path)


if __name__ == "__main__":
    main()
