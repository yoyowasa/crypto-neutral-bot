"""Bitgetから落としたFunding/OHLCV（3年分）をバックテスト用フィードに整形し、必要なら1日リプレイを実行するスクリプト。"""

from __future__ import annotations

import argparse
import csv
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Sequence

import yaml

# プロジェクトルートをPYTHONPATHに追加してローカルモジュールを読めるようにする
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bot.config.loader import load_config


def _load_csv_rows(path: Path) -> Iterable[dict[str, str]]:
    """CSVを読み込み、dict行をyieldする。"""
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def _parse_date(s: str) -> datetime:
    """YYYY-MM-DD を datetime (naive) にパースする。"""
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=None)


def build_price_feed(
    *,
    data_dir: Path,
    symbols: Sequence[str],
    ohlcv_pattern: str,
    out_path: Path,
    spread_bps: float = 10.0,
    spread_bps_by_symbol: dict[str, float] | None = None,
) -> int:
    """BitgetのOHLCV（closeをbid/ask/lastに流用）をまとめて価格フィードCSVにする。"""
    rows: list[tuple[int, str, float]] = []
    for sym in symbols:
        src = data_dir / ohlcv_pattern.format(symbol=sym)
        # 合成BBOの総スプレッド[bps]（例: 10bps=0.10%）。銘柄別指定があればそちらを優先。
        bps = float((spread_bps_by_symbol or {}).get(sym, spread_bps))
        if bps < 0:
            bps = 0.0
        half = (bps / 2.0) / 10_000.0
        for r in _load_csv_rows(src):
            ts = int(r["timestamp_ms"])
            close = float(r["close"])
            # 簡易スプレッドを作る（bid<ask 条件を満たすため）
            bid = close * (1.0 - half)
            ask = close * (1.0 + half)
            rows.append((ts, sym, bid, ask, close))

    rows.sort(key=lambda x: (x[0], x[1]))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ts", "symbol", "bid", "ask", "last"])
        for ts, sym, bid, ask, last in rows:
            w.writerow([ts, sym, bid, ask, last])
    return len(rows)


def build_funding_feed(
    *,
    data_dir: Path,
    symbols: Sequence[str],
    funding_pattern: str,
    out_path: Path,
    min_ts: datetime | None = None,
    max_ts: datetime | None = None,
) -> tuple[int, dict[str, int]]:
    """Bitget Funding履歴をまとめてFundingフィードCSVにする。シンボル別件数も返す。
    min_ts/max_ts を指定するとその期間に絞る（min_ts 以上、max_ts 未満）。
    """
    rows: list[tuple[int, str, float]] = []
    per_symbol: dict[str, int] = {}
    for sym in symbols:
        src = data_dir / funding_pattern.format(symbol=sym)
        for r in _load_csv_rows(src):
            ts = int(r["fundingRateTimestamp_ms"])
            rate = float(r["fundingRate"])
            ts_dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            if min_ts and ts_dt < min_ts:
                continue
            if max_ts and ts_dt >= max_ts:
                continue
            rows.append((ts, sym, rate))
            per_symbol[sym] = per_symbol.get(sym, 0) + 1

    rows.sort(key=lambda x: (x[0], x[1]))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ts", "symbol", "rate"])
        for ts, sym, rate in rows:
            ts_iso = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
            w.writerow([ts_iso, sym, rate])
    return len(rows), per_symbol


def run_backtest(
    *,
    prices: Path,
    funding: Path,
    date: str,
    step_sec: float,
    log_file: Path | None,
    append: bool = False,
    config_path: Path | None = None,
    db_url: str | None = None,
) -> None:
    """bot.backtest.replay を呼び出して1日分リプレイする。ログをファイルにも残せる。
    append=True なら既存ログに追記する。
    """
    cmd = [
        sys.executable,
        "-m",
        "bot.backtest.replay",
        "--prices",
        str(prices),
        "--funding",
        str(funding),
        "--date",
        date,
        "--step-sec",
        str(step_sec),
    ]

    if config_path is not None:
        cmd.extend(["--config", str(config_path)])
    if db_url is not None:
        cmd.extend(["--db-url", db_url])

    if log_file is None:
        subprocess.run(cmd, check=True)
        return

    log_file.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if append else "w"
    with log_file.open(mode, encoding="utf-8") as lf:
        if append:
            lf.write(f"\n===== backtest date: {date} =====\n")
        proc = subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT, text=True)
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="BitgetのFunding/OHLCV生CSVをバックテスト用フィードに変換し、任意でリプレイを実行するツール"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).parent / "backtest_config.yaml",
        help="YAML設定ファイル（未指定なら tests/backtest/backtest_config.yaml）",
    )
    parser.add_argument("--symbols", nargs="+", help="対象シンボル一覧（未指定ならconfigかデフォルト）")
    parser.add_argument(
        "--data-dir",
        default=Path(__file__).parent,
        type=Path,
        help="Funding/OHLCVの生CSVがあるディレクトリ（デフォルト: このスクリプトのある場所）",
    )
    parser.add_argument("--ohlcv-pattern", help="OHLCVファイル名パターン（{symbol}を含む文字列）")
    parser.add_argument("--funding-pattern", help="Fundingファイル名パターン（{symbol}を含む文字列）")
    parser.add_argument("--prices-out", type=Path, help="生成する価格フィードCSVの出力先")
    parser.add_argument("--funding-out", type=Path, help="生成するFundingフィードCSVの出力先")
    parser.add_argument(
        "--spread-bps",
        type=float,
        default=None,
        help="合成BBOの総スプレッド[bps]（例: 10=0.10%）。未指定ならconfigのspread_bpsかデフォルト。",
    )
    parser.add_argument(
        "--spread-bps-by-symbol",
        nargs="+",
        default=None,
        help="銘柄別の総スプレッド[bps]（例: BTCUSDT=2 ETHUSDT=4）。未指定ならconfigのspread_bps_by_symbol。",
    )
    parser.add_argument("--run-date", help="指定すると1日分バックテストを実行（UTC日付 YYYY-MM-DD）")
    parser.add_argument("--start-date", help="開始日(UTC) YYYY-MM-DD（end-dateとセットで期間実行。片方のみ指定時は同日扱い）")
    parser.add_argument("--end-date", help="終了日(UTC) YYYY-MM-DD（start-dateとセット）")
    parser.add_argument("--log-file", type=Path, help="バックテスト実行ログを保存するパス（複数日実行時はlog-dir優先）")
    parser.add_argument("--log-dir", type=Path, help="複数日実行時のログ保存ディレクトリ（未指定なら data_dir/logs）")
    parser.add_argument("--reset-db", action="store_true", help="各日付の実行前にSQLiteファイルを削除して作り直す")
    parser.add_argument("--step-sec", type=float, default=3.0, help="バックテスト時のstep間隔秒数")
    parser.add_argument(
        "--app-config",
        type=Path,
        default=Path("config/app.yaml"),
        help="バックテストランナーが読むAppConfig（strategy/risk/db_urlの参照元。未指定なら config/app.yaml）",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg_dict: dict = {}
    if args.config:
        try:
            cfg_loaded = load_config(config_path=args.config)
            cfg_dict = cfg_loaded.model_dump() if hasattr(cfg_loaded, "model_dump") else dict(cfg_loaded)
        except Exception:
            with Path(args.config).open("r", encoding="utf-8") as f:
                cfg_dict = yaml.safe_load(f) or {}

    def pick(key: str, cli_val, default):
        return cli_val if cli_val is not None else cfg_dict.get(key, default)

    def _parse_spread_map(val) -> dict[str, float] | None:
        """CLI/Configの spread_bps_by_symbol を正規化して dict にする。"""
        if val is None:
            return None
        if isinstance(val, dict):
            out: dict[str, float] = {}
            for k, v in val.items():
                try:
                    out[str(k)] = float(v)
                except Exception:
                    continue
            return out or None
        if isinstance(val, (list, tuple)):
            out: dict[str, float] = {}
            for item in val:
                s = str(item)
                if "=" not in s:
                    continue
                sym, bps = s.split("=", 1)
                try:
                    out[sym.strip()] = float(bps)
                except Exception:
                    continue
            return out or None
        return None

    data_dir = Path(pick("data_dir", args.data_dir, Path(__file__).parent))
    symbols = list(pick("symbols", args.symbols, ["BTCUSDT", "ETHUSDT", "SOLUSDT"]))
    ohlcv_pattern = pick("ohlcv_pattern", args.ohlcv_pattern, "{symbol}_USDTFUTURES_4H_3y_ohlcv_bitget.csv")
    funding_pattern = pick("funding_pattern", args.funding_pattern, "{symbol}_funding_3y_bitget.csv")
    prices_out = Path(pick("prices_out", args.prices_out, Path(__file__).parent / "prices_feed.csv"))
    funding_out = Path(pick("funding_out", args.funding_out, Path(__file__).parent / "funding_feed.csv"))
    spread_bps = float(pick("spread_bps", args.spread_bps, 10.0))
    spread_bps_by_symbol = _parse_spread_map(pick("spread_bps_by_symbol", args.spread_bps_by_symbol, None))
    run_date = pick("run_date", args.run_date, None)
    start_date = pick("start_date", args.start_date, None)
    end_date = pick("end_date", args.end_date, None)
    step_sec = float(pick("step_sec", args.step_sec, 3.0))
    log_file = pick("log_file", args.log_file, None)
    log_dir = pick("log_dir", args.log_dir, None)
    reset_db = bool(pick("reset_db", args.reset_db, False))
    db_url = pick("db_url", None, None)
    app_config = args.app_config
    # CLIで --run-date を明示した場合は、config側の start/end を無視して「単日実行」を優先する
    if args.run_date:
        start_date = None
        end_date = None

    # Fundingフィードを必要期間に絞るための境界（start/end があればその範囲、run_dateのみならその1日）
    min_ts = None
    max_ts = None
    if start_date:
        min_ts = _parse_date(start_date).replace(tzinfo=timezone.utc)
    elif run_date:
        min_ts = _parse_date(run_date).replace(tzinfo=timezone.utc)
    if end_date:
        max_ts = (_parse_date(end_date) + timedelta(days=1)).replace(tzinfo=timezone.utc)
    elif run_date:
        max_ts = (_parse_date(run_date) + timedelta(days=1)).replace(tzinfo=timezone.utc)

    prices_count = build_price_feed(
        data_dir=data_dir,
        symbols=symbols,
        ohlcv_pattern=ohlcv_pattern,
        out_path=prices_out,
        spread_bps=spread_bps,
        spread_bps_by_symbol=spread_bps_by_symbol,
    )
    funding_count = build_funding_feed(
        data_dir=data_dir,
        symbols=symbols,
        funding_pattern=funding_pattern,
        out_path=funding_out,
        min_ts=min_ts,
        max_ts=max_ts,
    )
    funding_total, funding_per_symbol = funding_count
    print(f"prices_feed: {prices_out} rows={prices_count}")
    print(f"funding_feed: {funding_out} rows={funding_total} per_symbol={funding_per_symbol}")

    # バックテスト実行開始時刻をログファイル名に含めて、期間名と組み合わせる
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    def _reset_sqlite(db_url: str) -> None:
        """sqlite/aiosqlite のファイルを削除して再生成できるようにする。"""
        if not db_url.startswith("sqlite"):
            print(f"skip reset-db (非SQLite): {db_url}")
            return
        # sqlite+aiosqlite:///./db/trading.db 形式を想定
        path_part = db_url.split("///")[-1]
        db_path = Path(path_part).resolve()
        if db_path.exists():
            db_path.unlink()
            print(f"reset db: removed {db_path}")
        else:
            print(f"reset db: file not found {db_path}, ok to create new")

    if start_date or end_date:
        sd = start_date or end_date
        ed = end_date or start_date
        if sd is None or ed is None:
            raise ValueError("start-date/end-dateの指定が不正です")
        d0 = _parse_date(sd).date()
        d1 = _parse_date(ed).date()
        if d1 < d0:
            raise ValueError("end-date は start-date 以降の日付を指定してください")
        ld = log_dir or (data_dir / "logs")
        ld.mkdir(parents=True, exist_ok=True)
        range_label = f"{d0.isoformat()}_to_{d1.isoformat()}__run_{run_ts}"
        log_file_path = ld / f"backtest_{range_label}.log"
        cur = d0
        while cur <= d1:
            day = cur.isoformat()
            if reset_db:
                _reset_sqlite(db_url or "sqlite+aiosqlite:///./db/trading.db")
            run_backtest(
                prices=prices_out,
                funding=funding_out,
                date=day,
                step_sec=step_sec,
                log_file=log_file_path,
                append=True,
                config_path=app_config,
                db_url=db_url,
            )
            print(f"backtest log saved to: {log_file_path}")
            cur += timedelta(days=1)
    elif run_date:
        if log_file is None:
            log_file = data_dir / f"backtest_{run_date}__run_{run_ts}.log"
        if reset_db:
            _reset_sqlite(db_url or "sqlite+aiosqlite:///./db/trading.db")
        run_backtest(
            prices=prices_out,
            funding=funding_out,
            date=run_date,
            step_sec=step_sec,
            log_file=log_file,
            config_path=app_config,
            db_url=db_url,
        )
        print(f"backtest log saved to: {log_file}")


if __name__ == "__main__":
    main()
