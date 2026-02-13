"""バックテスト結果（SQLite DBの funding_event/trade_log）を日次・累積PnLで可視化するスクリプト。"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine

from bot.config.loader import load_config


def load_frames(db_url: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """funding_event と trade_log を読み込んで DataFrame を返す。"""
    eng = create_engine(db_url.replace("+aiosqlite", ""))  # pandasはsyncドライバでOK
    df_funding = pd.read_sql_query("SELECT ts, symbol, rate, notional, realized_pnl FROM funding_event", eng)
    df_trades = pd.read_sql_query("SELECT ts, symbol, side, qty, price, fee FROM trade_log", eng)
    if not df_funding.empty:
        df_funding["ts"] = pd.to_datetime(df_funding["ts"], utc=True)
    if not df_trades.empty:
        df_trades["ts"] = pd.to_datetime(df_trades["ts"], utc=True)
    return df_funding, df_trades


def filter_range(df: pd.DataFrame, start: str | None, end: str | None) -> pd.DataFrame:
    """日付範囲でフィルタ（ts列がある前提）。"""
    if df.empty:
        return df
    if start:
        s = pd.to_datetime(start, utc=True)
        if s.tzinfo is None:
            s = s.tz_localize("UTC")
        df = df[df["ts"] >= s]
    if end:
        e = pd.to_datetime(end, utc=True)
        if e.tzinfo is None:
            e = e.tz_localize("UTC")
        df = df[df["ts"] < e + pd.Timedelta(days=1)]
    return df


def compute_daily(df_funding: pd.DataFrame, df_trades: pd.DataFrame) -> pd.DataFrame:
    """日次の funding実現PnL と手数料をまとめ、netと累積を返す。"""
    if df_funding.empty and df_trades.empty:
        return pd.DataFrame(columns=["date", "funding", "fees", "net", "cum_net"])

    parts: list[pd.DataFrame] = []
    if not df_funding.empty:
        tmp = df_funding.copy()
        tmp["date"] = tmp["ts"].dt.date
        parts.append(tmp.groupby("date")["realized_pnl"].sum().rename("funding"))
    if not df_trades.empty:
        tmp = df_trades.copy()
        tmp["date"] = tmp["ts"].dt.date
        parts.append(tmp.groupby("date")["fee"].sum().rename("fees"))

    df = pd.concat(parts, axis=1).fillna(0.0)
    df["net"] = df.get("funding", 0.0) - df.get("fees", 0.0)
    df["cum_net"] = df["net"].cumsum()
    df = df.reset_index().rename(columns={"index": "date"})
    return df


def plot_pnl(df: pd.DataFrame, out: Path) -> None:
    """日次netと累積netを1枚の画像に保存する。"""
    if df.empty:
        print("No data to plot.")
        return
    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax1.bar(df["date"], df["net"], color="#4c9aff", alpha=0.6, label="daily net")
    ax1.set_ylabel("Daily Net PnL")
    ax1.set_xlabel("Date")
    ax1.tick_params(axis="x", rotation=45)

    ax2 = ax1.twinx()
    ax2.plot(df["date"], df["cum_net"], color="#d81b60", label="cum net", linewidth=2)
    ax2.set_ylabel("Cumulative Net PnL")

    lines, labels = [], []
    for ax in (ax1, ax2):
        lns, lbs = ax.get_legend_handles_labels()
        lines.extend(lns)
        labels.extend(lbs)
    ax1.legend(lines, labels, loc="upper left")
    fig.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out)
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="バックテスト結果を可視化（funding/trade -> 日次PnLグラフ）")
    parser.add_argument("--config", type=Path, default=Path("config/app.yaml"), help="設定ファイル（db_urlを読む）")
    parser.add_argument("--start-date", help="集計開始日 YYYY-MM-DD（省略可）")
    parser.add_argument("--end-date", help="集計終了日 YYYY-MM-DD（省略可）")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("tests/backtest/backtest_pnl.png"),
        help="出力先PNGパス",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(config_path=args.config)
    df_funding, df_trades = load_frames(cfg.db_url)
    df_funding = filter_range(df_funding, args.start_date, args.end_date)
    df_trades = filter_range(df_trades, args.start_date, args.end_date)
    df_daily = compute_daily(df_funding, df_trades)
    print(df_daily.tail())  # 最終行を簡易表示
    plot_pnl(df_daily, args.output)
    print(f"Saved chart to: {args.output}")


if __name__ == "__main__":
    main()
