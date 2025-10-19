"""これは「日次レポート（Markdown）を作成し、毎日自動生成するスケジューラも提供する」モジュールです。"""

from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from loguru import logger

from bot.core.time import sleep_until, utc_now
from bot.data.repo import Repo


async def generate_daily_report(*, repo: Repo, date_str: str | None = None, out_dir: str = "reports") -> Path:
    """これは何をする関数？
    → 指定日（UTC）のレポートを Markdown で生成し、`reports/YYYY-MM-DD.md` に保存してパスを返します。
      - Funding 実現PnL（合計/銘柄別）
      - 取引手数料（合計）
      - トレード件数/売買数量（概算）
      - 注文イベント件数（新規/取消/filled）
      - 簡易KPI（Funding比率/手数料率など）
    """

    if date_str is None:
        date_str = utc_now().date().isoformat()

    target_day = datetime.fromisoformat(date_str).date()
    start = datetime.combine(target_day, datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    # 既存Repo APIを使って全件取得→Pythonで日付フィルタ（MVP）
    fundings = [f for f in await repo.list_funding_events() if start <= f.ts.astimezone(timezone.utc) < end]
    trades = [t for t in await repo.list_trades() if start <= t.ts.astimezone(timezone.utc) < end]
    orders = [o for o in await repo.list_order_logs() if start <= o.ts.astimezone(timezone.utc) < end]

    # Funding 集計
    funding_sum = sum(f.realized_pnl for f in fundings)
    funding_by_symbol: dict[str, float] = defaultdict(float)
    for f in fundings:
        funding_by_symbol[f.symbol] += float(f.realized_pnl)

    # 手数料・出来高（概算）
    fees_sum = sum(t.fee for t in trades)
    trade_count = len(trades)
    traded_notional = sum(abs(t.qty * t.price) for t in trades)

    # 注文イベント件数（ステータス集計・MVP）
    by_status: dict[str, int] = defaultdict(int)
    for o in orders:
        by_status[o.status] += 1

    gross = float(funding_sum)
    net = gross - float(fees_sum)
    fee_rate = (fees_sum / traded_notional) if traded_notional > 0 else 0.0
    funding_share = (funding_sum / gross) if gross != 0 else 0.0  # gross=Fundingのみなので1.0想定

    # レポート本文（Markdown）
    lines: list[str] = []
    lines.append(f"# Daily Report — {date_str} (UTC)")
    lines.append("")
    lines.append("## PnL 概要")
    lines.append(f"- Funding PnL (sum): **{gross:.2f}**")
    lines.append(f"- 手数料 (sum): **{fees_sum:.2f}**")
    lines.append(f"- Net PnL: **{net:.2f}**")
    lines.append("")
    lines.append("## Funding 内訳（銘柄）")
    if funding_by_symbol:
        for sym, v in sorted(funding_by_symbol.items()):
            lines.append(f"- {sym}: {v:.4f}")
    else:
        lines.append("- なし")
    lines.append("")
    lines.append("## トレードと注文（概況・MVP）")
    lines.append(f"- トレード件数: {trade_count}")
    lines.append(f"- 推定売買代金（名目合計）: {traded_notional:.2f}")
    lines.append("- 注文イベント（status別）: " + ", ".join(f"{k}={v}" for k, v in sorted(by_status.items())))
    lines.append("")
    lines.append("## 簡易KPI（MVP）")
    lines.append(f"- Fee率（手数料/名目）: {fee_rate:.6f}")
    lines.append(f"- Funding寄与比率（Funding/Gross）: {funding_share:.2f}")
    lines.append("")
    lines.append("> 注記: トレード損益（売買差益）やβ、最大DD、ヘッジ遅延などはMVPでは未算出です。")
    lines.append("> 今後、DBと板近傍価格の再生から決定論的に算出します。")

    # 保存
    out_dir_path = Path(out_dir)
    out_dir_path.mkdir(parents=True, exist_ok=True)
    out_path = out_dir_path / f"{date_str}.md"
    out_path.write_text("\n".join(lines), encoding="utf-8")

    logger.info("daily report generated: {}", out_path)
    return out_path


class ReportScheduler:
    """日次レポートを所定時刻（UTC）に作成するスケジューラ"""

    def __init__(self, *, repo: Repo, out_dir: str = "reports", hour_utc: int = 0, minute_utc: int = 5) -> None:
        """これは何をする関数？
        → 生成先（out_dir）と、UTC時刻（hour_utc:minute_utc）を受け取って毎日自動で生成します。
           既定は **UTC 00:05** に「前日分のレポート」を作成します。
        """

        self._repo = repo
        self._out_dir = out_dir
        self._hour = int(hour_utc)
        self._minute = int(minute_utc)

    async def run_forever(self) -> None:
        """これは何をする関数？
        → 常時動作し、次の発火時刻まで待機→前日分を生成、を繰り返します。
        """

        while True:
            now = utc_now()
            # 次の 00:05(UTC) を求める
            next_fire = now.replace(hour=self._hour, minute=self._minute, second=0, microsecond=0)
            if next_fire <= now:
                next_fire = next_fire + timedelta(days=1)
            await sleep_until(next_fire)

            # 前日分を作成
            target = (next_fire - timedelta(days=1)).date().isoformat()
            try:
                await generate_daily_report(repo=self._repo, date_str=target, out_dir=self._out_dir)
            except Exception as e:  # noqa: BLE001
                logger.exception("daily report generation failed: {}", e)


# ===== CLIエントリ（任意実行） =====


def main() -> None:
    """これは何をする関数？→ コマンドラインから日次レポートを生成します。"""

    parser = argparse.ArgumentParser(description="Generate daily report markdown")
    parser.add_argument("--date", type=str, default=None, help="UTC date YYYY-MM-DD (default: today)")
    parser.add_argument("--out", type=str, default="reports", help="output directory")
    args = parser.parse_args()

    async def _run() -> None:
        repo = Repo()  # 既定の SQLite (config と一致させる想定)
        await repo.create_all()
        await generate_daily_report(repo=repo, date_str=args.date, out_dir=args.out)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
