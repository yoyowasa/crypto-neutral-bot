"""このモジュールは『Paperモードでヘッジ戦略を試運転するランナー』です。
Bitget の REST データをベースに、Public WS（orderbook/trade）で BBO/last を更新しつつ
DB ログ・メトリクス・日次レポートを生成します。
"""

from __future__ import annotations

import argparse
import asyncio
from typing import Any

from loguru import logger

from bot.config.loader import load_config
from bot.core.logging import setup_logging
from bot.core.retry import retryable
from bot.data.repo import Repo
from bot.exchanges.bitget import BitgetGateway
from bot.monitor.metrics import MetricsLogger
from bot.monitor.report import ReportScheduler
from bot.oms.engine import OmsEngine
from bot.oms.fill_sim import PaperExchange
from bot.risk.guards import RiskManager
from bot.strategy.funding_basis.engine import FundingBasisStrategy


@retryable(tries=999999, wait_initial=1.0, wait_max=30.0)
async def _run_public_ws(
    data_ex: BitgetGateway,
    paper_ex: PaperExchange,
    symbols: list[str],
) -> None:
    """Bitget Public WS から orderbook/trade を購読し、PaperExchange に反映する。

    - channel: books1 → orderbook（BBO）
    - channel: trade  → 約定価格（last）
    """

    async def _public_trade_cb(msg: dict) -> None:
        await paper_ex.handle_public_msg(msg)

    async def _orderbook_cb(msg: dict) -> None:
        await paper_ex.handle_public_msg(msg)

    await data_ex.subscribe_public(symbols=symbols, callbacks={"trade": _public_trade_cb, "orderbook": _orderbook_cb})


async def _run(config_path: str | None) -> None:
    """設定・ログ・DB・ゲートウェイ・戦略を初期化し、WS/戦略/メトリクス/レポートを起動する。"""

    setup_logging(level="INFO")
    cfg = load_config(config_path)

    # DB 接続
    repo = Repo(db_url=cfg.db_url)
    await repo.create_all()

    # データソース（Bitget REST データのみを利用。実発注は行わない）
    data_ex = BitgetGateway(
        api_key=cfg.keys.api_key,
        api_secret=cfg.keys.api_secret,
        passphrase=cfg.keys.passphrase,
        environment=cfg.exchange.environment,
        use_uta=getattr(cfg.exchange, "use_uta", True),
    )

    # Paper Exchange（模擬約定）。OMS とは後で bind する。
    paper_ex = PaperExchange(data_source=data_ex, initial_usdt=100_000.0)

    # OMS
    oms = OmsEngine(ex=paper_ex, repo=repo, cfg=None)
    # Paper でも WS 古さガードは効かせておく（デフォルトだとブロックしないが、将来の拡張に備える）
    oms._ws_stale_block_ms = cfg.risk.ws_stale_block_ms
    # REJECT バースト → 一時的クールダウン
    oms._reject_burst_threshold = cfg.risk.reject_burst_threshold
    oms._reject_window_ms = cfg.risk.reject_burst_window_s * 1000
    oms._symbol_cooldown_ms = cfg.risk.symbol_cooldown_s * 1000
    paper_ex.bind_oms(oms)

    # Risk（flatten_all は strategy から呼び出す）
    strategy_holder: dict[str, Any] = {}

    async def _flatten_all() -> None:
        """現在のポジションをすべてクローズする。"""

        s = strategy_holder.get("strategy")
        if s:
            await s.flatten_all()

    rm = RiskManager(
        loss_cut_daily_jpy=cfg.risk.loss_cut_daily_jpy,
        flatten_all=_flatten_all,
    )

    # Strategy（Funding/Basis）
    strategy = FundingBasisStrategy(
        oms=oms,
        risk_config=cfg.risk,
        strategy_config=cfg.strategy,
        period_seconds=8.0 * 3600.0,
        risk_manager=rm,
    )
    strategy_holder["strategy"] = strategy

    # --- 戦略ループ（REST Funding + WS/BBO） ---
    async def _strategy_loop() -> None:
        """各シンボルの Funding/BBO を取得し、戦略 step を回す。"""

        while True:
            try:
                for sym in cfg.strategy.symbols:
                    # Funding（REST フォールバック）
                    funding = await paper_ex.get_funding_info(sym)

                    # 価格（perp は WS の BBO mid/last、spot は REST フォールバック）
                    perp_price = await paper_ex.get_ticker(sym)
                    spot_price = await paper_ex.get_ticker(f"{sym}_SPOT")

                    await strategy.step(
                        funding=funding,
                        spot_price=spot_price,
                        perp_price=perp_price,
                    )
                # Paper でも PostOnly 追従ロジックを回して挙動を確認する
                await oms.maintain_postonly_orders(cfg.strategy.symbols, cfg.strategy)
            except Exception as e:  # noqa: BLE001
                logger.exception("strategy step error: {}", e)
            await asyncio.sleep(3.0)

    strat_task = asyncio.create_task(_strategy_loop())

    # Public WS（books1/trade）を購読して PaperExchange に反映
    ws_task = asyncio.create_task(_run_public_ws(data_ex=data_ex, paper_ex=paper_ex, symbols=cfg.strategy.symbols))

    # メトリクス（30 秒間隔）と日次レポート（UTC 00:05）
    metrics = MetricsLogger(ex=paper_ex, repo=repo, symbols=cfg.strategy.symbols, risk=rm, oms=oms)
    metrics_task = asyncio.create_task(metrics.run_forever(interval_sec=30.0))
    reporter = ReportScheduler(repo=repo, out_dir="reports", hour_utc=0, minute_utc=5)
    report_task = asyncio.create_task(reporter.run_forever())

    # gather で WS/戦略/メトリクス/レポートを同時起動
    try:
        await asyncio.gather(strat_task, ws_task, metrics_task, report_task)
    except asyncio.CancelledError:
        pass
    finally:
        # 終了時はタスクをキャンセルしてから、Bitget(ccxt async) をクローズ
        for t in (strat_task, ws_task, metrics_task, report_task):
            t.cancel()
        await asyncio.gather(strat_task, ws_task, metrics_task, report_task, return_exceptions=True)

        close_coro = getattr(data_ex, "close", None)
        if callable(close_coro):
            try:
                await close_coro()
            except Exception as e:  # noqa: BLE001
                logger.warning("data_ex.close() failed: {}", e)


def main() -> None:
    """コマンドライン引数を受け取り、_run を起動するエントリポイント。"""

    parser = argparse.ArgumentParser(description="Paper runner for funding/basis strategy")
    parser.add_argument("--config", type=str, default=None, help="path to config/app.yaml (optional)")
    args = parser.parse_args()
    try:
        asyncio.run(_run(args.config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
