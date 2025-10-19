"""これは「Paperモードで戦略を回すエントリポイント」です。
Bybit Public WSのBBOを使って疑似約定し、DBへログ/トレードを記録します。
"""
from __future__ import annotations

import argparse
import asyncio
from typing import Any

from loguru import logger

from bot.config.loader import load_config
from bot.core.logging import setup_logging
from bot.data.repo import Repo
from bot.exchanges.bybit import BybitGateway
from bot.oms.engine import OmsEngine
from bot.oms.fill_sim import PaperExchange
from bot.risk.guards import RiskManager
from bot.strategy.funding_basis.engine import FundingBasisStrategy


async def _run(config_path: str | None) -> None:
    """これは何をする関数？
    → 設定/ログ/DB/ゲートウェイ/戦略を初期化し、WS購読と戦略ループを並行実行します。
    """

    setup_logging(level="INFO")
    cfg = load_config(config_path)

    # DB 準備
    repo = Repo(db_url=cfg.db_url)
    await repo.create_all()

    # データソース（Bybit v5 REST/WSデータ専用、発注はしない）
    data_ex = BybitGateway(
        api_key=cfg.keys.api_key,
        api_secret=cfg.keys.api_secret,
        environment=cfg.exchange.environment,
    )

    # Paper Exchange（疑似約定）。OMSは後でバインドする
    paper_ex = PaperExchange(data_source=data_ex, initial_usdt=100_000.0)

    # OMS
    oms = OmsEngine(ex=paper_ex, repo=repo, cfg=None)
    paper_ex.bind_oms(oms)

    # Risk（flatten_all は strategy 生成後に呼べるようクロージャで保持）
    strategy_holder: dict[str, Any] = {}

    async def _flatten_all() -> None:
        """これは何をする関数？→ 現在の戦略ホールドを全クローズします。"""

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

    # --- WS 購読（Public: orderbook.1 / publicTrade） ---
    async def _public_trade_cb(msg: dict) -> None:
        """これは何をする関数？→ publicTrade メッセージを PaperExchange へ伝えて last を更新する"""

        await paper_ex.handle_public_msg(msg)

    async def _orderbook_cb(msg: dict) -> None:
        """これは何をする関数？→ orderbook.1 メッセージを PaperExchange へ伝えて BBO を更新＆指値約定を試行する"""

        await paper_ex.handle_public_msg(msg)

    ws_task = asyncio.create_task(
        data_ex.subscribe_public(
            symbols=cfg.strategy.symbols,
            callbacks={"publicTrade": _public_trade_cb, "orderbook": _orderbook_cb},
        )
    )

    # --- 戦略ループ（数秒ごとに step） ---
    async def _strategy_loop() -> None:
        """これは何をする関数？→ 各シンボルについてFunding/価格を取得し、戦略stepを実行します。"""

        while True:
            try:
                for sym in cfg.strategy.symbols:
                    # Funding（REST委譲）
                    funding = await paper_ex.get_funding_info(sym)

                    # 価格（perpはWSのBBO→mid/last、spotはRESTフォールバック）
                    perp_price = await paper_ex.get_ticker(sym)
                    spot_price = await paper_ex.get_ticker(f"{sym}_SPOT")

                    await strategy.step(
                        funding=funding,
                        spot_price=spot_price,
                        perp_price=perp_price,
                    )
            except Exception as e:  # noqa: BLE001
                logger.exception("strategy step error: {}", e)
            await asyncio.sleep(3.0)

    strat_task = asyncio.create_task(_strategy_loop())

    # 終了待ち（Ctrl+Cでキャンセル）
    try:
        await asyncio.gather(ws_task, strat_task)
    except asyncio.CancelledError:
        pass


def main() -> None:
    """これは何をする関数？→ コマンドライン引数を受け取り、イベントループで _run を起動します。"""

    parser = argparse.ArgumentParser(description="Paper runner for funding/basis strategy")
    parser.add_argument("--config", type=str, default=None, help="path to config/app.yaml (optional)")
    args = parser.parse_args()
    try:
        asyncio.run(_run(args.config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
