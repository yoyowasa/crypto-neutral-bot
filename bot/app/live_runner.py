from __future__ import annotations

# これは「Live運用エントリポイント（Testnet/Mainnet、dry-run対応）」を実装するファイルです。
# --env で環境切替、--dry-run で PaperExchange を使い実発注せずに全経路を通します。
import argparse
import asyncio
from typing import Any

from loguru import logger

from bot.config.loader import load_config
from bot.core.logging import setup_logging
from bot.core.retry import retryable
from bot.data.repo import Repo
from bot.exchanges.base import ExchangeGateway
from bot.exchanges.bybit import BybitGateway
from bot.monitor.metrics import MetricsLogger
from bot.monitor.report import ReportScheduler
from bot.oms.engine import OmsEngine
from bot.oms.fill_sim import PaperExchange
from bot.risk.guards import RiskManager
from bot.strategy.funding_basis.engine import FundingBasisStrategy

# Bybit v5 -> OMS ステータス正規化対応表
STATUS_MAP_BYBIT_TO_OMS = {
    "Created": "NEW",
    "New": "NEW",
    "PartiallyFilled": "PARTIAL",
    "Filled": "FILLED",
    "Cancelled": "CANCELED",  # Bybit側は"Cancelled"表記
    "Canceled": "CANCELED",  # 念のため表記ゆれも同値扱い
    "Rejected": "REJECTED",
    "Untriggered": "NEW",  # 条件注文が未発火のときはNEW相当として扱う
}

# ===== Bybit Privateメッセージ → OMSイベント の最小変換（MVP） =====


def _status_map(s: str | None) -> str:
    """これは何をする関数？
    → Bybitのステータス文字列をOMS内部の表現にざっくり正規化します。
    """

    s = (s or "").lower()
    if s in {"new", "created"}:
        return "new"
    if s in {"partiallyfilled", "partialfilled", "partially_filled", "partial"}:
        return "partially_filled"
    if s in {"filled", "closed", "done"}:
        return "filled"
    if s in {"canceled", "cancelled", "expired"}:
        return "canceled"
    if s in {"rejected"}:
        return "rejected"
    return s or "new"


async def _handle_private_order(msg: dict, oms: OmsEngine) -> None:
    """これは何をする関数？
    → Private 'order' トピックをOMSイベントに変換します（fill量は0、状態更新メイン）。
    """

    for row in msg.get("data", []):
        event = {
            "client_id": row.get("orderLinkId") or row.get("clientOrderId") or row.get("orderId"),
            "status": _status_map(row.get("orderStatus") or row.get("status")),
            "last_filled_qty": 0.0,
            "cum_filled_qty": float(row.get("cumExecQty") or 0),
            "avg_fill_price": float(row.get("avgPrice")) if row.get("avgPrice") is not None else None,
        }
        # --- ステータス正規化（Bybit -> OMS） ---
        status_raw = row.get("orderStatus") or row.get("order_status")
        event["status"] = STATUS_MAP_BYBIT_TO_OMS.get(status_raw, status_raw or "NEW")
        # --- 識別子の受け渡し ---
        event["order_id"] = row.get("orderId")
        # --- 進捗情報（部分約定の累積） ---
        event["filled_qty"] = row.get("cumExecQty") or row.get("cumFilledQty")
        event["avg_price"] = row.get("avgPrice")
        # --- 約定（1回ごとの出来）情報を付与 ---
        event["order_id"] = row.get("orderId")
        event["last_fill_qty"] = row.get("execQty")
        event["last_fill_price"] = row.get("execPrice")
        client_order_id = row.get("orderLinkId") or row.get("clOrdId") or row.get("clientOrderId") or None
        event["client_order_id"] = client_order_id
        event["updated_at"] = (
            row.get("updatedTime") or row.get("uTime") or row.get("updateTime") or row.get("ts") or row.get("t")
        )  # この注文更新の発生時刻（後段で順序判定に使う）
        await oms.on_execution_event(event)


async def _handle_private_execution(msg: dict, oms: OmsEngine) -> None:
    """これは何をする関数？
    → Private 'execution'（約定）トピックをOMSイベントに変換します（増分fillあり）。
    """

    for row in msg.get("data", []):
        event = {
            "client_id": row.get("orderLinkId") or row.get("clientOrderId") or row.get("orderId"),
            "status": "filled",  # 単一実行は「filled相当の増分イベント」として扱う（MVP）
            "last_filled_qty": float(row.get("execQty") or row.get("lastFillQty") or 0),
            "cum_filled_qty": float(row.get("cumExecQty") or 0),
            "avg_fill_price": float(row.get("execPrice") or row.get("price") or 0),
        }
        client_order_id = row.get("orderLinkId") or row.get("clOrdId") or row.get("clientOrderId") or None
        event["client_order_id"] = client_order_id
        event["updated_at"] = (
            row.get("execTime") or row.get("updatedTime") or row.get("uTime") or row.get("ts") or row.get("t")
        )  # この出来の発生時刻（順序判定に使う）
        await oms.on_execution_event(event)


# ===== Live ランナー本体 =====


async def _cancel_all_open_orders(ex: ExchangeGateway) -> None:
    """これは何をする関数？
    → 残っている未約定注文を整理します（安全のため起動時にキャンセル）。
    """

    try:
        opens = await ex.get_open_orders()
    except Exception as e:  # noqa: BLE001
        logger.warning("get_open_orders failed at startup: {}", e)
        return
    if not opens:
        return
    logger.info("startup: cancel {} open orders", len(opens))
    for o in opens:
        try:
            sym: str | None = getattr(o, "symbol", None)
            if not sym:
                logger.warning("skip cancel: open order symbol unknown id={}", getattr(o, "order_id", None))
                continue
            await ex.cancel_order(
                symbol=sym,
                order_id=o.order_id,
                client_order_id=getattr(o, "client_order_id", None) or o.client_id,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("cancel failed: order_id={} err={}", getattr(o, "order_id", None), e)


@retryable(tries=999999, wait_initial=1.0, wait_max=30.0)
async def _run_private_ws(ex: BybitGateway, oms: OmsEngine, *, symbols: list[str]) -> None:
    """これは何をする関数？
    → Private WS に接続し、order/execution/position を購読して OMS へ流し続けます（断線時は自動再接続）。
    """

    async def _on_order(msg: dict) -> None:
        await _handle_private_order(msg, oms)

    async def _on_exec(msg: dict) -> None:
        await _handle_private_execution(msg, oms)

    async def _on_position(msg: dict) -> None:
        # 位置情報はMVPではスルー（必要に応じてポジション照合を追加）
        pass

    await ex.subscribe_private({"order": _on_order, "execution": _on_exec, "position": _on_position})
    # Private WS 接続直後に、取引所に残る open 注文の client_order_id を復元
    try:
        await oms.reconcile_inflight_open_orders(symbols)
    except Exception as e:  # noqa: BLE001
        logger.warning("reconcile inflight open orders failed: {}", e)


@retryable(tries=999999, wait_initial=1.0, wait_max=30.0)
async def _run_public_ws_for_paper(data_ex: BybitGateway, paper_ex: PaperExchange, symbols: list[str]) -> None:
    """これは何をする関数？
    → Paperモード用に Public WS を購読し、BBO/トレードを PaperExchange に転送します。
    """

    async def _public_trade_cb(msg: dict) -> None:
        await paper_ex.handle_public_msg(msg)

    async def _orderbook_cb(msg: dict) -> None:
        await paper_ex.handle_public_msg(msg)
        # BybitGateway にもBBOをキャッシュさせる（PostOnly調整の低遅延化）
        try:
            topic = (msg.get("topic") or "").lower()
            if topic.startswith("orderbook"):
                data_obj = msg.get("data")
                item = None
                if isinstance(data_obj, list):
                    item = data_obj[0] if data_obj else None
                elif isinstance(data_obj, dict):
                    item = data_obj
                if item:
                    # b/a は [[price, size], ...] の場合と、bp/ap/ bid1Price/ask1Price の場合がある
                    bid = None
                    ask = None
                    b = item.get("b")
                    a = item.get("a")
                    if isinstance(b, list) and b:
                        try:
                            bid = b[0][0]
                        except Exception:
                            bid = None
                    if isinstance(a, list) and a:
                        try:
                            ask = a[0][0]
                        except Exception:
                            ask = None
                    if bid is None:
                        bid = item.get("bp") or item.get("bid1Price")
                    if ask is None:
                        ask = item.get("ap") or item.get("ask1Price")
                    sym = item.get("symbol") or item.get("s")
                    if not sym and "." in topic:
                        try:
                            sym = topic.split(".")[-1]
                        except Exception:
                            sym = None
                    if sym and (bid is not None or ask is not None):
                        data_ex.update_bbo(sym, bid, ask, item.get("ts") or item.get("t"))
        except Exception:
            pass

    await data_ex.subscribe_public(
        symbols=symbols,
        callbacks={"publicTrade": _public_trade_cb, "orderbook": _orderbook_cb},
    )


@retryable(tries=999999, wait_initial=0.5, wait_max=10.0)
async def _strategy_step_once(
    strategy: FundingBasisStrategy,
    *,
    symbols: list[str],
    price_source: ExchangeGateway,
    funding_source: ExchangeGateway,
) -> None:
    """これは何をする関数？
    → 戦略の step() を各シンボルに対して1サイクル実行します（429等でも自動リトライ）。
    """

    for sym in symbols:
        funding = await funding_source.get_funding_info(sym)
        perp_price = await price_source.get_ticker(sym)
        spot_price = await price_source.get_ticker(f"{sym}_SPOT")
        await strategy.step(funding=funding, spot_price=spot_price, perp_price=perp_price)


async def _main_async(env: str, cfg_path: str | None, dry_run: bool, flatten_on_exit: bool) -> None:
    """これは何をする関数？
    → 設定・ログ・DB を初期化し、dry-run/実発注のいずれかで戦略を起動します。
    """

    setup_logging(level="INFO")
    cfg = load_config(cfg_path)

    # CLIの --env を優先
    exchange_env = env or cfg.exchange.environment

    # DB
    repo = Repo(db_url=cfg.db_url)
    await repo.create_all()

    # 実データ源（REST/WS）。dry-run でもデータ源として使用
    data_ex = BybitGateway(api_key=cfg.keys.api_key, api_secret=cfg.keys.api_secret, environment=exchange_env)

    # 発注先（dry-run は PaperExchange、live は BybitGateway）
    if dry_run:
        trade_ex: ExchangeGateway = PaperExchange(data_source=data_ex, initial_usdt=100_000.0)
        oms = OmsEngine(ex=trade_ex, repo=repo, cfg=None)
        assert isinstance(trade_ex, PaperExchange)
        trade_ex.bind_oms(oms)
    else:
        trade_ex = BybitGateway(api_key=cfg.keys.api_key, api_secret=cfg.keys.api_secret, environment=exchange_env)
        oms = OmsEngine(ex=trade_ex, repo=repo, cfg=None)

    # RiskManager（flatten_all の実体は strategy 経由にする）
    strategy_holder: dict[str, Any] = {}

    async def _flatten_all() -> None:
        """これは何をする関数？→ 現在の戦略ホールドを全クローズします。"""

        s = strategy_holder.get("strategy")
        if s:
            await s.flatten_all()

    rm = RiskManager(
        loss_cut_daily_jpy=cfg.risk.loss_cut_daily_jpy,
        ws_disconnect_threshold_sec=30.0,
        hedge_delay_p95_threshold_sec=2.0,
        api_error_max_in_60s=10,
        flatten_all=_flatten_all,
    )

    # Strategy
    strategy = FundingBasisStrategy(
        oms=oms,
        risk_config=cfg.risk,
        strategy_config=cfg.strategy,
        period_seconds=8.0 * 3600.0,
        risk_manager=rm,
    )
    strategy_holder["strategy"] = strategy

    # 起動時ハウスキーピング（孤立注文の整理）
    await _cancel_all_open_orders(trade_ex)

    # タスク群
    tasks: list[asyncio.Task] = []
    price_source: ExchangeGateway

    if dry_run:
        # Public WS（BBO/トレード）を購読し、PaperExchangeへ転送
        assert isinstance(trade_ex, PaperExchange)
        tasks.append(
            asyncio.create_task(
                _run_public_ws_for_paper(data_ex=data_ex, paper_ex=trade_ex, symbols=cfg.strategy.symbols)
            )
        )
        price_source = trade_ex  # perpはBBO中値/last、spotはRESTフォールバック
    else:
        # Private WS：注文/約定イベントをOMSへ
        assert isinstance(trade_ex, BybitGateway)
        tasks.append(asyncio.create_task(_run_private_ws(ex=trade_ex, oms=oms, symbols=cfg.strategy.symbols)))
        price_source = data_ex  # 価格はRESTのtickerで取得

    # 戦略ループ（数秒ごとに step、429/一時失敗はデコレータで再実行）
    async def _loop() -> None:
        """これは何をする関数？→ 数秒おきに1サイクル分の戦略stepを回します。"""

        while True:
            try:
                await _strategy_step_once(
                    strategy,
                    symbols=cfg.strategy.symbols,
                    price_source=price_source,
                    funding_source=data_ex,
                )
            except Exception as e:  # noqa: BLE001
                logger.exception("strategy loop error: {}", e)
            await asyncio.sleep(3.0)

    tasks.append(asyncio.create_task(_loop()))

    # メトリクス心拍（30秒おき）
    metrics = MetricsLogger(ex=trade_ex, repo=repo, symbols=cfg.strategy.symbols, risk=rm)
    tasks.append(asyncio.create_task(metrics.run_forever(interval_sec=30.0)))

    # 日次レポート（UTC 00:05 に前日分を作成）
    reporter = ReportScheduler(repo=repo, out_dir="reports", hour_utc=0, minute_utc=5)
    tasks.append(asyncio.create_task(reporter.run_forever()))

    # 終了待ち
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        if flatten_on_exit:
            logger.warning("on-exit: flatten_all()")
            try:
                await strategy.flatten_all()
            except Exception as e:  # noqa: BLE001
                logger.exception("flatten_all on exit failed: {}", e)


def main() -> None:
    """これは何をする関数？
    → コマンドライン引数を読み取り、イベントループで Live ランナーを起動します。
    """

    parser = argparse.ArgumentParser(
        description="Live runner for funding/basis strategy (testnet/mainnet, dry-run supported)"
    )
    parser.add_argument("--env", choices=["testnet", "mainnet"], default="testnet", help="exchange environment")
    parser.add_argument("--config", type=str, default=None, help="path to config/app.yaml (loader reads by default)")
    parser.add_argument("--dry-run", action="store_true", help="use PaperExchange (no real orders)")
    parser.add_argument("--flatten-on-exit", action="store_true", help="flatten all positions on shutdown")
    args = parser.parse_args()

    try:
        asyncio.run(
            _main_async(
                env=args.env,
                cfg_path=args.config,
                dry_run=bool(args.dry_run),
                flatten_on_exit=bool(args.flatten_on_exit),
            )
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
