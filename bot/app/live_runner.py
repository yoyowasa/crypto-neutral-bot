from __future__ import annotations

# これは「Live運用エントリポイント（Testnet/Mainnet、dry-run対応）」を実装するファイルです。
# --env で環境切替、--dry-run で PaperExchange を使い実発注せずに全経路を通します。
import argparse
import asyncio
from typing import Any

from loguru import logger

from bot.config.loader import load_config
from bot.core.errors import ConfigError  # 本番禁止のときは起動を止めるために使う
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
        oms.touch_private_ws(
            row.get("updatedTime") or row.get("uTime") or row.get("ts") or row.get("t")
        )  # Private WSの受信をOMSに合図
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
        event["symbol"] = row.get("symbol") or row.get("s")
        event["side"] = row.get("side") or row.get("S")
        event["fee"] = row.get("execFee") or row.get("commission")
        event["fee_currency"] = row.get("feeCurrency") or row.get("commissionAsset")
        client_order_id = row.get("orderLinkId") or row.get("clOrdId") or row.get("clientOrderId") or None
        event["client_order_id"] = client_order_id
        event["updated_at"] = (
            row.get("execTime") or row.get("updatedTime") or row.get("uTime") or row.get("ts") or row.get("t")
        )  # この出来の発生時刻（順序判定に使う）
        oms.touch_private_ws(
            row.get("execTime") or row.get("updatedTime") or row.get("uTime") or row.get("ts") or row.get("t")
        )  # 約定メッセージでも合図
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


async def _main_async(
    env: str,
    cfg_path: str | None,
    dry_run: bool,
    flatten_on_exit: bool,
    ops_check: bool = False,
    log_level: str = "INFO",
    ops_out_csv: str | None = None,
    ops_out_json: str | None = None,
) -> None:
    """これは何をする関数？
    → 設定・ログ・DB を初期化し、dry-run/実発注のいずれかで戦略を起動します。
    """

    setup_logging(level=log_level)
    cfg = load_config(cfg_path)

    # 本番(mainnet)でallow_live=Falseなら、ここで止める（誤起動防止の安全弁）
    is_mainnet = (str(env or "").lower() == "mainnet") or (
        str(getattr(cfg.exchange, "environment", "")).lower() == "mainnet"
    )
    if is_mainnet and not bool(getattr(cfg.exchange, "allow_live", False)):
        raise ConfigError(
            "Live trading is disabled by config. Set exchange.allow_live=true in"
            " config/app.yaml (or EXCHANGE__ALLOW_LIVE=true) to enable mainnet."
        )

    # CLIの --env を優先
    exchange_env = env or cfg.exchange.environment

    # DB
    repo = Repo(db_url=cfg.db_url)
    await repo.create_all()

    # 実データ源（REST/WS）。dry-run でもデータ源として使用
    data_ex = BybitGateway(api_key=cfg.keys.api_key, api_secret=cfg.keys.api_secret, environment=exchange_env)
    # --- 設定の適用：BybitGateway（REST/BBO/価格ガード） ---
    data_ex._bbo_max_age_ms = cfg.exchange.bbo_max_age_ms  # BBO鮮度ガード（STEP28）
    data_ex._rest_max_concurrency = cfg.exchange.rest_max_concurrency  # REST同時実行上限（STEP29）
    data_ex._rest_semaphore = asyncio.Semaphore(data_ex._rest_max_concurrency)  # 新しい上限でセマフォを再構築
    data_ex._rest_cb_fail_threshold = cfg.exchange.rest_cb_fail_threshold  # サーキット連続失敗回数（STEP31）
    data_ex._rest_cb_open_seconds = cfg.exchange.rest_cb_open_seconds  # サーキット休止秒（STEP31）
    data_ex._instrument_info_ttl_s = cfg.exchange.instrument_info_ttl_s  # instruments-infoの自動リフレッシュ間隔（秒）
    data_ex._price_dev_bps_limit = cfg.risk.price_dev_bps_limit  # 価格逸脱ガード[bps]（STEP34）

    # 健診モード：各シンボルの Funding / BBO / 認証REST（open orders）を点検して終了する
    if ops_check:
        syms = list(getattr(cfg.strategy, "symbols", []))
        # Use already imported classes; keep local stdlib imports only
        import csv
        import json
        import os
        from datetime import datetime, timezone

        repo_ops = Repo(db_url=cfg.db_url)
        await repo_ops.create_all()
        oms_ops = OmsEngine(ex=data_ex, repo=repo_ops, cfg=None)
        flip_min = float(os.environ.get("RISK__FUNDING_FLIP_MIN_ABS", "0") or 0)
        flip_n = int(os.environ.get("RISK__FUNDING_FLIP_CONSECUTIVE", "1") or 1)
        rm_ops = RiskManager(
            loss_cut_daily_jpy=cfg.risk.loss_cut_daily_jpy,
            ws_disconnect_threshold_sec=30.0,
            hedge_delay_p95_threshold_sec=2.0,
            api_error_max_in_60s=10,
            flatten_all=lambda: asyncio.sleep(0),
            funding_flip_min_abs=flip_min,
            funding_flip_consecutive=flip_n,
        )
        taker_bps = float(os.environ.get("STRATEGY__TAKER_FEE_BPS_ROUNDTRIP", "6.0") or 6.0)
        slip_bps = float(os.environ.get("STRATEGY__ESTIMATED_SLIPPAGE_BPS", "5.0") or 5.0)
        strat_ops = FundingBasisStrategy(
            oms=oms_ops,
            risk_config=cfg.risk,
            strategy_config=cfg.strategy,
            period_seconds=8.0 * 3600.0,
            taker_fee_bps_roundtrip=taker_bps,
            estimated_slippage_bps=slip_bps,
            risk_manager=rm_ops,
        )

        rows: list[dict] = []
        for sym in syms:
            fi = await data_ex.get_funding_info(sym)
            bid, ask = await data_ex.get_bbo(sym)
            # Private auth check
            auth_ok = True
            try:
                oo = await data_ex.get_open_orders(sym)
                open_n = len(oo)
            except Exception:
                auth_ok = False
                open_n = 0
            logger.info(
                "ops.check symbol={} funding={} next={} bbo=({}, {}) open={}",
                sym,
                getattr(fi, "predicted_rate", None),
                getattr(fi, "next_funding_time", None),
                bid,
                ask,
                open_n,
            )
            # Decision preview
            try:
                d = strat_ops.evaluate(
                    funding=fi,
                    spot_price=await data_ex.get_ticker(f"{sym}_SPOT"),
                    perp_price=await data_ex.get_ticker(sym),
                )
                action = getattr(getattr(d, "action", None), "name", str(getattr(d, "action", "")))
                reason = getattr(d, "reason", None)
                apr = getattr(d, "predicted_apr", None)
            except Exception as e:
                action, reason, apr = ("ERR", str(e), None)
            logger.info(
                "ops.health sym={} auth={} decision={} apr={} reason={}",
                sym,
                "OK" if auth_ok else "NG",
                action,
                apr,
                reason,
            )
            rows.append(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "symbol": sym,
                    "funding_predicted": getattr(fi, "predicted_rate", None),
                    "next_funding_time": str(getattr(fi, "next_funding_time", None)),
                    "bbo_bid": bid,
                    "bbo_ask": ask,
                    "auth": bool(auth_ok),
                    "open_orders": int(open_n),
                    "decision": action,
                    "predicted_apr": float(apr) if apr is not None else None,
                    "reason": reason,
                    "taker_fee_bps_roundtrip": taker_bps,
                    "estimated_slippage_bps": slip_bps,
                    "min_expected_apr": getattr(cfg.strategy, "min_expected_apr", None),
                }
            )
        await data_ex.close()
        # Optional export
        if ops_out_csv:
            try:
                fieldnames = list(rows[0].keys()) if rows else []
                with open(ops_out_csv, "w", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=fieldnames)
                    if fieldnames:
                        w.writeheader()
                    for r in rows:
                        w.writerow(r)
                logger.info("ops.export csv={} rows={}", ops_out_csv, len(rows))
            except Exception as e:
                logger.warning("ops.export csv failed: {}", e)
        if ops_out_json:
            try:
                with open(ops_out_json, "w", encoding="utf-8") as f:
                    json.dump(rows, f, ensure_ascii=False, indent=2)
                logger.info("ops.export json={} rows={}", ops_out_json, len(rows))
            except Exception as e:
                logger.warning("ops.export json failed: {}", e)
        return

    # 発注先（dry-run は PaperExchange、live は BybitGateway）
    if dry_run:
        trade_ex: ExchangeGateway = PaperExchange(data_source=data_ex, initial_usdt=100_000.0)
        oms = OmsEngine(ex=trade_ex, repo=repo, cfg=None)
        # --- 設定の適用：OMS（WS古さブロック、STEP32） ---
        oms._ws_stale_block_ms = cfg.risk.ws_stale_block_ms
        # --- 設定の適用：REJECT連発→シンボル一時停止（STEP37） ---
        oms._reject_burst_threshold = cfg.risk.reject_burst_threshold
        oms._reject_window_ms = cfg.risk.reject_burst_window_s * 1000
        oms._symbol_cooldown_ms = cfg.risk.symbol_cooldown_s * 1000
        assert isinstance(trade_ex, PaperExchange)
        trade_ex.bind_oms(oms)
    else:
        trade_ex = BybitGateway(api_key=cfg.keys.api_key, api_secret=cfg.keys.api_secret, environment=exchange_env)
        # --- 設定の適用：BybitGateway（REST/BBO/価格ガード） ---
        trade_ex._bbo_max_age_ms = cfg.exchange.bbo_max_age_ms  # BBO鮮度ガード（STEP28）
        trade_ex._rest_max_concurrency = cfg.exchange.rest_max_concurrency  # REST同時実行上限（STEP29）
        trade_ex._rest_semaphore = asyncio.Semaphore(trade_ex._rest_max_concurrency)  # 新しい上限でセマフォを再構築
        trade_ex._rest_cb_fail_threshold = cfg.exchange.rest_cb_fail_threshold  # サーキット連続失敗回数（STEP31）
        trade_ex._rest_cb_open_seconds = cfg.exchange.rest_cb_open_seconds  # サーキット休止秒（STEP31）
        trade_ex._instrument_info_ttl_s = (
            cfg.exchange.instrument_info_ttl_s
        )  # instruments-infoの自動リフレッシュ間隔（秒）
        trade_ex._price_dev_bps_limit = cfg.risk.price_dev_bps_limit  # 価格逸脱ガード[bps]（STEP34）
        oms = OmsEngine(ex=trade_ex, repo=repo, cfg=None)
        # --- 設定の適用：OMS（WS古さブロック、STEP32） ---
        oms._ws_stale_block_ms = cfg.risk.ws_stale_block_ms
        # --- 設定の適用：REJECT連発→シンボル一時停止（STEP37） ---
        oms._reject_burst_threshold = cfg.risk.reject_burst_threshold
        oms._reject_window_ms = cfg.risk.reject_burst_window_s * 1000
        oms._symbol_cooldown_ms = cfg.risk.symbol_cooldown_s * 1000

    # RiskManager（flatten_all の実体は strategy 経由にする）
    strategy_holder: dict[str, Any] = {}

    async def _flatten_all() -> None:
        """これは何をする関数？→ 現在の戦略ホールドを全クローズします。"""

        s = strategy_holder.get("strategy")
        if s:
            await s.flatten_all()

    import os

    _flip_min = float(os.environ.get("RISK__FUNDING_FLIP_MIN_ABS", "0") or 0)
    _flip_n = int(os.environ.get("RISK__FUNDING_FLIP_CONSECUTIVE", "1") or 1)
    rm = RiskManager(
        loss_cut_daily_jpy=cfg.risk.loss_cut_daily_jpy,
        ws_disconnect_threshold_sec=30.0,
        hedge_delay_p95_threshold_sec=2.0,
        api_error_max_in_60s=10,
        flatten_all=_flatten_all,
        funding_flip_min_abs=_flip_min,
        funding_flip_consecutive=_flip_n,
    )

    # Strategy
    taker_bps = float(os.environ.get("STRATEGY__TAKER_FEE_BPS_ROUNDTRIP", "6.0") or 6.0)
    slip_bps = float(os.environ.get("STRATEGY__ESTIMATED_SLIPPAGE_BPS", "5.0") or 5.0)
    strategy = FundingBasisStrategy(
        oms=oms,
        risk_config=cfg.risk,
        strategy_config=cfg.strategy,
        period_seconds=8.0 * 3600.0,
        taker_fee_bps_roundtrip=taker_bps,
        estimated_slippage_bps=slip_bps,
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
                # PostOnly指値の安全な価格追従（控えめにアメンド）
                await oms.maintain_postonly_orders(cfg.strategy.symbols, cfg.strategy)
            except Exception as e:  # noqa: BLE001
                logger.exception("strategy loop error: {}", e)
            await asyncio.sleep(3.0)

    tasks.append(asyncio.create_task(_loop()))

    # メトリクス心拍（30秒おき）
    metrics = MetricsLogger(ex=trade_ex, repo=repo, symbols=cfg.strategy.symbols, risk=rm, oms=oms)
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
            try:
                oms._log.info("shutdown.begin flatten_on_exit=true")  # 構造化ログ（STEP38）
            except Exception:
                pass
            try:
                await oms.drain_and_flatten(cfg.strategy.symbols, strategy, timeout_s=20)  # 安全ドレインを実施
            except Exception:
                pass


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
    parser.add_argument("--log-level", type=str, default="INFO", help="logging level (e.g. DEBUG, INFO, WARNING)")
    parser.add_argument("--ops-out-csv", type=str, default=None, help="write ops-check summary to CSV path")
    parser.add_argument("--ops-out-json", type=str, default=None, help="write ops-check summary to JSON path")
    parser.add_argument(
        "--ops-check",
        action="store_true",
        help="本番前ドライラン健診モード（Funding/BBO/認証RESTを確認して即終了）",
    )
    args = parser.parse_args()

    try:
        asyncio.run(
            _main_async(
                env=args.env,
                cfg_path=args.config,
                dry_run=bool(args.dry_run),
                flatten_on_exit=bool(args.flatten_on_exit),
                ops_check=bool(getattr(args, "ops_check", False)),
                log_level=str(getattr(args, "log_level", "INFO")),
                ops_out_csv=getattr(args, "ops_out_csv", None),
                ops_out_json=getattr(args, "ops_out_json", None),
            )
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
