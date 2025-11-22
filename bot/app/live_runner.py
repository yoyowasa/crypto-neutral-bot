from __future__ import annotations

# これは「Live運用エントリポイント（Testnet/Mainnet、dry-run対応）」を実装するファイルです。
# --env で環境切替、--dry-run で PaperExchange を使い実発注せずに全経路を通します。
import argparse
import asyncio
import json  # 起動時に ops-check.json を読み、API鍵の事前チェック結果をログ出力するため（importはファイル冒頭に統一）
import os
from typing import Any

from loguru import logger

from bot.config.loader import load_config
from bot.core.errors import ConfigError  # 本番禁止のときは起動を止めるために使う
from bot.core.logging import setup_logging
from bot.core.retry import retryable
from bot.data.repo import Repo
from bot.exchanges.base import ExchangeGateway
from bot.exchanges.bitget import BitgetGateway
from bot.monitor.metrics import MetricsLogger
from bot.monitor.report import ReportScheduler
from bot.oms.engine import OmsEngine
from bot.oms.fill_sim import PaperExchange
from bot.ops.check import (
    _cooldown_info_for_symbol,  # ops-check見える化: クールダウンの現在地
    _get_bybit_gateway,  # ops-check見える化: Bybit GW取得
    _is_bbo_valid,  # ops-check見える化: BBOの妥当性
    _market_data_ready_for_ops,  # ops-check見える化: 市場データREADY/理由
    _min_limits_for_symbol,
    _price_state_for_symbol,  # ops-check見える化: 価格ガード状態
    _qty_common_step,
    _qty_steps_for_symbol,
    _scale_ready_for_symbol,  # ops-check見える化: スケール準備状態
)
from bot.risk.guards import RiskManager
from bot.strategy.funding_basis.engine import FundingBasisStrategy

_DEBUG_PRIVATE_WS: bool = False  # Private WS のデバッグログ出力フラグ


def _format_guard_context(_ldict: dict) -> dict:
    """guard.skip用の文脈を安全に抽出して返す。"""

    keys = (
        "ready_count",
        "ready_required",
        "cooldown_until",
        "next_ready_at",
        "wait_ms",
        "since_prime_ms",
        "since_tick_ms",
    )
    ctx = {}
    for key in keys:
        value = _ldict.get(key)
        if value is not None:
            ctx[key] = value
    return ctx


# Bybit v5 -> OMS ステータス正規化対応表
STATUS_MAP_EXCHANGE_TO_OMS = {
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
    if s in {"new", "created", "init"}:
        return "new"
    if s in {"partiallyfilled", "partialfilled", "partially_filled", "partial", "partial-fill"}:
        return "partially_filled"
    if s in {"filled", "closed", "done", "full-fill"}:
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
        if _DEBUG_PRIVATE_WS:
            try:
                logger.debug("order.ack.ws.raw: row={}", row)
            except Exception:
                pass
        event = {
            "client_id": row.get("orderLinkId") or row.get("clientOrderId") or row.get("orderId"),
            "status": _status_map(row.get("orderStatus") or row.get("status")),
            "last_filled_qty": 0.0,
            "cum_filled_qty": float(row.get("cumExecQty") or 0),
            "avg_fill_price": float(row.get("avgPrice")) if row.get("avgPrice") is not None else None,
        }
        # --- ステータス正規化（Bybit -> OMS） ---
        status_raw = row.get("orderStatus") or row.get("order_status")
        event["status"] = STATUS_MAP_EXCHANGE_TO_OMS.get(status_raw, status_raw or "NEW")
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
        event["source"] = "ws.execution"
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
        event["fee"] = row.get("execFee") or row.get("fillFee") or row.get("commission")
        event["fee_currency"] = row.get("feeCurrency") or row.get("fillFeeCcy") or row.get("commissionAsset")
        event["order_id"] = row.get("orderId")
        event["exec_id"] = row.get("execId") or row.get("execIdStr") or row.get("execID")
        raw_flags: dict[str, object] = {}
        if "timeInForce" in row and row.get("timeInForce") not in (None, ""):
            raw_flags["time_in_force"] = row.get("timeInForce")
        if "reduceOnly" in row and row.get("reduceOnly") is not None:
            raw_flags["reduce_only"] = row.get("reduceOnly")
        if "orderType" in row and row.get("orderType") not in (None, ""):
            raw_flags["order_type"] = row.get("orderType")
        if raw_flags:
            event["raw_order_flags"] = raw_flags
        liq = row.get("liquidity")
        if liq is not None:
            try:
                liq_str = str(liq).upper()
                if liq_str in {"MAKER", "TAKER"}:
                    event["liquidity"] = liq_str
            except Exception:
                pass
        elif "isMaker" in row:
            try:
                event["liquidity"] = "MAKER" if bool(row.get("isMaker")) else "TAKER"
            except Exception:
                pass
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
async def _run_private_ws(ex: BitgetGateway, oms: OmsEngine, *, symbols: list[str]) -> None:
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
async def _run_public_ws_for_paper(data_ex: BitgetGateway, paper_ex: PaperExchange, symbols: list[str]) -> None:
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

    await data_ex.subscribe_public(symbols=symbols, callbacks={"trade": _public_trade_cb, "orderbook": _orderbook_cb})


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
        # 初期スケール未確定時は評価/発注をスキップ（2回連続で正規化確認）
        try:
            if hasattr(funding_source, "is_price_scale_ready") and not funding_source.is_price_scale_ready(sym, 2):
                # instruments-info を用いた priceScale プライミングを試行し、結果をログに残す
                try:
                    res = getattr(funding_source, "_try_prime_scale", lambda *_: None)(sym)
                    logger.info("scale.prime.call sym={} result={}", sym, res)
                except Exception as e:
                    logger.exception("scale.prime.error sym=%s reason=%s", sym, e)
                reason = "price_scale_not_ready"
                logger.info(
                    "guard.skip sym={} reason={} ctx={}",
                    sym,
                    reason,
                    _format_guard_context(locals()),
                )
                continue
        except Exception:
            pass
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

    global _DEBUG_PRIVATE_WS
    env_flag = os.environ.get("DEBUG_PRIVATE_WS") or os.environ.get("EXCHANGE__DEBUG_PRIVATE_WS")
    if env_flag is not None:
        _DEBUG_PRIVATE_WS = str(env_flag).lower() in {"1", "true", "yes", "on"}
    else:
        _DEBUG_PRIVATE_WS = False

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

    # DB (optional)
    disable_db = str(getattr(cfg, "db_url", "") or "").lower() in {"", "disabled", "none"}
    repo = None if disable_db else Repo(db_url=cfg.db_url)
    if repo is not None:
        await repo.create_all()

    # 実データ源（REST）。dry-run でもデータ源として使用（Bitget）
    data_ex = BitgetGateway(
        api_key=cfg.keys.api_key,
        api_secret=cfg.keys.api_secret,
        passphrase=cfg.keys.passphrase,
        environment=exchange_env,
        use_uta=getattr(cfg.exchange, "use_uta", True),
    )
    if hasattr(data_ex, "_debug_private_ws"):
        data_ex._debug_private_ws = _DEBUG_PRIVATE_WS
    # --- 設定の適用：BitgetGateway（REST/BBO/価格ガード） ---
    data_ex._bbo_max_age_ms = cfg.exchange.bbo_max_age_ms  # BBO鮮度ガード（STEP28）
    data_ex._rest_max_concurrency = cfg.exchange.rest_max_concurrency  # REST同時実行上限（STEP29）
    data_ex._rest_semaphore = asyncio.Semaphore(data_ex._rest_max_concurrency)  # 新しい上限でセマフォを再構築
    if hasattr(data_ex, "_rest_cb_fail_threshold"):
        data_ex._rest_cb_fail_threshold = cfg.exchange.rest_cb_fail_threshold  # サーキット連続失敗回数（STEP31）
    if hasattr(data_ex, "_rest_cb_open_seconds"):
        data_ex._rest_cb_open_seconds = cfg.exchange.rest_cb_open_seconds  # サーキット休止秒（STEP31）
    if hasattr(data_ex, "_instrument_info_ttl_s"):
        data_ex._instrument_info_ttl_s = cfg.exchange.instrument_info_ttl_s  # instruments-infoの自動リフレッシュ間隔（秒）
    if hasattr(data_ex, "_price_dev_bps_limit"):
        data_ex._price_dev_bps_limit = cfg.risk.price_dev_bps_limit  # 価格逸脱ガード[bps]（STEP34）

    # 健診モード：各シンボルの Funding / BBO / 認証REST（open orders）を点検して終了する
    if ops_check:
        syms = list(getattr(cfg.strategy, "symbols", []))
        # Use already imported classes; keep local stdlib imports only
        import csv
        import json
        from datetime import datetime, timezone

        repo_ops = None if disable_db else Repo(db_url=cfg.db_url)
        if repo_ops is not None:
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

        # 理由文字列の文字化けを簡易補正（戦略側の固定文の既知パターン）
        def _normalize_reason(reason: str | None) -> str | None:
            if not reason:
                return reason
            mapping = {
                "�ΏۊO�V���{��": "対象外シンボル",
                "�\\�z�s���̂��߈�U����": "予測消失のためクローズ",
                "Funding�������]�ŃN���[�Y": "Fundingがマイナスのためクローズ",
                "APR��臒l����": "APRが閾値未満",
                "�f���^�����ɂ��ăw�b�W": "デルタ乖離のためヘッジ",
                "�z�[���h�p��": "ホールド継続",
                "���X�N�Ǘ��ŐV�K��~": "リスク管理で新規停止",
                "Funding�\\�z���擾�ł��Ȃ�": "Funding予測値を取得できない",
                "����Funding�͐V�K�ΏۊO": "負のFundingは新規対象外",
                "���ڏ���ɂ�茚�ĕs��": "余力制限により不可",
                "���Ҏ��v���R�X�g����": "期待収益がコスト未満",
                "Funding�@��ɂ��V�K����": "Funding条件により新規実行",
            }
            return mapping.get(reason, reason)

        rows: list[dict] = []
        for sym in syms:
            fi = await data_ex.get_funding_info(sym)
            bid, ask = await data_ex.get_bbo(sym)
            # Private auth check
            auth_ok = True
            auth_err: str | None = None
            try:
                oo = await data_ex.get_open_orders(sym)
                open_n = len(oo)
            except Exception as e:
                auth_ok = False
                auth_err = str(e)
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
                reason = _normalize_reason(getattr(d, "reason", None))
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
            # 何をする？→ Bybitゲートウェイを取得（スケール準備・価格状態を参照するため）
            gw = _get_bybit_gateway(data_ex) or data_ex
            # 数量刻みと最小制約の内訳（ops-checkの“丸めの根拠”を可視化）
            qty_step_spot = qty_step_perp = qty_common = None
            min_qty_spot = min_qty_perp = min_notional_spot = min_notional_perp = None
            try:
                if gw is not None:
                    qs, qp = _qty_steps_for_symbol(gw, sym)
                    qty_step_spot, qty_step_perp = qs, qp
                    qty_common = _qty_common_step(gw, sym)
                    mqs, mqp, mns, mnp = _min_limits_for_symbol(gw, sym)
                    min_qty_spot, min_qty_perp, min_notional_spot, min_notional_perp = mqs, mqp, mns, mnp
            except Exception:
                pass
            rows.append(
                {
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "symbol": sym,
                    "funding_predicted": getattr(fi, "predicted_rate", None),
                    "next_funding_time": str(getattr(fi, "next_funding_time", None)),
                    "bbo_bid": bid,
                    "bbo_ask": ask,
                    # 何をする？→ BBOが“ふつう”かどうかを判定して、ops-checkに書き出す
                    "bbo_valid": _is_bbo_valid(bid, ask),
                    # 何をする？→ 価格スケールの準備状況（True/False）をops-checkに書き出す
                    "price_scale_ready": bool(_scale_ready_for_symbol(gw, sym)) if gw else False,
                    # 何をする？→ 価格ガードの現在状態（READY/FROZEN/NO_ANCHOR/UNKNOWN）をops-checkに書き出す
                    "price_state": _price_state_for_symbol(gw, sym) if gw else "UNKNOWN",
                    # 何をする？→ 市場データREADY判定（Strategyの内部判定を優先。無ければフォールバック）
                    "md_ready": bool(_market_data_ready_for_ops(strat_ops, sym, bid, ask)[0]),
                    "md_reason": str(_market_data_ready_for_ops(strat_ops, sym, bid, ask)[1]),
                    # 何をする？→ OMSのクールダウン現在地を可視化（残り時間ms含む）
                    "cooldown_active": bool(_cooldown_info_for_symbol(oms_ops, sym)[0]),
                    "cooldown_left_ms": int(_cooldown_info_for_symbol(oms_ops, sym)[1]),
                    # 追加: 数量刻み(spot/perp)・共通刻み・最小数量/名目額
                    "qty_step_spot": qty_step_spot,
                    "qty_step_perp": qty_step_perp,
                    "qty_common_step": qty_common,
                    "min_qty_spot": min_qty_spot,
                    "min_qty_perp": min_qty_perp,
                    "min_notional_spot": min_notional_spot,
                    "min_notional_perp": min_notional_perp,
                    "auth": bool(auth_ok),
                    "auth_message": str(getattr(oms_ops, "auth_message", "")),  # 認証理由の可視化（最小追加）
                    "open_orders": int(open_n),
                    "decision": action,
                    "predicted_apr": float(apr) if apr is not None else None,
                    "reason": reason,
                    "auth_error": auth_err,
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
        trade_ex = BitgetGateway(
            api_key=cfg.keys.api_key,
            api_secret=cfg.keys.api_secret,
            passphrase=cfg.keys.passphrase,
            environment=exchange_env,
            use_uta=getattr(cfg.exchange, "use_uta", True),
        )
        if hasattr(trade_ex, "_debug_private_ws"):
            trade_ex._debug_private_ws = _DEBUG_PRIVATE_WS
        # --- 設定の適用：BitgetGateway（REST/BBO/価格ガード） ---
        trade_ex._bbo_max_age_ms = cfg.exchange.bbo_max_age_ms  # BBO鮮度ガード（STEP28）
        trade_ex._rest_max_concurrency = cfg.exchange.rest_max_concurrency  # REST同時実行上限（STEP29）
        trade_ex._rest_semaphore = asyncio.Semaphore(trade_ex._rest_max_concurrency)  # 新しい上限でセマフォを再構築
        if hasattr(trade_ex, "_rest_cb_fail_threshold"):
            trade_ex._rest_cb_fail_threshold = cfg.exchange.rest_cb_fail_threshold  # サーキット連続失敗回数（STEP31）
        if hasattr(trade_ex, "_rest_cb_open_seconds"):
            trade_ex._rest_cb_open_seconds = cfg.exchange.rest_cb_open_seconds  # サーキット休止秒（STEP31）
        if hasattr(trade_ex, "_instrument_info_ttl_s"):
            trade_ex._instrument_info_ttl_s = cfg.exchange.instrument_info_ttl_s  # instruments-infoの自動リフレッシュ間隔（秒）
        if hasattr(trade_ex, "_price_dev_bps_limit"):
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

    _maybe_reset_kill_on_start(rm)  # 起動時に必要ならKILLラッチを自動解除（RISK__RESET_KILL_ON_START=true のときだけ）

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
        assert isinstance(trade_ex, BitgetGateway)
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
    if repo is not None:
        reporter = ReportScheduler(repo=repo, out_dir="reports", hour_utc=0, minute_utc=5)
        tasks.append(asyncio.create_task(reporter.run_forever()))

    # 終了待ち
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        # ccxt リソース解放
        try:
            close_fn = getattr(trade_ex, "close", None)
            if callable(close_fn):
                await close_fn()  # ccxt async close で警告抑制
        except Exception:
            pass
        try:
            close_fn = getattr(data_ex, "close", None)
            if callable(close_fn):
                await close_fn()
        except Exception:
            pass
        if flatten_on_exit:
            try:
                oms._log.info("shutdown.begin flatten_on_exit=true")  # 構造化ログ（STEP38）
            except Exception:
                pass
            try:
                await oms.drain_and_flatten(cfg.strategy.symbols, strategy, timeout_s=20)  # 安全ドレインを実施
            except Exception:
                pass


def _precheck_api_key_from_opscheck(path: str = "ops-check.json") -> bool:
    """起動時に一度だけ実行する“API鍵プレチェック”（動作は変えずに警告だけ出す）。
    - 目的: 直近の ops-check 結果（認証可否/retCode 相当）を読み、人がすぐ気づけるようにする。
    - 返り値: True=問題なさそう / False=問題の可能性あり（この関数では停止しない）
    """

    # 関数内で標準loggingを使う（loguruと並存可）
    import logging
    import re

    logger_std = logging.getLogger(__name__)

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        logger_std.info("API precheck: ops-check.json not found (skip)")
        return True
    except Exception as e:  # 破損JSONなどは警告のみ
        logger_std.warning("API precheck: failed to read/parse ops-check.json: %s", e)
        return True

    problems: list[str] = []

    def _scan_entry(entry: dict) -> None:
        sym = entry.get("symbol") or entry.get("sym") or "?"
        # 1) 明示のauthフラグ
        auth = entry.get("auth")
        if isinstance(auth, bool) and not auth:
            msg = entry.get("auth_message") or entry.get("auth_error") or "auth=false"
            problems.append(f"{sym}: {msg}")
        # 2) retCodeが文字列中に埋まっているケース
        for k in ("auth_message", "auth_error", "message", "error"):
            s = entry.get(k)
            if isinstance(s, str):
                m = re.search(r"retCode\"?\s*:\s*(-?\d+)", s)
                if m and int(m.group(1)) != 0:
                    problems.append(f"{sym}: retCode={m.group(1)} {s}")
        # 3) 直下にretCodeがあるケース
        rc = entry.get("retCode")
        try:
            if rc is not None and int(rc) != 0:
                problems.append(f"{sym}: retCode={rc} {entry.get('retMsg') or entry.get('retMessage') or ''}")
        except Exception:
            pass

    if isinstance(data, list):
        for row in data:
            if isinstance(row, dict):
                _scan_entry(row)
    elif isinstance(data, dict):
        # 1件のみの形式にも対応
        _scan_entry(data)
    else:
        logger_std.info("API precheck: unsupported ops-check format (skip)")
        return True

    if problems:
        # ここでは停止しない。明確なエラーとして通知だけ行う。
        joined = " | ".join(problems)
        if len(joined) > 800:
            joined = joined[:800] + "..."
        logger_std.error("API precheck: NG -- possible auth issue(s): %s", joined)
        return False

    logger_std.info("API precheck: OK -- no recent auth errors in ops-check.json")
    return True


def _maybe_reset_kill_on_start(risk) -> None:
    """
    何をする関数？:
      起動直後に、環境変数 RISK__RESET_KILL_ON_START が "true" のときだけ
      RiskManager.reset_kill(...) を呼んで KILL ラッチを解除する“運用用の入口”。
      - 他のガードやしきい値には影響しない（解除はラッチ解除のみ）
      - なぜ解除したかをログに残し、後から追跡できるようにする
    """
    import logging
    from os import getenv  # この関数内でしか使わないので、例外的にここで import（規約どおり）

    if str(getenv("RISK__RESET_KILL_ON_START", "false")).lower() == "true":
        try:
            risk.reset_kill(reason="env RISK__RESET_KILL_ON_START=true")  # 起動時の“再開ボタン”
        except Exception as e:
            logging.getLogger(__name__).warning("KILL-RESET: failed to reset on start: %s", e)  # 失敗もログに残す


def main() -> None:
    """これは何をする関数？
    → コマンドライン引数を読み取り、イベントループで Live ランナーを起動します。
    """

    _precheck_api_key_from_opscheck()  # 起動時に一度だけAPI鍵の事前チェック結果をログで可視化（動作は止めない）

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
