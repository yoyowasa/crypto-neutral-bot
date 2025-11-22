"""ops-check の拡張用ヘルパー群と簡易エクスポータ。

最小の“見える化”として、以下を提供します。
- bbo_valid: BBO が正常か（bid/ask > 0 かつ bid < ask）
- price_scale_ready: Bybit ゲートウェイの価格スケールが準備完了か
- price_state: 価格ガードの現在状態（READY/FROZEN/NO_ANCHOR/UNKNOWN）
"""

from __future__ import annotations

import time  # 何をする？→ クールダウン残り時間(ms)を計算するため現在時刻を使う

# 何をする？→ ヘルパー関数の型ヒントに使う（読みやすさのため）
from typing import Any, Optional, Tuple  # 何をする？→ ヘルパーの戻り値 (bool, str) に Tuple を使うために追加


def _is_bbo_valid(bid: Optional[float], ask: Optional[float]) -> bool:
    """何をする関数？→ BBO（最良気配）が“ふつう”か判定する。
    bid/ask が正で、かつ bid < ask を満たすこと。
    """

    if bid is None or ask is None:
        return False
    try:
        bid_f = float(bid)
        ask_f = float(ask)
    except Exception:
        return False
    if bid_f <= 0 or ask_f <= 0:
        return False
    return bid_f < ask_f  # 同値や逆転は不正とみなす


def _get_bybit_gateway(engine: Any) -> Optional[Any]:
    """何をする関数？→ Engineの中からBybitゲートウェイ(価格スケールや状態を持つ)を“ていねいに探す”。
    よくある置き場所を順に探す（どれも無ければ None）。
    """

    # 直接そのものの場合
    if engine is None:
        return None
    if hasattr(engine, "_scale_cache") and hasattr(engine, "_price_state"):
        return engine
    # よくある属性名を順に見る
    for name in ("bybit_gateway", "bybit", "gateway", "exchange", "ex", "data_ex", "price_source"):
        gw = getattr(engine, name, None)
        if gw is not None and hasattr(gw, "_scale_cache"):
            return gw
    return None


def _scale_ready_for_symbol(gw: Any, symbol: str) -> bool:
    """何をする関数？→ シンボルの“価格スケール準備OKか”をゲートウェイのキャッシュから調べる。"""

    try:
        # BybitGateway._scale_cache には {sym: {"priceScale": int, ...}} が入る
        info = getattr(gw, "_scale_cache", {}).get(symbol) or {}
        return info.get("priceScale") is not None
    except Exception:
        return False


def _price_state_for_symbol(gw: Any, symbol: str) -> str:
    """何をする関数？→ 価格ガードの現在状態（READY/FROZEN/NO_ANCHORなど）を取り出す。"""

    try:
        return getattr(gw, "_price_state", {}).get(symbol, "UNKNOWN")
    except Exception:
        return "UNKNOWN"


def _qty_steps_for_symbol(gw: Any, symbol: str) -> Tuple[Optional[float], Optional[float]]:
    """何をする関数？→ ゲートウェイのキャッシュから spot/perp の数量刻み(qtyStep)を取り出す。"""
    try:
        info = getattr(gw, "_scale_cache", {}).get(symbol) or {}
        q_spot = info.get("qtyStep_spot")
        q_perp = info.get("qtyStep_perp")
        return (float(q_spot) if q_spot is not None else None, float(q_perp) if q_perp is not None else None)
    except Exception:
        return None, None


def _qty_common_step(gw: Any, symbol: str) -> Optional[float]:
    """何をする関数？→ 両足で通る“共通刻み(LCM)”を返す。ゲートウェイに実装があればそれを使い、無ければ None。"""
    try:
        if hasattr(gw, "_common_qty_step"):
            step = gw._common_qty_step(symbol)
            return float(step) if step is not None else None
    except Exception:
        pass
    return None


def _min_limits_for_symbol(
    gw: Any, symbol: str
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """何をする関数？→ 最小数量(minQty)と最小名目額(minNotional)を spot/perp 別に取り出す。"""
    try:
        info = getattr(gw, "_scale_cache", {}).get(symbol) or {}
        mq_s = info.get("minQty_spot")
        mq_p = info.get("minQty_perp")
        mn_s = info.get("minNotional_spot")
        mn_p = info.get("minNotional_perp")
        return (
            float(mq_s) if mq_s is not None else None,
            float(mq_p) if mq_p is not None else None,
            float(mn_s) if mn_s is not None else None,
            float(mn_p) if mn_p is not None else None,
        )
    except Exception:
        return None, None, None, None


def _get_oms(engine: Any) -> Optional[Any]:
    """何をする関数？→ Engineの中からOMSエンジン(クールダウン状態を持つ)を“ていねいに探す”。"""

    for name in ("oms", "order_manager", "om", "broker"):
        oms = getattr(engine, name, None)
        if oms is not None:
            return oms
    # Engine自体がOMSの可能性
    if getattr(engine, "submit", None) and getattr(engine, "cancel", None):
        return engine
    return None


def _cooldown_info_for_symbol(oms: Any, symbol: str) -> Tuple[bool, int]:
    """何をする関数？→ 指定シンボルのクールダウン中かどうかと、残り時間[ms]を返す。"""

    now_ms = int(time.time() * 1000)
    active = False
    left_ms = 0

    # 代表的な実装に対応: *_cooldown_until / *symbol*_cooldown_until に解除予定のUNIX時刻(ms/秒)を保持
    for attr in ("_symbol_cooldown_until", "symbol_cooldown_until", "_cooldown_until", "cooldown_until"):
        try:
            until_map = getattr(oms, attr, {}) or {}
            until_ts = until_map.get(symbol)
            if isinstance(until_ts, (int, float)):
                # 秒/ミリ秒の両対応
                until_ms = int(until_ts * 1000) if until_ts < 1_000_000_000_000 else int(until_ts)
                remain = max(0, until_ms - now_ms)
                if remain > 0:
                    active = True
                    left_ms = remain
                    break
        except Exception:
            continue

    # フラグ（_cooldown_active）があれば優先的に活用（重複防止フラグをそのまま可視化）
    try:
        flag = getattr(oms, "_cooldown_active", {}) or {}
        if isinstance(flag, dict):
            active = bool(flag.get(symbol, active))
    except Exception:
        pass

    return active, int(left_ms)


def _market_data_ready_for_ops(
    engine: Any, symbol: str, bid: Optional[float], ask: Optional[float]
) -> Tuple[bool, str]:
    """Engine判定を優先し、無ければ(スケール/価格状態/BBO)でREADYかを判定して理由も返す。"""

    # 1) Engine側の内部判定があればそれを使う（ステップ6で追加した _market_data_ready）
    fn = getattr(engine, "_market_data_ready", None)
    if callable(fn):
        try:
            ok, reason = fn(symbol)
            return bool(ok), str(reason)
        except Exception:
            pass

    # 2) フォールバック：ゲートウェイから状態を読む
    gw = _get_bybit_gateway(engine)
    if not gw:
        return False, "no_gateway"

    # 2a) スケール準備（priceScale）が無ければNG
    if not _scale_ready_for_symbol(gw, symbol):
        return False, "price_scale_not_ready"

    # 2b) 価格ガード状態（READY 以外はNG）
    state = _price_state_for_symbol(gw, symbol)
    if state != "READY":
        return False, f"price_state={state}"

    # 2c) BBOの常識性（bid<ask かつ正の値）を確認
    if not _is_bbo_valid(bid, ask):
        return False, "bbo_invalid"

    return True, "OK"


async def export_ops_check(engine: Any, symbols: list[str]) -> list[dict]:
    """何をする関数？→ 最小限の ops-check 行を構築して返す（既存フローに影響しないユーティリティ）。

    - BBO と Funding は engine に get_bbo/get_funding_info がある場合のみ取得（なければ None）。
    - bbo_valid / price_scale_ready / price_state を追記して“見える化”。
    """

    rows: list[dict] = []
    gw = _get_bybit_gateway(engine)

    for sym in symbols:
        # 価格・Funding の取得（失敗しても None 埋め）
        bid = ask = None
        funding_pred = None
        next_time = None
        try:
            if hasattr(engine, "get_bbo"):
                bid, ask = await engine.get_bbo(sym)
        except Exception:
            bid, ask = (None, None)
        try:
            if hasattr(engine, "get_funding_info"):
                fi = await engine.get_funding_info(sym)
                funding_pred = getattr(fi, "predicted_rate", None)
                next_time = getattr(fi, "next_funding_time", None)
        except Exception:
            funding_pred, next_time = (None, None)

        row: dict = {
            "symbol": sym,
            "funding_predicted": funding_pred,
            "next_funding_time": str(next_time) if next_time is not None else None,
            "bbo_bid": bid,
            "bbo_ask": ask,
        }

        # 何をする？→ BBOが“ふつう”かどうかを判定して、ops-checkに書き出す
        row["bbo_valid"] = _is_bbo_valid(row.get("bbo_bid"), row.get("bbo_ask"))

        # 何をする？→ 価格スケールの準備状況（True/False）をops-checkに書き出す
        sym_str = str(row.get("symbol")) if row.get("symbol") is not None else ""
        row["price_scale_ready"] = bool(_scale_ready_for_symbol(gw, sym_str)) if gw else False

        # 何をする？→ 価格ガードの現在状態（READY/FROZEN/NO_ANCHOR/UNKNOWN）をops-checkに書き出す
        row["price_state"] = _price_state_for_symbol(gw, sym_str) if gw else "UNKNOWN"

        # 何をする？→ 市場データREADY判定（Engineの実装があればそれを優先。無ければフォールバック）
        md_ok, md_reason = _market_data_ready_for_ops(engine, sym_str, row.get("bbo_bid"), row.get("bbo_ask"))
        row["md_ready"] = bool(md_ok)
        row["md_reason"] = str(md_reason)

        # 何をする？→ 数量刻み(spot/perp)と共通刻み、最小数量/名目額をops-checkに追記して“丸めの根拠”を可視化する
        if gw:
            q_spot, q_perp = _qty_steps_for_symbol(gw, sym_str)
            row["qty_step_spot"] = q_spot
            row["qty_step_perp"] = q_perp
            row["qty_common_step"] = _qty_common_step(gw, sym_str)
            min_qty_spot, min_qty_perp, min_notional_spot, min_notional_perp = _min_limits_for_symbol(gw, sym_str)
            row["min_qty_spot"] = min_qty_spot
            row["min_qty_perp"] = min_qty_perp
            row["min_notional_spot"] = min_notional_spot
            row["min_notional_perp"] = min_notional_perp

        # 何をする？→ OMSからクールダウン状態を取得して、ops-checkに書き出す
        oms = _get_oms(engine)
        if oms is not None:
            cd_active, cd_left_ms = _cooldown_info_for_symbol(oms, sym_str)
            row["cooldown_active"] = bool(cd_active)
            row["cooldown_left_ms"] = int(cd_left_ms)
        else:
            row["cooldown_active"] = False
            row["cooldown_left_ms"] = 0

        rows.append(row)

    return rows


def _get_exchange_gateway(engine: Any) -> Optional[Any]:
    """汎用のゲートウェイ探索ヘルパー（Bybit/Bitget 等）。
    実装は後方互換性のため _get_bybit_gateway をそのまま利用する。
    """

    return _get_bybit_gateway(engine)
