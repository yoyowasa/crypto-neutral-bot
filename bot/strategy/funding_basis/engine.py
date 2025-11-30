"""Funding/Basis 戦略の評価・実行ロジック。"""

from __future__ import annotations

import datetime
import inspect  # 非同期関数検出のため  # 何をする？→ 型ヒント（BBO取得と妥当性チェック用）
import math  # 何をする？→ 共通刻みによる“切り下げ丸め”で使用
from dataclasses import dataclass, field
from typing import Any, Optional, Tuple

from loguru import logger

from bot.config.models import RiskConfig, StrategyFundingConfig
from bot.exchanges.types import FundingInfo, OrderRequest
from bot.oms.engine import OmsEngine
from bot.risk.guards import RiskManager
from bot.risk.limits import PreTradeContext, precheck_open_order

from .models import Decision, DecisionAction, annualize_rate, net_delta_base, notional_candidate


def _safe_float(val: Any) -> float | None:
    try:
        return float(val) if val is not None else None
    except Exception:
        return None


@dataclass(slots=True)
class _HoldingEntry:
    """単一シンボルの建玉を管理する内部用レコード。"""

    symbol: str
    spot_qty: float = 0.0
    perp_qty: float = 0.0
    spot_price: float = 0.0
    perp_price: float = 0.0

    def total_notional(self) -> float:
        """現状のスポット/パーペ名目を合計して返す。"""

        return abs(self.spot_qty * self.spot_price) + abs(self.perp_qty * self.perp_price)

    def net_delta(self) -> float:
        """ネットデルタ（ベース数量）を返す。"""

        return net_delta_base(self.spot_qty, self.perp_qty)

    def dominant_base_qty(self) -> float:
        """デルタ比率算出に用いる代表量（スポット/パーペのうち大きい方）。"""

        return max(abs(self.spot_qty), abs(self.perp_qty))


@dataclass
class _Holdings:
    """全シンボルの建玉を集計し、名目・デルタを算出する補助。"""

    _entries: dict[str, _HoldingEntry] = field(default_factory=dict)

    def get(self, symbol: str) -> _HoldingEntry | None:
        """対象シンボルの建玉レコードを返す（存在しない場合は None）。"""

        return self._entries.get(symbol)

    def update_open(
        self,
        symbol: str,
        *,
        spot_qty: float,
        spot_price: float,
        perp_qty: float,
        perp_price: float,
    ) -> None:
        """新規建てまたは積み増しの結果を反映する。"""

        entry = self._entries.get(symbol)
        if entry is None:
            entry = _HoldingEntry(symbol=symbol)
            self._entries[symbol] = entry
        entry.spot_qty += spot_qty
        entry.perp_qty += perp_qty
        entry.spot_price = spot_price
        entry.perp_price = perp_price

    def clear(self, symbol: str) -> None:
        """シンボルの建玉をクリアする。"""

        self._entries.pop(symbol, None)

    def used_total_notional(self) -> float:
        """全シンボルで使用中の名目合計を返す。"""

        return sum(entry.total_notional() for entry in self._entries.values())

    def used_symbol_notional(self, symbol: str) -> float:
        """対象シンボルで使用中の名目を返す。"""

        entry = self._entries.get(symbol)
        return entry.total_notional() if entry else 0.0

    def symbols(self) -> list[str]:
        """現在ポジションを持っているシンボル一覧を返す。"""

        return list(self._entries.keys())


class FundingBasisStrategy:
    """Funding/ベーシス戦略の最小実装。"""

    def __init__(
        self,
        *,
        oms: OmsEngine,
        risk_config: RiskConfig,
        strategy_config: StrategyFundingConfig,
        period_seconds: float = 8.0 * 3600.0,
        taker_fee_bps_roundtrip: float | None = None,
        estimated_slippage_bps: float | None = None,
        risk_manager: RiskManager | None = None,
    ) -> None:
        """必要な依存（OMS/設定/リスク管理）を受け取り初期化する。"""

        self._oms = oms
        self._risk_config = risk_config
        self._strategy_config = strategy_config
        self._period_seconds = period_seconds
        self._taker_fee_bps_roundtrip = taker_fee_bps_roundtrip or strategy_config.taker_fee_bps_roundtrip
        self._estimated_slippage_bps = estimated_slippage_bps or strategy_config.estimated_slippage_bps
        self._min_hold_periods = getattr(strategy_config, "min_hold_periods", 1.0)
        self._holdings = _Holdings()
        self._risk_manager = risk_manager or RiskManager(
            loss_cut_daily_jpy=risk_config.loss_cut_daily_jpy,
            flatten_all=self.flatten_all,
            skip_funding_flip_when_flat=True,
        )
        # リスク側に「フラット判定」プローブを渡して、ポジションが無いときの誤KILLを防ぐ
        try:
            self._risk_manager.set_flat_probe(lambda: self._holdings.used_total_notional())
        except Exception:
            pass

    async def step(
        self,
        *,
        funding: FundingInfo,
        spot_price: float,
        perp_price: float,
    ) -> Decision:
        """マーケット情報を受け取り評価→実行をまとめて行う。"""

        decision = self.evaluate(funding=funding, spot_price=spot_price, perp_price=perp_price)
        await self.execute(decision, spot_price=spot_price, perp_price=perp_price)
        return decision

    def evaluate(
        self,
        *,
        funding: FundingInfo,
        spot_price: float,
        perp_price: float,
    ) -> Decision:
        """Fundingと価格から次のアクションを判定する。"""
        candidate: float | None = None
        expected_gain: float | None = None
        expected_cost: float | None = None
        time_to_event_min: float | None = None
        try:
            if funding.next_funding_time:
                delta = funding.next_funding_time - datetime.datetime.now(tz=funding.next_funding_time.tzinfo)
                time_to_event_min = delta.total_seconds() / 60.0
        except Exception:
            time_to_event_min = None

        symbol = funding.symbol
        if symbol not in self._strategy_config.symbols:
            return self._log_decision(
                Decision(action=DecisionAction.SKIP, symbol=symbol, reason="対象外シンボル"),
                symbol=symbol,
                predicted_rate=funding.predicted_rate,
                apr=None,
                candidate=candidate,
                expected_gain=expected_gain,
                expected_cost=expected_cost,
                time_to_event_min=time_to_event_min,
            )

        predicted_rate = funding.predicted_rate
        if predicted_rate is not None:
            self._risk_manager.update_funding_predicted(
                symbol=symbol,
                predicted_rate=predicted_rate,
            )
        apr = (
            annualize_rate(predicted_rate, period_seconds=self._period_seconds) if predicted_rate is not None else None
        )

        holding = self._holdings.get(symbol)

        if holding:
            if predicted_rate is None:
                return self._log_decision(
                    Decision(
                        action=DecisionAction.CLOSE,
                        symbol=symbol,
                        reason="予想なしのためクローズ",
                        predicted_apr=None,
                    ),
                    symbol=symbol,
                    predicted_rate=predicted_rate,
                    apr=apr,
                    candidate=candidate,
                    expected_gain=expected_gain,
                    expected_cost=expected_cost,
                    time_to_event_min=time_to_event_min,
                )
            if predicted_rate <= 0:
                return self._log_decision(
                    Decision(
                        action=DecisionAction.CLOSE,
                        symbol=symbol,
                        reason="Funding符号反転でクローズ",
                        predicted_apr=apr,
                    ),
                    symbol=symbol,
                    predicted_rate=predicted_rate,
                    apr=apr,
                    candidate=candidate,
                    expected_gain=expected_gain,
                    expected_cost=expected_cost,
                    time_to_event_min=time_to_event_min,
                )

            net_delta = holding.net_delta()
            dominant_qty = holding.dominant_base_qty()
            if dominant_qty > 0:
                delta_bps = abs(net_delta) / dominant_qty * 10000.0
                if delta_bps > self._strategy_config.rebalance_band_bps:
                    return self._log_decision(
                        Decision(
                            action=DecisionAction.HEDGE,
                            symbol=symbol,
                            reason="デルタ乖離によりヘッジ",
                            predicted_apr=apr,
                            delta_to_neutral=-net_delta,
                        ),
                        symbol=symbol,
                        predicted_rate=predicted_rate,
                        apr=apr,
                        candidate=candidate,
                        expected_gain=expected_gain,
                        expected_cost=expected_cost,
                        time_to_event_min=time_to_event_min,
                    )

            return self._log_decision(
                Decision(action=DecisionAction.SKIP, symbol=symbol, reason="ホールド継続", predicted_apr=apr),
                symbol=symbol,
                predicted_rate=predicted_rate,
                apr=apr,
                candidate=candidate,
                expected_gain=expected_gain,
                expected_cost=expected_cost,
                time_to_event_min=time_to_event_min,
            )

        if self._risk_manager.disable_new_orders:
            return self._log_decision(
                Decision(action=DecisionAction.SKIP, symbol=symbol, reason="リスク管理で新規停止", predicted_apr=apr),
                symbol=symbol,
                predicted_rate=predicted_rate,
                apr=apr,
                candidate=candidate,
                expected_gain=expected_gain,
                expected_cost=expected_cost,
                time_to_event_min=time_to_event_min,
            )

        if predicted_rate is None:
            return self._log_decision(
                Decision(action=DecisionAction.SKIP, symbol=symbol, reason="Funding予想が取得できない"),
                symbol=symbol,
                predicted_rate=predicted_rate,
                apr=apr,
                candidate=candidate,
                expected_gain=expected_gain,
                expected_cost=expected_cost,
                time_to_event_min=time_to_event_min,
            )

        if predicted_rate <= 0:
            return self._log_decision(
                Decision(
                    action=DecisionAction.SKIP,
                    symbol=symbol,
                    reason="負のFundingは新規対象外",
                    predicted_apr=apr,
                ),
                symbol=symbol,
                predicted_rate=predicted_rate,
                apr=apr,
                candidate=candidate,
                expected_gain=expected_gain,
                expected_cost=expected_cost,
                time_to_event_min=time_to_event_min,
            )

        if apr is not None and apr < self._strategy_config.min_expected_apr:
            return self._log_decision(
                Decision(action=DecisionAction.SKIP, symbol=symbol, reason="APRが閾値未満", predicted_apr=apr),
                symbol=symbol,
                predicted_rate=predicted_rate,
                apr=apr,
                candidate=candidate,
                expected_gain=expected_gain,
                expected_cost=expected_cost,
                time_to_event_min=time_to_event_min,
            )

        used_total = self._holdings.used_total_notional()
        used_symbol = self._holdings.used_symbol_notional(symbol)
        candidate = notional_candidate(
            risk=self._risk_config,
            used_total_notional=used_total,
            used_symbol_notional=used_symbol,
        )
        if candidate <= 0:
            return self._log_decision(
                Decision(
                    action=DecisionAction.SKIP,
                    symbol=symbol,
                    reason="利用可能な名目なし",
                    predicted_apr=apr,
                ),
                symbol=symbol,
                predicted_rate=predicted_rate,
                apr=apr,
                candidate=candidate,
                expected_gain=expected_gain,
                expected_cost=expected_cost,
                time_to_event_min=time_to_event_min,
            )

        expected_gain = predicted_rate * candidate * self._min_hold_periods
        total_cost_bps = self._taker_fee_bps_roundtrip + self._estimated_slippage_bps
        expected_cost = candidate * total_cost_bps / 10000.0
        if expected_gain <= expected_cost:
            return self._log_decision(
                Decision(action=DecisionAction.SKIP, symbol=symbol, reason="期待収益がコスト未満", predicted_apr=apr),
                symbol=symbol,
                predicted_rate=predicted_rate,
                apr=apr,
                candidate=candidate,
                expected_gain=expected_gain,
                expected_cost=expected_cost,
                time_to_event_min=time_to_event_min,
            )

        return self._log_decision(
            Decision(
                action=DecisionAction.OPEN,
                symbol=symbol,
                reason="Funding期待が十分なため新規建て",
                predicted_apr=apr,
                notional=candidate,
                perp_side="sell",
                spot_side="buy",
            ),
            symbol=symbol,
            predicted_rate=predicted_rate,
            apr=apr,
            candidate=candidate,
            expected_gain=expected_gain,
            expected_cost=expected_cost,
            time_to_event_min=time_to_event_min,
        )

    def _log_decision(
        self,
        decision: Decision,
        *,
        symbol: str,
        predicted_rate: float | None,
        apr: float | None,
        candidate: float | None,
        expected_gain: float | None,
        expected_cost: float | None,
        time_to_event_min: float | None,
    ) -> Decision:
        """evaluate の判定内容をデバッグ出力する。"""
        try:
            logger.debug(
                "decide sym={} act={} reason={} prate={} apr={} cand={} gain={} cost={} tmin={}",
                symbol,
                decision.action.value,
                decision.reason,
                predicted_rate,
                apr,
                candidate,
                expected_gain,
                expected_cost,
                time_to_event_min,
            )
        except Exception:
            pass
        return decision

    async def execute(self, decision: Decision, *, spot_price: float, perp_price: float) -> None:
        """Evaluateで得たアクションを実際の発注へ変換する。"""

        if decision.action is DecisionAction.SKIP:
            logger.debug("FundingBasis: skip -> {}", decision.reason)
            return

        if decision.action is DecisionAction.HEDGE:
            delta = decision.delta_to_neutral
            if delta == 0:
                holding = self._holdings.get(decision.symbol)
                if holding:
                    delta = -holding.net_delta()
            if delta != 0:
                await self._oms.submit_hedge(decision.symbol, delta)
                holding = self._holdings.get(decision.symbol)
                if holding:
                    holding.perp_qty += delta
            return

        if decision.action is DecisionAction.CLOSE:
            await self._close_symbol(decision.symbol)
            return

        if decision.action is DecisionAction.OPEN:
            await self._open_basis_position(decision, spot_price=spot_price, perp_price=perp_price)
            return

        logger.debug("FundingBasis: 未対応アクション {}", decision.action)

    async def flatten_all(self) -> None:
        """全シンボルの建玉を成行で解消する。"""

        for symbol in list(self._holdings.symbols()):
            await self._close_symbol(symbol)

    async def _open_basis_position(self, decision: Decision, *, spot_price: float, perp_price: float) -> None:
        """新規でFunding/Basisポジションを組成する。"""

        # 何をする？→ OPENの直前に“市場データがREADYか”を確認し、ダメなら安全にスキップする
        ready, reason = self._market_data_ready(decision.symbol)
        if not ready:
            logger.info("decision.skip: market data not ready sym={} reason={}", decision.symbol, reason)
            return  # データが整っていないので発注しない（監視のみ運転）

        if self._risk_manager.disable_new_orders:
            logger.warning("FundingBasis: リスク制限により新規建てをスキップ")
            return

        if decision.notional <= 0:
            logger.warning("FundingBasis: 名目が0以下のため新規建てをスキップ")
            return

        if spot_price <= 0 or perp_price <= 0:
            raise ValueError("spot_price/perp_price must be positive")

        ctx = PreTradeContext(
            used_total_notional=self._holdings.used_total_notional(),
            used_symbol_notional=self._holdings.used_symbol_notional(decision.symbol),
            predicted_net_delta_after=0.0,
            estimated_slippage_bps=self._estimated_slippage_bps,
        )
        precheck_open_order(symbol=decision.symbol, add_notional=decision.notional, ctx=ctx, risk=self._risk_config)

        # 何をする？→ 両足の数量を“アンカー価格で一本化”し、初回から同一数量にそろえる
        base_qty = self._compute_open_base_qty(decision.symbol, decision.notional)
        if base_qty is None or base_qty <= 0.0:
            logger.info("decision.skip: base qty not computable sym={}", decision.symbol)
            return None  # データが整っていないので発注しない（監視のみ運転）
        # 何をする？→ 共通刻みで丸め、最小数量/名目額を満たすか確認してから両足に同一数量を設定する
        symbol = decision.symbol
        qty_final = self._round_qty_to_common_step(symbol, float(base_qty))
        if qty_final is None or qty_final <= 0.0:
            logger.info("decision.skip: qty_non_positive_after_round sym={}", symbol)
            return None
        anchor_px = self._anchor_price(symbol)
        ok_limits, reason_limits = self._min_limits_ok(symbol, qty_final, anchor_px)
        if not ok_limits:
            logger.info("decision.skip: {} sym={}", reason_limits, symbol)
            return None
        spot_qty = float(qty_final)
        perp_qty = float(qty_final)
        signed_spot_qty = spot_qty if (decision.spot_side or "buy") == "buy" else -spot_qty
        signed_perp_qty = -perp_qty if (decision.perp_side or "sell") == "sell" else perp_qty

        perp_req = OrderRequest(
            symbol=decision.symbol,
            side=decision.perp_side or "sell",
            type="market",
            qty=abs(perp_qty),
            time_in_force="IOC",
            reduce_only=False,
            post_only=False,
        )
        spot_req = OrderRequest(
            symbol=decision.symbol,
            side=decision.spot_side or "buy",
            type="market",
            qty=abs(spot_qty),
            time_in_force="IOC",
            reduce_only=False,
            post_only=False,
        )

        logger.info(
            "FundingBasis: open {} notional={} perp_side={} spot_side={}",
            decision.symbol,
            decision.notional,
            decision.perp_side,
            decision.spot_side,
        )

        await self._oms.submit(perp_req)
        await self._oms.submit(spot_req)
        self._holdings.update_open(
            decision.symbol,
            spot_qty=signed_spot_qty,
            spot_price=spot_price,
            perp_qty=signed_perp_qty,
            perp_price=perp_price,
        )

    async def _close_symbol(self, symbol: str) -> None:
        """指定シンボルの建玉を解消する。"""

        holding = self._holdings.get(symbol)
        if not holding:
            return

        logger.info("FundingBasis: close {}", symbol)

        if holding.perp_qty != 0:
            side = "buy" if holding.perp_qty < 0 else "sell"
            req = OrderRequest(
                symbol=symbol,
                side=side,
                type="market",
                qty=abs(holding.perp_qty),
                time_in_force="IOC",
                reduce_only=True,
                post_only=False,
            )
            await self._oms.submit(req)

        if holding.spot_qty != 0:
            side = "sell" if holding.spot_qty > 0 else "buy"
            req = OrderRequest(
                symbol=symbol,
                side=side,
                type="market",
                qty=abs(holding.spot_qty),
                time_in_force="IOC",
                reduce_only=True,  # 全クローズでは必ずreduce-onlyにして新規建てを防ぐ
                post_only=False,
            )
            await self._oms.submit(req)

        self._holdings.clear(symbol)

    def _market_data_ready(self, symbol: str) -> Tuple[bool, str]:
        """何をする関数？→ 価格スケールと価格ガードの状態を調べ、発注してよいか（READYか）を判定する。"""

        # ゲートウェイ（BitgetGatewayなど）を“ていねいに探す”（属性名の揺れを吸収）
        gw: Any | None = None
        try:
            for name in ("bitget_gateway", "bitget", "gateway", "exchange", "ex"):
                cand = getattr(self, name, None)
                if cand is not None and hasattr(cand, "_scale_cache"):
                    gw = cand
                    break
            # 直接見つからなければ OMS 経由でも探す
            if gw is None:
                oms = getattr(self, "_oms", None)
                if oms is not None:
                    for name in ("bitget_gateway", "bitget", "gateway", "exchange", "ex", "_ex"):
                        cand = getattr(oms, name, None)
                        if cand is None and hasattr(oms, "_ex"):
                            cand = getattr(oms, "_ex", None)
                        if cand is not None and hasattr(cand, "_scale_cache"):
                            gw = cand
                            break
        except Exception:
            gw = None

        if gw is None:
            return False, "no_gateway"  # ゲートウェイが見つからない→発注は安全に見送る

        # スケール準備チェック（priceScaleが未準備ならダメ）
        try:
            scale_info = getattr(gw, "_scale_cache", {}).get(symbol) or {}
        except Exception:
            scale_info = {}
        ready_scale = scale_info.get("priceScale") is not None
        scale_keys = []
        try:
            scale_keys = sorted(getattr(gw, "_scale_cache", {}).keys())
        except Exception:
            scale_keys = []

        try:
            state = getattr(gw, "_price_state", {}).get(symbol, "UNKNOWN")
        except Exception:
            state = "UNKNOWN"
        ready_state = state == "READY"

        bid, ask = self._get_bbo(symbol)
        ready_bbo = self._is_bbo_valid(bid, ask)

        if not ready_scale:
            return (
                False,
                f"price_scale_not_ready scale_meta={ready_scale} bbo={ready_bbo} state={state} cache_keys={scale_keys}",
            )
        if not ready_state:
            return False, f"price_state={state} scale_meta={ready_scale} bbo={ready_bbo} cache_keys={scale_keys}"
        if not ready_bbo:
            return False, f"bbo_invalid scale_meta={ready_scale} state={state} cache_keys={scale_keys}"

        return True, "OK"  # READY：発注してよい

    def _get_bbo(self, symbol: str) -> Tuple[Optional[float], Optional[float]]:
        """何をする関数？→ 現在の最良気配(BBO)を“ていねいに探して” (bid, ask) を返す。見つからなければ (None, None)。"""

        # 1) Strategy自身に get_bbo があれば使う
        if hasattr(self, "get_bbo"):
            try:
                b = self.get_bbo(symbol)
                if isinstance(b, (list, tuple)) and len(b) >= 2:
                    return float(b[0]), float(b[1])
                if isinstance(b, dict):
                    return (
                        _safe_float(b.get("bid")),
                        _safe_float(b.get("ask")),
                    )
            except Exception:
                pass

        # 2) よくある集約器（market / md）に get_bbo があれば使う
        for comp_name in ("market", "md"):
            comp = getattr(self, comp_name, None)
            if comp is not None and hasattr(comp, "get_bbo"):
                try:
                    b = comp.get_bbo(symbol)
                    if isinstance(b, (list, tuple)) and len(b) >= 2:
                        return float(b[0]), float(b[1])
                    if isinstance(b, dict):
                        return (
                            _safe_float(b.get("bid")),
                            _safe_float(b.get("ask")),
                        )
                except Exception:
                    pass

        # 3) ゲートウェイから拝借（属性名の揺れに対応）
        gw = None
        for name in ("bitget_gateway", "bitget", "gateway", "exchange", "ex"):
            cand = getattr(self, name, None)
            if cand is not None:
                gw = cand
                break
        if gw is None:
            oms = getattr(self, "_oms", None)
            if oms is not None:
                for name in ("bitget_gateway", "bitget", "gateway", "exchange", "ex", "_ex"):
                    cand = getattr(oms, name, None)
                    if cand is None and hasattr(oms, "_ex"):
                        cand = getattr(oms, "_ex", None)
                    if cand is not None:
                        gw = cand
                        break
        if gw is None:
            oms = getattr(self, "_oms", None)
            if oms is not None:
                for name in ("bitget_gateway", "bitget", "gateway", "exchange", "ex", "_ex"):
                    cand = getattr(oms, name, None)
                    if cand is None and hasattr(oms, "_ex"):
                        cand = getattr(oms, "_ex", None)
                    if cand is not None:
                        gw = cand
                        break
        if gw is not None:
            # 3a) メソッドがあれば最優先（同期想定。非同期の場合は下のキャッシュにフォールバック）
            if hasattr(gw, "get_bbo"):
                # get_bbo が async def の場合はここでは呼ばない（未await警告を避ける）
                fn = gw.get_bbo
                if not inspect.iscoroutinefunction(fn):
                    try:
                        b = fn(symbol)
                        if isinstance(b, (list, tuple)) and len(b) >= 2:
                            return float(b[0]), float(b[1])
                        if isinstance(b, dict):
                            return (
                                _safe_float(b.get("bid")),
                                _safe_float(b.get("ask")),
                            )
                    except Exception:
                        pass
            # 3b) 代表的な属性から推測（_bbo_cacheなど）
            for attr in ("_bbo_cache", "_last_bbo", "bbo", "_bbo"):
                store = getattr(gw, attr, None)
                if isinstance(store, dict) and symbol in store:
                    v = store[symbol]
                    try:
                        if isinstance(v, (list, tuple)) and len(v) >= 2:
                            return float(v[0]), float(v[1])
                        if isinstance(v, dict):
                            return (
                                _safe_float(v.get("bid")),
                                _safe_float(v.get("ask")),
                            )
                    except Exception:
                        pass
            # 3c) orderbook から先頭気配を拾う（形式が合えば）
            for ob_attr in ("orderbook", "_orderbook", "_books"):
                ob = getattr(gw, ob_attr, None)
                if isinstance(ob, dict) and symbol in ob:
                    try:
                        book = ob[symbol]
                        bids = book.get("bids") or []
                        asks = book.get("asks") or []
                        bid_px = float(bids[0][0]) if bids and isinstance(bids[0], (list, tuple)) else None
                        ask_px = float(asks[0][0]) if asks and isinstance(asks[0], (list, tuple)) else None
                        if bid_px is not None or ask_px is not None:
                            return bid_px, ask_px
                    except Exception:
                        pass

        # 4) 見つからなければ (None, None)
        return None, None

    def _is_bbo_valid(self, bid: Optional[float], ask: Optional[float]) -> bool:
        """何をする関数？→ BBOが“ふつう”か（正の値で bid < ask）を判定する。"""
        try:
            if bid is None or ask is None:
                return False
            b = float(bid)
            a = float(ask)
            if not (b > 0.0 and a > 0.0):
                return False
            return b < a  # 同値や逆転は不正
        except Exception:
            return False

    def _round_qty_to_common_step(self, symbol: str, qty: float) -> Optional[float]:
        """何をする関数？→ ゲートウェイの“共通刻み”に合わせて数量を安全側（切り下げ）で丸める。"""
        gw = None
        for name in ("bitget_gateway", "bitget", "gateway", "exchange", "ex"):
            cand = getattr(self, name, None)
            if cand is not None:
                gw = cand
                break
        if gw is None or not hasattr(gw, "_common_qty_step"):
            return round(float(qty), 8)
        step = gw._common_qty_step(symbol)
        if not step or step <= 0.0:
            return round(float(qty), 8)
        k = math.floor(float(qty) / float(step))
        rounded = float(k) * float(step)
        return round(max(0.0, rounded), 8)

    def _min_limits_ok(self, symbol: str, qty: float, anchor_px: Optional[float]) -> Tuple[bool, str]:
        """何をする関数？→ 両足の“最小数量/最小名目額”を同時に満たすか判定し、理由を返す。"""
        gw = None
        for name in ("bitget_gateway", "bitget", "gateway", "exchange", "ex"):
            cand = getattr(self, name, None)
            if cand is not None and hasattr(cand, "_scale_cache"):
                gw = cand
                break
        if gw is None:
            oms = getattr(self, "_oms", None)
            if oms is not None:
                for name in ("bitget_gateway", "bitget", "gateway", "exchange", "ex", "_ex"):
                    cand = getattr(oms, name, None)
                    if cand is None and hasattr(oms, "_ex"):
                        cand = getattr(oms, "_ex", None)
                    if cand is not None and hasattr(cand, "_scale_cache"):
                        gw = cand
                        break
        if gw is None:
            return False, "no_gateway"

        info = getattr(gw, "_scale_cache", {}).get(symbol) or {}
        min_qty_spot = float(info.get("minQty_spot") or 0.0)
        min_qty_perp = float(info.get("minQty_perp") or 0.0)
        min_qty = max(min_qty_spot, min_qty_perp)
        if min_qty > 0.0 and qty < min_qty:
            return False, f"qty_below_min(min={min_qty})"

        min_notional_spot = float(info.get("minNotional_spot") or 0.0)
        min_notional_perp = float(info.get("minNotional_perp") or 0.0)
        min_notional = max(min_notional_spot, min_notional_perp)
        if min_notional > 0.0 and (anchor_px and anchor_px > 0.0):
            if (anchor_px * qty) < min_notional:
                return False, f"notional_below_min(min={min_notional})"

        return True, "OK"

    def _anchor_price(self, symbol: str) -> Optional[float]:
        """何をする関数？→ 両足の“基準”となるアンカー価格（spot→indexの順）を取得する。"""
        # ゲートウェイを Strategy 自身→OMS 経由の順で“ていねいに探索”
        gw = None
        for owner in (self, getattr(self, "_oms", None)):
            if owner is None:
                continue
            for name in ("bitget_gateway", "bitget", "gateway", "exchange", "ex", "_ex"):
                cand = getattr(owner, name, None)
                if cand is not None:
                    gw = cand
                    break
            if gw is not None:
                break
        if gw is None:
            return None
        # BitgetGateway が保持する最新の現物/インデックス価格を参照（どちらかあれば採用）
        spot_px = getattr(gw, "_last_spot_px", {}).get(symbol) if hasattr(gw, "_last_spot_px") else None
        index_px = getattr(gw, "_last_index_px", {}).get(symbol) if hasattr(gw, "_last_index_px") else None
        val = spot_px or index_px
        return float(val) if val is not None else None

    def _compute_open_base_qty(self, symbol: str, notional_usd: float) -> Optional[float]:
        """何をする関数？→ OPEN時の両足に共通で使う“ベース数量”を、アンカー価格で1回だけ計算する。"""
        anchor_px = self._anchor_price(symbol)
        if not anchor_px or anchor_px <= 0.0:
            return None
        # 小数誤差での微差を避けるため、8桁に丸める（取引所のlotStepが判明したら置換）
        return round(float(notional_usd) / float(anchor_px), 8)
