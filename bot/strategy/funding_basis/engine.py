"""Funding/Basis 戦略の評価・実行ロジック。"""

from __future__ import annotations

import asyncio  # close/flatten の二重送信を防ぐ排他制御で使用
import datetime
import inspect  # FundingBasisStrategyがどこから呼ばれているか(呼び出し元スタック)を調べるために使う標準ライブラリ
import math  # 何をする？→ 共通刻みによる“切り下げ丸め”で使用
import os  # バックテスト時に負のFunding制限を環境変数で無効化するために使う
import types  # primary_gatewayがSimpleNamespaceでラップされている場合に中の本物のBitgetGatewayを取り出すために使う
import uuid  # サイクルID（open→closeの相関）生成に使う
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
        primary_gateway: Any | None = None,
    ) -> None:
        """必要な依存（OMS/設定/リスク管理）を受け取り初期化する。"""

        self._oms = oms
        if primary_gateway is None and hasattr(oms, "_ex"):
            primary_gateway = oms._ex
        # primary_gatewayがSimpleNamespaceなどのラッパーになっている場合に、中に入っている本物のBitgetGatewayを取り出す
        pg = primary_gateway  # 作業用変数に一度コピーする
        if isinstance(pg, types.SimpleNamespace):
            # SimpleNamespaceで2重ラップされているケースに対応する
            visited_ids = set()  # 無限ループ防止のため、たどったオブジェクトIDを覚えておく
            while isinstance(pg, types.SimpleNamespace) and id(pg) not in visited_ids:
                visited_ids.add(id(pg))
                # 一番内側にいる本物のゲートウェイ候補(inner_gw)を優先して探す
                if hasattr(pg, "inner_gw"):
                    pg = pg.inner_gw
                    break  # inner_gwを見つけたらそこでunwrap終了
                # outer_wrapperがさらに内側のラッパを指しているケース
                if hasattr(pg, "outer_wrapper"):
                    pg = pg.outer_wrapper
                    continue
                # 一般的なラッパが._exの下に本物のゲートウェイを持っているケース
                if hasattr(pg, "_ex"):
                    pg = pg._ex
                    continue
                # これ以上たどれない場合はループを抜ける
                break
            primary_gateway = pg  # unwrap後のオブジェクトを新しいprimary_gatewayとする

        self._primary_gateway = primary_gateway
        self._risk_config = risk_config
        self._strategy_config = strategy_config
        self._period_seconds = period_seconds
        self._taker_fee_bps_roundtrip = taker_fee_bps_roundtrip or strategy_config.taker_fee_bps_roundtrip
        self._estimated_slippage_bps = estimated_slippage_bps or strategy_config.estimated_slippage_bps
        self._min_hold_periods = getattr(strategy_config, "min_hold_periods", 1.0)
        self._holdings = _Holdings()
        # flatten_all と通常CLOSEが同時に走ると同一シンボルで決済注文が二重に出るので、ここで排他する
        self._close_guard_lock = asyncio.Lock()
        self._closing_symbols: set[str] = set()
        # open→close の一連（2レッグ）を相関させるためのサイクルID
        self._cycle_id_by_symbol: dict[str, str] = {}
        self._log = logger
        self._risk_manager = risk_manager or RiskManager(
            loss_cut_daily_jpy=risk_config.loss_cut_daily_jpy,
            flatten_all=self.flatten_all,
            skip_funding_flip_when_flat=True,
        )
        self._gw_log_once: set[str] = set()
        self._last_gw_used: dict[str, int] = {}
        # 戦略が最終的に掴んだprimary_gatewayの正体と、その中にBitgetGatewayが隠れていないかを調べるためのログ
        try:
            caller_frame = inspect.stack()[1]  # FundingBasisStrategy.__init__を呼び出した1つ上のフレーム情報を取得する
            # 呼び出し元の関数名/ファイル名/行番号を文字列にまとめる
            caller_info = f"{caller_frame.function}@{caller_frame.filename}:{caller_frame.lineno}"
        except Exception:
            caller_info = "unknown"

        underlying = getattr(
            self._primary_gateway, "_ex", None
        )  # primary_gatewayがさらに._exという属性で中に本物のゲートウェイを持っていないか調べる

        try:
            init_msg = (
                "strategy.init primary_gateway type={} obj_id={} gw_id_attr={} "
                "underlying_type={} underlying_obj_id={} underlying_gw_id_attr={} caller={}"
            )
            logger.info(
                init_msg,
                type(self._primary_gateway),
                hex(id(self._primary_gateway)) if self._primary_gateway is not None else None,
                getattr(self._primary_gateway, "gw_id", None),
                type(underlying),
                hex(id(underlying)) if underlying is not None else None,
                getattr(underlying, "gw_id", None) if underlying is not None else None,
                caller_info,
            )
        except Exception:
            pass
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

        self._log.info("strategy.step.enter sym={}", funding.symbol)
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

        if self._risk_manager.disable_new_orders and os.getenv("BACKTEST_DISABLE_RISK_GUARD") != "1":  # BACKTEST_DISABLE_RISK_GUARD=1でないときだけリスク管理を理由に新規建てをスキップする
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

        if predicted_rate <= 0 and os.getenv("BACKTEST_ALLOW_NEGATIVE_FUNDING") != "1":  # BACKTEST_ALLOW_NEGATIVE_FUNDING=1でないときだけ負のFundingを理由に新規建てをスキップする
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
            logger.info(
                "eval decision sym={} act={} reason={} prate={} apr={} cand={} gain={} cost={} tmin={}",
                symbol,
                decision.action.value,
                getattr(decision, "reason", None),
                predicted_rate,
                apr,
                candidate,
                expected_gain,
                expected_cost,
                time_to_event_min,
            )
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
            self._log.info(
                "strategy.step.skip_before_market_data reason=decision_skip sym={} decision_reason={}",
                decision.symbol,
                getattr(decision, "reason", None),
            )  # _market_data_readyを呼ぶ前にSKIP判定でstepを抜けたことを記録するログ
            logger.debug("FundingBasis: skip -> {}", decision.reason)
            return

        if decision.action is DecisionAction.HEDGE:
            delta = decision.delta_to_neutral
            if delta == 0:
                holding = self._holdings.get(decision.symbol)
                if holding:
                    delta = -holding.net_delta()
            if delta != 0:
                cycle_id = self._cycle_id_by_symbol.get(decision.symbol)
                created = await self._oms.submit_hedge(
                    decision.symbol,
                    delta,
                    meta={
                        "intent": "rebalance",
                        "source": "strategy",
                        "cycle_id": cycle_id,
                        "mode": "HEDGED",
                        "reason": getattr(decision, "reason", None),
                    },
                )
                # OMS側で skip（最小未満など）された場合は、内部holdingsも更新しない（ズレ防止）
                if created is not None:
                    holding = self._holdings.get(decision.symbol)
                    if holding:
                        holding.perp_qty += delta
            self._log.info(
                "strategy.step.skip_before_market_data reason=hedge_action sym={} delta_to_neutral={}",
                decision.symbol,
                delta,
            )  # _market_data_readyを呼ぶ前にHEDGE処理でstepを終えていることを記録するログ
            return

        if decision.action is DecisionAction.CLOSE:
            await self._close_symbol(decision.symbol, source="decision_close", reason=getattr(decision, "reason", None))
            self._log.info(
                "strategy.step.skip_before_market_data reason=close_action sym={}",
                decision.symbol,
            )  # _market_data_readyを呼ぶ前にCLOSE処理でstepを終えていることを記録するログ
            return

        if decision.action is DecisionAction.OPEN:
            await self._open_basis_position(decision, spot_price=spot_price, perp_price=perp_price)
            return

        logger.debug("FundingBasis: 未対応アクション {}", decision.action)

    async def flatten_all(self) -> None:
        """全シンボルの建玉を成行で解消する。"""

        for symbol in list(self._holdings.symbols()):
            await self._close_symbol(symbol, source="flatten_all", reason="flatten_all")

    async def _open_basis_position(self, decision: Decision, *, spot_price: float, perp_price: float) -> None:
        """新規でFunding/Basisポジションを組成する。"""

        # 何をする？→ OPENの直前に“市場データがREADYか”を確認し、ダメなら安全にスキップする
        await self._prime_market_metadata(decision.symbol)
        ready, reason = self._market_data_ready(decision.symbol)
        if not ready:
            logger.info("decision.skip: market data not ready sym={} reason={}", decision.symbol, reason)
            return  # データが整っていないので発注しない（監視のみ運転）
        self._log.info(
            "decision.eval.start gw_type=%s gw_gw_id=%s gw_obj_id=%s",
            type(self._primary_gateway),
            getattr(self._primary_gateway, "gw_id", None),
            hex(id(self._primary_gateway)),
        )  # _market_data_readyを通過して実際にOPEN/HEDGE/CLOSE評価に入ったタイミングを記録するログ


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
            symbol=f"{decision.symbol}_SPOT",
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

        cycle_id = f"c_{uuid.uuid4().hex}"
        meta_base = {
            "intent": "open",
            "source": "strategy",
            "cycle_id": cycle_id,
            "mode": "ENTERING",
            "reason": getattr(decision, "reason", None),
        }
        created_perp = await self._oms.submit(perp_req, meta={**meta_base, "leg": "perp"})
        created_spot = await self._oms.submit(spot_req, meta={**meta_base, "leg": "spot"})
        if created_perp is None or created_spot is None:
            logger.warning(
                "FundingBasis: open incomplete -> skip holdings update sym={} perp_ok={} spot_ok={} cycle_id={}",
                decision.symbol,
                created_perp is not None,
                created_spot is not None,
                cycle_id,
            )
            return
        self._cycle_id_by_symbol[decision.symbol] = cycle_id
        self._holdings.update_open(
            decision.symbol,
            spot_qty=signed_spot_qty,
            spot_price=spot_price,
            perp_qty=signed_perp_qty,
            perp_price=perp_price,
        )

    async def _close_symbol(self, symbol: str, *, source: str = "strategy", reason: str | None = None) -> None:
        """指定シンボルの建玉を解消する。"""

        # flatten_all と通常CLOSEが同時に走ると「同一シンボルの決済注文」が二重送信されるため排他する
        async with self._close_guard_lock:
            if symbol in self._closing_symbols:
                logger.info("FundingBasis: close skip (already closing) {}", symbol)
                return
            holding = self._holdings.get(symbol)
            if not holding:
                return
            self._closing_symbols.add(symbol)

        logger.info("FundingBasis: close {}", symbol)

        try:
            cycle_id = self._cycle_id_by_symbol.get(symbol)
            perp_ok = True
            spot_ok = True

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
                created = await self._oms.submit(
                    req,
                    meta={
                        "intent": "close",
                        "source": source,
                        "cycle_id": cycle_id,
                        "mode": "EXITING",
                        "reason": reason,
                        "leg": "perp",
                    },
                )
                perp_ok = created is not None

            if holding.spot_qty != 0:
                side = "sell" if holding.spot_qty > 0 else "buy"
                req = OrderRequest(
                    symbol=f"{symbol}_SPOT",
                    side=side,
                    type="market",
                    qty=abs(holding.spot_qty),
                    time_in_force="IOC",
                    reduce_only=True,  # 全クローズでは必ずreduce-onlyにして新規建てを防ぐ
                    post_only=False,
                )
                created = await self._oms.submit(
                    req,
                    meta={
                        "intent": "close",
                        "source": source,
                        "cycle_id": cycle_id,
                        "mode": "EXITING",
                        "reason": reason,
                        "leg": "spot",
                    },
                )
                spot_ok = created is not None

            # どちらかが skip/失敗した場合は内部holdingsを消さず、次のstepで再試行できるようにする
            if perp_ok and spot_ok:
                self._holdings.clear(symbol)
                self._cycle_id_by_symbol.pop(symbol, None)
            else:
                logger.warning(
                    "FundingBasis: close incomplete -> keep holdings sym={} perp_ok={} spot_ok={} source={} reason={}",
                    symbol,
                    perp_ok,
                    spot_ok,
                    source,
                    reason,
                )
        finally:
            # CancelledError を含む例外時でもフラグが残らないように、await を含まない形で必ず解除する
            self._closing_symbols.discard(symbol)

    def _locate_gateway_with_scale(self) -> Any | None:
        """スケール情報を持つゲートウェイを探して返す。"""

        try:
            if self._primary_gateway is not None and hasattr(self._primary_gateway, "_scale_cache"):
                return self._primary_gateway

            cands: list[Any] = []

            def _push(owner: Any) -> None:
                for name in ("bitget_gateway", "bitget", "gateway", "exchange", "ex", "_ex"):
                    cand = getattr(owner, name, None)
                    if cand is None:
                        continue
                    if hasattr(cand, "_scale_cache"):
                        cands.append(cand)

            _push(self)
            oms = getattr(self, "_oms", None)
            if oms is not None:
                _push(oms)

            # 優先度: _prime_scale_from_markets を持つ -> _scale_cacheにpriceScaleが入っている -> 先頭
            def _score(obj: Any) -> tuple[int, int]:
                has_prime = int(bool(getattr(obj, "_prime_scale_from_markets", None)))
                cache = getattr(obj, "_scale_cache", {}) or {}
                has_price = 1 if any((info or {}).get("priceScale") is not None for info in cache.values()) else 0
                return (has_prime, has_price)

            cands.sort(key=_score, reverse=True)
            return cands[0] if cands else None
        except Exception:
            return None

    def _market_data_ready(self, symbol: str) -> Tuple[bool, str]:
        """内部用: 価格スケール/BBOの準備状態をチェックしてREADY/理由を返す。"""

        gw_cache = getattr(self, "_gw_cache", {})
        gw: Any | None = gw_cache.get(symbol) if isinstance(gw_cache, dict) else None
        if gw is None:
            gw = self._locate_gateway_with_scale()
        if gw is None:
            return False, "no_gateway"  # ゲートウェイが見つからない場合は早期に諦める
        try:
            gid = id(gw)
            if self._last_gw_used.get(symbol) != gid:
                self._last_gw_used[symbol] = gid
                logger.info(
                    "market_ready.gw sym={} gw_id={} cache_keys={} state={}",
                    symbol,
                    gid,
                    sorted(getattr(gw, "_scale_cache", {}).keys()),
                    getattr(gw, "_price_state", {}).get(symbol, "UNKNOWN"),
                )
        except Exception:
            pass

        # 戦略が参照しているゲートウェイ(gw)のキャッシュ状態を詳しくログに出して、
        # PaperExchange側の_scale_cache/_bbo_cache/_price_stateが本当に埋まっているか調べる
        scale_cache = getattr(gw, "_scale_cache", None)
        bbo_cache = getattr(gw, "_bbo_cache", None)
        price_state = getattr(gw, "_price_state", None)

        try:
            logger.info(
                "market_ready.cache_debug gw_type={} gw_gw_id={} gw_obj_id={} scale_cache_id={} scale_cache_keys={} "
                "bbo_cache_id={} bbo_cache_keys={} price_state={}",
                type(gw),
                getattr(gw, "gw_id", None),
                hex(id(gw)),
                hex(id(scale_cache)) if scale_cache is not None else None,
                list(scale_cache.keys()) if isinstance(scale_cache, dict) else None,
                hex(id(bbo_cache)) if bbo_cache is not None else None,
                list(bbo_cache.keys()) if isinstance(bbo_cache, dict) else None,
                price_state,
            )
        except Exception:
            pass

        # 各キーごとに、スケール/BBO/price_stateがそろっているかを詳しくログに出す
        if isinstance(scale_cache, dict) and isinstance(bbo_cache, dict) and isinstance(price_state, dict):
            all_keys = sorted(set(list(scale_cache.keys()) + list(bbo_cache.keys()) + list(price_state.keys())))
            for key in all_keys:
                self._log.info(
                    "market_ready.key_detail key={} has_scale={} has_bbo={} price_state={}",
                    key,
                    key in scale_cache,
                    key in bbo_cache,
                    price_state.get(key),
                )  # 各キーについて、スケール/BBO/price_stateの有無と状態をログに出す

        try:
            scale_info = getattr(gw, "_scale_cache", {}).get(symbol) or {}
        except Exception:
            scale_info = {}
        ready_scale = scale_info.get("priceScale") is not None
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
                (
                    f"price_scale_not_ready scale_meta={ready_scale} bbo={ready_bbo} "
                    f"state={state} cache_keys={scale_keys} gw_id={id(gw)}"
                ),
            )
        if not ready_state:
            return False, (
                f"price_state={state} scale_meta={ready_scale} bbo={ready_bbo} "
                f"cache_keys={scale_keys} gw_id={id(gw)}"
            )
        if not ready_bbo:
            return False, (f"bbo_invalid scale_meta={ready_scale} state={state} cache_keys={scale_keys} gw_id={id(gw)}")

        return True, "OK"  # READY: 問題なければ通す

    async def _prime_market_metadata(self, symbol: str) -> None:
        """スケール/価格状態が未初期化ならここで温める。"""

        gw = self._locate_gateway_with_scale()
        if gw is None:
            return

        try:
            if symbol not in self._gw_log_once:
                self._gw_log_once.add(symbol)
                logger.info(
                    "prime.meta.start sym={} gw_id={} cache_keys={}",
                    symbol,
                    id(gw),
                    sorted(getattr(gw, "_scale_cache", {}).keys()),
                )
        except Exception:
            pass

        try:
            scale_info = getattr(gw, "_scale_cache", {}).get(symbol) or {}
            ready_scale = scale_info.get("priceScale") is not None
        except Exception:
            ready_scale = False

        if not ready_scale:
            prime = getattr(gw, "_prime_scale_from_markets", None)
            if prime and inspect.iscoroutinefunction(prime):
                try:
                    await prime(symbol)
                except Exception as e:  # noqa: BLE001
                    logger.debug("prime.scale.error sym={} err={}", symbol, e)

        try:
            state = getattr(gw, "_price_state", {}).get(symbol)
        except Exception:
            state = None
        if state != "READY":
            get_bbo = getattr(gw, "get_bbo", None)
            if get_bbo and inspect.iscoroutinefunction(get_bbo):
                try:
                    await get_bbo(symbol)
                except Exception as e:  # noqa: BLE001
                    logger.debug("prime.bbo.error sym={} err={}", symbol, e)
        try:
            cache = getattr(self, "_gw_cache", None)
            if not isinstance(cache, dict):
                cache = {}
            cache[symbol] = gw
            self._gw_cache = cache
        except Exception:
            pass

    def _get_bbo(self, symbol: str) -> Tuple[Optional[float], Optional[float]]:
        """内部用補助関数。 現在の最良気配(BBO)を取れる範囲で返す。なければ (None, None)。"""

        # 1) Strategy自身が get_bbo を持つなら優先的に使う
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

        # 2) 共通コンポーネント(market / md)が get_bbo を持っていれば使う
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

        # 3) ゲートウェイのBBOキャッシュを直接読む（_gw_cache / _locate_gateway_with_scale から取得）
        try:
            gw_cache = getattr(self, "_gw_cache", {})
            gw = gw_cache.get(symbol) if isinstance(gw_cache, dict) else None
            if gw is None:
                gw = self._locate_gateway_with_scale()
            if gw is not None:
                bbo = getattr(gw, "_bbo_cache", {}).get(symbol) or {}
                bid = _safe_float(bbo.get("bid"))
                ask = _safe_float(bbo.get("ask"))
                if bid is not None or ask is not None:
                    return bid, ask
        except Exception:
            pass

        # 4) ここまでで取れなければ最後に None を返す
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
