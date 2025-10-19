"""Funding/Basis 戦略の評価・実行ロジック。"""

from __future__ import annotations

from dataclasses import dataclass, field

from loguru import logger

from bot.config.models import RiskConfig, StrategyFundingConfig
from bot.exchanges.types import FundingInfo, OrderRequest
from bot.oms.engine import OmsEngine
from bot.risk.guards import RiskManager
from bot.risk.limits import PreTradeContext, precheck_open_order

from .models import Decision, DecisionAction, annualize_rate, net_delta_base, notional_candidate


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
        taker_fee_bps_roundtrip: float = 6.0,
        estimated_slippage_bps: float = 5.0,
        risk_manager: RiskManager | None = None,
    ) -> None:
        """必要な依存（OMS/設定/リスク管理）を受け取り初期化する。"""

        self._oms = oms
        self._risk_config = risk_config
        self._strategy_config = strategy_config
        self._period_seconds = period_seconds
        self._taker_fee_bps_roundtrip = taker_fee_bps_roundtrip
        self._estimated_slippage_bps = estimated_slippage_bps
        self._holdings = _Holdings()
        self._risk_manager = risk_manager or RiskManager(
            loss_cut_daily_jpy=risk_config.loss_cut_daily_jpy,
            flatten_all=self.flatten_all,
        )

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
        """Funding情報と価格から次のアクションを判定する。"""

        symbol = funding.symbol
        if symbol not in self._strategy_config.symbols:
            return Decision(action=DecisionAction.SKIP, symbol=symbol, reason="対象外シンボル")

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
                return Decision(
                    action=DecisionAction.CLOSE,
                    symbol=symbol,
                    reason="予想不明のため一旦解消",
                    predicted_apr=None,
                )
            if predicted_rate <= 0:
                return Decision(
                    action=DecisionAction.CLOSE,
                    symbol=symbol,
                    reason="Funding符号反転でクローズ",
                    predicted_apr=apr,
                )
            if apr is not None and apr < self._strategy_config.min_expected_apr:
                return Decision(action=DecisionAction.CLOSE, symbol=symbol, reason="APRが閾値未満", predicted_apr=apr)

            net_delta = holding.net_delta()
            dominant_qty = holding.dominant_base_qty()
            if dominant_qty > 0:
                delta_bps = abs(net_delta) / dominant_qty * 10000.0
                if delta_bps > self._strategy_config.rebalance_band_bps:
                    return Decision(
                        action=DecisionAction.HEDGE,
                        symbol=symbol,
                        reason="デルタ乖離により再ヘッジ",
                        predicted_apr=apr,
                        delta_to_neutral=-net_delta,
                    )

            return Decision(action=DecisionAction.SKIP, symbol=symbol, reason="ホールド継続", predicted_apr=apr)

        if self._risk_manager.disable_new_orders:
            return Decision(action=DecisionAction.SKIP, symbol=symbol, reason="リスク管理で新規停止", predicted_apr=apr)

        if predicted_rate is None:
            return Decision(action=DecisionAction.SKIP, symbol=symbol, reason="Funding予想が取得できない")

        if predicted_rate <= 0:
            return Decision(
                action=DecisionAction.SKIP,
                symbol=symbol,
                reason="負のFundingは新規対象外",
                predicted_apr=apr,
            )

        if apr is not None and apr < self._strategy_config.min_expected_apr:
            return Decision(action=DecisionAction.SKIP, symbol=symbol, reason="APRが閾値未満", predicted_apr=apr)

        used_total = self._holdings.used_total_notional()
        used_symbol = self._holdings.used_symbol_notional(symbol)
        candidate = notional_candidate(
            risk=self._risk_config,
            used_total_notional=used_total,
            used_symbol_notional=used_symbol,
        )
        if candidate <= 0:
            return Decision(
                action=DecisionAction.SKIP,
                symbol=symbol,
                reason="名目上限により建て不可",
                predicted_apr=apr,
            )

        expected_gain = predicted_rate * candidate
        total_cost_bps = self._taker_fee_bps_roundtrip + self._estimated_slippage_bps
        expected_cost = candidate * total_cost_bps / 10000.0
        if expected_gain <= expected_cost:
            return Decision(action=DecisionAction.SKIP, symbol=symbol, reason="期待収益がコスト未満", predicted_apr=apr)

        return Decision(
            action=DecisionAction.OPEN,
            symbol=symbol,
            reason="Funding機会により新規建て",
            predicted_apr=apr,
            notional=candidate,
            perp_side="sell",
            spot_side="buy",
        )

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

        spot_qty = decision.notional / spot_price
        perp_qty = decision.notional / perp_price
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
                reduce_only=False,
                post_only=False,
            )
            await self._oms.submit(req)

        self._holdings.clear(symbol)
