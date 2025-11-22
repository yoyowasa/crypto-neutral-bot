# このモジュールは、リスク管理用のキルスイッチ（flatten_all + disable_new_orders）をまとめたガードロジックです。
from __future__ import annotations

import asyncio
import os  # 環境変数で“フラット時はKILLをスキップ”を制御するため
from collections import deque
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from loguru import logger


@dataclass
class _ApiErrorWindow:
    """API エラーの発生回数を一定時間ウィンドウで管理するヘルパ。"""

    max_in_window: int = 5
    window_sec: float = 60.0
    events: deque[float] = field(default_factory=deque)

    def record(self, now_ts: float) -> int:
        """
        現在時刻を記録し、ウィンドウ内（window_sec 秒）に発生したイベント数を返す。
        過去の古いイベントは自動的に捨てる。
        """
        self.events.append(now_ts)
        # 古いイベントをドロップ
        while self.events and (now_ts - self.events[0]) > self.window_sec:
            self.events.popleft()
        return len(self.events)


class RiskManager:
    """
    リスク管理とキルスイッチを担当するクラス。

    - pre-trade のチェックは bot/risk/limits.py の precheck_open_order で実施
    - このクラスの守備範囲（MVP）:
        * WS 切断時間 > ws_disconnect_threshold_sec
        * ヘッジ遅延 p95 > hedge_delay_p95_threshold_sec
        * 日次損益 < -loss_cut_daily_jpy
        * Funding サイン反転（ヒステリシス付き）
        * API エラーのバースト
    - 実際のアクション:
        * flatten_all() を実行（全ポジション・注文をクローズ）
        * disable_new_orders = True にして新規注文を止める
    """

    def __init__(
        self,
        *,
        loss_cut_daily_jpy: float,
        ws_disconnect_threshold_sec: float = 30.0,
        hedge_delay_p95_threshold_sec: float = 2.0,
        api_error_max_in_60s: int = 10,
        flatten_all: Callable[[], Awaitable[None]],
        funding_flip_min_abs: float = 0.0,
        funding_flip_consecutive: int = 1,
    ) -> None:
        """
        各種しきい値と flatten_all コールバックを受け取って初期化する。
        flatten_all は実際のポジション・注文をすべてクローズする処理を想定。
        """
        self._loss_cut_daily_jpy = float(loss_cut_daily_jpy)
        self._ws_disconnect_threshold_sec = float(ws_disconnect_threshold_sec)
        self._hedge_delay_p95_threshold_sec = float(hedge_delay_p95_threshold_sec)
        self._flatten_all = flatten_all

        self.disable_new_orders: bool = False
        self._killed: bool = False

        # 累積状態
        self._daily_net_pnl_jpy: float = 0.0
        self._last_funding_predicted: dict[str, float] = {}

        # ヘッジレイテンシ（秒）のローリング窓
        self._hedge_latencies_sec: deque[float] = deque(maxlen=200)

        # Funding sign-flip ヒステリシス
        self._funding_flip_min_abs: float = float(funding_flip_min_abs)
        self._funding_flip_consecutive: int = int(funding_flip_consecutive)
        self._funding_flip_counts: dict[str, int] = {}

        # API エラーのウィンドウカウンタ
        self._api_errors = _ApiErrorWindow(max_in_window=api_error_max_in_60s, window_sec=60.0)

    # ---------- 状態更新メソッド（キル条件を評価する） ----------

    def update_daily_pnl(self, *, net_pnl_jpy: float) -> None:
        """当日のネット PnL（JPY）を更新し、日次損失カットを判定する。"""
        self._daily_net_pnl_jpy = float(net_pnl_jpy)
        if self._daily_net_pnl_jpy < -abs(self._loss_cut_daily_jpy):
            asyncio.create_task(self._trigger_kill("daily loss cut"))

    def record_ws_disconnected(self, *, duration_sec: float) -> None:
        """WS 切断時間を記録し、しきい値超えなら KILL を発火する。"""
        if duration_sec > self._ws_disconnect_threshold_sec:
            asyncio.create_task(self._trigger_kill(f"ws disconnected {duration_sec:.1f}s"))

    def record_hedge_latency(self, *, seconds: float) -> None:
        """
        ヘッジ注文のレイテンシ（秒）を記録し、十分なサンプル数が溜まったら
        p95 を計算してしきい値超えをチェックする。
        """
        self._hedge_latencies_sec.append(float(seconds))
        if len(self._hedge_latencies_sec) >= 20:  # ある程度サンプルが溜まってから判定
            p95 = _percentile(list(self._hedge_latencies_sec), 95.0)
            if p95 > self._hedge_delay_p95_threshold_sec:
                asyncio.create_task(self._trigger_kill(f"hedge latency p95 {p95:.3f}s"))

    def record_api_error(self, *, now_ts: float) -> None:
        """API エラー発生時に呼び出し、一定時間内の回数をチェックする。"""
        n = self._api_errors.record(now_ts)
        if n > self._api_errors.max_in_window:
            asyncio.create_task(self._trigger_kill(f"api errors burst {n}/60s"))

    def update_funding_predicted(self, *, symbol: str, predicted_rate: float) -> None:
        """
        Funding の予測レートを更新し、サイン反転がヒステリシス条件を満たしたら
        KILL を検討する。
        """
        prev = self._last_funding_predicted.get(symbol)
        self._last_funding_predicted[symbol] = predicted_rate

        # 初回はサイン比較できないのでリセットして終了
        if prev is None:
            self._funding_flip_counts.pop(symbol, None)
            return

        # 両方とも絶対値が小さい領域ならノイズ扱いでリセット
        if abs(prev) < self._funding_flip_min_abs and abs(predicted_rate) < self._funding_flip_min_abs:
            self._funding_flip_counts.pop(symbol, None)
            return

        if (prev * predicted_rate) < 0:
            cnt = self._funding_flip_counts.get(symbol, 0) + 1
            self._funding_flip_counts[symbol] = cnt
            if cnt >= max(1, self._funding_flip_consecutive):
                self._funding_flip_counts[symbol] = 0
                # フラットなら Funding 反転による KILL をスキップ（環境変数でON/OFF；既定は有効）
                if os.getenv("RISK__FUNDING_FLIP_SKIP_WHEN_FLAT", "true").lower() == "true" and self._is_flat_safely():
                    logger.info("KILL-SKIP: funding sign flip detected while portfolio is flat -> skip kill")
                    return
                asyncio.create_task(self._trigger_kill(f"funding sign flip {symbol}: {prev} -> {predicted_rate}"))
        else:
            self._funding_flip_counts.pop(symbol, None)

    def _is_flat_safely(self) -> bool:
        """
        現在の合計ノーションが 0（=フラット）かを“安全に”判定するヘルパ。

        - 利用可能なオブジェクト（portfolio / oms）を順に試す
        - 情報が取れない・例外発生時は False（=フラットではない扱い＝安全側）で返す
        """
        try:
            portfolio = getattr(self, "portfolio", None)
            if portfolio is not None:
                get_total = getattr(portfolio, "total_notional_abs", None)
                if callable(get_total):
                    return float(get_total()) == 0.0
                notional = getattr(portfolio, "notional_abs", None)
                if notional is not None:
                    return float(notional) == 0.0
        except Exception:  # noqa: BLE001
            # 取得に失敗した場合は安全側（フラットではない扱い）
            pass

        try:
            oms = getattr(self, "oms", None)
            if oms is not None:
                get_total = getattr(oms, "total_notional_abs", None)
                if callable(get_total):
                    return float(get_total()) == 0.0
                has_pos = getattr(oms, "has_open_positions", None)
                if callable(has_pos):
                    return not bool(has_pos())
        except Exception:  # noqa: BLE001
            # こちらも取得に失敗した場合は安全側で扱う
            pass

        return False  # 情報が取れない時は“フラットではない”扱い（安全側）

    # ---------- 実際の KILL スイッチ ----------

    async def _trigger_kill(self, reason: str) -> None:
        """
        flatten_all を実行し、新規注文を停止する。
        すでに KILL 済みであれば何もしない。
        """
        if self._killed:
            return
        self._killed = True
        self.disable_new_orders = True
        logger.error("KILL-SWITCH: {} -> flatten_all()", reason)
        try:
            await self._flatten_all()
        except Exception as exc:  # noqa: BLE001
            logger.exception("flatten_all failed: {}", exc)

    def reset_kill(self, reason: str = "manual reset") -> bool:
        """
        何をする関数？:
          - 非常停止フラグ（self._killed / self.disable_new_orders）の“ラッチ”を手動で解除する。
          - 運用オペの明示的な解除手段。戦略や他ガードのロジックには影響を与えない。
        戻り値:
          - True  = どちらか一方でも変更した（=実際に解除が発生）
          - False = もともと解除済みで変更なし（冪等で安全）
        """
        import logging

        logger_std = logging.getLogger(__name__)  # ここでの解除操作をログに残して可観測性を確保する
        changed = False

        # _killed（内部ラッチ）を解除
        if getattr(self, "_killed", False):
            self._killed = False
            changed = True

        # strategy 側が参照する disable_new_orders も解除
        if getattr(self, "disable_new_orders", False):
            self.disable_new_orders = False
            changed = True

        # 解除を強めのログで可視化（だれが/なぜ解除したかが後で追えるように reason を残す）
        if changed:
            logger_std.warning("KILL-RESET: %s (disable_new_orders=False, killed=False)", reason)
        else:
            logger_std.info("KILL-RESET: already reset (no change)")

        return changed


def _percentile(xs: list[float], p: float) -> float:
    """シンプルなパーセンタイル計算（MVP 用）。xs は数値リスト。"""
    xs_sorted = sorted(xs)
    if not xs_sorted:
        return 0.0
    k = (len(xs_sorted) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(xs_sorted) - 1)
    if f == c:
        return xs_sorted[int(k)]
    d0 = xs_sorted[f] * (c - k)
    d1 = xs_sorted[c] * (k - f)
    return d0 + d1
