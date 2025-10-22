# これは「運用中の異常を監視し、必要ならキルスイッチ（flatten_all + disable_new_orders）を発火する」ガード集です。
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from loguru import logger


@dataclass
class _ApiErrorWindow:
    """これは何をする型？
    → 一定時間内のAPIエラー回数を数える簡易ウィンドウ（MVP）。
    """

    max_in_window: int = 5
    window_sec: float = 60.0
    events: deque[float] = field(default_factory=deque)

    def record(self, now_ts: float) -> int:
        """これは何をする関数？
        → いまの時刻を追加し、ウィンドウ外の古い記録を落として現在件数を返す。
        """
        self.events.append(now_ts)
        # 古いものを捨てる
        while self.events and (now_ts - self.events[0]) > self.window_sec:
            self.events.popleft()
        return len(self.events)


class RiskManager:
    """運用中のリスク監視とキルスイッチ。
    - pre-trade は bot/risk/limits.py の precheck_open_order を使用（ここでは事後系を扱う）
    - 監視対象（MVP）：
        * WS切断 > threshold_sec
        * ヘッジ遅延 p95 > threshold_sec
        * 日次損失 > loss_cut_daily_jpy
        * Funding 符号反転
        * APIエラー連発（一定時間内の上限）
    - 発火時の動作：
        * flatten_all() を呼ぶ（市場成行で全クローズを想定）
        * disable_new_orders = True
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
        """これは何をする関数？
        → しきい値と、発火時に実際に全クローズする flatten_all コールバックを受け取ります。
        """
        self._loss_cut_daily_jpy = float(loss_cut_daily_jpy)
        self._ws_disconnect_threshold_sec = float(ws_disconnect_threshold_sec)
        self._hedge_delay_p95_threshold_sec = float(hedge_delay_p95_threshold_sec)
        self._flatten_all = flatten_all

        self.disable_new_orders: bool = False
        self._killed: bool = False

        # 状態
        self._daily_net_pnl_jpy: float = 0.0
        self._last_funding_predicted: dict[str, float] = {}

        # ヘッジ遅延分布（最近N件）
        self._hedge_latencies_sec: deque[float] = deque(maxlen=200)

        # Funding sign-flip hysteresis
        self._funding_flip_min_abs: float = float(funding_flip_min_abs)
        self._funding_flip_consecutive: int = int(funding_flip_consecutive)
        self._funding_flip_counts: dict[str, int] = {}

        # APIエラー監視
        self._api_errors = _ApiErrorWindow(max_in_window=api_error_max_in_60s, window_sec=60.0)

    # ---------- 更新メソッド（監視対象の値を更新する） ----------

    def update_daily_pnl(self, *, net_pnl_jpy: float) -> None:
        """これは何をする関数？
        → 本日のネットPnL（JPY換算）を更新します。しきい値を下回れば即キル。
        """
        self._daily_net_pnl_jpy = float(net_pnl_jpy)
        if self._daily_net_pnl_jpy < -abs(self._loss_cut_daily_jpy):
            asyncio.create_task(self._trigger_kill("daily loss cut"))

    def record_ws_disconnected(self, *, duration_sec: float) -> None:
        """これは何をする関数？
        → WS切断時間を記録し、しきい値を超えたらキルを発火します。
        """
        if duration_sec > self._ws_disconnect_threshold_sec:
            asyncio.create_task(self._trigger_kill(f"ws disconnected {duration_sec:.1f}s"))

    def record_hedge_latency(self, *, seconds: float) -> None:
        """これは何をする関数？
        → ヘッジ（発注→約定）にかかったレイテンシを記録し、p95 を超えたらキルします。
        """
        self._hedge_latencies_sec.append(float(seconds))
        if len(self._hedge_latencies_sec) >= 20:  # 十分なサンプルがたまったときだけ判定
            p95 = _percentile(list(self._hedge_latencies_sec), 95.0)
            if p95 > self._hedge_delay_p95_threshold_sec:
                asyncio.create_task(self._trigger_kill(f"hedge latency p95 {p95:.3f}s"))

    def record_api_error(self, *, now_ts: float) -> None:
        """これは何をする関数？
        → APIエラーを時刻付きで記録し、1分間の件数が上限を超えたらキルします。
        """
        n = self._api_errors.record(now_ts)
        if n > self._api_errors.max_in_window:
            asyncio.create_task(self._trigger_kill(f"api errors burst {n}/60s"))

    def update_funding_predicted(self, *, symbol: str, predicted_rate: float) -> None:
        """これは何をする関数？
        → Funding の予想が「符号反転」したら、キルを発火します（戦略の方向と真逆になるため）。
        """
        prev = self._last_funding_predicted.get(symbol)
        self._last_funding_predicted[symbol] = predicted_rate
        # 初回は比較対象なし
        if prev is None:
            self._funding_flip_counts.pop(symbol, None)
            return
        # 微小ノイズ無視（両方の絶対値が閾値未満）
        if abs(prev) < self._funding_flip_min_abs and abs(predicted_rate) < self._funding_flip_min_abs:
            self._funding_flip_counts.pop(symbol, None)
            return
        if (prev * predicted_rate) < 0:
            cnt = self._funding_flip_counts.get(symbol, 0) + 1
            self._funding_flip_counts[symbol] = cnt
            if cnt >= max(1, self._funding_flip_consecutive):
                self._funding_flip_counts[symbol] = 0
                asyncio.create_task(self._trigger_kill(f"funding sign flip {symbol}: {prev} -> {predicted_rate}"))
        else:
            self._funding_flip_counts.pop(symbol, None)

    # ---------- 内部：キルスイッチ ----------

    async def _trigger_kill(self, reason: str) -> None:
        """これは何をする関数？
        → flatten_all を呼び、新規発注を禁止します（多重起動を避ける）。
        """
        if self._killed:
            return
        self._killed = True
        self.disable_new_orders = True
        logger.error("KILL-SWITCH: {} -> flatten_all()", reason)
        try:
            await self._flatten_all()
        except Exception as e:  # noqa: BLE001
            logger.exception("flatten_all failed: {}", e)


def _percentile(xs: list[float], p: float) -> float:
    """これは何をする関数？
    → 単純なパーセンタイル計算（MVP）。xs は非空を想定。
    """
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
