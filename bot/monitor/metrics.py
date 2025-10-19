"""これは「運用に必要な最小メトリクス（心拍、デルタ、名目、PnL概算）を定期ログ出力する」モジュールです。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterable

from loguru import logger

from bot.data.repo import Repo
from bot.exchanges.base import ExchangeGateway
from bot.exchanges.types import Balance, Position


@dataclass
class _Holdings:
    """これは何を表す型？
    → 単一銘柄の現物/先物（ロング/ショート）をベース数量で表現したもの。
    """

    spot_qty: float = 0.0
    perp_long_qty: float = 0.0
    perp_short_qty: float = 0.0


class MetricsLogger:
    """メトリクス心拍ロガー（一定間隔でログへ出力）"""

    def __init__(self, *, ex: ExchangeGateway, repo: Repo, symbols: list[str], risk: object | None = None) -> None:
        """これは何をする関数？
        → 参照先の ExchangeGateway/Repo/対象銘柄（symbols）を受け取り、心拍ログの準備をします。
        """

        self._ex = ex
        self._repo = repo
        self._symbols = symbols
        self._risk = risk

    # ===== パブリックAPI =====

    async def run_forever(self, *, interval_sec: float = 30.0) -> None:
        """これは何をする関数？
        → interval_sec ごとに one_shot() を呼び続け、失敗しても継続します。
        """

        while True:
            try:
                await self.one_shot()
            except Exception as e:  # noqa: BLE001
                logger.exception("metrics one_shot failed: {}", e)
            await asyncio.sleep(interval_sec)

    async def one_shot(self) -> None:
        """これは何をする関数？
        → いま時点の主要メトリクスを収集してログ出力します。
        """

        balances = await self._ex.get_balances()
        positions = await self._ex.get_positions()

        # リスクフラグ（キル状態）
        kill = getattr(self._risk, "disable_new_orders", False)

        # 日付
        today = datetime.now(timezone.utc).date()

        # 日次PnL概算（Fundingのみ＋手数料を引く簡易版）
        funding_pnl, fees_sum = await self._estimate_daily_pnl(today)
        gross = funding_pnl
        net = gross - fees_sum

        # 銘柄ごとにデルタ/名目/価格を収集
        for sym in self._symbols:
            px_spot = await self._safe_ticker(sym + "_SPOT")
            px_perp = await self._safe_ticker(sym)
            hold = self._aggregate_holdings(sym, balances, positions)
            net_delta = hold.spot_qty + hold.perp_long_qty - hold.perp_short_qty
            total_base = abs(hold.spot_qty) + abs(hold.perp_long_qty) + abs(hold.perp_short_qty)
            net_delta_bps = (abs(net_delta) / total_base * 1e4) if total_base > 0 else 0.0
            notional = max(
                abs(hold.spot_qty) * px_spot,
                max(abs(hold.perp_long_qty), abs(hold.perp_short_qty)) * px_perp,
            )

            logger.info(
                "metrics heartbeat: sym={} px_spot={} px_perp={} net_delta_bps={} notional={} "
                "daily_pnl_gross={} fees={} daily_pnl_net={} killed={}",
                sym,
                round(px_spot, 6),
                round(px_perp, 6),
                round(net_delta_bps, 3),
                round(notional, 2),
                round(gross, 2),
                round(fees_sum, 2),
                round(net, 2),
                kill,
            )

    # ===== 内部ユーティリティ =====

    async def _safe_ticker(self, symbol: str) -> float:
        """これは何をする関数？→ 例外を飲み込みつつティッカーを取得（失敗時は0）。"""

        try:
            return await self._ex.get_ticker(symbol)
        except Exception:
            return 0.0

    def _aggregate_holdings(self, symbol: str, balances: Iterable[Balance], positions: Iterable[Position]) -> _Holdings:
        """これは何をする関数？
        → 指定シンボルの現物/先物ベース数量を集計します（MVPの簡易正規化）。
        """

        base = symbol[:-4].upper() if symbol.upper().endswith("USDT") else symbol.upper()
        spot_qty = 0.0
        for b in balances:
            if b.asset.upper() == base:
                spot_qty = float(b.total)
                break

        perp_long = 0.0
        perp_short = 0.0
        for p in positions:
            sym_norm = p.symbol.replace("/", "").replace(":USDT", "").upper()
            if sym_norm == symbol.upper():
                if p.side.lower() == "long":
                    perp_long += float(p.size)
                else:
                    perp_short += float(p.size)

        return _Holdings(spot_qty=spot_qty, perp_long_qty=perp_long, perp_short_qty=perp_short)

    async def _estimate_daily_pnl(self, day: date) -> tuple[float, float]:
        """これは何をする関数？
        → 指定日（UTC）の Funding 実現PnL合計と、TradeLogの手数料合計を返します（MVP）。
        """

        # 既存のRepo API（list_*）を利用してPython側で日付フィルタ（件数はMVP想定で少ない）
        ff = await self._repo.list_funding_events()
        tt = await self._repo.list_trades()

        def _is_same_day(ts: datetime) -> bool:
            d = ts.astimezone(timezone.utc).date()
            return d == day

        funding_sum = sum(f.realized_pnl for f in ff if _is_same_day(f.ts))
        fees_sum = sum(t.fee for t in tt if _is_same_day(t.ts))
        return float(funding_sum), float(fees_sum)
