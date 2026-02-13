from __future__ import annotations

import argparse
import asyncio
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from loguru import logger

from bot.config.loader import load_config
from bot.config.models import RiskConfig, StrategyFundingConfig
from bot.cost.model import CostModel
from bot.data.repo import Repo
from bot.exchanges.base import ExchangeGateway
from bot.exchanges.types import Balance, FundingInfo, Order, OrderRequest, Position
from bot.oms.engine import OmsEngine
from bot.oms.fill_sim import PaperExchange
from bot.risk.guards import RiskManager
from bot.strategy.funding_basis.engine import FundingBasisStrategy

# ===== 価格フィード =====


@dataclass
class PriceTick:
    """これは何を表す型？
    → 単一ティック（時刻・銘柄・BBO/last）を表現します。
    """

    ts: datetime
    symbol: str
    bid: float | None
    ask: float | None
    last: float | None


class CsvPriceFeed:
    """CSV/Parquetから価格データを読み、時系列順にティックを返すフィード"""

    def __init__(self, *, path: str) -> None:
        """これは何をする関数？
        → path のCSV/Parquetを読み込み、必要な列を揃えます。
           必須列: ts, symbol, bid, ask, last（ bid/ask/last は None 可 ）
           ts は ISO8601 か epoch(ms/秒) を許容。
        """

        self._path = Path(path)
        self._df = self._load(self._path)

    def _load(self, p: Path) -> pd.DataFrame:
        """これは何をする関数？→ CSV/Parquet を読み込み、標準列に整形します。"""

        if p.suffix.lower() in {".parquet", ".pq"}:
            df = pd.read_parquet(p)
        else:
            df = pd.read_csv(p)
        cols = {c.lower(): c for c in df.columns}
        need = ["ts", "symbol", "bid", "ask", "last"]
        for k in need:
            if k not in cols:
                raise ValueError(f"missing column: {k}")
        # ts 正規化
        ts_col = cols["ts"]
        ser = df[ts_col]
        if pd.api.types.is_numeric_dtype(ser):
            # 13桁→ms、10桁→秒
            if ser.max() > 1e12:
                ser = pd.to_datetime(ser, unit="ms", utc=True)
            else:
                ser = pd.to_datetime(ser, unit="s", utc=True)
        else:
            ser = pd.to_datetime(ser, utc=True)
        df["_ts"] = ser.dt.tz_convert("UTC")
        out = pd.DataFrame(
            {
                "ts": df["_ts"],
                "symbol": df[cols["symbol"]].astype(str),
                "bid": pd.to_numeric(df[cols["bid"]], errors="coerce"),
                "ask": pd.to_numeric(df[cols["ask"]], errors="coerce"),
                "last": pd.to_numeric(df[cols["last"]], errors="coerce"),
            }
        ).sort_values(["ts", "symbol"], kind="mergesort")
        return out

    def iter_ticks(self, *, date_utc: str) -> Iterable[PriceTick]:
        """これは何をする関数？
        → 指定UTC日（YYYY-MM-DD）に属するティックを、時刻順に返します。
        """

        d = pd.to_datetime(date_utc).date()
        # pandas 2.x の Timestamp.combine は tz 引数を受けないため、標準の datetime を用いる
        start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        df = self._df[(self._df["ts"] >= pd.Timestamp(start)) & (self._df["ts"] < pd.Timestamp(end))]
        for row in df.itertuples(index=False):
            yield PriceTick(ts=row.ts.to_pydatetime(), symbol=row.symbol, bid=row.bid, ask=row.ask, last=row.last)


# ===== Funding スケジュール =====


@dataclass
class FundingRateEvent:
    """これは何を表す型？
    → Funding発生時刻とレート（期間当たり、符号付）を表現します。
    """

    ts: datetime
    symbol: str
    rate: float


class FundingSchedule:
    """CSVから Funding スケジュールを読み、現在時刻に対する予想レート/次回時刻・満期発生を管理"""

    def __init__(self, *, path: str) -> None:
        """これは何をする関数？
        → path のCSV（列: ts, symbol, rate）を読み込んで保持します。
           rate は「その期間の実現レート」（符号付）で、MVPでは predicted=next_rate として扱います。
        """

        self._path = Path(path)
        self._events: list[FundingRateEvent] = self._load(self._path)
        self._applied_idx: dict[str, int] = {}  # 銘柄ごとに「どこまで適用済みか」を持つ

    def _load(self, p: Path) -> list[FundingRateEvent]:
        df = pd.read_csv(p)
        cols = {c.lower(): c for c in df.columns}
        for k in ["ts", "symbol", "rate"]:
            if k not in cols:
                raise ValueError(f"missing column in funding csv: {k}")
        ts = pd.to_datetime(df[cols["ts"]], utc=True)
        out: list[FundingRateEvent] = []
        for t, sym, r in zip(ts, df[cols["symbol"]], df[cols["rate"]], strict=False):
            out.append(FundingRateEvent(ts=t.to_pydatetime(), symbol=str(sym), rate=float(r)))
        out.sort(key=lambda x: (x.ts, x.symbol))
        return out

    def next_rate_and_time(self, *, symbol: str, now: datetime) -> tuple[float | None, datetime | None]:
        """これは何をする関数？
        → 現在時刻に対する「次のFundingレートと時刻」を返します（なければ両方None）。
        """

        for ev in self._events:
            if ev.symbol == symbol and ev.ts > now:
                return ev.rate, ev.ts
        return None, None

    def due_events(self, *, now: datetime) -> list[FundingRateEvent]:
        """これは何をする関数？
        → まだ適用していない「期限到来のFundingイベント」をすべて返します。
        """

        out: list[FundingRateEvent] = []
        for sym in set(e.symbol for e in self._events):
            idx = self._applied_idx.get(sym, 0)
            while idx < len(self._events):
                ev = self._events[idx]
                if ev.symbol != sym:
                    idx += 1
                    continue
                if ev.ts <= now:
                    out.append(ev)
                    idx += 1
                else:
                    break
            self._applied_idx[sym] = idx
        # 時刻順に返す
        out.sort(key=lambda x: (x.ts, x.symbol))
        return out


# ===== Funding 情報提供（Strategy が呼ぶ get_funding_info 用の簡易データ源） =====


class ReplayDataSource(ExchangeGateway):
    """バックテスト用の最小データ源。
    - get_funding_info: FundingSchedule から predicted と next_time を返す
    - get_ticker: 直近の last/mid を返す（PaperExchangeのフォールバック用）
    """

    def __init__(self, *, schedule: FundingSchedule) -> None:
        self._schedule = schedule
        self._now: datetime = datetime.now(timezone.utc)
        self._tickers: dict[str, float] = {}

    def set_now(self, now: datetime) -> None:
        """これは何をする関数？→ シミュレーション現在時刻を更新します。"""

        self._now = now

    def update_price(self, symbol: str, *, bid: float | None, ask: float | None, last: float | None) -> None:
        """これは何をする関数？→ フォールバック用の近似価格を内部更新します。"""

        if last is not None:
            self._tickers[symbol] = float(last)
        elif bid is not None and ask is not None:
            self._tickers[symbol] = (float(bid) + float(ask)) / 2.0

    async def get_funding_info(self, symbol: str) -> FundingInfo:
        """これは何をする関数？→ 次のFunding予想レートと時刻を返します（currentは未使用）。"""

        rate, t = self._schedule.next_rate_and_time(symbol=symbol, now=self._now)
        return FundingInfo(symbol=symbol, current_rate=None, predicted_rate=rate, next_funding_time=t)

    async def get_ticker(self, symbol: str) -> float:
        """これは何をする関数？→ 近似価格（last→mid）を返します。"""

        return float(self._tickers.get(symbol, 0.0))

    # 未使用API（MVP）
    async def get_balances(self) -> list[Balance]:  # pragma: no cover - 未使用
        return []

    async def get_positions(self) -> list[Position]:  # pragma: no cover - 未使用
        return []

    async def get_open_orders(self, symbol: str | None = None):  # pragma: no cover - 未使用
        return []

    async def place_order(self, req: OrderRequest) -> Order:  # pragma: no cover - 未使用
        raise NotImplementedError

    async def cancel_order(  # type: ignore[override]
        self, symbol: str, order_id: str | None = None, client_order_id: str | None = None
    ):  # pragma: no cover - 未使用
        raise NotImplementedError

    async def subscribe_private(self, callbacks):  # pragma: no cover - 未使用
        pass

    async def subscribe_public(self, symbols, callbacks):  # pragma: no cover - 未使用
        pass


# ===== ランナー本体 =====


@dataclass
class BacktestResult:
    """これは何を表す型？
    → バックテスト1日分の集計結果（最小）。
    """

    date: str
    funding_events: int
    trades: int
    net_pnl: float
    funding_pnl: float
    trading_pnl: float
    fees_est: float
    slippage_est: float
    entries: int
    exits: int
    avg_hold_seconds: float | None


class BacktestRunner:
    """1日のティックを順に適用し、戦略 step() を回すリプレイランナー"""

    def __init__(
        self,
        *,
        price_feed: CsvPriceFeed,
        funding_schedule: FundingSchedule | None,
        strategy_cfg: StrategyFundingConfig,
        risk_cfg: RiskConfig,
        db_url: str = "sqlite+aiosqlite:///./db/trading.db",
        step_sec: float = 3.0,
    ) -> None:
        """これは何をする関数？
        → 価格フィード・Fundingスケジュールと各種設定を受け取り、バックテスト環境を初期化します。
        """

        self._feed = price_feed
        self._schedule = funding_schedule
        self._step_sec = float(step_sec)
        self._repo = Repo(db_url=db_url)
        # backtestでのコスト前提は env で上書き可能（感度分析用）
        taker_fee_bps_roundtrip = float(
            os.environ.get("STRATEGY__TAKER_FEE_BPS_ROUNDTRIP", str(strategy_cfg.taker_fee_bps_roundtrip)) or 0.0
        )
        estimated_slippage_bps = float(
            os.environ.get("STRATEGY__ESTIMATED_SLIPPAGE_BPS", str(strategy_cfg.estimated_slippage_bps)) or 0.0
        )
        extra_spread_bps = float(os.environ.get("STRATEGY__EXTRA_SPREAD_BPS", "1.0") or 1.0)
        self._cost_model = CostModel(
            spot_taker_fee_bps=taker_fee_bps_roundtrip / 2.0,
            perp_taker_fee_bps=taker_fee_bps_roundtrip / 2.0,
            slippage_bps=estimated_slippage_bps,
            extra_spread_bps=extra_spread_bps,
        )

        # Strategy の対象シンボル
        self._symbols = list(strategy_cfg.symbols)

        # Funding 情報提供のためのデータ源（スケジュールなしならダミー）
        self._data_src = ReplayDataSource(schedule=funding_schedule or FundingSchedule(path=self._empty_schedule_csv()))

        # 疑似約定の取引所
        self._paper = PaperExchange(data_source=self._data_src, initial_usdt=100_000.0, cost_model=self._cost_model)

        # 取引部品
        self._oms = OmsEngine(ex=self._paper, repo=self._repo, cfg=None)

        # RiskManager（flatten は strategy へ委譲）
        self._strategy_holder: dict[str, Any] = {}

        async def _flatten_all() -> None:
            """これは何をする関数？→ Strategy へ全クローズを依頼します。"""

            s = self._strategy_holder.get("strategy")
            if s:
                await s.flatten_all()

        self._risk = RiskManager(
            loss_cut_daily_jpy=risk_cfg.loss_cut_daily_jpy,
            ws_disconnect_threshold_sec=30.0,
            hedge_delay_p95_threshold_sec=2.0,
            api_error_max_in_60s=10,
            flatten_all=_flatten_all,
            # Funding/Basis は「ホールドして稼ぐ」が本質なので、バックテストでは sign flip によるKILLを原則無効化する。
            # 必要なら env で上書きできるようにしておく（例：BACKTEST__FUNDING_FLIP_MIN_ABS=0.00005）。
            funding_flip_min_abs=float(os.environ.get("BACKTEST__FUNDING_FLIP_MIN_ABS", "1.0") or 1.0),
            funding_flip_consecutive=int(os.environ.get("BACKTEST__FUNDING_FLIP_CONSECUTIVE", "999999") or 999999),
        )

        # Strategy（実装の引数名に合わせて渡す）
        self._strategy = FundingBasisStrategy(
            oms=self._oms,
            risk_config=risk_cfg,
            strategy_config=strategy_cfg,
            period_seconds=8.0 * 3600.0,
            taker_fee_bps_roundtrip=taker_fee_bps_roundtrip,
            estimated_slippage_bps=estimated_slippage_bps,
            risk_manager=self._risk,
        )
        self._strategy_holder["strategy"] = self._strategy

        # OMS を PaperExchange に結線
        self._paper.bind_oms(self._oms)

    def _empty_schedule_csv(self) -> str:
        """これは何をする関数？→ 空のFunding CSVを一時生成してパスを返します（スケジュール省略時のダミー）。"""

        tmp = Path(".backtest_empty_funding.csv")
        if not tmp.exists():
            tmp.write_text("ts,symbol,rate\n", encoding="utf-8")
        return str(tmp)

    async def _apply_funding_if_due(self, now: datetime) -> None:
        """これは何をする関数？
        → 現在時刻に到来した Funding イベントを適用し、DB に FundingEvent を記録します。
           実現PnLは「ポジション側（long/short）の向き」に応じ、+rate×notional（shortは受取り）、-rate×notional（longは支払い）。
        """

        if not self._schedule:
            return
        for ev in self._schedule.due_events(now=now):
            # 対象銘柄の perp ポジション名目を計算
            positions = await self._paper.get_positions()
            last_px = await self._paper.get_ticker(ev.symbol) or 0.0
            notional = 0.0
            realized = 0.0
            for p in positions:
                sym_norm = p.symbol.replace("/", "").replace(":USDT", "").upper()
                if sym_norm != ev.symbol.upper():
                    continue
                # long: 支払い（-）、short: 受取（+）
                if p.side.lower() == "long":
                    realized += -ev.rate * float(p.size) * float(last_px)
                    notional += abs(float(p.size) * float(last_px))
                elif p.side.lower() == "short":
                    realized += ev.rate * float(p.size) * float(last_px)
                    notional += abs(float(p.size) * float(last_px))
            await self._repo.add_funding_event(
                ts=ev.ts,
                symbol=ev.symbol,
                rate=ev.rate,
                notional=notional,
                realized_pnl=realized,
            )
            logger.info(
                "BT funding applied: {} {} rate={} notional={} realized={}",
                ev.ts.isoformat(),
                ev.symbol,
                ev.rate,
                round(notional, 2),
                round(realized, 4),
            )

    async def run_one_day(self, *, date_utc: str) -> BacktestResult:
        """これは何をする関数？
        → 指定UTC日のティックを順に適用し、一定間隔で strategy.step() を呼んで1日を完走します。
           結果（Funding件数・トレード件数・NetPnL概算）を返します。
        """

        await self._repo.create_all()

        last_step_at: datetime | None = None

        # ティックを順次適用
        for tick in self._feed.iter_ticks(date_utc=date_utc):
            # 現在時刻を進め、Fundingの予告＆適用
            self._data_src.set_now(tick.ts)
            self._data_src.update_price(tick.symbol, bid=tick.bid, ask=tick.ask, last=tick.last)

            # PaperExchange に BBO・trade を通知（Bitget 形式に擬態）
            if tick.bid is not None or tick.ask is not None:
                msg_ob = {
                    "topic": f"orderbook.1.{tick.symbol}",
                    "data": [{"b": [[tick.bid or 0.0, "0"]], "a": [[tick.ask or 0.0, "0"]]}],
                }
                await self._paper.handle_public_msg(msg_ob)
            if tick.last is not None:
                msg_tr = {
                    "topic": f"publicTrade.{tick.symbol}",
                    "data": [{"p": str(tick.last)}],
                }
                await self._paper.handle_public_msg(msg_tr)

            # backtest補助：Strategyの市場データREADY判定を通すための擬似スケール/ガード/アンカーをPaperExchangeに付与
            # - _scale_cache: priceScale が存在すれば「スケール準備OK」と判定される
            # - _price_state: READY にして価格ガードを通す
            # - _last_spot_px/_last_index_px: アンカー価格（spot→index）に利用される
            try:
                if not hasattr(self._paper, "_scale_cache"):
                    self._paper._scale_cache = {}
                if not hasattr(self._paper, "_price_state"):
                    self._paper._price_state = {}
                if not hasattr(self._paper, "_bbo_cache"):
                    self._paper._bbo_cache = {}
                sc = self._paper._scale_cache
                ps = self._paper._price_state
                bbo = self._paper._bbo_cache
                # backtest用の最低限メタ（OMSのサイズガードに必要）
                sc[tick.symbol] = {
                    "priceScale": 2,
                    # qtyの刻み/最小（実取引所とは異なるが、dust注文を抑止できる程度の値にする）
                    "qtyStep_perp": 1e-6,
                    "qtyStep_spot": 1e-6,
                    "minQty_perp": 1e-6,
                    "minQty_spot": 1e-6,
                    # 最小名目（USDT想定）。5USDT未満は skip して現実のrejectに近づける
                    "minNotional_perp": 5.0,
                    "minNotional_spot": 5.0,
                }
                ps[tick.symbol] = "READY"
                bbo[tick.symbol] = {"bid": tick.bid, "ask": tick.ask}
                if tick.last is not None:
                    if not hasattr(self._paper, "_last_spot_px"):
                        self._paper._last_spot_px = {}
                    if not hasattr(self._paper, "_last_index_px"):
                        self._paper._last_index_px = {}
                    self._paper._last_spot_px[tick.symbol] = float(tick.last)
                    self._paper._last_index_px[tick.symbol] = float(tick.last)
            except Exception:
                pass  # backtest便宜用の付帯属性なので、失敗時は黙殺

            # Funding 適用
            await self._apply_funding_if_due(tick.ts)

            # step() 実行（一定間隔）
            if (last_step_at is None) or ((tick.ts - last_step_at).total_seconds() >= self._step_sec):
                try:
                    for sym in self._symbols:
                        # Funding 情報と価格を取得し Strategy を1ステップ進める
                        f_info = await self._data_src.get_funding_info(sym)
                        px = await self._paper.get_ticker(sym)
                        await self._strategy.step(funding=f_info, spot_price=px, perp_price=px)
                except Exception as e:  # noqa: BLE001
                    logger.exception("backtest step error: {}", e)
                last_step_at = tick.ts

        # 1日終了時点の集計
        ff = await self._repo.list_funding_events()
        tt = await self._repo.list_trades()
        funding_today = [f for f in ff if f.ts.date().isoformat() == date_utc]
        trades_today = [t for t in tt if t.ts.date().isoformat() == date_utc]
        funding_pnl = sum(f.realized_pnl for f in funding_today)

        def _calc_trade_metrics(trades: list) -> tuple[float, float, float, int, int, float | None]:
            trades_sorted = sorted(trades, key=lambda x: x.ts)
            realized = 0.0
            fees = 0.0
            slippage = 0.0
            entries = 0
            exits = 0
            hold_durations: list[float] = []
            pos: dict[str, dict[str, Any]] = defaultdict(lambda: {"size": 0.0, "avg": 0.0, "entry_ts": None})

            for t in trades_sorted:
                price = float(t.price)
                qty = float(t.qty)
                side = (t.side or "").lower()
                signed_qty = qty if side == "buy" else -qty
                notional = abs(qty * price)

                fee_val = float(getattr(t, "fee", 0.0) or 0.0)
                fees += fee_val if fee_val > 0.0 else self._cost_model.taker_fee(symbol=t.symbol, qty=qty, price=price)
                slippage += self._cost_model.slippage_cost(notional=notional)

                p = pos[t.symbol]
                size = float(p["size"])
                avg = float(p["avg"])
                entry_ts = p["entry_ts"]

                if size == 0 or size * signed_qty >= 0:
                    new_size = size + signed_qty
                    new_avg = (
                        ((avg * abs(size)) + price * abs(signed_qty)) / abs(new_size) if new_size != 0 else price
                    )
                    if size == 0 and new_size != 0:
                        entries += 1
                        entry_ts = t.ts
                    p["size"] = new_size
                    p["avg"] = new_avg
                    p["entry_ts"] = entry_ts
                    continue

                close_qty = min(abs(size), abs(signed_qty))
                if size > 0:
                    realized += (price - avg) * close_qty
                else:
                    realized += (avg - price) * close_qty

                new_size = size + signed_qty
                if abs(new_size) < 1e-12:
                    exits += 1
                    if entry_ts:
                        hold_durations.append((t.ts - entry_ts).total_seconds())
                    p["size"] = 0.0
                    p["avg"] = 0.0
                    p["entry_ts"] = None
                else:
                    p["size"] = new_size
                    p["avg"] = price
                    p["entry_ts"] = t.ts
                    entries += 1

            avg_hold = sum(hold_durations) / len(hold_durations) if hold_durations else None
            return realized, fees, slippage, entries, exits, avg_hold

        trading_pnl, fees_est, slippage_est, entries, exits, avg_hold = _calc_trade_metrics(trades_today)
        # slippage_est は「どれくらい滑ったか/補正が乗ったか」の診断値。
        # PaperExchange の約定価格はすでに slippage(+extra_spread) を織り込んでいるため、
        # ここで net からさらに控除すると二重計上になる。
        net = funding_pnl + trading_pnl - fees_est

        return BacktestResult(
            date=date_utc,
            funding_events=len(funding_today),
            trades=len(trades_today),
            net_pnl=float(net),
            funding_pnl=float(funding_pnl),
            trading_pnl=float(trading_pnl),
            fees_est=float(fees_est),
            slippage_est=float(slippage_est),
            entries=entries,
            exits=exits,
            avg_hold_seconds=avg_hold,
        )


# ===== CLI（任意実行） =====


def main() -> None:
    """これは何をする関数？
    → コマンドラインから1日リプレイを実行します。
       例：
         poetry run python -m bot.backtest.replay \
           --prices data/prices.csv \
           --funding data/funding.csv \
           --date 2025-01-01 \
           --config config/app.yaml
    """

    parser = argparse.ArgumentParser(description="Backtest 1-day replay (paper fill from CSV/Parquet)")
    parser.add_argument("--prices", required=True, help="CSV/Parquet with columns: ts,symbol,bid,ask,last")
    parser.add_argument("--funding", default=None, help="CSV with columns: ts,symbol,rate (period rate, signed)")
    parser.add_argument("--date", required=True, help="UTC date YYYY-MM-DD")
    parser.add_argument("--step-sec", type=float, default=3.0, help="strategy step interval seconds")
    parser.add_argument(
        "--config",
        default="config/app.yaml",
        help="設定ファイルパス（db_urlなどを取得）。未指定なら config/app.yaml",
    )
    parser.add_argument("--db-url", help="DB接続文字列。指定があればconfigより優先")
    args = parser.parse_args()

    async def _run() -> None:
        cfg = load_config(config_path=args.config)
        db_url = args.db_url or cfg.db_url
        feed = CsvPriceFeed(path=args.prices)
        sched = FundingSchedule(path=args.funding) if args.funding else None
        runner = BacktestRunner(
            price_feed=feed,
            funding_schedule=sched,
            strategy_cfg=cfg.strategy,
            risk_cfg=cfg.risk,
            db_url=db_url,
            step_sec=args.step_sec,
        )
        res = await runner.run_one_day(date_utc=args.date)
        logger.info(
            "Backtest done: date={} funding_events={} trades={} net_pnl={} funding_pnl={} trading_pnl={} fees_est={} slippage_est={} entries={} exits={} avg_hold_s={}",
            res.date,
            res.funding_events,
            res.trades,
            round(res.net_pnl, 4),
            round(res.funding_pnl, 4),
            round(res.trading_pnl, 4),
            round(res.fees_est, 4),
            round(res.slippage_est, 4),
            res.entries,
            res.exits,
            None if res.avg_hold_seconds is None else round(res.avg_hold_seconds, 2),
        )

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
