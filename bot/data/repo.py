# これは「DBの作成とCRUD（保存/取得）を提供する」リポジトリの実装です。
# 非同期SQLAlchemy（aiosqlite）を使います。

from __future__ import annotations

from pathlib import Path

from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from bot.core.time import utc_now  # テストや既定値で使う

from .schema import Base, DailyPnl, FundingEvent, OrderLog, PositionSnap, TradeLog


class Repo:
    """アプリがDBへアクセスするための窓口（create_allとCRUDを提供）"""

    def __init__(self, db_url: str = "sqlite+aiosqlite:///./db/trading.db") -> None:
        """これは何をする関数？
        → 接続文字列を受け取り、非同期エンジンとセッションファクトリを準備します。
          SQLiteのときはDBファイルの親ディレクトリを自動作成します。
        """
        self._db_url = db_url
        self._ensure_sqlite_dir(db_url)
        self._engine: AsyncEngine = create_async_engine(db_url, echo=False, future=True)
        self._sessionmaker: async_sessionmaker[AsyncSession] = async_sessionmaker(self._engine, expire_on_commit=False)

    # ---------- 内部：SQLiteパスのディレクトリ自動作成 ----------

    @staticmethod
    def _ensure_sqlite_dir(db_url: str) -> None:
        """これは何をする関数？
        → DBがSQLiteのとき、ファイルの親ディレクトリ（例: ./db）を自動で作ります。
        """
        try:
            url = make_url(db_url)
        except Exception:
            return
        if not url.drivername.startswith("sqlite"):
            return
        # URL.database は相対/絶対パスのことがある
        if not url.database:
            return
        p = Path(url.database)
        if not p.is_absolute():
            p = Path.cwd() / p
        p.parent.mkdir(parents=True, exist_ok=True)

    # ---------- スキーマ作成 ----------

    async def create_all(self) -> None:
        """これは何をする関数？
        → モデルに基づく全テーブルを作成します（既にあれば何もしません）。
        """
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    # ---------- TradeLog ----------

    async def add_trade(
        self,
        *,
        ts=None,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        fee: float,
        exchange_order_id: str,
        client_id: str | None = None,
    ) -> TradeLog:
        """これは何をする関数？→ トレード行を1件保存して返します。"""
        row = TradeLog(
            ts=ts or utc_now(),
            symbol=symbol,
            side=side,
            qty=qty,
            price=price,
            fee=fee,
            exchange_order_id=exchange_order_id,
            client_id=client_id,
        )
        async with self._sessionmaker() as s:
            s.add(row)
            await s.commit()
            await s.refresh(row)
        return row

    async def list_trades(self, *, symbol: str | None = None) -> list[TradeLog]:
        """これは何をする関数？→ 条件（任意）でトレード一覧を返します。"""
        async with self._sessionmaker() as s:
            stmt = select(TradeLog).order_by(TradeLog.id.desc())
            if symbol:
                stmt = stmt.where(TradeLog.symbol == symbol)
            res = await s.execute(stmt)
            return list(res.scalars().all())

    # ---------- OrderLog ----------

    async def add_order_log(
        self,
        *,
        ts=None,
        symbol: str,
        side: str,
        type: str,
        qty: float,
        price: float | None,
        status: str,
        exchange_order_id: str,
        client_id: str | None = None,
    ) -> OrderLog:
        """これは何をする関数？→ 注文イベント行を1件保存して返します。"""
        row = OrderLog(
            ts=ts or utc_now(),
            symbol=symbol,
            side=side,
            type=type,
            qty=qty,
            price=price,
            status=status,
            exchange_order_id=exchange_order_id,
            client_id=client_id,
        )
        async with self._sessionmaker() as s:
            s.add(row)
            await s.commit()
            await s.refresh(row)
        return row

    async def list_order_logs(self, *, symbol: str | None = None) -> list[OrderLog]:
        """これは何をする関数？→ 条件（任意）で注文イベント一覧を返します。"""
        async with self._sessionmaker() as s:
            stmt = select(OrderLog).order_by(OrderLog.id.desc())
            if symbol:
                stmt = stmt.where(OrderLog.symbol == symbol)
            res = await s.execute(stmt)
            return list(res.scalars().all())

    # ---------- PositionSnap ----------

    async def add_position_snap(
        self,
        *,
        ts=None,
        symbol: str,
        side: str,
        size: float,
        entry_price: float,
        upnl: float,
    ) -> PositionSnap:
        """これは何をする関数？→ ポジションスナップショットを1件保存して返します。"""
        row = PositionSnap(
            ts=ts or utc_now(),
            symbol=symbol,
            side=side,
            size=size,
            entry_price=entry_price,
            upnl=upnl,
        )
        async with self._sessionmaker() as s:
            s.add(row)
            await s.commit()
            await s.refresh(row)
        return row

    async def list_position_snaps(self, *, symbol: str | None = None) -> list[PositionSnap]:
        """これは何をする関数？→ 条件（任意）でポジションスナップ一覧を返します。"""
        async with self._sessionmaker() as s:
            stmt = select(PositionSnap).order_by(PositionSnap.id.desc())
            if symbol:
                stmt = stmt.where(PositionSnap.symbol == symbol)
            res = await s.execute(stmt)
            return list(res.scalars().all())

    # ---------- FundingEvent ----------

    async def add_funding_event(
        self,
        *,
        ts=None,
        symbol: str,
        rate: float,
        notional: float,
        realized_pnl: float,
    ) -> FundingEvent:
        """これは何をする関数？→ Funding実績を1件保存して返します。"""
        row = FundingEvent(
            ts=ts or utc_now(),
            symbol=symbol,
            rate=rate,
            notional=notional,
            realized_pnl=realized_pnl,
        )
        async with self._sessionmaker() as s:
            s.add(row)
            await s.commit()
            await s.refresh(row)
        return row

    async def list_funding_events(self, *, symbol: str | None = None) -> list[FundingEvent]:
        """これは何をする関数？→ 条件（任意）でFunding実績一覧を返します。"""
        async with self._sessionmaker() as s:
            stmt = select(FundingEvent).order_by(FundingEvent.id.desc())
            if symbol:
                stmt = stmt.where(FundingEvent.symbol == symbol)
            res = await s.execute(stmt)
            return list(res.scalars().all())

    # ---------- DailyPnl ----------

    async def add_daily_pnl(
        self,
        *,
        date: str,
        gross: float,
        fees: float,
        net: float,
    ) -> DailyPnl:
        """これは何をする関数？→ 日付キーで日次PnLを追加（同日が複数あっても良いMVP仕様）"""
        row = DailyPnl(date=date, gross=gross, fees=fees, net=net)
        async with self._sessionmaker() as s:
            s.add(row)
            await s.commit()
            await s.refresh(row)
        return row

    async def list_daily_pnl(self, *, date: str | None = None) -> list[DailyPnl]:
        """これは何をする関数？→ 条件（任意）で日次PnLの一覧を返します。"""
        async with self._sessionmaker() as s:
            stmt = select(DailyPnl).order_by(DailyPnl.id.desc())
            if date:
                stmt = stmt.where(DailyPnl.date == date)
            res = await s.execute(stmt)
            return list(res.scalars().all())

    # ---------- 終了処理 ----------

    async def dispose(self) -> None:
        """これは何をする関数？→ エンジンを明示的に閉じます（任意）。"""
        await self._engine.dispose()
