# これは「DBテーブルの形（SQLAlchemy ORMモデル）」を定義するファイルです。
# 戦略・OMS・モニタが後から同じ形で読み返せるようにします。

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """全テーブルの親クラス（SQLAlchemy 2.0の宣言ベース）"""

    pass


class TradeLog(Base):
    """約定（トレード）を記録するテーブル"""

    __tablename__ = "trade_log"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))  # 約定時刻（UTC）
    symbol: Mapped[str] = mapped_column(String(32))
    side: Mapped[str] = mapped_column(String(8))  # "buy" / "sell"
    qty: Mapped[float] = mapped_column(Float)  # ベース数量
    price: Mapped[float] = mapped_column(Float)
    fee: Mapped[float] = mapped_column(Float)
    exchange_order_id: Mapped[str] = mapped_column(String(64))
    client_id: Mapped[str | None] = mapped_column(String(64), nullable=True)


class OrderLog(Base):
    """注文イベント（発注・更新・取消など）を記録するテーブル"""

    __tablename__ = "order_log"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    symbol: Mapped[str] = mapped_column(String(32))
    side: Mapped[str] = mapped_column(String(8))
    type: Mapped[str] = mapped_column(String(16))  # "limit" / "market" など
    qty: Mapped[float] = mapped_column(Float)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[str] = mapped_column(String(32))  # "new","filled","canceled" など
    exchange_order_id: Mapped[str] = mapped_column(String(64))
    client_id: Mapped[str | None] = mapped_column(String(64), nullable=True)


class PositionSnap(Base):
    """建玉スナップショット（定期的に保存してポジションの推移を追う）"""

    __tablename__ = "position_snap"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    symbol: Mapped[str] = mapped_column(String(32))
    side: Mapped[str] = mapped_column(String(8))  # "long" / "short"
    size: Mapped[float] = mapped_column(Float)
    entry_price: Mapped[float] = mapped_column(Float)
    upnl: Mapped[float] = mapped_column(Float)


class FundingEvent(Base):
    """Funding 受払の実績を記録（検証/レポートで使用）"""

    __tablename__ = "funding_event"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True))  # 実支払の時刻（UTC）
    symbol: Mapped[str] = mapped_column(String(32))
    rate: Mapped[float] = mapped_column(Float)  # その期間の実現レート（符号付）
    notional: Mapped[float] = mapped_column(Float)  # 基準名目
    realized_pnl: Mapped[float] = mapped_column(Float)


class DailyPnl(Base):
    """日次PnLの集計（レポートで使用）"""

    __tablename__ = "daily_pnl"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    date: Mapped[str] = mapped_column(String(10))  # "YYYY-MM-DD"
    gross: Mapped[float] = mapped_column(Float)
    fees: Mapped[float] = mapped_column(Float)
    net: Mapped[float] = mapped_column(Float)
