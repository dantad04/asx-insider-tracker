"""Cluster Portfolio models for the rules-based paper portfolio."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class ClusterPortfolio(Base):
    """Top-level paper portfolio configuration and cash state."""

    __tablename__ = "cluster_portfolios"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    strategy_key: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    starting_cash: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    current_cash: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class ClusterPortfolioPosition(Base):
    """One opened paper-portfolio position tied to a detected cluster window."""

    __tablename__ = "cluster_portfolio_positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    portfolio_id: Mapped[int] = mapped_column(
        ForeignKey("cluster_portfolios.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    entry_cluster_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    cluster_start_date: Mapped[date] = mapped_column(Date, nullable=False)
    cluster_end_date: Mapped[date] = mapped_column(Date, nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    sector: Mapped[str | None] = mapped_column(String(100), nullable=True)
    industry: Mapped[str | None] = mapped_column(String(100), nullable=True)
    source_status: Mapped[str] = mapped_column(String(20), nullable=False)

    buy_date: Mapped[date] = mapped_column(Date, nullable=False)
    planned_exit_date: Mapped[date] = mapped_column(Date, nullable=False)
    sell_date: Mapped[date | None] = mapped_column(Date, nullable=True)

    entry_price: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)
    entry_price_date: Mapped[date] = mapped_column(Date, nullable=False)
    exit_price: Mapped[float | None] = mapped_column(Numeric(12, 4), nullable=True)
    exit_price_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    quantity: Mapped[float] = mapped_column(Numeric(18, 6), nullable=False)
    allocated_aud: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)

    status: Mapped[str] = mapped_column(String(10), nullable=False, default="open")
    buy_reason: Mapped[str] = mapped_column(Text, nullable=False)
    sell_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        UniqueConstraint(
            "portfolio_id",
            "ticker",
            "cluster_start_date",
            "cluster_end_date",
            name="uq_cluster_portfolio_position_key",
        ),
        Index(
            "uq_cluster_portfolio_open_ticker",
            "portfolio_id",
            "ticker",
            unique=True,
            postgresql_where=text("status = 'open'"),
        ),
    )


class ClusterPortfolioEvent(Base):
    """Append-only event log for portfolio actions and notification outcomes."""

    __tablename__ = "cluster_portfolio_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    portfolio_id: Mapped[int] = mapped_column(
        ForeignKey("cluster_portfolios.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    position_id: Mapped[int | None] = mapped_column(
        ForeignKey("cluster_portfolio_positions.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    event_time: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    payload_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
