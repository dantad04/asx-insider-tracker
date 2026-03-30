"""Portfolio position model for Smart Money Portfolio tracking."""

from datetime import date, datetime
from uuid import uuid4

from sqlalchemy import DateTime, Date, Integer, Numeric, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class PortfolioPosition(Base):
    """
    Tracks a single Smart Money Portfolio position.

    A position is opened when a smart money signal fires (2+ directors buy
    within 7 days) and we have available cash. It closes either after 90 days
    or when a sell signal fires (2+ directors sell within 7 days).
    """

    __tablename__ = "portfolio_positions"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    company_name: Mapped[str] = mapped_column(String(255), nullable=False)

    # Signal metadata
    signal_date: Mapped[date] = mapped_column(Date, nullable=False)
    signal_directors: Mapped[str] = mapped_column(Text, nullable=False)  # JSON list

    # Entry
    entry_date: Mapped[date] = mapped_column(Date, nullable=False)
    entry_price: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    cost_basis: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)

    # Exit (null until closed)
    exit_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    exit_price: Mapped[float | None] = mapped_column(Numeric(12, 4), nullable=True)
    exit_reason: Mapped[str | None] = mapped_column(
        String(20), nullable=True
    )  # "90_days" | "sell_signal"

    # Status & results
    status: Mapped[str] = mapped_column(
        String(10), nullable=False, default="open"
    )  # "open" | "closed"
    pnl_aud: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    pnl_pct: Mapped[float | None] = mapped_column(Numeric(8, 4), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )

    __table_args__ = (
        # One position per ticker per signal date
        UniqueConstraint("ticker", "signal_date", name="uq_portfolio_ticker_signal"),
    )
