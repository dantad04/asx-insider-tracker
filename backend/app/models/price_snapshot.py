"""PriceSnapshot model for storing historical stock price data"""

from datetime import date
from uuid import uuid4

from sqlalchemy import Date, Integer, Numeric, String, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class PriceSnapshot(Base):
    """
    Represents daily OHLCV (Open, High, Low, Close, Volume) data for a stock.
    Used to calculate performance metrics for insider trades.
    """

    __tablename__ = "price_snapshots"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    open: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)
    high: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)
    low: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)
    close: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)
    volume: Mapped[int] = mapped_column(Integer, nullable=False)

    __table_args__ = (
        UniqueConstraint("ticker", "date", name="uq_ticker_date"),
        Index("idx_ticker", "ticker"),
        Index("idx_date", "date"),
    )
