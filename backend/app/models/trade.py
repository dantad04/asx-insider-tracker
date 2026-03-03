"""Trade model for tracking director insider trades"""

from datetime import date
from enum import Enum
from uuid import uuid4

from sqlalchemy import Date, Enum as SQLEnum, ForeignKey, Integer, Numeric, String, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class TradeType(str, Enum):
    """Enumeration of trade types"""

    ON_MARKET_BUY = "on_market_buy"
    ON_MARKET_SELL = "on_market_sell"
    OFF_MARKET = "off_market"
    EXERCISE_OPTIONS = "exercise_options"
    OTHER = "other"


class Trade(Base):
    """
    Represents a single insider trade by a director.
    Includes trade date, quantity, price, and type of trade.
    """

    __tablename__ = "trades"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    director_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), ForeignKey("directors.id"), nullable=False
    )
    company_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), ForeignKey("companies.id"), nullable=False
    )
    date_of_trade: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    date_lodged: Mapped[date] = mapped_column(Date, nullable=False)
    trade_type: Mapped[TradeType] = mapped_column(
        SQLEnum(TradeType, native_enum=False), nullable=False
    )
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    price_per_share: Mapped[float] = mapped_column(Numeric(12, 4), nullable=True)
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="seed_json")
    # source values: "seed_json" (unreliable dates) | "pdf_parser" (verified ASX dates)

    __table_args__ = (Index("idx_date_of_trade", "date_of_trade"),)
