"""Company model for tracking ASX-listed companies"""

from datetime import datetime
from uuid import uuid4

from sqlalchemy import Boolean, Integer, String, DateTime, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Company(Base):
    """
    Represents an ASX-listed company.
    Stores company metadata including ticker, sector, and market classification.
    """

    __tablename__ = "companies"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    ticker: Mapped[str] = mapped_column(String(10), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    sector: Mapped[str | None] = mapped_column(String(100))
    industry_group: Mapped[str | None] = mapped_column(String(100))
    market_cap: Mapped[int | None] = mapped_column(Integer)
    is_asx200: Mapped[bool] = mapped_column(Boolean, default=False)
    is_asx300: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (Index("idx_ticker", "ticker"),)
