"""Announcement model for tracking company announcements"""

from datetime import datetime, date
from uuid import uuid4

from sqlalchemy import Date, Boolean, String, DateTime, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Announcement(Base):
    """
    Represents an ASX announcement for a company.
    Tracks announcement type, title, and whether it's price-sensitive.
    """

    __tablename__ = "announcements"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    announcement_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    announcement_type: Mapped[str] = mapped_column(String(100))
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    url: Mapped[str] = mapped_column(String(500), nullable=True)
    is_price_sensitive: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_ticker", "ticker"),
        Index("idx_announcement_date", "announcement_date"),
    )
