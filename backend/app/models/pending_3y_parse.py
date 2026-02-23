"""Model for tracking pending Appendix 3Y PDF parsing tasks"""

from datetime import date, datetime
from enum import Enum
from uuid import uuid4

from sqlalchemy import Date, DateTime, Enum as SQLEnum, Integer, String, Text, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class ParseStatus(str, Enum):
    """Status of PDF parsing workflow"""

    PENDING = "pending"
    PARSED = "parsed"
    FAILED = "failed"


class Pending3YParse(Base):
    """
    Tracks Appendix 3Y PDFs awaiting parsing.
    Created by scraper, consumed by parser module.
    """

    __tablename__ = "pending_3y_parses"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    pdf_path: Mapped[str] = mapped_column(String(500), nullable=False, unique=True)
    pdf_url: Mapped[str] = mapped_column(String(500), nullable=False, unique=True)
    document_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    announcement_header: Mapped[str] = mapped_column(String(500), nullable=False)
    status: Mapped[ParseStatus] = mapped_column(
        SQLEnum(ParseStatus, native_enum=False),
        nullable=False,
        default=ParseStatus.PENDING,
        index=True
    )
    parse_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        Index("idx_pending_3y_parses_ticker", "ticker"),
        Index("idx_pending_3y_parses_status", "status"),
        Index("idx_pending_3y_parses_document_date", "document_date"),
    )
