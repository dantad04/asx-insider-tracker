"""Director model for tracking company directors/executives"""

from datetime import datetime
from uuid import uuid4

from sqlalchemy import String, DateTime, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Director(Base):
    """
    Represents a company director or executive.
    Directors can be associated with multiple companies through DirectorCompany.
    """

    __tablename__ = "directors"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    full_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (Index("idx_full_name", "full_name"),)
