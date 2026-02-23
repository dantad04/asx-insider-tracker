"""DirectorCompany junction model for many-to-many director/company relationships"""

from uuid import uuid4

from sqlalchemy import Boolean, Float, Integer, String, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class DirectorCompany(Base):
    """
    Junction table representing the relationship between directors and companies.
    Tracks director roles, trade statistics, and smart money scores.
    """

    __tablename__ = "director_companies"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    director_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), ForeignKey("directors.id"), nullable=False
    )
    company_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), ForeignKey("companies.id"), nullable=False
    )
    role: Mapped[str | None] = mapped_column(String(100))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    smart_money_score_30d: Mapped[float | None] = mapped_column(Float)
    smart_money_score_60d: Mapped[float | None] = mapped_column(Float)
    smart_money_score_90d: Mapped[float | None] = mapped_column(Float)
    trade_count: Mapped[int] = mapped_column(Integer, default=0)

    __table_args__ = (
        UniqueConstraint("director_id", "company_id", name="uq_director_company"),
    )
