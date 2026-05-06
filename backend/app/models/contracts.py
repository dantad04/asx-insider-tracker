"""Federal government contract ingestion models."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Index, Numeric, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Contract(Base):
    """One AusTender OCDS contract notice release."""

    __tablename__ = "contracts"

    cn_id: Mapped[str] = mapped_column(String, primary_key=True, nullable=False)
    publish_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    last_modified: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    start_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    end_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    value_aud: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True, index=True)
    agency: Mapped[str] = mapped_column(String, nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    category_unspsc: Mapped[str | None] = mapped_column(String, nullable=True)
    procurement_method: Mapped[str | None] = mapped_column(String, nullable=True)
    supplier_abn: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    supplier_name_raw: Mapped[str] = mapped_column(String, nullable=False, index=True)
    is_amendment: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    parent_cn_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    raw_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )

    __table_args__ = (UniqueConstraint("cn_id", name="uq_contracts_cn_id"),)


class ContractSupplier(Base):
    """Supplier registry for later ABN to ASX ticker mapping."""

    __tablename__ = "contract_suppliers"

    supplier_abn: Mapped[str] = mapped_column(String(20), primary_key=True, nullable=False)
    supplier_name: Mapped[str] = mapped_column(String, nullable=False)
    parent_ticker: Mapped[str | None] = mapped_column(String(10), nullable=True, index=True)
    confidence: Mapped[str] = mapped_column(String, nullable=False, default="unmapped")
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow, onupdate=_utcnow
    )


class ContractAlert(Base):
    """Generated contract signal alert placeholder for later sessions."""

    __tablename__ = "contract_alerts"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), primary_key=True, default=lambda: str(uuid4())
    )
    cn_id: Mapped[str] = mapped_column(String, ForeignKey("contracts.cn_id"), nullable=False, index=True)
    ticker: Mapped[str | None] = mapped_column(String(10), nullable=True, index=True)
    supplier_abn: Mapped[str] = mapped_column(String(20), nullable=False)
    supplier_name: Mapped[str] = mapped_column(String, nullable=False)
    alert_type: Mapped[str] = mapped_column(String, nullable=False)
    priority: Mapped[str] = mapped_column(String, nullable=False)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    contract_value_aud: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    is_defence_contract: Mapped[bool] = mapped_column(Boolean, nullable=False)
    defence_reason: Mapped[str | None] = mapped_column(String, nullable=True)
    notified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    __table_args__ = (
        Index("uq_contract_alerts_cn_id_alert_type", "cn_id", "alert_type", unique=True),
    )
