"""Add federal contract ingestion tables.

Revision ID: 010
Revises: 009
Create Date: 2026-05-06 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "010"
down_revision: Union[str, Sequence[str], None] = "009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "contracts",
        sa.Column("cn_id", sa.String(), nullable=False),
        sa.Column("publish_date", sa.Date(), nullable=False),
        sa.Column("last_modified", sa.DateTime(timezone=True), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("value_aud", sa.Numeric(18, 2), nullable=True),
        sa.Column("agency", sa.String(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("category_unspsc", sa.String(), nullable=True),
        sa.Column("procurement_method", sa.String(), nullable=True),
        sa.Column("supplier_abn", sa.String(20), nullable=False),
        sa.Column("supplier_name_raw", sa.String(), nullable=False),
        sa.Column("is_amendment", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("parent_cn_id", sa.String(), nullable=True),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("cn_id", name="uq_contracts_cn_id"),
    )
    op.create_index("idx_contracts_publish_date", "contracts", ["publish_date"])
    op.create_index("idx_contracts_last_modified", "contracts", ["last_modified"])
    op.create_index("idx_contracts_value_aud", "contracts", ["value_aud"])
    op.create_index("idx_contracts_agency", "contracts", ["agency"])
    op.create_index("idx_contracts_supplier_abn", "contracts", ["supplier_abn"])
    op.create_index("idx_contracts_supplier_name_raw", "contracts", ["supplier_name_raw"])
    op.create_index("idx_contracts_parent_cn_id", "contracts", ["parent_cn_id"])

    op.create_table(
        "contract_suppliers",
        sa.Column("supplier_abn", sa.String(20), nullable=False),
        sa.Column("supplier_name", sa.String(), nullable=False),
        sa.Column("parent_ticker", sa.String(10), nullable=True),
        sa.Column("confidence", sa.String(), nullable=False, server_default="unmapped"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("supplier_abn"),
    )
    op.create_index("idx_contract_suppliers_parent_ticker", "contract_suppliers", ["parent_ticker"])

    op.create_table(
        "contract_alerts",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("cn_id", sa.String(), nullable=False),
        sa.Column("ticker", sa.String(10), nullable=True),
        sa.Column("supplier_abn", sa.String(20), nullable=False),
        sa.Column("supplier_name", sa.String(), nullable=False),
        sa.Column("alert_type", sa.String(), nullable=False),
        sa.Column("priority", sa.String(), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("contract_value_aud", sa.Numeric(18, 2), nullable=True),
        sa.Column("is_defence_contract", sa.Boolean(), nullable=False),
        sa.Column("defence_reason", sa.String(), nullable=True),
        sa.Column("notified", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.ForeignKeyConstraint(["cn_id"], ["contracts.cn_id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_contract_alerts_cn_id", "contract_alerts", ["cn_id"])
    op.create_index("idx_contract_alerts_ticker", "contract_alerts", ["ticker"])
    op.create_index("idx_contract_alerts_detected_at", "contract_alerts", ["detected_at"])
    op.create_index(
        "uq_contract_alerts_cn_id_alert_type",
        "contract_alerts",
        ["cn_id", "alert_type"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_contract_alerts_cn_id_alert_type", table_name="contract_alerts")
    op.drop_index("idx_contract_alerts_detected_at", table_name="contract_alerts")
    op.drop_index("idx_contract_alerts_ticker", table_name="contract_alerts")
    op.drop_index("idx_contract_alerts_cn_id", table_name="contract_alerts")
    op.drop_table("contract_alerts")

    op.drop_index("idx_contract_suppliers_parent_ticker", table_name="contract_suppliers")
    op.drop_table("contract_suppliers")

    op.drop_index("idx_contracts_parent_cn_id", table_name="contracts")
    op.drop_index("idx_contracts_supplier_name_raw", table_name="contracts")
    op.drop_index("idx_contracts_supplier_abn", table_name="contracts")
    op.drop_index("idx_contracts_agency", table_name="contracts")
    op.drop_index("idx_contracts_value_aud", table_name="contracts")
    op.drop_index("idx_contracts_last_modified", table_name="contracts")
    op.drop_index("idx_contracts_publish_date", table_name="contracts")
    op.drop_table("contracts")
