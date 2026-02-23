"""Initial schema creation.

Revision ID: 001
Revises: None
Create Date: 2024-02-17 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create initial database schema."""

    # Create companies table
    op.create_table(
        "companies",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("sector", sa.String(100), nullable=True),
        sa.Column("industry_group", sa.String(100), nullable=True),
        sa.Column("market_cap", sa.Integer(), nullable=True),
        sa.Column("is_asx200", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("is_asx300", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ticker"),
    )
    op.create_index("idx_companies_ticker", "companies", ["ticker"])

    # Create directors table
    op.create_table(
        "directors",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("full_name", sa.String(255), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_full_name", "directors", ["full_name"])

    # Create director_companies junction table
    op.create_table(
        "director_companies",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("director_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("company_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("role", sa.String(100), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("smart_money_score_30d", sa.Float(), nullable=True),
        sa.Column("smart_money_score_60d", sa.Float(), nullable=True),
        sa.Column("smart_money_score_90d", sa.Float(), nullable=True),
        sa.Column("trade_count", sa.Integer(), nullable=False, server_default="0"),
        sa.ForeignKeyConstraint(["company_id"], ["companies.id"]),
        sa.ForeignKeyConstraint(["director_id"], ["directors.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("director_id", "company_id", name="uq_director_company"),
    )

    # Create trades table
    op.create_table(
        "trades",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("director_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("company_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("date_of_trade", sa.Date(), nullable=False),
        sa.Column("date_lodged", sa.Date(), nullable=False),
        sa.Column("trade_type", sa.String(50), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("price_per_share", sa.Numeric(12, 4), nullable=True),
        sa.ForeignKeyConstraint(["company_id"], ["companies.id"]),
        sa.ForeignKeyConstraint(["director_id"], ["directors.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_date_of_trade", "trades", ["date_of_trade"])

    # Create price_snapshots table
    op.create_table(
        "price_snapshots",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("open", sa.Numeric(12, 4), nullable=False),
        sa.Column("high", sa.Numeric(12, 4), nullable=False),
        sa.Column("low", sa.Numeric(12, 4), nullable=False),
        sa.Column("close", sa.Numeric(12, 4), nullable=False),
        sa.Column("volume", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ticker", "date", name="uq_ticker_date"),
    )
    op.create_index("idx_price_snapshots_ticker", "price_snapshots", ["ticker"])
    op.create_index("idx_price_snapshots_date", "price_snapshots", ["date"])

    # Create announcements table
    op.create_table(
        "announcements",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("announcement_date", sa.Date(), nullable=False),
        sa.Column("announcement_type", sa.String(100), nullable=True),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("url", sa.String(500), nullable=True),
        sa.Column("is_price_sensitive", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_announcements_ticker", "announcements", ["ticker"])
    op.create_index("idx_announcements_date", "announcements", ["announcement_date"])


def downgrade() -> None:
    """Drop all tables."""
    op.drop_table("announcements")
    op.drop_table("price_snapshots")
    op.drop_table("trades")
    op.drop_table("director_companies")
    op.drop_table("directors")
    op.drop_table("companies")
