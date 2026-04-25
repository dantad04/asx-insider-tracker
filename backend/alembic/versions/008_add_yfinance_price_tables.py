"""Add yfinance price and company-info tables.

Revision ID: 008
Revises: 007
Create Date: 2026-04-25 00:00:00.000000

These tables were previously created only by the ad-hoc price fetch script.
Cluster detection and return computation depend on them, so fresh production
databases need them under Alembic management.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "008"
down_revision: Union[str, Sequence[str], None] = "007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    tables = set(inspector.get_table_names())

    if "yf_daily_prices" not in tables:
        op.create_table(
            "yf_daily_prices",
            sa.Column("ticker", sa.String(20), nullable=False),
            sa.Column("date", sa.Date(), nullable=False),
            sa.Column("close", sa.Numeric(12, 4), nullable=False),
            sa.PrimaryKeyConstraint("ticker", "date"),
        )

    index_names = {
        index["name"] for index in inspector.get_indexes("yf_daily_prices")
    } if "yf_daily_prices" in set(sa.inspect(bind).get_table_names()) else set()
    if "idx_yfdp_ticker" not in index_names:
        op.create_index("idx_yfdp_ticker", "yf_daily_prices", ["ticker"])
    if "idx_yfdp_date" not in index_names:
        op.create_index("idx_yfdp_date", "yf_daily_prices", ["date"])

    if "yf_ticker_sectors" not in tables:
        op.create_table(
            "yf_ticker_sectors",
            sa.Column("ticker", sa.String(20), nullable=False),
            sa.Column("sector", sa.String(100), nullable=False),
            sa.PrimaryKeyConstraint("ticker"),
        )

    if "yf_company_info" not in tables:
        op.create_table(
            "yf_company_info",
            sa.Column("ticker", sa.String(20), nullable=False),
            sa.Column("market_cap", sa.BigInteger(), nullable=True),
            sa.Column("industry", sa.String(100), nullable=True),
            sa.Column("fetched_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
            sa.PrimaryKeyConstraint("ticker"),
        )
    else:
        company_columns = {
            column["name"] for column in inspector.get_columns("yf_company_info")
        }
        with op.batch_alter_table("yf_company_info") as batch_op:
            if "market_cap" not in company_columns:
                batch_op.add_column(sa.Column("market_cap", sa.BigInteger(), nullable=True))
            if "industry" not in company_columns:
                batch_op.add_column(sa.Column("industry", sa.String(100), nullable=True))
            if "fetched_at" not in company_columns:
                batch_op.add_column(
                    sa.Column("fetched_at", sa.DateTime(), nullable=False, server_default=sa.func.now())
                )


def downgrade() -> None:
    op.drop_table("yf_company_info")
    op.drop_table("yf_ticker_sectors")
    op.drop_index("idx_yfdp_date", table_name="yf_daily_prices")
    op.drop_index("idx_yfdp_ticker", table_name="yf_daily_prices")
    op.drop_table("yf_daily_prices")
