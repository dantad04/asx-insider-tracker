"""Add portfolio_positions table for Smart Money Portfolio tracking.

Revision ID: 004
Revises: 003
Create Date: 2026-03-15 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "004"
down_revision: Union[str, Sequence[str], None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "portfolio_positions",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("company_name", sa.String(255), nullable=False),
        # Signal
        sa.Column("signal_date", sa.Date(), nullable=False),
        sa.Column("signal_directors", sa.Text(), nullable=False),
        # Entry
        sa.Column("entry_date", sa.Date(), nullable=False),
        sa.Column("entry_price", sa.Numeric(12, 4), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("cost_basis", sa.Numeric(12, 2), nullable=False),
        # Exit
        sa.Column("exit_date", sa.Date(), nullable=True),
        sa.Column("exit_price", sa.Numeric(12, 4), nullable=True),
        sa.Column("exit_reason", sa.String(20), nullable=True),
        # Results
        sa.Column("status", sa.String(10), nullable=False, server_default="open"),
        sa.Column("pnl_aud", sa.Numeric(12, 2), nullable=True),
        sa.Column("pnl_pct", sa.Numeric(8, 4), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ticker", "signal_date", name="uq_portfolio_ticker_signal"),
    )
    op.create_index("idx_portfolio_ticker", "portfolio_positions", ["ticker"])
    op.create_index("idx_portfolio_status", "portfolio_positions", ["status"])
    op.create_index("idx_portfolio_signal_date", "portfolio_positions", ["signal_date"])


def downgrade() -> None:
    op.drop_table("portfolio_positions")
