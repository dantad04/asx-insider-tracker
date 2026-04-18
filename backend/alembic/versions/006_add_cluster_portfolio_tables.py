"""Add Cluster Portfolio paper-trading tables.

Revision ID: 006
Revises: 005
Create Date: 2026-04-18 00:00:00.000000

Notes:
  - Durable cluster identity for this portfolio is
    (portfolio_id, ticker, cluster_start_date, cluster_end_date).
  - entry_cluster_id is a nullable snapshot reference for debugging only.
    It is not authoritative and is never used for dedupe/business logic
    because the current cluster detector rewrites cluster rows by ticker/window.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "006"
down_revision: Union[str, Sequence[str], None] = "005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "cluster_portfolios",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("strategy_key", sa.String(100), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("starting_cash", sa.Numeric(12, 2), nullable=False),
        sa.Column("current_cash", sa.Numeric(12, 2), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("strategy_key", name="uq_cluster_portfolios_strategy_key"),
    )
    op.create_index("idx_cluster_portfolios_active", "cluster_portfolios", ["is_active"])

    op.create_table(
        "cluster_portfolio_positions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("portfolio_id", sa.Integer(), nullable=False),
        sa.Column("entry_cluster_id", sa.Integer(), nullable=True),
        sa.Column("cluster_start_date", sa.Date(), nullable=False),
        sa.Column("cluster_end_date", sa.Date(), nullable=False),
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("sector", sa.String(100), nullable=True),
        sa.Column("industry", sa.String(100), nullable=True),
        sa.Column("source_status", sa.String(20), nullable=False),
        sa.Column("buy_date", sa.Date(), nullable=False),
        sa.Column("planned_exit_date", sa.Date(), nullable=False),
        sa.Column("sell_date", sa.Date(), nullable=True),
        sa.Column("entry_price", sa.Numeric(12, 4), nullable=False),
        sa.Column("entry_price_date", sa.Date(), nullable=False),
        sa.Column("exit_price", sa.Numeric(12, 4), nullable=True),
        sa.Column("exit_price_date", sa.Date(), nullable=True),
        sa.Column("quantity", sa.Numeric(18, 6), nullable=False),
        sa.Column("allocated_aud", sa.Numeric(12, 2), nullable=False),
        sa.Column("status", sa.String(10), nullable=False, server_default="open"),
        sa.Column("buy_reason", sa.Text(), nullable=False),
        sa.Column("sell_reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(
            ["portfolio_id"],
            ["cluster_portfolios.id"],
            name="fk_cluster_portfolio_positions_portfolio",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "portfolio_id",
            "ticker",
            "cluster_start_date",
            "cluster_end_date",
            name="uq_cluster_portfolio_position_key",
        ),
    )
    op.create_index(
        "idx_cluster_portfolio_positions_portfolio",
        "cluster_portfolio_positions",
        ["portfolio_id"],
    )
    op.create_index(
        "idx_cluster_portfolio_positions_entry_cluster_id",
        "cluster_portfolio_positions",
        ["entry_cluster_id"],
    )
    op.create_index(
        "idx_cluster_portfolio_positions_status",
        "cluster_portfolio_positions",
        ["status"],
    )
    op.create_index(
        "idx_cluster_portfolio_positions_buy_date",
        "cluster_portfolio_positions",
        ["buy_date"],
    )
    op.create_index(
        "idx_cluster_portfolio_positions_sell_date",
        "cluster_portfolio_positions",
        ["sell_date"],
    )
    op.create_index(
        "uq_cluster_portfolio_open_ticker",
        "cluster_portfolio_positions",
        ["portfolio_id", "ticker"],
        unique=True,
        postgresql_where=sa.text("status = 'open'"),
    )

    op.create_table(
        "cluster_portfolio_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("portfolio_id", sa.Integer(), nullable=False),
        sa.Column("position_id", sa.Integer(), nullable=True),
        sa.Column("event_type", sa.String(40), nullable=False),
        sa.Column("event_time", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(
            ["portfolio_id"],
            ["cluster_portfolios.id"],
            name="fk_cluster_portfolio_events_portfolio",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["position_id"],
            ["cluster_portfolio_positions.id"],
            name="fk_cluster_portfolio_events_position",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_cluster_portfolio_events_portfolio",
        "cluster_portfolio_events",
        ["portfolio_id"],
    )
    op.create_index(
        "idx_cluster_portfolio_events_position",
        "cluster_portfolio_events",
        ["position_id"],
    )
    op.create_index(
        "idx_cluster_portfolio_events_time",
        "cluster_portfolio_events",
        ["event_time"],
    )


def downgrade() -> None:
    op.drop_index("idx_cluster_portfolio_events_time", table_name="cluster_portfolio_events")
    op.drop_index("idx_cluster_portfolio_events_position", table_name="cluster_portfolio_events")
    op.drop_index("idx_cluster_portfolio_events_portfolio", table_name="cluster_portfolio_events")
    op.drop_table("cluster_portfolio_events")

    op.drop_index("uq_cluster_portfolio_open_ticker", table_name="cluster_portfolio_positions")
    op.drop_index("idx_cluster_portfolio_positions_sell_date", table_name="cluster_portfolio_positions")
    op.drop_index("idx_cluster_portfolio_positions_buy_date", table_name="cluster_portfolio_positions")
    op.drop_index("idx_cluster_portfolio_positions_status", table_name="cluster_portfolio_positions")
    op.drop_index("idx_cluster_portfolio_positions_entry_cluster_id", table_name="cluster_portfolio_positions")
    op.drop_index("idx_cluster_portfolio_positions_portfolio", table_name="cluster_portfolio_positions")
    op.drop_table("cluster_portfolio_positions")

    op.drop_index("idx_cluster_portfolios_active", table_name="cluster_portfolios")
    op.drop_table("cluster_portfolios")
