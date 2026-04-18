"""Add cluster tables for director-trade cluster analysis.

Revision ID: 005
Revises: 004
Create Date: 2026-04-16 00:00:00.000000

Three tables:
  clusters          - one row per detected multi-director cluster
  cluster_returns   - 90d/180d x trade-date/disclosure-date return variants
  cluster_trades    - junction: which Trade rows made up each cluster

FK trade_id -> trades.id is UUID (trades.id is postgresql.UUID(as_uuid=False)).
ON DELETE CASCADE on cluster_returns.cluster_id and cluster_trades.cluster_id
so deleting a cluster wipes its returns and trade links.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "005"
down_revision: Union[str, Sequence[str], None] = "004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "clusters",
        sa.Column("cluster_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("sector", sa.String(100), nullable=True),
        sa.Column("industry", sa.String(100), nullable=True),
        sa.Column("market_cap_bucket", sa.String(20), nullable=True),
        sa.Column("market_cap_aud", sa.Numeric(20, 2), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("max_disclosure_date", sa.Date(), nullable=True),
        sa.Column("director_count", sa.Integer(), nullable=False),
        sa.Column("total_value_aud", sa.Numeric(20, 2), nullable=False),
        sa.Column("pct_market_cap", sa.Numeric(12, 6), nullable=True),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("cluster_id"),
        sa.UniqueConstraint("ticker", "start_date", "end_date", name="uq_clusters_ticker_window"),
    )
    op.create_index("idx_clusters_ticker", "clusters", ["ticker"])
    op.create_index("idx_clusters_status", "clusters", ["status"])
    op.create_index("idx_clusters_start", "clusters", ["start_date"])

    op.create_table(
        "cluster_returns",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("horizon_days", sa.Integer(), nullable=False),
        sa.Column("entry_basis", sa.String(20), nullable=False),
        sa.Column("absolute_return", sa.Numeric(12, 6), nullable=True),
        sa.Column("alpha_vs_asx200", sa.Numeric(12, 6), nullable=True),
        sa.Column("alpha_vs_sector", sa.Numeric(12, 6), nullable=True),
        sa.Column("computed_at", sa.DateTime(), nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["cluster_id"], ["clusters.cluster_id"],
            name="fk_cluster_returns_cluster",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "cluster_id", "horizon_days", "entry_basis",
            name="uq_cluster_returns_variant",
        ),
    )
    op.create_index("idx_cluster_returns_cluster", "cluster_returns", ["cluster_id"])

    op.create_table(
        "cluster_trades",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("cluster_id", sa.Integer(), nullable=False),
        sa.Column("trade_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["cluster_id"], ["clusters.cluster_id"],
            name="fk_cluster_trades_cluster",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["trade_id"], ["trades.id"],
            name="fk_cluster_trades_trade",
        ),
        sa.UniqueConstraint("cluster_id", "trade_id", name="uq_cluster_trades_pair"),
    )
    op.create_index("idx_cluster_trades_cluster", "cluster_trades", ["cluster_id"])
    op.create_index("idx_cluster_trades_trade", "cluster_trades", ["trade_id"])


def downgrade() -> None:
    op.drop_index("idx_cluster_trades_trade", table_name="cluster_trades")
    op.drop_index("idx_cluster_trades_cluster", table_name="cluster_trades")
    op.drop_table("cluster_trades")

    op.drop_index("idx_cluster_returns_cluster", table_name="cluster_returns")
    op.drop_table("cluster_returns")

    op.drop_index("idx_clusters_start", table_name="clusters")
    op.drop_index("idx_clusters_status", table_name="clusters")
    op.drop_index("idx_clusters_ticker", table_name="clusters")
    op.drop_table("clusters")
