"""Align Cluster Portfolio position columns with the approved schema.

Revision ID: 007
Revises: 006
Create Date: 2026-04-18 00:30:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "007"
down_revision: Union[str, Sequence[str], None] = "006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    column_names = {
        column["name"] for column in inspector.get_columns("cluster_portfolio_positions")
    }
    index_names = {
        index["name"] for index in inspector.get_indexes("cluster_portfolio_positions")
    }

    with op.batch_alter_table("cluster_portfolio_positions") as batch_op:
        if "cluster_id" in column_names and "entry_cluster_id" not in column_names:
            batch_op.alter_column(
                "cluster_id",
                new_column_name="entry_cluster_id",
                existing_type=sa.Integer(),
                existing_nullable=False,
                nullable=True,
            )
        elif "entry_cluster_id" in column_names:
            batch_op.alter_column(
                "entry_cluster_id",
                existing_type=sa.Integer(),
                existing_nullable=True,
                nullable=True,
            )

        if "entry_price_date" not in column_names:
            batch_op.add_column(sa.Column("entry_price_date", sa.Date(), nullable=True))
        if "exit_price_date" not in column_names:
            batch_op.add_column(sa.Column("exit_price_date", sa.Date(), nullable=True))

    inspector = sa.inspect(bind)
    refreshed_columns = {
        column["name"] for column in inspector.get_columns("cluster_portfolio_positions")
    }
    if "entry_price_date" in refreshed_columns:
        op.execute(
            """
            UPDATE cluster_portfolio_positions
            SET entry_price_date = buy_date
            WHERE entry_price_date IS NULL
              AND buy_date IS NOT NULL
            """
        )

        with op.batch_alter_table("cluster_portfolio_positions") as batch_op:
            batch_op.alter_column(
                "entry_price_date",
                existing_type=sa.Date(),
                nullable=False,
            )

    if "idx_cluster_portfolio_positions_cluster_id" in index_names:
        op.drop_index(
            "idx_cluster_portfolio_positions_cluster_id",
            table_name="cluster_portfolio_positions",
        )
    if "idx_cluster_portfolio_positions_entry_cluster_id" not in index_names:
        op.create_index(
            "idx_cluster_portfolio_positions_entry_cluster_id",
            "cluster_portfolio_positions",
            ["entry_cluster_id"],
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    column_names = {
        column["name"] for column in inspector.get_columns("cluster_portfolio_positions")
    }
    index_names = {
        index["name"] for index in inspector.get_indexes("cluster_portfolio_positions")
    }

    if "idx_cluster_portfolio_positions_entry_cluster_id" in index_names:
        op.drop_index(
            "idx_cluster_portfolio_positions_entry_cluster_id",
            table_name="cluster_portfolio_positions",
        )
    if "entry_cluster_id" in column_names and "idx_cluster_portfolio_positions_cluster_id" not in index_names:
        op.create_index(
            "idx_cluster_portfolio_positions_cluster_id",
            "cluster_portfolio_positions",
            ["entry_cluster_id"],
        )

    with op.batch_alter_table("cluster_portfolio_positions") as batch_op:
        if "exit_price_date" in column_names:
            batch_op.drop_column("exit_price_date")
        if "entry_price_date" in column_names:
            batch_op.drop_column("entry_price_date")
        if "entry_cluster_id" in column_names and "cluster_id" not in column_names:
            batch_op.alter_column(
                "entry_cluster_id",
                new_column_name="cluster_id",
                existing_type=sa.Integer(),
                existing_nullable=True,
                nullable=False,
            )
