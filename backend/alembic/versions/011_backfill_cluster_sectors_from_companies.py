"""Backfill cluster sectors from companies.

Revision ID: 011
Revises: 010
Create Date: 2026-05-07 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op

revision: str = "011"
down_revision: Union[str, Sequence[str], None] = "010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE clusters c
        SET sector = co.sector
        FROM companies co
        WHERE c.ticker = co.ticker
          AND c.sector IS NULL
          AND co.sector IS NOT NULL
        """
    )


def downgrade() -> None:
    pass
