"""Add pending_3y_parses table for tracking PDF parsing workflow.

Revision ID: 002
Revises: 001
Create Date: 2024-02-23 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "002"
down_revision: Union[str, Sequence[str], None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create pending_3y_parses table."""

    op.create_table(
        "pending_3y_parses",
        sa.Column("id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("pdf_path", sa.String(500), nullable=False),
        sa.Column("pdf_url", sa.String(500), nullable=False),
        sa.Column("document_date", sa.Date(), nullable=False),
        sa.Column("announcement_header", sa.String(500), nullable=False),
        sa.Column("status", sa.String(50), nullable=False, server_default="pending"),
        sa.Column("parse_attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(), nullable=False, server_default=sa.func.now()
        ),
        sa.Column(
            "updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now()
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("pdf_path", name="uq_pdf_path"),
        sa.UniqueConstraint("pdf_url", name="uq_pdf_url"),
    )

    # Create indexes with globally unique names
    op.create_index(
        "idx_pending_3y_parses_ticker", "pending_3y_parses", ["ticker"]
    )
    op.create_index(
        "idx_pending_3y_parses_status", "pending_3y_parses", ["status"]
    )
    op.create_index(
        "idx_pending_3y_parses_document_date", "pending_3y_parses", ["document_date"]
    )


def downgrade() -> None:
    """Drop pending_3y_parses table."""
    op.drop_table("pending_3y_parses")
