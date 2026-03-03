"""Add source field to trades table

Revision ID: 003
Revises: 002
Create Date: 2026-03-03

Adds a 'source' column to distinguish:
  - seed_json: historical data imported from JSON (dateReadable is unreliable as date_lodged)
  - pdf_parser: verified data from 3Y PDF scraper (date_lodged is the real ASX filing date)

Only pdf_parser trades should be used for compliance monitoring.
"""
from alembic import op
import sqlalchemy as sa

revision = '003'
down_revision = '002'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add source column, defaulting all existing records to 'seed_json'
    op.add_column(
        'trades',
        sa.Column('source', sa.String(20), nullable=False, server_default='seed_json')
    )


def downgrade() -> None:
    op.drop_column('trades', 'source')
