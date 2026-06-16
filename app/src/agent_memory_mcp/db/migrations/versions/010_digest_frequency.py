"""Add frequency_hours to digest_configs for configurable digest cadence.

Revision ID: 010
Revises: 009
Create Date: 2026-06-16
"""

from typing import Sequence, Union

from alembic import op

revision: str = "010"
down_revision: Union[str, None] = "009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # frequency_hours: how often the digest is sent.
    # 6/12 = several times a day, 24 = daily, 48 = every 2 days, 168 = weekly.
    op.execute("""
        ALTER TABLE digest_configs
        ADD COLUMN IF NOT EXISTS frequency_hours INTEGER NOT NULL DEFAULT 24;
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE digest_configs DROP COLUMN IF EXISTS frequency_hours;")
