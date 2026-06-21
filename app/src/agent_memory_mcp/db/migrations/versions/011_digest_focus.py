"""Add focus to digest_configs for configurable digest extraction emphasis.

Revision ID: 011
Revises: 010
Create Date: 2026-06-21
"""

from typing import Sequence, Union

from alembic import op

revision: str = "011"
down_revision: Union[str, None] = "010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # focus: optional free-text instruction steering what the digest emphasizes
    # (e.g. "deadlines, decisions, open questions" for a work chat). NULL = the
    # default news-oriented extraction policy.
    op.execute("""
        ALTER TABLE digest_configs
        ADD COLUMN IF NOT EXISTS focus TEXT;
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE digest_configs DROP COLUMN IF EXISTS focus;")
