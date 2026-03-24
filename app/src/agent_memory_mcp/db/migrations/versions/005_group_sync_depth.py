"""Add sync_depth to domain_groups, pinned flag to domains.

Revision ID: 005
Revises: 004
Create Date: 2026-02-15
"""

from typing import Sequence, Union

from alembic import op

revision: str = "005"
down_revision: Union[str, None] = "004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE domain_groups
        ADD COLUMN IF NOT EXISTS sync_depth VARCHAR(8);
    """)
    op.execute("""
        ALTER TABLE domains
        ADD COLUMN IF NOT EXISTS pinned BOOLEAN DEFAULT false;
    """)
    # Mark domains not in any group as pinned (they were added individually)
    op.execute("""
        UPDATE domains SET pinned = true
        WHERE id NOT IN (SELECT domain_id FROM domain_group_members);
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE domain_groups DROP COLUMN IF EXISTS sync_depth;")
    op.execute("ALTER TABLE domains DROP COLUMN IF EXISTS pinned;")
