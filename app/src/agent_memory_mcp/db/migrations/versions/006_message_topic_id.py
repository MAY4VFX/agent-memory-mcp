"""Add topic_id to messages for forum topic links.

Revision ID: 006
Revises: 005
Create Date: 2026-02-15
"""

from typing import Sequence, Union

from alembic import op

revision: str = "006"
down_revision: Union[str, None] = "005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE messages ADD COLUMN IF NOT EXISTS topic_id BIGINT;
    """)
    # Backfill from raw_json for existing messages
    op.execute("""
        UPDATE messages SET topic_id = (raw_json->'reply_to'->>'reply_to_top_id')::bigint
        WHERE raw_json IS NOT NULL
          AND raw_json->'reply_to'->>'reply_to_top_id' IS NOT NULL
          AND topic_id IS NULL;
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS topic_id;")
