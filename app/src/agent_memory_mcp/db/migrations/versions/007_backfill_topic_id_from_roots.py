"""Backfill topic_id for forum messages that reply directly to topic root.

Telethon sets reply_to_top_id only for nested replies, not for direct
replies to the topic root. For those, reply_to_msg_id IS the topic root.
We identify topic roots from messages that already have topic_id set,
then propagate to siblings that reply to the same root.

Revision ID: 007
Revises: 006
Create Date: 2026-02-15
"""

from typing import Sequence, Union

from alembic import op

revision: str = "007"
down_revision: Union[str, None] = "006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # For each domain, find known topic roots (distinct topic_id values)
    # from messages that already have topic_id set.
    # Then update messages where topic_id IS NULL but reply_to_msg_id
    # matches a known topic root in the same domain.
    op.execute("""
        UPDATE messages m
        SET topic_id = m.reply_to_msg_id
        FROM (
            SELECT DISTINCT domain_id, topic_id AS root_msg_id
            FROM messages
            WHERE topic_id IS NOT NULL
        ) roots
        WHERE m.domain_id = roots.domain_id
          AND m.reply_to_msg_id = roots.root_msg_id
          AND m.topic_id IS NULL;
    """)


def downgrade() -> None:
    pass
