"""Add messages.read_at — when the owner read an inbound message (live).

Revision ID: 014
Revises: 013
Create Date: 2026-06-24
"""

from typing import Sequence, Union

from alembic import op

revision: str = "014"
down_revision: Union[str, None] = "013"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # read_at: timestamp the owner read this inbound message, captured live by
    # the read-listener (UpdateReadHistoryInbox / UpdateReadChannelInbox). NULL
    # for outbound messages and for anything read before the listener existed —
    # the workload resolver falls back to publish time / word-count there.
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS read_at TIMESTAMPTZ;")


def downgrade() -> None:
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS read_at;")
