"""Add domains.peer_type so basic groups (PeerChat) can be ingested too.

Revision ID: 015
Revises: 014
Create Date: 2026-06-24
"""

from typing import Sequence, Union

from alembic import op

revision: str = "015"
down_revision: Union[str, None] = "014"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # "channel" (channel/supergroup → PeerChannel, the existing default) or
    # "chat" (basic group → PeerChat). Existing rows are all channels.
    op.execute(
        "ALTER TABLE domains ADD COLUMN IF NOT EXISTS peer_type VARCHAR(16) DEFAULT 'channel';"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE domains DROP COLUMN IF EXISTS peer_type;")
