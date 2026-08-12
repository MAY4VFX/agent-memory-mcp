"""messages.fwd_from_* — preserve forward attribution (issue #24).

A forwarded message was losing its original author on the raw_json -> Processed
conversion — the agent then credited whoever forwarded it. Adds flat columns
for the original author/channel instead of relying on raw_json (which was
never actually populated on prod — see issue #24 investigation):
- fwd_from_id       — original sender/channel telegram id
- fwd_from_name     — original sender/channel display name (or from_name for
                      hidden-account forwards)
- fwd_from_username — original sender/channel @username, when resolvable
                      without extra Telegram API calls
- fwd_date          — original message's publish date

Revision ID: 019
Revises: 018
Create Date: 2026-08-12
"""
from typing import Sequence, Union

from alembic import op

revision: str = "019"
down_revision: Union[str, None] = "018"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS fwd_from_id BIGINT;")
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS fwd_from_name VARCHAR(255);")
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS fwd_from_username VARCHAR(255);")
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS fwd_date TIMESTAMPTZ;")


def downgrade() -> None:
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS fwd_date;")
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS fwd_from_username;")
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS fwd_from_name;")
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS fwd_from_id;")
