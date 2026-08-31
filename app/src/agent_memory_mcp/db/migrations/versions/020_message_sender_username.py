"""messages.sender_username — @ник автора сообщения (issue #27).

Раньше про автора хранились только sender_id (telegram id) и sender_name
(display name из title/first_name) — по display name человека не найти и не
написать ему. msg.sender.username доступен в том же месте, где уже читается
sender_name (_paginated_fetch), без доп. запросов к Telegram.

Nullable, без бэкфила: у части людей ника просто нет (username=None — норма,
не пустая строка), а бэкфил старых сообщений — отдельная задача (повторный
проход по истории всех источников).

Revision ID: 020
Revises: 019
Create Date: 2026-08-31
"""
from typing import Sequence, Union

from alembic import op

revision: str = "020"
down_revision: Union[str, None] = "019"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS sender_username VARCHAR(255);")


def downgrade() -> None:
    op.execute("ALTER TABLE messages DROP COLUMN IF EXISTS sender_username;")
