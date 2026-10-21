"""users.home_thread_id + conversations.tg_thread_id (bot private-chat topics).

В личке бота с включёнными темами нет General: каждое сообщение лежит в
какой-то теме, а сообщение «из списка тем» Telegram сам кладёт в новую тему.
Поэтому меню живёт в отдельной теме «🏠 Menu», которую бот создаёт сам
(users.home_thread_id), а каждая остальная тема — свой диалог с агентом
(conversations.tg_thread_id), а не один общий active_conversation на все.

Revision ID: 024
Revises: 023
Create Date: 2026-10-21
"""
from typing import Sequence, Union

from alembic import op

revision: str = "024"
down_revision: Union[str, None] = "023"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS home_thread_id BIGINT;")
    op.execute("ALTER TABLE conversations ADD COLUMN IF NOT EXISTS tg_thread_id BIGINT;")
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_conversations_user_thread "
        "ON conversations (user_id, tg_thread_id);"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_conversations_user_thread;")
    op.execute("ALTER TABLE conversations DROP COLUMN IF EXISTS tg_thread_id;")
    op.execute("ALTER TABLE users DROP COLUMN IF EXISTS home_thread_id;")
