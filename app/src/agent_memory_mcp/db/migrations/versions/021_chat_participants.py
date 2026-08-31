"""chat_participants + domains.participants_sync_* (issue #28).

Состав чата (все участники, включая молчунов), а не только авторы сообщений.
Отдельная таблица, заполняется через Telethon iter_participants:
- chat_participants: domain_id, user_id, username/first_name/last_name,
  is_bot, is_admin, first_seen/last_seen. username=NULL — норма (ник есть не
  у всех), человек не выбрасывается.
- domains.participants_synced_at/status/error: отметка последней попытки и
  её результат. КРИТИЧНО: "участников нет" (status=ok, 0 строк) и "не
  смогли" (status=forbidden/error, с причиной в participants_sync_error) —
  разные вещи, агент должен получать причину, а не молчаливый пустой список
  (см. инцидент 2026-08-30 из issue #28).

Revision ID: 021
Revises: 020
Create Date: 2026-08-31
"""
from typing import Sequence, Union

from alembic import op

revision: str = "021"
down_revision: Union[str, None] = "020"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE domains ADD COLUMN IF NOT EXISTS participants_synced_at TIMESTAMPTZ;"
    )
    op.execute(
        "ALTER TABLE domains ADD COLUMN IF NOT EXISTS participants_sync_status VARCHAR(16);"
    )
    op.execute(
        "ALTER TABLE domains ADD COLUMN IF NOT EXISTS participants_sync_error TEXT;"
    )

    op.execute("""
        CREATE TABLE IF NOT EXISTS chat_participants (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            domain_id UUID NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
            user_id BIGINT NOT NULL,
            username VARCHAR(255),
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            is_bot BOOLEAN DEFAULT false,
            is_admin BOOLEAN DEFAULT false,
            first_seen TIMESTAMPTZ DEFAULT now(),
            last_seen TIMESTAMPTZ DEFAULT now(),
            UNIQUE (domain_id, user_id)
        );
    """)
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_chat_participants_domain "
        "ON chat_participants (domain_id);"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS chat_participants;")
    op.execute("ALTER TABLE domains DROP COLUMN IF EXISTS participants_sync_error;")
    op.execute("ALTER TABLE domains DROP COLUMN IF EXISTS participants_sync_status;")
    op.execute("ALTER TABLE domains DROP COLUMN IF EXISTS participants_synced_at;")
