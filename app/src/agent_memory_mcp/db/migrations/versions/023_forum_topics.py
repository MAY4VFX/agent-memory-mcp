"""forum_topics + domains.forum_topics_sync_* (real forum topic titles).

Названия тем форума супергруппы. Раньше list_topics выводил «имя» из текста
корневого сообщения темы, но корень настоящей темы — служебное сообщение
«тема создана», которое не сохраняется, поэтому имя было NULL, а у остальных
тем «именем» оказывался первый ответ. Теперь настоящий title берётся из
Telegram (channels.GetForumTopicsRequest) и хранится здесь.

- forum_topics: ключ (channel_id, topic_id) — название темы общее для всех
  владельцев, подключивших один и тот же чат (channel_id — «голый» id, как в
  domains.channel_id). updated_at обновляется при переименовании.
- domains.forum_topics_synced_at/status: когда последний раз тянули темы и
  результат (ok | not_applicable | error), чтобы не дёргать API на каждом
  тике синка (троттлинг в scheduler._should_sync_topics).

Бэкфилл отдельным вызовом не нужен: forum_topics_synced_at=NULL у всех
доменов ⇒ первый же инкрементальный синк каждого источника стянет темы.

Revision ID: 023
Revises: 022
Create Date: 2026-10-11
"""
from typing import Sequence, Union

from alembic import op

revision: str = "023"
down_revision: Union[str, None] = "022"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS forum_topics (
            channel_id BIGINT NOT NULL,
            topic_id BIGINT NOT NULL,
            title TEXT NOT NULL,
            is_closed BOOLEAN DEFAULT false,
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (channel_id, topic_id)
        );
    """)
    op.execute(
        "ALTER TABLE domains ADD COLUMN IF NOT EXISTS forum_topics_synced_at TIMESTAMPTZ;"
    )
    op.execute(
        "ALTER TABLE domains ADD COLUMN IF NOT EXISTS forum_topics_sync_status VARCHAR(16);"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE domains DROP COLUMN IF EXISTS forum_topics_sync_status;")
    op.execute("ALTER TABLE domains DROP COLUMN IF EXISTS forum_topics_synced_at;")
    op.execute("DROP TABLE IF EXISTS forum_topics;")
