"""Conversations, messages, context payloads, feedback.

Revision ID: 002
Revises: 001
Create Date: 2025-08-22
"""

from typing import Sequence, Union

from alembic import op

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE conversations (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id         BIGINT NOT NULL REFERENCES users(telegram_id),
            domain_id       UUID REFERENCES domains(id) ON DELETE SET NULL,
            title           VARCHAR(255),
            is_active       BOOLEAN DEFAULT true,
            message_count   INTEGER DEFAULT 0,
            created_at      TIMESTAMPTZ DEFAULT now(),
            updated_at      TIMESTAMPTZ DEFAULT now()
        );
    """)

    op.execute("""
        CREATE TABLE conversation_messages (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            conversation_id     UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
            role                VARCHAR(16) NOT NULL,
            content             TEXT NOT NULL,
            context_payload_id  UUID,
            langfuse_trace_id   VARCHAR(128),
            token_count         INTEGER DEFAULT 0,
            created_at          TIMESTAMPTZ DEFAULT now()
        );
    """)

    op.execute("""
        CREATE INDEX idx_conv_messages_conv_id
            ON conversation_messages(conversation_id, created_at);
    """)

    op.execute("""
        CREATE TABLE context_payloads (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            conversation_id     UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
            query_text          TEXT NOT NULL,
            payload_json        JSONB NOT NULL,
            token_count         INTEGER DEFAULT 0,
            chunks_count        INTEGER DEFAULT 0,
            graph_entities_count INTEGER DEFAULT 0,
            langfuse_trace_id   VARCHAR(128),
            created_at          TIMESTAMPTZ DEFAULT now()
        );
    """)

    op.execute("""
        CREATE TABLE feedback (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            message_id  UUID NOT NULL REFERENCES conversation_messages(id) ON DELETE CASCADE,
            user_id     BIGINT NOT NULL REFERENCES users(telegram_id),
            score       SMALLINT NOT NULL CHECK (score IN (-1, 1)),
            created_at  TIMESTAMPTZ DEFAULT now(),
            UNIQUE(message_id, user_id)
        );
    """)

    # Add active_conversation_id to users
    op.execute("""
        ALTER TABLE users ADD COLUMN active_conversation_id UUID
            REFERENCES conversations(id) ON DELETE SET NULL;
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE users DROP COLUMN IF EXISTS active_conversation_id;")
    op.execute("DROP TABLE IF EXISTS feedback CASCADE;")
    op.execute("DROP TABLE IF EXISTS context_payloads CASCADE;")
    op.execute("DROP TABLE IF EXISTS conversation_messages CASCADE;")
    op.execute("DROP TABLE IF EXISTS conversations CASCADE;")
