"""Initial schema.

Revision ID: 001
Revises: None
Create Date: 2025-08-22
"""

from typing import Sequence, Union

from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto";')

    op.execute("""
        CREATE TABLE users (
            telegram_id   BIGINT PRIMARY KEY,
            username      VARCHAR(255),
            language      VARCHAR(8) DEFAULT 'ru',
            detail_level  VARCHAR(16) DEFAULT 'normal',
            active_domain_id UUID,
            created_at    TIMESTAMPTZ DEFAULT now()
        );
    """)

    op.execute("""
        CREATE TABLE domains (
            id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            owner_id      BIGINT NOT NULL REFERENCES users(telegram_id),
            channel_id    BIGINT NOT NULL,
            channel_username VARCHAR(128),
            channel_name  VARCHAR(255),
            emoji         VARCHAR(8) DEFAULT '\U0001f4da',
            display_name  VARCHAR(255),
            domain_type   VARCHAR(64),
            sync_depth    VARCHAR(16) NOT NULL,
            sync_frequency_minutes INTEGER NOT NULL,
            sync_from_date TIMESTAMPTZ,
            last_synced_at TIMESTAMPTZ,
            last_synced_message_id BIGINT DEFAULT 0,
            next_sync_at  TIMESTAMPTZ,
            message_count INTEGER DEFAULT 0,
            entity_count  INTEGER DEFAULT 0,
            relation_count INTEGER DEFAULT 0,
            is_active     BOOLEAN DEFAULT true,
            created_at    TIMESTAMPTZ DEFAULT now(),
            UNIQUE(owner_id, channel_id)
        );
    """)

    op.execute("""
        ALTER TABLE users ADD CONSTRAINT fk_active_domain
            FOREIGN KEY (active_domain_id) REFERENCES domains(id) ON DELETE SET NULL;
    """)

    op.execute("""
        CREATE TABLE channel_schemas (
            id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            domain_id     UUID NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
            version       INTEGER NOT NULL DEFAULT 1,
            schema_json   JSONB NOT NULL,
            detected_domain VARCHAR(64),
            entity_types  JSONB,
            relation_types JSONB,
            is_active     BOOLEAN DEFAULT true,
            langfuse_trace_id VARCHAR(128),
            created_at    TIMESTAMPTZ DEFAULT now(),
            UNIQUE(domain_id, version)
        );
    """)

    op.execute("""
        CREATE TABLE messages (
            id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            domain_id     UUID NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
            telegram_msg_id BIGINT NOT NULL,
            reply_to_msg_id BIGINT,
            thread_id     UUID,
            sender_id     BIGINT,
            sender_name   VARCHAR(255),
            content       TEXT,
            content_type  VARCHAR(32) DEFAULT 'text',
            raw_json      JSONB,
            language      VARCHAR(8),
            hashtags      JSONB,
            is_noise      BOOLEAN DEFAULT false,
            msg_date      TIMESTAMPTZ NOT NULL,
            created_at    TIMESTAMPTZ DEFAULT now(),
            UNIQUE(domain_id, telegram_msg_id)
        );
    """)

    op.execute("""
        CREATE INDEX idx_messages_domain_date
            ON messages(domain_id, msg_date DESC);
    """)

    op.execute("""
        CREATE INDEX idx_messages_reply
            ON messages(domain_id, reply_to_msg_id)
            WHERE reply_to_msg_id IS NOT NULL;
    """)

    op.execute("""
        CREATE TABLE threads (
            id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            domain_id     UUID NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
            root_message_id UUID REFERENCES messages(id),
            msg_count     INTEGER DEFAULT 1,
            combined_text TEXT,
            first_msg_date TIMESTAMPTZ,
            last_msg_date TIMESTAMPTZ,
            created_at    TIMESTAMPTZ DEFAULT now()
        );
    """)

    op.execute("""
        CREATE TABLE sync_jobs (
            id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            domain_id     UUID NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
            job_type      VARCHAR(16) NOT NULL,
            status        VARCHAR(16) DEFAULT 'pending',
            messages_fetched INTEGER DEFAULT 0,
            messages_filtered INTEGER DEFAULT 0,
            messages_processed INTEGER DEFAULT 0,
            messages_total INTEGER,
            entities_extracted INTEGER DEFAULT 0,
            error_message TEXT,
            langfuse_trace_id VARCHAR(128),
            started_at    TIMESTAMPTZ,
            completed_at  TIMESTAMPTZ,
            created_at    TIMESTAMPTZ DEFAULT now()
        );
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS sync_jobs CASCADE;")
    op.execute("DROP TABLE IF EXISTS threads CASCADE;")
    op.execute("DROP TABLE IF EXISTS messages CASCADE;")
    op.execute("DROP TABLE IF EXISTS channel_schemas CASCADE;")
    op.execute("ALTER TABLE users DROP CONSTRAINT IF EXISTS fk_active_domain;")
    op.execute("DROP TABLE IF EXISTS domains CASCADE;")
    op.execute("DROP TABLE IF EXISTS users CASCADE;")
