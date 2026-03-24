"""Domain groups and digest tables.

Revision ID: 004
Revises: 003
Create Date: 2026-02-15
"""

from typing import Sequence, Union

from alembic import op

revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- Domain groups ---
    op.execute("""
        CREATE TABLE domain_groups (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            owner_id    BIGINT NOT NULL REFERENCES users(telegram_id),
            name        VARCHAR(255) NOT NULL,
            emoji       VARCHAR(8) DEFAULT '📁',
            tg_folder_id INTEGER,
            is_active   BOOLEAN DEFAULT true,
            created_at  TIMESTAMPTZ DEFAULT now(),
            UNIQUE(owner_id, name)
        );
    """)

    op.execute("""
        CREATE TABLE domain_group_members (
            group_id    UUID NOT NULL REFERENCES domain_groups(id) ON DELETE CASCADE,
            domain_id   UUID NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
            added_at    TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (group_id, domain_id)
        );
    """)

    # --- Digest ---
    op.execute("""
        CREATE TABLE digest_configs (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id         BIGINT NOT NULL REFERENCES users(telegram_id),
            name            VARCHAR(255) DEFAULT 'Daily Digest',
            scope_type      VARCHAR(16) DEFAULT 'all',
            scope_id        UUID,
            send_hour_utc   INTEGER DEFAULT 8,
            is_active       BOOLEAN DEFAULT true,
            last_sent_at    TIMESTAMPTZ,
            created_at      TIMESTAMPTZ DEFAULT now(),
            UNIQUE(user_id, name)
        );
    """)

    op.execute("""
        CREATE TABLE digest_runs (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            config_id       UUID NOT NULL REFERENCES digest_configs(id) ON DELETE CASCADE,
            user_id         BIGINT,
            status          VARCHAR(16) DEFAULT 'pending',
            domain_count    INTEGER,
            message_count   INTEGER,
            digest_text     TEXT,
            langfuse_trace_id VARCHAR(128),
            error_message   TEXT,
            started_at      TIMESTAMPTZ,
            completed_at    TIMESTAMPTZ,
            created_at      TIMESTAMPTZ DEFAULT now()
        );
    """)

    # --- ALTER existing tables ---
    op.execute("""
        ALTER TABLE users
            ADD COLUMN IF NOT EXISTS active_scope_type VARCHAR(16) DEFAULT 'domain',
            ADD COLUMN IF NOT EXISTS active_group_id UUID REFERENCES domain_groups(id) ON DELETE SET NULL;
    """)

    op.execute("""
        ALTER TABLE conversations
            ADD COLUMN IF NOT EXISTS scope_type VARCHAR(16) DEFAULT 'domain',
            ADD COLUMN IF NOT EXISTS group_id UUID REFERENCES domain_groups(id) ON DELETE SET NULL;
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE conversations DROP COLUMN IF EXISTS group_id;")
    op.execute("ALTER TABLE conversations DROP COLUMN IF EXISTS scope_type;")
    op.execute("ALTER TABLE users DROP COLUMN IF EXISTS active_group_id;")
    op.execute("ALTER TABLE users DROP COLUMN IF EXISTS active_scope_type;")
    op.execute("DROP TABLE IF EXISTS digest_runs CASCADE;")
    op.execute("DROP TABLE IF EXISTS digest_configs CASCADE;")
    op.execute("DROP TABLE IF EXISTS domain_group_members CASCADE;")
    op.execute("DROP TABLE IF EXISTS domain_groups CASCADE;")
