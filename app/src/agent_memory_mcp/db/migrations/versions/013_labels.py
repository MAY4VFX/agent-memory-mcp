"""Abstract label entity + chat→label attribution (cross-source workload).

Revision ID: 013
Revises: 012
Create Date: 2026-06-24
"""

from typing import Sequence, Union

from alembic import op

revision: str = "013"
down_revision: Union[str, None] = "012"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # labels: generic cross-cutting tag. `type` is open-ended (project, task,
    # client, topic, …); project/task are the first consumers. Keeps the public
    # product free of a hard-coded project taxonomy.
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS labels (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            owner_id    BIGINT NOT NULL REFERENCES users(telegram_id),
            type        VARCHAR(32) NOT NULL,
            name        VARCHAR(255) NOT NULL,
            aliases     JSONB,
            created_at  TIMESTAMPTZ DEFAULT now(),
            UNIQUE (owner_id, type, name)
        );
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_labels_owner_type ON labels (owner_id, type);"
    )

    # domain_labels: chat → label attribution (the reliable "chat = project"
    # signal). Message/thread-level attribution comes later.
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS domain_labels (
            domain_id   UUID NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
            label_id    UUID NOT NULL REFERENCES labels(id) ON DELETE CASCADE,
            confidence  DOUBLE PRECISION,
            source      VARCHAR(16),
            created_at  TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (domain_id, label_id)
        );
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS domain_labels;")
    op.execute("DROP TABLE IF EXISTS labels;")
