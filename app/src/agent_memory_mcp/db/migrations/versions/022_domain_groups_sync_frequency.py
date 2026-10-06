"""domain_groups.sync_frequency_minutes (per-folder sync frequency).

Чаты, которые листенер/импорт добавляют из TG-папки, получают частоту
синхронизации группы. NULL = «по умолчанию 60».

Revision ID: 022
Revises: 021
Create Date: 2026-10-07
"""
from typing import Sequence, Union

from alembic import op

revision: str = "022"
down_revision: Union[str, None] = "021"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE domain_groups ADD COLUMN IF NOT EXISTS sync_frequency_minutes INTEGER;"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE domain_groups DROP COLUMN IF EXISTS sync_frequency_minutes;")
