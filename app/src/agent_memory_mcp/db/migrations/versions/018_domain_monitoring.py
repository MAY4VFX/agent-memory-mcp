"""domains.monitoring — opt-in отдача источника в observe-layer/workload.

Revision ID: 018
Revises: 017
"""
from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "018"
down_revision: Union[str, None] = "017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "domains",
        sa.Column("monitoring", sa.Boolean, server_default="false", nullable=False),
    )


def downgrade() -> None:
    op.drop_column("domains", "monitoring")
