"""Allow user-level credit transactions (bonus/admin top-up) with no api_key.

After migration 009 the canonical balance is users.points_balance, so a
transaction is not necessarily tied to a specific API key. The welcome bonus
(topup_user_direct) inserts a credit_transactions row without api_key_id, which
failed against the NOT NULL constraint — new users silently got no bonus.
Make api_key_id nullable.

Revision ID: 012
Revises: 011
Create Date: 2026-06-21
"""

from typing import Sequence, Union

from alembic import op

revision: str = "012"
down_revision: Union[str, None] = "011"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE credit_transactions ALTER COLUMN api_key_id DROP NOT NULL;")


def downgrade() -> None:
    # Re-adding NOT NULL would fail if any user-level rows exist; left as no-op.
    pass
