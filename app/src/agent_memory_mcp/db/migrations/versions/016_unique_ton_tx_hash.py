"""Prevent double-crediting the same TON payment.

A single on-chain TON transfer must be credited at most once. Before this,
credit_transactions.ton_tx_hash had no uniqueness, so races / re-polling could
credit one payment several times. Add a partial UNIQUE index (partial because
ton_tx_hash is NULL for non-TON rows: welcome bonus, admin top-up, spend).

NOTE: if pre-existing duplicate ton_tx_hash rows exist (from the old bug), this
index creation will fail — clean up duplicates manually first, then re-run.

Revision ID: 016
Revises: 015
Create Date: 2026-07-02
"""

from typing import Sequence, Union

from alembic import op

revision: str = "016"
down_revision: Union[str, None] = "015"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_credit_tx_ton_tx_hash "
        "ON credit_transactions (ton_tx_hash) WHERE ton_tx_hash IS NOT NULL;"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_credit_tx_ton_tx_hash;")
