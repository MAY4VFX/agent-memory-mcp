"""Move balance from api_keys to users (points_balance).

Revision ID: 009
Revises: 008
"""

from alembic import op
import sqlalchemy as sa

revision = "009"
down_revision = "008"


def upgrade() -> None:
    # Add points_balance to users
    op.add_column("users", sa.Column("points_balance", sa.Integer, server_default="0", nullable=False))
    op.add_column("users", sa.Column("total_points_spent", sa.Integer, server_default="0", nullable=False))

    # Migrate: sum credits_balance from all api_keys per user → users.points_balance
    op.execute("""
        UPDATE users SET points_balance = COALESCE((
            SELECT SUM(credits_balance) FROM api_keys
            WHERE api_keys.telegram_id = users.telegram_id
        ), 0)
    """)
    op.execute("""
        UPDATE users SET total_points_spent = COALESCE((
            SELECT SUM(total_credits_used) FROM api_keys
            WHERE api_keys.telegram_id = users.telegram_id
        ), 0)
    """)

    # Make credit_transactions reference user directly (optional FK)
    op.add_column("credit_transactions",
        sa.Column("telegram_id", sa.BigInteger, sa.ForeignKey("users.telegram_id"), nullable=True))

    # Backfill telegram_id from api_keys
    op.execute("""
        UPDATE credit_transactions SET telegram_id = (
            SELECT telegram_id FROM api_keys WHERE api_keys.id = credit_transactions.api_key_id
        )
    """)


def downgrade() -> None:
    op.drop_column("credit_transactions", "telegram_id")
    op.drop_column("users", "total_points_spent")
    op.drop_column("users", "points_balance")
