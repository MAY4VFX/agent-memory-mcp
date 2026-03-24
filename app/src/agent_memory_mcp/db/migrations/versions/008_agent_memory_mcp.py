"""Agent Memory MCP: api_keys, credit_transactions, ton_wallets, decision_items, telegram_sessions.

Revision ID: 008
Revises: 007
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "008"
down_revision = "007"


def upgrade() -> None:
    # --- API Keys ---
    op.create_table(
        "api_keys",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("key_hash", sa.String(64), unique=True, nullable=False),
        sa.Column("key_prefix", sa.String(16), nullable=False),
        sa.Column("telegram_id", sa.BigInteger, sa.ForeignKey("users.telegram_id"), nullable=False),
        sa.Column("name", sa.String(128), server_default="default"),
        sa.Column("credits_balance", sa.Integer, server_default="0", nullable=False),
        sa.Column("total_credits_used", sa.Integer, server_default="0", nullable=False),
        sa.Column("is_active", sa.Boolean, server_default="true", nullable=False),
        sa.Column("rate_limit_rpm", sa.Integer, server_default="60"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("idx_api_keys_hash", "api_keys", ["key_hash"])
    op.create_index("idx_api_keys_telegram_id", "api_keys", ["telegram_id"])

    # --- Credit Transactions ---
    op.create_table(
        "credit_transactions",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("api_key_id", UUID(as_uuid=True), sa.ForeignKey("api_keys.id"), nullable=False),
        sa.Column("amount", sa.Integer, nullable=False),
        sa.Column("type", sa.String(16), nullable=False),  # topup | usage | bonus | refund
        sa.Column("endpoint", sa.String(128), nullable=True),
        sa.Column("ton_tx_hash", sa.String(128), nullable=True),
        sa.Column("balance_after", sa.Integer, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("idx_credits_key_date", "credit_transactions", ["api_key_id", "created_at"])

    # --- TON Wallets ---
    op.create_table(
        "ton_wallets",
        sa.Column("telegram_id", sa.BigInteger, sa.ForeignKey("users.telegram_id"), nullable=False),
        sa.Column("wallet_address", sa.String(128), nullable=False),
        sa.Column("connected_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("telegram_id", "wallet_address"),
    )

    # --- Decision Items ---
    op.create_table(
        "decision_items",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("domain_id", UUID(as_uuid=True), sa.ForeignKey("domains.id"), nullable=False),
        sa.Column("item_type", sa.String(16), nullable=False),  # decision | action_item | open_question
        sa.Column("content", sa.Text, nullable=False),
        sa.Column("topic", sa.String(255), nullable=True),
        sa.Column("source_message_ids", sa.JSON, nullable=True),
        sa.Column("confidence", sa.Float, nullable=True),
        sa.Column("extracted_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
    )
    op.create_index("idx_decisions_domain_type", "decision_items", ["domain_id", "item_type"])

    # --- Telegram Sessions (multi-user Telethon) ---
    op.create_table(
        "telegram_sessions",
        sa.Column("telegram_id", sa.BigInteger, sa.ForeignKey("users.telegram_id"), primary_key=True),
        sa.Column("session_data", sa.LargeBinary, nullable=False),  # AES-encrypted StringSession
        sa.Column("phone_hash", sa.String(64), nullable=True),
        sa.Column("is_active", sa.Boolean, server_default="true", nullable=False),
        sa.Column("connected_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("telegram_sessions")
    op.drop_table("decision_items")
    op.drop_table("ton_wallets")
    op.drop_table("credit_transactions")
    op.drop_table("api_keys")
