"""Persist ephemeral in-memory state (issue #18 LOW hardening).

Moves OAuth clients/auth-codes, async jobs, and pending TON payments out of
process memory into Postgres so they survive restarts and work across instances:
- oauth_clients      — Dynamic Client Registration records (was oauth._clients)
- oauth_auth_codes   — short-lived authorization codes (was oauth._auth_codes)
- jobs               — async digest/decisions results (was jobs._jobs)
- pending_payments   — in-flight TON top-ups (was a bare asyncio task)

Revision ID: 017
Revises: 016
Create Date: 2026-07-02
"""

from typing import Sequence, Union

from alembic import op

revision: str = "017"
down_revision: Union[str, None] = "016"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS oauth_clients (
            client_id     TEXT PRIMARY KEY,
            client_secret TEXT NOT NULL,
            redirect_uris JSONB NOT NULL DEFAULT '[]'::jsonb,
            client_name   TEXT,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS oauth_auth_codes (
            code                  TEXT PRIMARY KEY,
            api_key               TEXT NOT NULL,
            expires_at            TIMESTAMPTZ NOT NULL,
            code_challenge        TEXT,
            code_challenge_method TEXT,
            created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS jobs (
            job_id       TEXT PRIMARY KEY,
            owner_id     BIGINT NOT NULL DEFAULT 0,
            status       TEXT NOT NULL DEFAULT 'running',
            result       JSONB,
            error        TEXT,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            completed_at TIMESTAMPTZ
        );

        CREATE TABLE IF NOT EXISTS pending_payments (
            payment_id  TEXT PRIMARY KEY,
            api_key_id  UUID NOT NULL,
            chat_id     BIGINT NOT NULL,
            amount_ton  DOUBLE PRECISION NOT NULL,
            status      TEXT NOT NULL DEFAULT 'pending',
            tx_hash     TEXT,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            resolved_at TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS idx_pending_payments_status
            ON pending_payments (status) WHERE status = 'pending';
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS pending_payments;
        DROP TABLE IF EXISTS jobs;
        DROP TABLE IF EXISTS oauth_auth_codes;
        DROP TABLE IF EXISTS oauth_clients;
        """
    )
