"""API key authentication and credit management."""

from __future__ import annotations

import hashlib
import secrets
from typing import Annotated
from uuid import UUID

import structlog
from fastapi import Depends, Header, HTTPException
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_memory_mcp.db.engine import async_engine

log = structlog.get_logger(__name__)

# Point costs per endpoint (1 point ≈ $0.01)
CREDIT_COSTS: dict[str, int] = {
    "search": 3,
    "digest": 25,
    "decisions": 12,
    "agent_context": 15,
    "analysis/deep": 50,
    # Free: add_source, list, status, auth, health
}


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def generate_api_key() -> tuple[str, str, str]:
    """Generate a new API key. Returns (full_key, key_hash, key_prefix)."""
    raw = secrets.token_urlsafe(32)
    full_key = f"amk_{raw}"
    key_hash = _hash_key(full_key)
    key_prefix = full_key[:16]
    return full_key, key_hash, key_prefix


async def get_api_key_by_hash(engine: AsyncEngine, key_hash: str) -> dict | None:
    """Look up an API key by its hash."""
    from sqlalchemy import text
    async with engine.begin() as conn:
        row = await conn.execute(
            text("""
                SELECT id, key_hash, key_prefix, telegram_id, name,
                       credits_balance, total_credits_used, is_active, rate_limit_rpm
                FROM api_keys WHERE key_hash = :h
            """),
            {"h": key_hash},
        )
        r = row.mappings().first()
        return dict(r) if r else None


async def create_api_key_for_user(
    engine: AsyncEngine, telegram_id: int, name: str = "default", bonus_credits: int = 0,
) -> tuple[str, dict]:
    """Create a new API key for a user. Returns (full_key, key_record).

    bonus_credits is added to USER balance (not per-key).
    """
    full_key, key_hash, key_prefix = generate_api_key()
    from sqlalchemy import text
    async with engine.begin() as conn:
        row = await conn.execute(
            text("""
                INSERT INTO api_keys (key_hash, key_prefix, telegram_id, name)
                VALUES (:h, :p, :tid, :n)
                RETURNING id, key_prefix
            """),
            {"h": key_hash, "p": key_prefix, "tid": telegram_id, "n": name},
        )
        rec = dict(row.mappings().first())

    # Add bonus to USER balance (not key)
    if bonus_credits > 0:
        balance = await topup_user_direct(engine, telegram_id, bonus_credits, tx_type="bonus")
        rec["credits_balance"] = balance
    else:
        # Read current user balance
        from sqlalchemy import text as sa_text
        async with engine.begin() as conn:
            r = await conn.execute(sa_text("SELECT points_balance FROM users WHERE telegram_id = :tid"), {"tid": telegram_id})
            rec["credits_balance"] = r.scalar() or 0

    return full_key, rec


async def charge_credits(engine: AsyncEngine, api_key_id: UUID, amount: int, endpoint: str) -> int:
    """Deduct points from USER balance (not per-key). Returns new balance."""
    from sqlalchemy import text
    async with engine.begin() as conn:
        # Get user from api_key
        key_row = await conn.execute(
            text("SELECT telegram_id FROM api_keys WHERE id = :id"),
            {"id": api_key_id},
        )
        tid = key_row.scalar()
        if not tid:
            raise ValueError("API key not found")

        row = await conn.execute(
            text("SELECT points_balance FROM users WHERE telegram_id = :tid FOR UPDATE"),
            {"tid": tid},
        )
        balance = row.scalar()
        if balance is None or balance < amount:
            raise ValueError(f"Insufficient points: have {balance}, need {amount}")
        new_balance = balance - amount
        await conn.execute(
            text("""
                UPDATE users
                SET points_balance = :nb, total_points_spent = total_points_spent + :amt
                WHERE telegram_id = :tid
            """),
            {"nb": new_balance, "amt": amount, "tid": tid},
        )
        # Update last_used on the key
        await conn.execute(
            text("UPDATE api_keys SET last_used_at = now() WHERE id = :id"),
            {"id": api_key_id},
        )
        await conn.execute(
            text("""
                INSERT INTO credit_transactions (api_key_id, telegram_id, amount, type, endpoint, balance_after)
                VALUES (:kid, :tid, :amt, 'usage', :ep, :bal)
            """),
            {"kid": api_key_id, "tid": tid, "amt": -amount, "ep": endpoint, "bal": new_balance},
        )
        return new_balance


async def topup_credits(engine: AsyncEngine, api_key_id: UUID, amount: int, ton_tx_hash: str | None = None) -> int:
    """Add points to USER balance. Returns new balance."""
    from sqlalchemy import text
    async with engine.begin() as conn:
        # Get user from api_key
        key_row = await conn.execute(
            text("SELECT telegram_id FROM api_keys WHERE id = :id"),
            {"id": api_key_id},
        )
        tid = key_row.scalar()
        if not tid:
            raise ValueError("API key not found")

        row = await conn.execute(
            text("SELECT points_balance FROM users WHERE telegram_id = :tid FOR UPDATE"),
            {"tid": tid},
        )
        balance = row.scalar() or 0
        new_balance = balance + amount
        await conn.execute(
            text("UPDATE users SET points_balance = :nb WHERE telegram_id = :tid"),
            {"nb": new_balance, "tid": tid},
        )
        await conn.execute(
            text("""
                INSERT INTO credit_transactions (api_key_id, telegram_id, amount, type, ton_tx_hash, balance_after)
                VALUES (:kid, :tid, :amt, 'topup', :tx, :bal)
            """),
            {"kid": api_key_id, "tid": tid, "amt": amount, "tx": ton_tx_hash, "bal": new_balance},
        )
        return new_balance


async def topup_user_direct(engine: AsyncEngine, telegram_id: int, amount: int, tx_type: str = "bonus") -> int:
    """Add points directly to user (for bonus, no api_key needed). Returns new balance."""
    from sqlalchemy import text
    async with engine.begin() as conn:
        row = await conn.execute(
            text("SELECT points_balance FROM users WHERE telegram_id = :tid FOR UPDATE"),
            {"tid": telegram_id},
        )
        balance = row.scalar() or 0
        new_balance = balance + amount
        await conn.execute(
            text("UPDATE users SET points_balance = :nb WHERE telegram_id = :tid"),
            {"nb": new_balance, "tid": telegram_id},
        )
        await conn.execute(
            text("""
                INSERT INTO credit_transactions (telegram_id, amount, type, balance_after)
                VALUES (:tid, :amt, :tp, :bal)
            """),
            {"tid": telegram_id, "amt": amount, "tp": tx_type, "bal": new_balance},
        )
        return new_balance


# --- FastAPI Dependencies ---

async def verify_api_key(
    authorization: Annotated[str, Header()],
) -> dict:
    """Extract and verify API key from Authorization header."""
    key = authorization.removeprefix("Bearer ").strip()
    if not key.startswith("amk_"):
        raise HTTPException(status_code=401, detail="Invalid API key format")

    key_hash = _hash_key(key)
    api_key = await get_api_key_by_hash(async_engine, key_hash)
    if not api_key or not api_key["is_active"]:
        raise HTTPException(status_code=401, detail="Invalid or deactivated API key")
    return api_key


def require_credits(endpoint: str):
    """FastAPI dependency that checks and charges credits for an endpoint."""
    cost = CREDIT_COSTS.get(endpoint, 0)
    if cost == 0:
        return verify_api_key  # free endpoint, just verify key

    async def _dep(api_key: dict = Depends(verify_api_key)) -> dict:
        if api_key["credits_balance"] < cost:
            raise HTTPException(
                status_code=402,
                detail={
                    "error": "insufficient_credits",
                    "balance": api_key["credits_balance"],
                    "required": cost,
                    "topup_url": "https://t.me/AgentMemoryBot?start=topup",
                },
            )
        # Charge will happen after successful response via middleware/callback
        # For now, charge immediately
        new_balance = await charge_credits(async_engine, api_key["id"], cost, endpoint)
        api_key["credits_balance"] = new_balance
        api_key["_credits_charged"] = cost
        return api_key

    return _dep
