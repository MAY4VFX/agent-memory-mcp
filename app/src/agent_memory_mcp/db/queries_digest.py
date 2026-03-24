"""Async CRUD for digest configs and runs."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_memory_mcp.db.tables import digest_configs, digest_runs


# ---------------------------------------------------------------------------
# Digest configs
# ---------------------------------------------------------------------------

async def create_digest_config(
    engine: AsyncEngine,
    user_id: int,
    name: str = "Daily Digest",
    scope_type: str = "all",
    scope_id: UUID | None = None,
    send_hour_utc: int = 8,
) -> dict:
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    stmt = (
        pg_insert(digest_configs)
        .values(
            user_id=user_id,
            name=name,
            scope_type=scope_type,
            scope_id=scope_id,
            send_hour_utc=send_hour_utc,
        )
        .on_conflict_do_update(
            constraint="digest_configs_user_id_name_key",
            set_={
                "scope_type": scope_type,
                "scope_id": scope_id,
                "send_hour_utc": send_hour_utc,
                "is_active": True,
            },
        )
        .returning(*digest_configs.c)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one()
        return dict(row)


async def get_digest_config(
    engine: AsyncEngine,
    config_id: UUID,
) -> dict | None:
    stmt = select(digest_configs).where(digest_configs.c.id == config_id)
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


async def get_user_digest_config(
    engine: AsyncEngine,
    user_id: int,
) -> dict | None:
    """Get first active digest config for a user."""
    stmt = (
        select(digest_configs)
        .where(
            digest_configs.c.user_id == user_id,
            digest_configs.c.is_active.is_(True),
        )
        .order_by(digest_configs.c.created_at)
        .limit(1)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


async def update_digest_config(
    engine: AsyncEngine,
    config_id: UUID,
    **kwargs,
) -> dict | None:
    if not kwargs:
        return await get_digest_config(engine, config_id)
    stmt = (
        update(digest_configs)
        .where(digest_configs.c.id == config_id)
        .values(**kwargs)
        .returning(*digest_configs.c)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


async def get_due_digests(engine: AsyncEngine, hour_utc: int) -> list[dict]:
    """Get all active digest configs due at the given hour.

    A config is "due" when:
    - is_active = true
    - send_hour_utc matches
    - last_sent_at is NULL or more than 20 hours ago
    """
    from sqlalchemy import text as sa_text

    stmt = (
        select(digest_configs)
        .where(
            digest_configs.c.is_active.is_(True),
            digest_configs.c.send_hour_utc == hour_utc,
            sa_text(
                "(last_sent_at IS NULL OR last_sent_at < now() - interval '20 hours')"
            ),
        )
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Digest runs
# ---------------------------------------------------------------------------

async def create_digest_run(
    engine: AsyncEngine,
    config_id: UUID,
    user_id: int,
) -> dict:
    stmt = (
        insert(digest_runs)
        .values(
            config_id=config_id,
            user_id=user_id,
            status="running",
            started_at=datetime.now(timezone.utc),
        )
        .returning(*digest_runs.c)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one()
        return dict(row)


async def update_digest_run(
    engine: AsyncEngine,
    run_id: UUID,
    **kwargs,
) -> dict | None:
    if not kwargs:
        return None
    stmt = (
        update(digest_runs)
        .where(digest_runs.c.id == run_id)
        .values(**kwargs)
        .returning(*digest_runs.c)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


async def get_digest_run(engine: AsyncEngine, run_id: UUID) -> dict | None:
    """Get a specific digest run by ID."""
    stmt = select(digest_runs).where(digest_runs.c.id == run_id)
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


async def get_last_completed_run(
    engine: AsyncEngine,
    config_id: UUID,
) -> dict | None:
    """Get the most recent completed digest run for a config."""
    stmt = (
        select(digest_runs)
        .where(
            digest_runs.c.config_id == config_id,
            digest_runs.c.status == "completed",
        )
        .order_by(digest_runs.c.completed_at.desc())
        .limit(1)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None
