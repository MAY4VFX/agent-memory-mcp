"""Async CRUD for domain groups and scope resolution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import delete, insert, select, update
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_memory_mcp.db.tables import domain_group_members, domain_groups, domains, users


@dataclass
class ScopeInfo:
    """Resolved scope: which domains/channels to search."""

    scope_type: str  # domain | group | all
    domain_ids: list[UUID] = field(default_factory=list)
    channel_ids: list[int] = field(default_factory=list)
    label: str = ""
    group_id: UUID | None = None
    since_date: datetime | None = None  # group sync_depth → date cutoff


# ---------------------------------------------------------------------------
# Groups CRUD
# ---------------------------------------------------------------------------

async def create_group(
    engine: AsyncEngine,
    owner_id: int,
    name: str,
    emoji: str = "\U0001f4c1",
    tg_folder_id: int | None = None,
    sync_depth: str | None = None,
) -> dict:
    stmt = (
        insert(domain_groups)
        .values(
            owner_id=owner_id,
            name=name,
            emoji=emoji,
            tg_folder_id=tg_folder_id,
            sync_depth=sync_depth,
        )
        .returning(*domain_groups.c)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one()
        return dict(row)


async def list_groups(engine: AsyncEngine, owner_id: int) -> list[dict]:
    stmt = (
        select(domain_groups)
        .where(
            domain_groups.c.owner_id == owner_id,
            domain_groups.c.is_active.is_(True),
        )
        .order_by(domain_groups.c.created_at)
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
        return [dict(r) for r in rows]


async def get_group(engine: AsyncEngine, group_id: UUID) -> dict | None:
    stmt = select(domain_groups).where(domain_groups.c.id == group_id)
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


async def delete_group(engine: AsyncEngine, group_id: UUID) -> None:
    stmt = delete(domain_groups).where(domain_groups.c.id == group_id)
    async with engine.begin() as conn:
        await conn.execute(stmt)


async def update_group(engine: AsyncEngine, group_id: UUID, **kwargs) -> dict | None:
    if not kwargs:
        return await get_group(engine, group_id)
    stmt = (
        update(domain_groups)
        .where(domain_groups.c.id == group_id)
        .values(**kwargs)
        .returning(*domain_groups.c)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# Group members
# ---------------------------------------------------------------------------

async def add_domains_to_group(
    engine: AsyncEngine,
    group_id: UUID,
    domain_ids: list[UUID],
) -> int:
    """Add domains to group. Returns count of actually inserted rows."""
    if not domain_ids:
        return 0
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    values = [{"group_id": group_id, "domain_id": did} for did in domain_ids]
    stmt = pg_insert(domain_group_members).values(values).on_conflict_do_nothing()
    async with engine.begin() as conn:
        result = await conn.execute(stmt)
        return result.rowcount


async def remove_domain_from_group(
    engine: AsyncEngine,
    group_id: UUID,
    domain_id: UUID,
) -> None:
    stmt = delete(domain_group_members).where(
        domain_group_members.c.group_id == group_id,
        domain_group_members.c.domain_id == domain_id,
    )
    async with engine.begin() as conn:
        await conn.execute(stmt)


async def get_group_domain_ids(
    engine: AsyncEngine,
    group_id: UUID,
) -> list[UUID]:
    stmt = (
        select(domain_group_members.c.domain_id)
        .where(domain_group_members.c.group_id == group_id)
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).scalars().all()
        return list(rows)


async def get_all_grouped_domain_ids(engine: AsyncEngine, owner_id: int) -> set[UUID]:
    """Get set of domain IDs that belong to at least one group (for this owner)."""
    stmt = (
        select(domain_group_members.c.domain_id)
        .join(domain_groups, domain_groups.c.id == domain_group_members.c.group_id)
        .where(domain_groups.c.owner_id == owner_id, domain_groups.c.is_active.is_(True))
        .distinct()
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).scalars().all()
        return set(rows)


async def get_exclusively_grouped_domain_ids(engine: AsyncEngine, owner_id: int) -> set[UUID]:
    """Domain IDs that should be hidden from individual list.

    A domain is "exclusively grouped" if it belongs to at least one group
    AND is NOT pinned.  Pinned domains (added individually by user) always
    remain visible as standalone sources even if they also belong to a list.
    """
    stmt = (
        select(domain_group_members.c.domain_id)
        .join(domain_groups, domain_groups.c.id == domain_group_members.c.group_id)
        .join(domains, domains.c.id == domain_group_members.c.domain_id)
        .where(
            domain_groups.c.owner_id == owner_id,
            domain_groups.c.is_active.is_(True),
            domains.c.pinned.isnot(True),
        )
        .distinct()
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).scalars().all()
        return set(rows)


async def get_group_domains(
    engine: AsyncEngine,
    group_id: UUID,
) -> list[dict]:
    """Get full domain info for all members of a group."""
    stmt = (
        select(domains)
        .join(domain_group_members, domains.c.id == domain_group_members.c.domain_id)
        .where(domain_group_members.c.group_id == group_id)
        .order_by(domains.c.created_at)
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Scope resolution
# ---------------------------------------------------------------------------

_DEPTH_MAP = {
    "1w": timedelta(weeks=1),
    "1m": timedelta(days=30),
    "3m": timedelta(days=90),
    "6m": timedelta(days=180),
    "1y": timedelta(days=365),
    "3y": timedelta(days=1095),
}


def _depth_to_date(depth: str | None) -> datetime | None:
    """Convert sync depth string to UTC cutoff date."""
    if not depth:
        return None
    delta = _DEPTH_MAP.get(depth)
    return (datetime.now(timezone.utc) - delta) if delta else None


async def resolve_scope(engine: AsyncEngine, user: dict) -> ScopeInfo:
    """Resolve user's active scope to domain_ids and channel_ids.

    user dict must contain: active_scope_type, active_domain_id, active_group_id, telegram_id.
    """
    scope_type = user.get("active_scope_type", "domain") or "domain"

    if scope_type == "group" and user.get("active_group_id"):
        group_id = user["active_group_id"]
        group = await get_group(engine, group_id)
        if not group:
            return ScopeInfo(scope_type="domain", label="(группа не найдена)")

        group_domains = await get_group_domains(engine, group_id)
        dids = [d["id"] for d in group_domains]
        cids = [d["channel_id"] for d in group_domains]
        since_date = _depth_to_date(group.get("sync_depth"))
        return ScopeInfo(
            scope_type="group",
            domain_ids=dids,
            channel_ids=cids,
            label=f"{group['emoji']} {group['name']}",
            group_id=group_id,
            since_date=since_date,
        )

    if scope_type == "all":
        from agent_memory_mcp.db.queries import list_domains

        all_domains = await list_domains(engine, user["telegram_id"])
        dids = [d["id"] for d in all_domains]
        cids = [d["channel_id"] for d in all_domains]
        return ScopeInfo(
            scope_type="all",
            domain_ids=dids,
            channel_ids=cids,
            label=f"\U0001f30d Все каналы ({len(dids)})",
        )

    # Default: single domain
    did = user.get("active_domain_id")
    if not did:
        return ScopeInfo(scope_type="domain", label="(канал не выбран)")
    from agent_memory_mcp.db.queries import get_domain

    domain = await get_domain(engine, did)
    if not domain:
        return ScopeInfo(scope_type="domain", label="(канал не найден)")
    return ScopeInfo(
        scope_type="domain",
        domain_ids=[did],
        channel_ids=[domain["channel_id"]],
        label=f"{domain['emoji']} {domain['display_name']}",
    )


async def update_user_scope(
    engine: AsyncEngine,
    telegram_id: int,
    scope_type: str,
    group_id: UUID | None = None,
) -> None:
    """Update user's active scope."""
    stmt = (
        update(users)
        .where(users.c.telegram_id == telegram_id)
        .values(active_scope_type=scope_type, active_group_id=group_id)
    )
    async with engine.begin() as conn:
        await conn.execute(stmt)
