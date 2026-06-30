"""Async CRUD for chat classification labels (project / personal).

Split out of queries.py: a chat carries exactly one classification label
(work project or personal category); these helpers back the label classifier
and the /activity export. Mirrors queries_groups / queries_conversations.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import delete, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_memory_mcp.db.tables import domain_labels, labels, messages


async def upsert_label(
    engine: AsyncEngine,
    owner_id: int,
    type_: str,
    name: str,
    aliases: list[str] | None = None,
) -> UUID:
    """Create or fetch a label by (owner, type, name). Merges aliases. Returns id."""
    ins = pg_insert(labels).values(
        owner_id=owner_id, type=type_, name=name, aliases=aliases
    )
    stmt = ins.on_conflict_do_update(
        index_elements=["owner_id", "type", "name"],
        set_={"aliases": ins.excluded.aliases},
    ).returning(labels.c.id)
    async with engine.begin() as conn:
        return (await conn.execute(stmt)).scalar_one()


async def get_sender_domains(engine: AsyncEngine, domain_ids: list[UUID]) -> list[tuple]:
    """Distinct (sender_id, domain_id) pairs across the given domains.

    Lets the classifier tell that a DM partner (whose user id == the DM domain's
    channel_id) also participates in a work group chat — an indirect signal that
    the DM is work-related, no hardcoding."""
    if not domain_ids:
        return []
    stmt = (
        select(messages.c.sender_id, messages.c.domain_id)
        .where(messages.c.domain_id.in_(domain_ids), messages.c.sender_id.isnot(None))
        .distinct()
    )
    async with engine.begin() as conn:
        return [(r[0], r[1]) for r in (await conn.execute(stmt)).all()]


async def set_domain_project_label(
    engine: AsyncEngine,
    domain_id: UUID,
    label_id: UUID,
    confidence: float | None,
    source: str | None,
) -> None:
    """Attach a project label to a chat, ensuring exactly ONE project per chat
    (removes any other project-type labels on this domain first)."""
    async with engine.begin() as conn:
        await conn.execute(
            delete(domain_labels).where(
                domain_labels.c.domain_id == domain_id,
                domain_labels.c.label_id.in_(
                    select(labels.c.id).where(labels.c.type.in_(["project", "personal"]))
                ),
                domain_labels.c.label_id != label_id,
            )
        )
        stmt = (
            pg_insert(domain_labels)
            .values(domain_id=domain_id, label_id=label_id, confidence=confidence, source=source)
            .on_conflict_do_update(
                index_elements=["domain_id", "label_id"],
                set_={"confidence": confidence, "source": source, "created_at": text("now()")},
            )
        )
        await conn.execute(stmt)


async def list_labels(
    engine: AsyncEngine, owner_id: int, type_: str | None = None
) -> list[dict]:
    stmt = select(labels).where(labels.c.owner_id == owner_id)
    if type_:
        stmt = stmt.where(labels.c.type == type_)
    stmt = stmt.order_by(labels.c.type, labels.c.name)
    async with engine.begin() as conn:
        return [dict(r) for r in (await conn.execute(stmt)).mappings().all()]


async def get_domain_label_map(engine: AsyncEngine, domain_ids: list[UUID]) -> dict:
    """domain_id → {label_id, name, type, confidence} — the chat's single
    classification label (type project=work or personal=non-work), highest
    confidence per domain. Used to stamp project/category on the /activity export."""
    if not domain_ids:
        return {}
    stmt = (
        select(
            domain_labels.c.domain_id,
            domain_labels.c.label_id,
            domain_labels.c.confidence,
            labels.c.name,
            labels.c.type,
        )
        .select_from(domain_labels.join(labels, domain_labels.c.label_id == labels.c.id))
        .where(
            domain_labels.c.domain_id.in_(domain_ids),
            labels.c.type.in_(["project", "personal"]),
        )
        .order_by(domain_labels.c.confidence.desc().nullslast())
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
    out: dict = {}
    for r in rows:  # first row per domain wins (highest confidence)
        if r["domain_id"] not in out:
            out[r["domain_id"]] = {
                "label_id": str(r["label_id"]),
                "name": r["name"],
                "type": r["type"],
                "confidence": r["confidence"],
            }
    return out
