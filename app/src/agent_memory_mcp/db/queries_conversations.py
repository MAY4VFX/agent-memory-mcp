"""Async CRUD for conversations, messages, payloads, feedback."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_memory_mcp.db.tables import (
    context_payloads,
    conversation_messages,
    conversations,
    domains,
    feedback,
    users,
)


# ---------------------------------------------------------------------------
# Conversations
# ---------------------------------------------------------------------------

async def create_conversation(
    engine: AsyncEngine,
    user_id: int,
    domain_id: UUID | None = None,
    title: str = "",
    scope_type: str = "domain",
    group_id: UUID | None = None,
) -> dict:
    values = dict(user_id=user_id, domain_id=domain_id, title=title, scope_type=scope_type)
    if group_id:
        values["group_id"] = group_id
    stmt = (
        insert(conversations)
        .values(**values)
        .returning(*conversations.c)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one()
        return dict(row)


async def get_conversation(engine: AsyncEngine, conv_id: UUID) -> dict | None:
    stmt = select(conversations).where(conversations.c.id == conv_id)
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


async def list_conversations(
    engine: AsyncEngine,
    user_id: int,
    limit: int = 20,
) -> list[dict]:
    stmt = (
        select(
            *conversations.c,
            domains.c.emoji.label("domain_emoji"),
        )
        .outerjoin(domains, conversations.c.domain_id == domains.c.id)
        .where(conversations.c.user_id == user_id)
        .order_by(conversations.c.updated_at.desc())
        .limit(limit)
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
        return [dict(r) for r in rows]


async def update_conversation(
    engine: AsyncEngine,
    conv_id: UUID,
    **kwargs,
) -> dict | None:
    if not kwargs:
        return await get_conversation(engine, conv_id)
    stmt = (
        update(conversations)
        .where(conversations.c.id == conv_id)
        .values(**kwargs)
        .returning(*conversations.c)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


async def delete_conversation(engine: AsyncEngine, conv_id: UUID) -> None:
    stmt = delete(conversations).where(conversations.c.id == conv_id)
    async with engine.begin() as conn:
        await conn.execute(stmt)


# ---------------------------------------------------------------------------
# Users — active conversation
# ---------------------------------------------------------------------------

async def set_active_conversation(
    engine: AsyncEngine,
    telegram_id: int,
    conv_id: UUID | None,
) -> None:
    stmt = (
        update(users)
        .where(users.c.telegram_id == telegram_id)
        .values(active_conversation_id=conv_id)
    )
    async with engine.begin() as conn:
        await conn.execute(stmt)


async def get_active_conversation_id(
    engine: AsyncEngine,
    telegram_id: int,
) -> UUID | None:
    stmt = select(users.c.active_conversation_id).where(
        users.c.telegram_id == telegram_id,
    )
    async with engine.begin() as conn:
        return (await conn.execute(stmt)).scalar()


# ---------------------------------------------------------------------------
# Conversation Messages
# ---------------------------------------------------------------------------

async def add_message(
    engine: AsyncEngine,
    conversation_id: UUID,
    role: str,
    content: str,
    token_count: int = 0,
    context_payload_id: UUID | None = None,
    langfuse_trace_id: str = "",
) -> dict:
    stmt = (
        insert(conversation_messages)
        .values(
            conversation_id=conversation_id,
            role=role,
            content=content,
            token_count=token_count,
            context_payload_id=context_payload_id,
            langfuse_trace_id=langfuse_trace_id,
        )
        .returning(*conversation_messages.c)
    )
    # Also bump conversation message_count + updated_at
    bump = (
        update(conversations)
        .where(conversations.c.id == conversation_id)
        .values(
            message_count=conversations.c.message_count + 1,
            updated_at=func.now(),
        )
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one()
        await conn.execute(bump)
        return dict(row)


async def get_recent_messages(
    engine: AsyncEngine,
    conversation_id: UUID,
    limit: int = 10,
) -> list[dict]:
    stmt = (
        select(conversation_messages)
        .where(conversation_messages.c.conversation_id == conversation_id)
        .order_by(conversation_messages.c.created_at.desc())
        .limit(limit)
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
        return [dict(r) for r in reversed(rows)]


async def get_all_messages(
    engine: AsyncEngine,
    conversation_id: UUID,
) -> list[dict]:
    stmt = (
        select(conversation_messages)
        .where(conversation_messages.c.conversation_id == conversation_id)
        .order_by(conversation_messages.c.created_at.asc())
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
        return [dict(r) for r in rows]


async def get_last_message(
    engine: AsyncEngine,
    conversation_id: UUID,
) -> dict | None:
    stmt = (
        select(conversation_messages)
        .where(conversation_messages.c.conversation_id == conversation_id)
        .order_by(conversation_messages.c.created_at.desc())
        .limit(1)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# Context Payloads
# ---------------------------------------------------------------------------

async def save_context_payload(
    engine: AsyncEngine,
    conversation_id: UUID,
    query_text: str,
    payload_json: dict,
    token_count: int = 0,
    chunks_count: int = 0,
    graph_entities_count: int = 0,
    langfuse_trace_id: str = "",
) -> dict:
    stmt = (
        insert(context_payloads)
        .values(
            conversation_id=conversation_id,
            query_text=query_text,
            payload_json=payload_json,
            token_count=token_count,
            chunks_count=chunks_count,
            graph_entities_count=graph_entities_count,
            langfuse_trace_id=langfuse_trace_id,
        )
        .returning(*context_payloads.c)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one()
        return dict(row)


async def get_context_payload(
    engine: AsyncEngine,
    payload_id: UUID,
) -> dict | None:
    stmt = select(context_payloads).where(context_payloads.c.id == payload_id)
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# Feedback
# ---------------------------------------------------------------------------

async def save_feedback(
    engine: AsyncEngine,
    message_id: UUID,
    user_id: int,
    score: int,
) -> dict:
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    stmt = (
        pg_insert(feedback)
        .values(message_id=message_id, user_id=user_id, score=score)
        .on_conflict_do_update(
            constraint="feedback_message_id_user_id_key",
            set_={"score": score},
        )
        .returning(*feedback.c)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one()
        return dict(row)
