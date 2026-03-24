"""Async CRUD functions using SQLAlchemy Core."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_memory_mcp.db.tables import (
    channel_schemas,
    domains,
    hashtag_summaries,
    messages,
    sync_jobs,
    threads,
    users,
)


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

async def upsert_user(
    engine: AsyncEngine,
    telegram_id: int,
    username: str | None = None,
) -> dict:
    stmt = (
        pg_insert(users)
        .values(telegram_id=telegram_id, username=username)
        .on_conflict_do_update(
            index_elements=[users.c.telegram_id],
            set_={"username": username},
        )
        .returning(*users.c)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one()
        return dict(row)


async def get_user(engine: AsyncEngine, telegram_id: int) -> dict | None:
    stmt = select(users).where(users.c.telegram_id == telegram_id)
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


async def update_user_active_domain(
    engine: AsyncEngine,
    telegram_id: int,
    domain_id: UUID | None,
) -> None:
    stmt = (
        update(users)
        .where(users.c.telegram_id == telegram_id)
        .values(active_domain_id=domain_id)
    )
    async with engine.begin() as conn:
        await conn.execute(stmt)


async def update_user_search_mode(
    engine: AsyncEngine,
    telegram_id: int,
    mode: str,
) -> None:
    stmt = (
        update(users)
        .where(users.c.telegram_id == telegram_id)
        .values(detail_level=mode)
    )
    async with engine.begin() as conn:
        await conn.execute(stmt)


# ---------------------------------------------------------------------------
# Domains
# ---------------------------------------------------------------------------

async def create_domain(
    engine: AsyncEngine,
    owner_id: int,
    channel_id: int,
    channel_username: str,
    channel_name: str,
    sync_depth: str,
    sync_frequency_minutes: int,
    emoji: str,
    display_name: str,
    pinned: bool = False,
) -> dict:
    stmt = (
        insert(domains)
        .values(
            owner_id=owner_id,
            channel_id=channel_id,
            channel_username=channel_username,
            channel_name=channel_name,
            sync_depth=sync_depth,
            sync_frequency_minutes=sync_frequency_minutes,
            emoji=emoji,
            display_name=display_name,
            pinned=pinned,
        )
        .returning(*domains.c)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one()
        return dict(row)


async def list_domains(engine: AsyncEngine, owner_id: int) -> list[dict]:
    stmt = (
        select(domains)
        .where(domains.c.owner_id == owner_id)
        .order_by(domains.c.created_at)
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
        return [dict(r) for r in rows]


async def get_domain(engine: AsyncEngine, domain_id: UUID) -> dict | None:
    stmt = select(domains).where(domains.c.id == domain_id)
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


async def update_domain(
    engine: AsyncEngine,
    domain_id: UUID,
    **kwargs,
) -> dict | None:
    if not kwargs:
        return await get_domain(engine, domain_id)
    stmt = (
        update(domains)
        .where(domains.c.id == domain_id)
        .values(**kwargs)
        .returning(*domains.c)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


async def delete_domain(engine: AsyncEngine, domain_id: UUID) -> None:
    stmt = delete(domains).where(domains.c.id == domain_id)
    async with engine.begin() as conn:
        await conn.execute(stmt)


async def get_domains_for_sync(engine: AsyncEngine) -> list[dict]:
    stmt = (
        select(domains)
        .where(
            domains.c.is_active.is_(True),
            domains.c.next_sync_at <= func.now(),
        )
        .order_by(domains.c.next_sync_at)
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------

async def bulk_insert_messages(
    engine: AsyncEngine,
    msgs: list[dict],
) -> None:
    if not msgs:
        return
    stmt = pg_insert(messages)
    stmt = stmt.on_conflict_do_update(
        constraint="messages_domain_id_telegram_msg_id_key",
        set_={"topic_id": stmt.excluded.topic_id},
        where=(messages.c.topic_id.is_(None) & stmt.excluded.topic_id.isnot(None)),
    )
    async with engine.begin() as conn:
        await conn.execute(stmt, msgs)


async def get_messages_by_ids(
    engine: AsyncEngine,
    message_ids: list[str],
) -> list[dict]:
    """Fetch full message rows by their UUIDs."""
    if not message_ids:
        return []
    uuids = [UUID(mid) for mid in message_ids]
    stmt = select(messages).where(messages.c.id.in_(uuids))
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
        return [dict(r) for r in rows]


async def get_last_message_id(
    engine: AsyncEngine,
    domain_id: UUID,
) -> int:
    stmt = (
        select(func.coalesce(func.max(messages.c.telegram_msg_id), 0))
        .where(messages.c.domain_id == domain_id)
    )
    async with engine.begin() as conn:
        result = (await conn.execute(stmt)).scalar()
        return result or 0


# ---------------------------------------------------------------------------
# Threads
# ---------------------------------------------------------------------------

async def bulk_insert_threads(
    engine: AsyncEngine,
    thread_rows: list[dict],
) -> None:
    if not thread_rows:
        return
    async with engine.begin() as conn:
        await conn.execute(insert(threads), thread_rows)


# ---------------------------------------------------------------------------
# Channel Schemas
# ---------------------------------------------------------------------------

async def save_channel_schema(
    engine: AsyncEngine,
    domain_id: UUID,
    schema_json: dict,
    detected_domain: str,
    entity_types: list,
    relation_types: list,
    langfuse_trace_id: str = "",
) -> dict:
    # Deactivate previous active schemas
    deactivate = (
        update(channel_schemas)
        .where(
            channel_schemas.c.domain_id == domain_id,
            channel_schemas.c.is_active.is_(True),
        )
        .values(is_active=False)
    )
    # Compute next version
    next_ver = (
        select(func.coalesce(func.max(channel_schemas.c.version), 0) + 1)
        .where(channel_schemas.c.domain_id == domain_id)
        .scalar_subquery()
    )
    ins = (
        insert(channel_schemas)
        .values(
            domain_id=domain_id,
            version=next_ver,
            schema_json=schema_json,
            detected_domain=detected_domain,
            entity_types=entity_types,
            relation_types=relation_types,
            is_active=True,
            langfuse_trace_id=langfuse_trace_id,
        )
        .returning(*channel_schemas.c)
    )
    async with engine.begin() as conn:
        await conn.execute(deactivate)
        row = (await conn.execute(ins)).mappings().one()
        return dict(row)


async def get_active_schema(
    engine: AsyncEngine,
    domain_id: UUID,
) -> dict | None:
    stmt = (
        select(channel_schemas)
        .where(
            channel_schemas.c.domain_id == domain_id,
            channel_schemas.c.is_active.is_(True),
        )
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# Sync Jobs
# ---------------------------------------------------------------------------

async def create_sync_job(
    engine: AsyncEngine,
    domain_id: UUID,
    job_type: str,
) -> dict:
    stmt = (
        insert(sync_jobs)
        .values(domain_id=domain_id, job_type=job_type)
        .returning(*sync_jobs.c)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one()
        return dict(row)


async def get_domain_messages(
    engine: AsyncEngine,
    domain_id: UUID,
) -> list[dict]:
    """Load all messages for a domain, oldest-first."""
    stmt = (
        select(messages)
        .where(messages.c.domain_id == domain_id)
        .order_by(messages.c.msg_date.asc())
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
        return [dict(r) for r in rows]


def _expand_keywords(keywords: list[str]) -> list[str]:
    """Split compound keywords into individual words for broader ILIKE matching."""
    expanded: list[str] = []
    seen: set[str] = set()
    for kw in keywords:
        # Keep original keyword
        kw_lower = kw.strip().lower()
        if kw_lower and kw_lower not in seen:
            expanded.append(kw.strip())
            seen.add(kw_lower)
        # Split multi-word keywords into individual words (3+ chars)
        for word in kw.split():
            w = word.strip().lower()
            if len(w) >= 3 and w not in seen:
                expanded.append(word.strip())
                seen.add(w)
    return expanded


async def search_messages_by_keywords(
    engine: AsyncEngine,
    domain_id: UUID,
    keywords: list[str],
    limit: int = 200,
) -> list[dict]:
    """Full-text search messages by keywords (ILIKE). Returns matching messages."""
    if not keywords:
        return []
    expanded = _expand_keywords(keywords)
    conditions = [messages.c.content.ilike(f"%{kw}%") for kw in expanded]
    from sqlalchemy import or_
    stmt = (
        select(messages)
        .where(
            messages.c.domain_id == domain_id,
            or_(*conditions),
        )
        .order_by(messages.c.msg_date.desc())
        .limit(limit)
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
        return [dict(r) for r in rows]


async def count_messages_by_keywords(
    engine: AsyncEngine,
    domain_id: UUID,
    keywords: list[str],
) -> int:
    """Count messages matching keywords."""
    if not keywords:
        return 0
    expanded = _expand_keywords(keywords)
    conditions = [messages.c.content.ilike(f"%{kw}%") for kw in expanded]
    from sqlalchemy import or_
    stmt = (
        select(func.count())
        .select_from(messages)
        .where(
            messages.c.domain_id == domain_id,
            or_(*conditions),
        )
    )
    async with engine.begin() as conn:
        return (await conn.execute(stmt)).scalar() or 0


# ---------------------------------------------------------------------------
# BM25 / Full-text search
# ---------------------------------------------------------------------------

_RU_STOP_WORDS = frozenset(
    "а без более бы был была были было быть в вам вас ваш весь вдруг ведь во "
    "вот впрочем все всё всего всех всю вы где да даже два для до другой его "
    "ее ей ему если есть ещё же за здесь и из или им иногда их к каждый как "
    "когда конечно которого которые кто куда ли лучше между меня мне много "
    "может можно можешь мой моя мы на надо наконец нам нас не него нее ней "
    "нельзя нет ни нибудь никогда ним них ничего но ну о об один он она они "
    "опять от перед по под после потом потому почему при про раз разве с сам "
    "свою себе себя сейчас со совсем так также такой там тебе тебя тем теперь "
    "то тогда тоже того той только том тот три тут ты у уж уже хорошо хоть "
    "чего чем через что чтоб чтобы чуть эти этих это этого этой этом этот я "
    "сколько какой какие какая какое".split()
)


def _build_bm25_query(query_text: str) -> str:
    """Build ParadeDB BM25 query with stop-word filtering and rare-term boost."""
    import re
    # Strip punctuation, split, filter short words and stop words
    raw = re.sub(r"[,\.!?\-—:;\"'()«»/]", " ", query_text)
    words = [w for w in raw.split() if len(w) >= 2 and w.lower() not in _RU_STOP_WORDS]
    if not words:
        words = [w for w in query_text.split() if len(w) >= 2]
    if not words:
        return f"content:{query_text}"

    # Boost long/rare words (heuristic: len >= 10 likely domain-specific)
    parts = []
    for w in words:
        if len(w) >= 10:
            parts.append(f"content:{w}^5")
        else:
            parts.append(f"content:{w}")
    return " OR ".join(parts)


async def search_messages_bm25(
    engine: AsyncEngine,
    domain_id: UUID,
    query_text: str,
    limit: int = 500,
) -> tuple[list[dict], int]:
    """BM25 search with scoring. ParadeDB primary, tsvector fallback.

    Returns (rows_with_score, total_count).
    """
    if not query_text:
        return [], 0

    from sqlalchemy import text as sa_text

    # Try ParadeDB BM25 first (each in its own transaction to avoid aborted state)
    try:
        bm25_query = _build_bm25_query(query_text)
        async with engine.begin() as conn:
            count_row = await conn.execute(sa_text("""
                SELECT COUNT(*) FROM messages
                WHERE domain_id = :domain_id
                  AND id @@@ paradedb.parse(:query)
            """), {"domain_id": domain_id, "query": bm25_query})
            total = count_row.scalar() or 0

            rows = await conn.execute(sa_text("""
                SELECT *, paradedb.score(id) AS bm25_score
                FROM messages
                WHERE domain_id = :domain_id
                  AND id @@@ paradedb.parse(:query)
                ORDER BY bm25_score DESC
                LIMIT :lim
            """), {"domain_id": domain_id, "query": bm25_query, "lim": limit})
            return [dict(r) for r in rows.mappings().all()], total
    except Exception:
        pass

    # Fallback: tsvector ts_rank (OR logic, Russian stemming)
    try:
        ts_query = " | ".join(
            w for w in query_text.split() if len(w) >= 2
        )
        if not ts_query:
            ts_query = query_text
        async with engine.begin() as conn:
            count_row = await conn.execute(sa_text("""
                SELECT COUNT(*) FROM messages
                WHERE domain_id = :domain_id
                  AND content_tsv @@ to_tsquery('russian', :tsq)
            """), {"domain_id": domain_id, "tsq": ts_query})
            total = count_row.scalar() or 0

            rows = await conn.execute(sa_text("""
                SELECT *, ts_rank(content_tsv, to_tsquery('russian', :tsq)) AS bm25_score
                FROM messages
                WHERE domain_id = :domain_id
                  AND content_tsv @@ to_tsquery('russian', :tsq)
                ORDER BY bm25_score DESC
                LIMIT :lim
            """), {"domain_id": domain_id, "tsq": ts_query, "lim": limit})
            return [dict(r) for r in rows.mappings().all()], total
    except Exception:
        pass

    # Last resort: ILIKE (original behavior)
    keywords = [w for w in query_text.split() if len(w) >= 3]
    if not keywords:
        return [], 0
    from sqlalchemy import or_
    conditions = [messages.c.content.ilike(f"%{kw}%") for kw in keywords]
    async with engine.begin() as conn:
        count_stmt = (
            select(func.count())
            .select_from(messages)
            .where(messages.c.domain_id == domain_id, or_(*conditions))
        )
        total = (await conn.execute(count_stmt)).scalar() or 0
        data_stmt = (
            select(messages)
            .where(messages.c.domain_id == domain_id, or_(*conditions))
            .order_by(messages.c.msg_date.desc())
            .limit(limit)
        )
        rows = (await conn.execute(data_stmt)).mappings().all()
        return [dict(r) | {"bm25_score": 1.0} for r in rows], total


# ---------------------------------------------------------------------------
# Hashtag search
# ---------------------------------------------------------------------------

async def search_messages_by_hashtag(
    engine: AsyncEngine,
    domain_id: UUID,
    hashtag: str,
    limit: int = 5000,
) -> tuple[list[dict], int]:
    """Exact hashtag JSONB containment match."""
    from sqlalchemy import text as sa_text
    import orjson

    tag_json = orjson.dumps([hashtag]).decode()
    async with engine.begin() as conn:
        count_row = await conn.execute(sa_text("""
            SELECT COUNT(*) FROM messages
            WHERE domain_id = :domain_id AND hashtags @> CAST(:tag AS jsonb)
        """), {"domain_id": domain_id, "tag": tag_json})
        total = count_row.scalar() or 0

        rows = await conn.execute(sa_text("""
            SELECT * FROM messages
            WHERE domain_id = :domain_id AND hashtags @> CAST(:tag AS jsonb)
            ORDER BY msg_date DESC
            LIMIT :lim
        """), {"domain_id": domain_id, "tag": tag_json, "lim": limit})
        return [dict(r) for r in rows.mappings().all()], total


# ---------------------------------------------------------------------------
# Hashtag summaries
# ---------------------------------------------------------------------------

async def get_hashtag_summary(
    engine: AsyncEngine,
    domain_id: UUID,
    hashtag: str,
) -> dict | None:
    stmt = (
        select(hashtag_summaries)
        .where(
            hashtag_summaries.c.domain_id == domain_id,
            hashtag_summaries.c.hashtag == hashtag,
        )
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


async def save_hashtag_summary(
    engine: AsyncEngine,
    domain_id: UUID,
    hashtag: str,
    summary: str,
    post_count: int,
) -> None:
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from sqlalchemy import text as sa_text

    stmt = (
        pg_insert(hashtag_summaries)
        .values(
            domain_id=domain_id,
            hashtag=hashtag,
            summary=summary,
            post_count=post_count,
            is_stale=False,
            posts_since_update=0,
            generated_at=sa_text("now()"),
        )
        .on_conflict_do_update(
            constraint="hashtag_summaries_domain_id_hashtag_key",
            set_={
                "summary": summary,
                "post_count": post_count,
                "is_stale": False,
                "posts_since_update": 0,
                "generated_at": sa_text("now()"),
            },
        )
    )
    async with engine.begin() as conn:
        await conn.execute(stmt)


async def mark_hashtag_summaries_stale(
    engine: AsyncEngine,
    domain_id: UUID,
    hashtags: list[str],
    new_posts: int,
) -> None:
    """Mark summaries as stale after new posts arrive."""
    if not hashtags:
        return
    stmt = (
        update(hashtag_summaries)
        .where(
            hashtag_summaries.c.domain_id == domain_id,
            hashtag_summaries.c.hashtag.in_(hashtags),
        )
        .values(
            is_stale=True,
            posts_since_update=hashtag_summaries.c.posts_since_update + new_posts,
        )
    )
    async with engine.begin() as conn:
        await conn.execute(stmt)


async def count_posts_per_hashtag(
    engine: AsyncEngine,
    domain_id: UUID,
) -> dict[str, int]:
    """Count posts per hashtag for a domain. Returns {hashtag: count}."""
    from sqlalchemy import text as sa_text

    async with engine.begin() as conn:
        rows = await conn.execute(sa_text("""
            SELECT tag, COUNT(*) AS cnt
            FROM messages, jsonb_array_elements_text(hashtags) AS tag
            WHERE domain_id = :domain_id AND hashtags IS NOT NULL
              AND jsonb_typeof(hashtags) = 'array'
            GROUP BY tag
            ORDER BY cnt DESC
        """), {"domain_id": domain_id})
        return {r.tag: r.cnt for r in rows}


async def sample_messages_by_hashtag(
    engine: AsyncEngine,
    domain_id: UUID,
    hashtag: str,
    sample_size: int = 50,
) -> list[dict]:
    """Sample messages with a specific hashtag (newest first, limited)."""
    import orjson
    from sqlalchemy import text as sa_text

    tag_json = orjson.dumps([hashtag]).decode()
    async with engine.begin() as conn:
        rows = await conn.execute(sa_text("""
            SELECT * FROM messages
            WHERE domain_id = :domain_id AND hashtags @> CAST(:tag AS jsonb)
            ORDER BY msg_date DESC
            LIMIT :lim
        """), {"domain_id": domain_id, "tag": tag_json, "lim": sample_size})
        return [dict(r) for r in rows.mappings().all()]


# ---------------------------------------------------------------------------
# Multi-domain BM25 search
# ---------------------------------------------------------------------------

async def search_messages_bm25_multi(
    engine: AsyncEngine,
    domain_ids: list[UUID],
    query_text: str,
    limit: int = 500,
) -> tuple[list[dict], int]:
    """BM25 search across multiple domains. Wrapper around single-domain search.

    For single domain, delegates to search_messages_bm25.
    For multiple, uses domain_id = ANY(:ids).
    """
    if not query_text or not domain_ids:
        return [], 0

    if len(domain_ids) == 1:
        return await search_messages_bm25(engine, domain_ids[0], query_text, limit)

    from sqlalchemy import text as sa_text

    # Try ParadeDB BM25 first
    try:
        bm25_query = _build_bm25_query(query_text)
        async with engine.begin() as conn:
            count_row = await conn.execute(sa_text("""
                SELECT COUNT(*) FROM messages
                WHERE domain_id = ANY(:ids)
                  AND id @@@ paradedb.parse(:query)
            """), {"ids": list(domain_ids), "query": bm25_query})
            total = count_row.scalar() or 0

            rows = await conn.execute(sa_text("""
                SELECT *, paradedb.score(id) AS bm25_score
                FROM messages
                WHERE domain_id = ANY(:ids)
                  AND id @@@ paradedb.parse(:query)
                ORDER BY bm25_score DESC
                LIMIT :lim
            """), {"ids": list(domain_ids), "query": bm25_query, "lim": limit})
            return [dict(r) for r in rows.mappings().all()], total
    except Exception:
        pass

    # Fallback: tsvector
    try:
        ts_query = " | ".join(w for w in query_text.split() if len(w) >= 2)
        if not ts_query:
            ts_query = query_text
        async with engine.begin() as conn:
            count_row = await conn.execute(sa_text("""
                SELECT COUNT(*) FROM messages
                WHERE domain_id = ANY(:ids)
                  AND content_tsv @@ to_tsquery('russian', :tsq)
            """), {"ids": list(domain_ids), "tsq": ts_query})
            total = count_row.scalar() or 0

            rows = await conn.execute(sa_text("""
                SELECT *, ts_rank(content_tsv, to_tsquery('russian', :tsq)) AS bm25_score
                FROM messages
                WHERE domain_id = ANY(:ids)
                  AND content_tsv @@ to_tsquery('russian', :tsq)
                ORDER BY bm25_score DESC
                LIMIT :lim
            """), {"ids": list(domain_ids), "tsq": ts_query, "lim": limit})
            return [dict(r) for r in rows.mappings().all()], total
    except Exception:
        pass

    # Last resort: ILIKE
    keywords = [w for w in query_text.split() if len(w) >= 3]
    if not keywords:
        return [], 0
    from sqlalchemy import or_
    conditions = [messages.c.content.ilike(f"%{kw}%") for kw in keywords]
    async with engine.begin() as conn:
        count_stmt = (
            select(func.count())
            .select_from(messages)
            .where(messages.c.domain_id.in_(domain_ids), or_(*conditions))
        )
        total = (await conn.execute(count_stmt)).scalar() or 0
        data_stmt = (
            select(messages)
            .where(messages.c.domain_id.in_(domain_ids), or_(*conditions))
            .order_by(messages.c.msg_date.desc())
            .limit(limit)
        )
        rows = (await conn.execute(data_stmt)).mappings().all()
        return [dict(r) | {"bm25_score": 1.0} for r in rows], total


# ---------------------------------------------------------------------------
# Multi-domain active schemas
# ---------------------------------------------------------------------------

async def get_active_schemas_multi(
    engine: AsyncEngine,
    domain_ids: list[UUID],
) -> list[dict]:
    """Get active schemas for multiple domains."""
    if not domain_ids:
        return []
    stmt = (
        select(channel_schemas)
        .where(
            channel_schemas.c.domain_id.in_(domain_ids),
            channel_schemas.c.is_active.is_(True),
        )
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Messages since date (for digest)
# ---------------------------------------------------------------------------

async def get_messages_since(
    engine: AsyncEngine,
    domain_ids: list[UUID],
    since: "datetime",
    limit: int = 5000,
) -> list[dict]:
    """Get messages across domains since a given datetime, newest first."""
    if not domain_ids:
        return []
    stmt = (
        select(messages)
        .where(
            messages.c.domain_id.in_(domain_ids),
            messages.c.msg_date >= since,
            messages.c.is_noise.is_(False),
        )
        .order_by(messages.c.msg_date.desc())
        .limit(limit)
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Hashtag summaries multi-domain
# ---------------------------------------------------------------------------

async def get_hashtag_summaries_multi(
    engine: AsyncEngine,
    domain_ids: list[UUID],
    hashtag: str,
) -> list[dict]:
    """Get hashtag summaries across multiple domains."""
    if not domain_ids:
        return []
    stmt = (
        select(hashtag_summaries)
        .where(
            hashtag_summaries.c.domain_id.in_(domain_ids),
            hashtag_summaries.c.hashtag == hashtag,
        )
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
        return [dict(r) for r in rows]


async def recover_stuck_sync_jobs(engine: AsyncEngine) -> int:
    """Mark all 'running' sync jobs as 'failed'. Returns count."""
    stmt = (
        update(sync_jobs)
        .where(sync_jobs.c.status == "running")
        .values(status="failed", error_message="Recovered: process restarted")
    )
    async with engine.begin() as conn:
        result = await conn.execute(stmt)
        return result.rowcount


async def get_domains_needing_pipeline(engine: AsyncEngine) -> list[dict]:
    """Get active domains that have messages but pipeline hasn't completed.

    Detects two cases:
    1. Messages in PG but no active schema
    2. Messages in PG with schema but 0 entities (pipeline failed mid-way)
    """
    has_messages = (
        select(func.count())
        .where(messages.c.domain_id == domains.c.id)
        .correlate(domains)
        .scalar_subquery()
    )
    has_schema = (
        select(func.count())
        .where(
            channel_schemas.c.domain_id == domains.c.id,
            channel_schemas.c.is_active.is_(True),
        )
        .correlate(domains)
        .scalar_subquery()
    )
    stmt = (
        select(domains)
        .where(
            domains.c.is_active.is_(True),
            has_messages > 0,
            # No schema OR schema exists but 0 entities extracted
            (has_schema == 0) | (
                (has_schema > 0) & (func.coalesce(domains.c.entity_count, 0) == 0)
            ),
        )
    )
    async with engine.begin() as conn:
        rows = (await conn.execute(stmt)).mappings().all()
        return [dict(r) for r in rows]


async def update_sync_job(
    engine: AsyncEngine,
    job_id: UUID,
    **kwargs,
) -> dict | None:
    if not kwargs:
        return None
    stmt = (
        update(sync_jobs)
        .where(sync_jobs.c.id == job_id)
        .values(**kwargs)
        .returning(*sync_jobs.c)
    )
    async with engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None
