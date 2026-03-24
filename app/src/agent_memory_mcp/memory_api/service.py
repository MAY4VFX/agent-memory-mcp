"""Service layer — bridges Memory API routes to existing pipeline functions."""

from __future__ import annotations

from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db import queries_groups as db_g
from agent_memory_mcp.db.engine import async_engine
from agent_memory_mcp.storage.embedding_client import EmbeddingClient
from agent_memory_mcp.storage.falkordb_client import FalkorDBStorage
from agent_memory_mcp.storage.milvus_client import MilvusStorage
from agent_memory_mcp.storage.reranker_client import RerankerClient

log = structlog.get_logger(__name__)


async def search_memory(
    query: str,
    owner_id: int,
    scope: str | None = None,
    limit: int = 10,
) -> dict:
    """Search memory using agent pipeline. Returns answer + sources."""
    from agent_memory_mcp.db import queries_conversations as qc
    from agent_memory_mcp.pipeline.agent_orchestrator import run_agent_pipeline

    # Resolve domain(s) for this owner
    domain_ids = await _resolve_scope(owner_id, scope)
    if not domain_ids:
        return {"answer": "Нет подключённых источников. Добавь канал через агента.", "sources": []}

    domain_id = domain_ids[0]
    domain = await db_q.get_domain(async_engine, domain_id)

    # Create temp conversation
    conv = await qc.create_conversation(
        async_engine, user_id=owner_id, domain_id=domain_id,
        title=f"[api] {query[:40]}",
    )

    milvus = MilvusStorage()
    graph = FalkorDBStorage()
    embedder = EmbeddingClient()
    reranker = RerankerClient()

    try:
        answer, payload = await run_agent_pipeline(
            query=query,
            user_id=owner_id,
            conversation_id=conv["id"],
            domain_ids=domain_ids,
            engine=async_engine,
            milvus=milvus,
            graph=graph,
            embedder=embedder,
            reranker=reranker,
        )
    finally:
        milvus.close()
        graph.close()
        await embedder.close()
        await reranker.close()
        await qc.delete_conversation(async_engine, conv["id"])

    sources = [
        {"msg_id": s.message_id, "url": s.url, "channel": s.channel_username}
        for s in (answer.sources or [])[:limit]
    ]
    return {"answer": answer.answer, "sources": sources}


async def list_sources(owner_id: int) -> list[dict]:
    """List all sources (domains) for a user."""
    domains = await db_q.get_domains_by_owner(async_engine, owner_id)
    return [
        {
            "id": str(d["id"]),
            "channel_username": d.get("channel_username"),
            "display_name": d.get("display_name"),
            "message_count": d.get("message_count", 0),
            "sync_depth": d.get("sync_depth"),
            "last_synced": str(d["last_synced_at"]) if d.get("last_synced_at") else None,
        }
        for d in domains
    ]


async def add_source(
    owner_id: int,
    handle: str,
    source_type: str = "channel",
    sync_range: str = "3m",
) -> dict:
    """Add a Telegram source for a user."""
    # This will be called by the agent tool — connects to user's Telethon session
    # For now, resolve channel and create domain
    return {"status": "queued", "handle": handle, "sync_range": sync_range}


async def get_digest(owner_id: int, scope: str, period: str = "7d") -> dict:
    """Generate a digest for a scope and period."""
    return {"digest": "Digest generation not yet implemented", "period": period}


async def get_decisions(owner_id: int, scope: str, topic: str | None = None) -> dict:
    """Extract decisions from memory."""
    return {"decisions": [], "topic": topic}


async def get_agent_context(owner_id: int, task: str, scope: str) -> dict:
    """Build a full context package for an agent task."""
    return {"context": "Context package not yet implemented", "task": task}


async def _resolve_scope(owner_id: int, scope: str | None) -> list[UUID]:
    """Resolve a scope string to domain IDs for the owner."""
    domains = await db_q.get_domains_by_owner(async_engine, owner_id)
    if not domains:
        return []

    if not scope:
        # All domains for this owner
        return [d["id"] for d in domains]

    # Try to match by channel_username
    for d in domains:
        username = d.get("channel_username", "")
        if scope.lstrip("@").lower() == username.lower():
            return [d["id"]]

    # Try UUID
    try:
        uid = UUID(scope)
        return [uid]
    except ValueError:
        pass

    # Default: all
    return [d["id"] for d in domains]
