"""MCP server tools — mounted as Streamable HTTP inside FastAPI on /mcp.

Auth flow:
1. Claude Code connects → sees OAuth metadata → opens browser
2. User enters API key on auth page
3. Claude Code gets Bearer token (= the API key)
4. All tool calls include Bearer token → we extract owner_id
"""

from __future__ import annotations

import asyncio
import hashlib
import json

import structlog
from fastmcp import FastMCP, Context

from agent_memory_mcp.memory_api import service
from agent_memory_mcp.memory_api.auth import get_api_key_by_hash, CREDIT_COSTS
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db.engine import async_engine

log = structlog.get_logger(__name__)

mcp = FastMCP(
    "agent-memory-mcp",
    instructions=(
        "Agent Memory MCP — persistent Telegram memory for AI agents.\n\n"
        "TWO MODES OF USE:\n"
        "1. HIGH-LEVEL (one call, AI-generated answer): search_memory, get_digest, get_decisions\n"
        "2. LOW-LEVEL (raw data, you control the strategy):\n"
        "   - keyword_search: BM25 full-text, exact terms/names/hashtags\n"
        "   - vector_search: semantic similarity by meaning\n"
        "   - graph_query: knowledge graph in natural language (entities, relationships)\n"
        "   - read_messages: full text by message IDs (after search)\n"
        "   - get_schema: entity/relation types in the graph\n\n"
        "STRATEGY: For simple questions use search_memory. For precise control, "
        "combine keyword_search + vector_search, then read_messages for full text. "
        "Use graph_query for 'who/what is connected to X' questions.\n\n"
        "MANAGEMENT: add_source, list_sources, remove_source, list_folders, sync_status, check_telegram_auth"
    ),
)

# Cache: key_hash → (api_key record, timestamp) — 5 min TTL
import time as _time
_key_cache: dict[str, tuple[dict, float]] = {}
_KEY_CACHE_TTL = 300


async def _resolve_owner(ctx: Context | None) -> int:
    """Extract owner_id from the Bearer token (API key) in MCP request."""
    key = await _resolve_api_key(ctx)
    if key:
        return key["telegram_id"]
    # No key found — check middleware auth (Bearer was validated there already)
    # The middleware in app.py already rejects unauthorized requests with 401,
    # so if we get here, there's an auth header but we can't parse it.
    # Last resort: read from the HTTP request directly
    try:
        from fastmcp.server.dependencies import get_http_request
        request = get_http_request()
        auth = request.headers.get("authorization", "")
        token = auth.removeprefix("Bearer ").strip()
        if token:
            # Try direct DB lookup
            key_hash = hashlib.sha256(token.encode()).hexdigest()
            api_key = await get_api_key_by_hash(async_engine, key_hash)
            if api_key and api_key["is_active"]:
                _key_cache[key_hash] = (api_key, _time.monotonic())
                return api_key["telegram_id"]
    except Exception:
        pass
    log.warning("resolve_owner_failed_no_key")
    from agent_memory_mcp.config import settings
    return settings.admin_telegram_id


async def _resolve_api_key(ctx: Context | None) -> dict | None:
    """Get full API key record from Bearer token."""
    api_key_raw = None
    try:
        from fastmcp.server.dependencies import get_http_request
        request = get_http_request()
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            api_key_raw = auth_header.removeprefix("Bearer ").strip()
    except Exception as e:
        log.debug("get_http_request_failed", error=str(e))

    if not api_key_raw:
        log.debug("no_api_key_in_request")
        return None

    key_hash = hashlib.sha256(api_key_raw.encode()).hexdigest()

    if key_hash in _key_cache:
        cached, ts = _key_cache[key_hash]
        if _time.monotonic() - ts < _KEY_CACHE_TTL:
            return cached
        del _key_cache[key_hash]

    api_key = await get_api_key_by_hash(async_engine, key_hash)
    if not api_key or not api_key["is_active"]:
        return None

    _key_cache[key_hash] = (api_key, _time.monotonic())
    return api_key


def _admin_id() -> int:
    from agent_memory_mcp.config import settings
    return settings.admin_telegram_id


async def _charge(ctx: Context | None, credits: int, endpoint: str) -> None:
    """Charge credits for an API call. Uses Bearer token to identify user."""
    if credits <= 0:
        return
    key = await _resolve_api_key(ctx)
    if not key:
        log.warning("charge_skipped_no_key", endpoint=endpoint, credits=credits)
        return
    # Admin is exempt from billing
    if key.get("telegram_id") == _admin_id():
        return
    try:
        from agent_memory_mcp.memory_api.auth import charge_credits
        await charge_credits(async_engine, key["id"], credits, endpoint)
    except Exception:
        log.warning("charge_credits_failed", key_id=str(key["id"]), credits=credits, exc_info=True)


def _ok(result, credits_used: int = 0) -> str:
    data = result if isinstance(result, dict) else {"data": result}
    if credits_used:
        data["credits_used"] = credits_used
    return json.dumps(data, ensure_ascii=False, default=str)


@mcp.tool()
async def search_memory(query: str, scope: str | None = None, limit: int = 10, since: str | None = None, ctx: Context = None) -> str:
    """Search Telegram memory by semantic query.

    Args:
        query: What to search for in the memory.
        scope: Optional scope. "@username" for one channel, "folder:Name" for a folder, or omit for all sources.
        limit: Maximum number of source references to return (default 10).
        since: Optional time filter. Examples: "2d" (last 2 days), "1w" (last week), "2026-03-23" (since date). Only returns messages after this date.

    Returns:
        Answer based on memory with source references.
    """
    owner_id = await _resolve_owner(ctx)
    result = await service.search_memory(query=query, owner_id=owner_id, scope=scope, limit=limit, since=since)
    await _charge(ctx, 3, "search")
    return _ok(result, credits_used=3)


@mcp.tool()
async def get_digest(scope: str, period: str = "7d", ctx: Context = None) -> str:
    """Get a digest of Telegram conversations for a period.

    For large channels this may take 1-2 minutes (embedding + clustering + LLM).
    If it takes too long, use keyword_search or vector_search for targeted queries instead.

    Args:
        scope: Source scope — "@username", "folder:Name", or domain_id.
        period: Time period for the digest: 1d, 3d, 7d, or 30d. Default: 7d.

    Returns:
        Structured digest with key topics and highlights.
    """
    owner_id = await _resolve_owner(ctx)
    try:
        result = await asyncio.wait_for(
            service.get_digest(owner_id=owner_id, scope=scope, period=period),
            timeout=180,
        )
    except asyncio.TimeoutError:
        result = {"digest": "Digest generation timed out. Try a shorter period or specific channel.", "period": period}
    await _charge(ctx, 25, "digest")
    return _ok(result, credits_used=25)


@mcp.tool()
async def get_decisions(scope: str, topic: str | None = None, ctx: Context = None) -> str:
    """Extract decisions, action items, and open questions from conversations.

    Args:
        scope: Source scope — "@username", "folder:Name", or domain_id.
        topic: Optional topic to filter decisions by.

    Returns:
        List of decisions, action items, and unresolved questions.
    """
    owner_id = await _resolve_owner(ctx)
    result = await service.get_decisions(owner_id=owner_id, scope=scope, topic=topic)
    await _charge(ctx, 12, "decisions")
    return _ok(result, credits_used=12)


@mcp.tool()
async def add_source(handle: str, source_type: str = "channel", sync_range: str = "3m", ctx: Context = None) -> str:
    """Connect a Telegram channel, group, or entire folder as a memory source.

    For single channels: handle = @username or t.me/link
    For folders: set source_type="folder" and handle = folder name (use list_folders to see available).
    Adding a folder imports ALL channels in it at once.

    Args:
        handle: Channel @username (or folder name when source_type="folder").
        source_type: "channel" for single channel, "folder" to import entire Telegram folder.
        sync_range: How far back to sync: 1w, 1m, 3m, 6m, or 1y. Default: 3m.

    Returns:
        Status of the source addition. For folders: list of added and skipped channels.
    """
    owner_id = await _resolve_owner(ctx)
    result = await service.add_source(
        owner_id=owner_id, handle=handle, source_type=source_type, sync_range=sync_range,
    )
    return _ok(result)


@mcp.tool()
async def list_folders(ctx: Context = None) -> str:
    """List user's Telegram folders with their channels.

    Use this to discover available folders before adding them with add_source(source_type="folder").

    Returns:
        List of Telegram folders with channel names and counts.
    """
    owner_id = await _resolve_owner(ctx)
    folders = await service.list_folders(owner_id=owner_id)
    return _ok({"folders": folders, "count": len(folders)})


@mcp.tool()
async def list_sources(ctx: Context = None) -> str:
    """List all connected memory sources (channels, groups, folders).

    Returns:
        List of sources with sync status and message counts.
    """
    owner_id = await _resolve_owner(ctx)
    sources = await service.list_sources(owner_id=owner_id)
    return _ok({"sources": sources, "count": len(sources)})


@mcp.tool()
async def check_telegram_auth(ctx: Context = None) -> str:
    """Check if the user has connected their Telegram account.

    Returns:
        Connection status. If not connected, includes a link to the bot for authorization.
    """
    owner_id = await _resolve_owner(ctx)
    result = await service.check_telegram_auth(owner_id=owner_id)
    return _ok(result)


@mcp.tool()
async def sync_status(ctx: Context = None) -> str:
    """Check synchronization status of all connected sources.

    Use this after add_source to monitor sync progress.
    Shows status of each source: pending, running, completed, or failed.

    Returns:
        List of sources with their current sync job status and progress.
    """
    owner_id = await _resolve_owner(ctx)
    result = await service.sync_status(owner_id=owner_id)
    return _ok(result)


@mcp.tool()
async def remove_source(source_id: str, ctx: Context = None) -> str:
    """Remove a connected memory source.

    Args:
        source_id: Domain ID of the source to remove (from list_sources or sync_status).

    Returns:
        Confirmation of removal.
    """
    owner_id = await _resolve_owner(ctx)
    from uuid import UUID
    try:
        domain_id = UUID(source_id)
    except ValueError:
        return _ok({"status": "error", "message": "Invalid source_id format"})
    domain = await db_q.get_domain(async_engine, domain_id)
    if not domain or domain["owner_id"] != owner_id:
        return _ok({"status": "error", "message": "Source not found"})
    await db_q.delete_domain(async_engine, domain_id)
    return _ok({"status": "removed", "channel": f"@{domain.get('channel_username', '')}"})


@mcp.tool()
async def get_agent_context(task: str, scope: str, ctx: Context = None) -> str:
    """Get a full context package for an agent task.

    Combines search results, digest, graph data, and decisions into
    a comprehensive context that an agent can use to accomplish a task.

    Args:
        task: Description of what the agent needs to accomplish.
        scope: Source scope — "@username", "folder:Name", or domain_id.

    Returns:
        Structured context package with all relevant information.
    """
    owner_id = await _resolve_owner(ctx)
    result = await service.get_agent_context(owner_id=owner_id, task=task, scope=scope)
    await _charge(ctx, 15, "agent_context")
    return _ok(result, credits_used=15)


# =========================================================================
# LOW-LEVEL RETRIEVAL TOOLS
#
# Direct access to search layers. External agents can compose their own
# retrieval strategy using these building blocks.
# Each returns raw data — no LLM processing, just search results.
# =========================================================================

async def _get_channel_ids(owner_id: int, scope: str | None) -> list[int]:
    """Helper: resolve scope → channel_ids for search filtering."""
    domain_ids = await service._resolve_scope(owner_id, scope)
    channels = []
    for did in domain_ids:
        d = await db_q.get_domain(async_engine, did)
        if d:
            channels.append(d["channel_id"])
    return channels


@mcp.tool()
async def keyword_search(query: str, scope: str | None = None, limit: int = 50, since: str | None = None, ctx: Context = None) -> str:
    """BM25 full-text search over messages. Best for exact terms, names, hashtags.

    Uses ParadeDB BM25 index with Russian stemming. Falls back to tsvector, then ILIKE.
    Returns raw message snippets with relevance scores — no LLM processing.

    Args:
        query: Search keywords (exact terms work best: names, hashtags, specific phrases).
        scope: "@channel", "folder:Name", or omit for all sources.
        limit: Max results (default 50).
        since: Time filter: "2d", "1w", "1m", "2026-03-23". Only messages after this date.

    Returns:
        List of matching messages with BM25 scores, dates, and channel info.
    """
    owner_id = await _resolve_owner(ctx)
    domain_ids = await service._resolve_scope(owner_id, scope)
    if not domain_ids:
        return _ok({"results": [], "total": 0})

    since_dt = service._parse_since(since)

    from agent_memory_mcp.db.queries import search_messages_bm25_multi
    rows, total = await search_messages_bm25_multi(async_engine, domain_ids, query, limit=limit)

    # Filter by date if specified
    if since_dt:
        rows = [r for r in rows if r.get("msg_date") and r["msg_date"] >= since_dt]
        total = len(rows)

    results = [
        {
            "id": str(r["id"]),
            "content": (r.get("content") or "")[:500],
            "score": round(r.get("bm25_score", 0), 3),
            "date": str(r["msg_date"]) if r.get("msg_date") else None,
            "channel_id": r.get("channel_id"),
            "sender": r.get("sender_name"),
        }
        for r in rows[:limit]
    ]
    await _charge(ctx, 1, "keyword_search")
    return _ok({"results": results, "total": total, "query": query}, credits_used=1)


@mcp.tool()
async def vector_search(query: str, scope: str | None = None, limit: int = 30, since: str | None = None, ctx: Context = None) -> str:
    """Semantic vector search. Finds relevant content by meaning, not just keywords.

    Uses BGE-M3 embeddings (1024-dim) with Milvus hybrid search (dense + sparse BM25).
    Best for conceptual queries: "discussions about visa problems", "opinions on taxes".

    Args:
        query: Natural language query (concepts, topics, questions).
        scope: "@channel", "folder:Name", or omit for all sources.
        limit: Max results (default 30).
        since: Time filter: "2d", "1w", "1m", "2026-03-23".

    Returns:
        List of semantically similar messages with similarity scores.
    """
    owner_id = await _resolve_owner(ctx)
    channel_ids = await _get_channel_ids(owner_id, scope)
    if not channel_ids:
        return _ok({"results": [], "total": 0})

    from agent_memory_mcp.storage.milvus_client import MilvusStorage
    from agent_memory_mcp.storage.embedding_client import EmbeddingClient

    embedder = EmbeddingClient()
    milvus = MilvusStorage()
    try:
        vectors = await embedder.embed([query])
        dense = vectors[0]

        since_dt = service._parse_since(since)
        if since_dt:
            from datetime import datetime, timezone
            hits = milvus.search_temporal(
                dense, channel_ids,
                date_from=int(since_dt.timestamp()),
                date_to=int(datetime.now(timezone.utc).timestamp()),
                limit=limit,
            )
        else:
            hits = milvus.search_multi_channel(dense, channel_ids, limit=limit, query_text=query)
    finally:
        milvus.close()
        await embedder.close()

    results = [
        {
            "id": str(h.get("id", "")),
            "content": (h.get("content") or "")[:500],
            "score": round(h.get("score", 0), 4),
            "date": str(h["msg_date"]) if h.get("msg_date") else None,
            "channel_id": h.get("channel_id"),
        }
        for h in hits
    ]
    await _charge(ctx, 1, "vector_search")
    return _ok({"results": results, "total": len(results), "query": query}, credits_used=1)


@mcp.tool()
async def graph_query(question: str, scope: str | None = None, ctx: Context = None) -> str:
    """Query the knowledge graph in natural language. Converts to Cypher automatically.

    The graph contains entities (people, organizations, projects, locations, etc.)
    and relationships extracted from messages by LLM. Useful for:
    - "Who is connected to X?"
    - "What projects does company Y have?"
    - "List all people mentioned"
    - "How many organizations are in the data?"

    Args:
        question: Natural language question about entities and their relationships.
        scope: "@channel", "folder:Name", or omit for all sources.

    Returns:
        Graph query results — entities, relationships, counts.
    """
    owner_id = await _resolve_owner(ctx)
    domain_ids = await service._resolve_scope(owner_id, scope)
    if not domain_ids:
        return _ok({"results": [], "message": "No sources"})

    from agent_memory_mcp.storage.falkordb_client import FalkorDBStorage
    from agent_memory_mcp.llm.client import llm_call
    from agent_memory_mcp.config import settings
    import re

    graph = FalkorDBStorage()
    domain_id_str = str(domain_ids[0])

    # Get schema for context
    schema = await db_q.get_active_schema(async_engine, domain_ids[0])
    schema_hint = ""
    if schema:
        s = schema.get("schema_json", schema)
        et = s.get("entity_types", [])
        rt = s.get("relation_types", [])
        if et:
            schema_hint += "Entity types: " + ", ".join(e.get("name", "") for e in et[:10])
        if rt:
            schema_hint += "\nRelation types: " + ", ".join(r.get("name", "") for r in rt[:10])

    # Generate Cypher via LLM
    try:
        cypher = await llm_call(
            model=settings.llm_tier1_model,
            messages=[
                {"role": "system", "content": f"Convert to Cypher for FalkorDB. Graph name: {graph._graph_name}. {schema_hint}\nOnly READ queries (MATCH/RETURN). No CREATE/DELETE/SET."},
                {"role": "user", "content": question},
            ],
            temperature=0.0,
            max_tokens=500,
        )
        # Extract cypher from markdown if wrapped
        cypher = cypher.strip()
        if "```" in cypher:
            m = re.search(r"```(?:cypher)?\s*(.*?)```", cypher, re.DOTALL)
            if m:
                cypher = m.group(1).strip()

        # Safety check
        if re.search(r"\b(CREATE|DELETE|SET|MERGE|REMOVE|DROP)\b", cypher, re.IGNORECASE):
            return _ok({"error": "Write operations not allowed", "cypher": cypher})

        rows = await graph.execute_cypher(cypher)
        graph.close()

        results = [dict(r) if hasattr(r, 'items') else r for r in rows[:50]]
        return _ok({"results": results, "cypher": cypher, "count": len(results)}, credits_used=2)
    except Exception as e:
        graph.close()
        return _ok({"error": str(e), "question": question})


@mcp.tool()
async def read_messages(message_ids: list[str], ctx: Context = None) -> str:
    """Read full message content by IDs. Use after search to get complete text.

    Search tools return truncated snippets (500 chars). Use this to read
    the full content of specific messages you want to examine in detail.

    Args:
        message_ids: List of message UUIDs from search results.

    Returns:
        Full message content with metadata (sender, date, channel).
    """
    if not message_ids:
        return _ok({"messages": []})

    rows = await db_q.get_messages_by_ids(async_engine, message_ids[:20])

    messages = [
        {
            "id": str(r["id"]),
            "content": r.get("content", ""),
            "sender": r.get("sender_name"),
            "date": str(r["msg_date"]) if r.get("msg_date") else None,
            "channel_id": r.get("channel_id"),
            "telegram_msg_id": r.get("telegram_msg_id"),
        }
        for r in rows
    ]
    return _ok({"messages": messages, "count": len(messages)})


@mcp.tool()
async def get_schema(scope: str | None = None, ctx: Context = None) -> str:
    """Get the knowledge graph schema — entity types, relation types, domain info.

    Shows what kinds of entities and relationships have been extracted
    from the synced channels. Useful for understanding what graph_query can answer.

    Args:
        scope: "@channel" or omit for first source.

    Returns:
        Entity types with examples, relation types, detected domain.
    """
    owner_id = await _resolve_owner(ctx)
    domain_ids = await service._resolve_scope(owner_id, scope)
    if not domain_ids:
        return _ok({"schema": None, "message": "No sources"})

    schemas = []
    for did in domain_ids[:5]:
        s = await db_q.get_active_schema(async_engine, did)
        if s:
            d = await db_q.get_domain(async_engine, did)
            schemas.append({
                "channel": f"@{d['channel_username']}" if d else "?",
                "domain_type": s.get("detected_domain") if isinstance(s, dict) else s.get("schema_json", {}).get("detected_domain"),
                "entity_types": (s.get("entity_types") or s.get("schema_json", {}).get("entity_types", []))[:15],
                "relation_types": (s.get("relation_types") or s.get("schema_json", {}).get("relation_types", []))[:10],
            })

    return _ok({"schemas": schemas, "count": len(schemas)})
