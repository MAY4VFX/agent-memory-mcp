"""MCP server tools — mounted as Streamable HTTP inside FastAPI on /mcp.

Auth flow:
1. Claude Code connects → sees OAuth metadata → opens browser
2. User enters API key on auth page
3. Claude Code gets Bearer token (= the API key)
4. All tool calls include Bearer token → we extract owner_id
"""

from __future__ import annotations

import asyncio
import json

import structlog
from fastmcp import FastMCP, Context

from agent_memory_mcp.memory_api import service
from agent_memory_mcp.memory_api.service import ScopeNotFound
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db.engine import async_engine
from agent_memory_mcp.models.messages import resolve_sender_label_from_row

log = structlog.get_logger(__name__)

mcp = FastMCP(
    "agent-memory-mcp",
    instructions=(
        "Agent Memory MCP — persistent Telegram memory for AI agents.\n\n"
        "TWO MODES OF USE:\n"
        "1. HIGH-LEVEL (one call, AI-generated answer): search_memory, get_digest, get_decisions\n"
        "2. LOW-LEVEL (raw data, you control the strategy):\n"
        "   - fetch_messages: bulk RAW messages by author/topic/time, paginated (no LLM)\n"
        "   - list_topics: discover supergroup forum topics (topic_id) to filter by\n"
        "   - keyword_search: BM25 full-text, exact terms/names/hashtags\n"
        "   - vector_search: semantic similarity by meaning\n"
        "   - graph_query: knowledge graph in natural language (entities, relationships)\n"
        "   - read_messages: full text by message IDs (after search)\n"
        "   - get_schema: entity/relation types in the graph\n\n"
        "STRATEGY: For simple questions use search_memory. To pull EVERYTHING a "
        "person wrote or a whole topic, use fetch_messages (page via next_cursor) — "
        "do NOT use get_digest/get_agent_context for bulk retrieval. For precise "
        "control, combine keyword_search + vector_search, then read_messages for "
        "full text. Use graph_query for 'who/what is connected to X' questions.\n\n"
        "SCOPING: Most tools accept a 'scope' parameter. Call list_scopes FIRST to discover "
        "valid values: \"all\" for everything, \"folder:Name\" for channel groups, \"@username\" "
        "for individual channels.\n\n"
        "MANAGEMENT: add_source, list_sources, list_scopes, remove_source, list_folders, sync_status, check_telegram_auth"
    ),
)

async def _resolve_owner(ctx: Context | None) -> int:
    """Extract owner_id from the Bearer token (API key) in the MCP request.

    Raises PermissionError if no valid key is present. The auth middleware
    (app.py) already rejects unauthenticated /mcp requests, so this is
    defense-in-depth — never fall back to an admin identity.
    """
    key = await _resolve_api_key(ctx)
    if key:
        return key["telegram_id"]
    log.warning("resolve_owner_failed_no_key")
    raise PermissionError("Unauthorized: valid API key required")


async def _resolve_api_key(ctx: Context | None) -> dict | None:
    """Get full API key record from the Bearer token via the shared validator."""
    from agent_memory_mcp.memory_api.auth import validate_bearer_token

    try:
        from fastmcp.server.dependencies import get_http_request
        request = get_http_request()
        auth_header = request.headers.get("authorization", "")
    except Exception as e:
        log.debug("get_http_request_failed", error=str(e))
        return None

    return await validate_bearer_token(auth_header)


def _admin_id() -> int:
    from agent_memory_mcp.config import settings
    return settings.admin_telegram_id


async def _charge(ctx: Context | None, credits: int, endpoint: str) -> None:
    """Charge credits for an API call. Uses Bearer token to identify user."""
    if credits <= 0:
        return
    key = await _resolve_api_key(ctx)
    if not key:
        log.warning("charge_denied_no_key", endpoint=endpoint, credits=credits)
        raise PermissionError("Unauthorized: valid API key required")
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


def _scope_not_found_response(e: ScopeNotFound) -> str:
    """Format a helpful error when scope is not found."""
    hints = e.available[:15] if e.available else []
    return _ok({
        "error": "scope_not_found",
        "message": f"Scope '{e.scope}' not found. Use list_scopes to see available options.",
        "available_scopes": hints,
    })


@mcp.tool()
async def search_memory(query: str, scope: str | None = None, limit: int = 10, since: str | None = None, ctx: Context = None) -> str:
    """Search Telegram memory by semantic query.

    Args:
        query: What to search for in the memory.
        scope: Optional scope — "all", "@username", "folder:Name", or domain_id. Use list_scopes to see available options. Omit for all sources.
        limit: Maximum number of source references to return (default 10).
        since: Optional time filter. Examples: "2d" (last 2 days), "1w" (last week), "2026-03-23" (since date). Only returns messages after this date.

    Returns:
        Answer based on memory with source references.
    """
    owner_id = await _resolve_owner(ctx)
    try:
        result = await service.search_memory(query=query, owner_id=owner_id, scope=scope, limit=limit, since=since)
    except ScopeNotFound as e:
        return _scope_not_found_response(e)
    await _charge(ctx, 3, "search")
    return _ok(result, credits_used=3)


@mcp.tool()
async def get_digest(scope: str, period: str = "7d", focus: str | None = None, ctx: Context = None) -> str:
    """Get a digest of Telegram conversations for a period.

    For large channels this may take 1-2 minutes (embedding + clustering + LLM).
    If it takes too long, use keyword_search or vector_search for targeted queries instead.

    Args:
        scope: Source scope — "all", "@username", "folder:Name", or domain_id. Use list_scopes to see available options.
        period: Time period for the digest: 1d, 3d, 7d, or 30d. Default: 7d.
        focus: Optional free-text instruction steering what to extract/emphasize.
            Default extraction targets news/announcements/products and drops
            chat-like replies, questions and opinions. Set a focus to override
            that — e.g. for a work chat: "deadlines, agreements, decisions, open
            questions, blockers". Omit for the default news policy.

    Returns:
        JSON with `digest`, `topics[]` and `links[]`.

        PRESENTATION: show the `digest` field VERBATIM to the user — it is
        ready-to-display markdown where each bullet ends with a clickable
        markdown arrow "[→](url)". Do NOT rewrite it into "source: <number>
        (url)" form; keep the small arrow and do not surface raw message ids
        or URLs. Use `topics[]`/`links[]` only for programmatic access
        (each bullet has text, telegram_msg_ids[], source_message_ids[],
        links[]). Private-channel bullets use a t.me/c/ link, or an explicit
        "url unavailable" note when no link can be built.
    """
    owner_id = await _resolve_owner(ctx)
    try:
        result = await asyncio.wait_for(
            service.get_digest(owner_id=owner_id, scope=scope, period=period, focus=focus),
            timeout=180,
        )
    except ScopeNotFound as e:
        return _scope_not_found_response(e)
    except asyncio.TimeoutError:
        result = {"digest": "Digest generation timed out. Try a shorter period or specific channel.", "period": period}
    await _charge(ctx, 25, "digest")
    return _ok(result, credits_used=25)


@mcp.tool()
async def get_decisions(scope: str, topic: str | None = None, ctx: Context = None) -> str:
    """Extract decisions, action items, and open questions from conversations.

    Args:
        scope: Source scope — "all", "@username", "folder:Name", or domain_id. Use list_scopes to see available options.
        topic: Optional topic to filter decisions by.

    Returns:
        List of decisions, action items, and unresolved questions.
    """
    owner_id = await _resolve_owner(ctx)
    try:
        result = await service.get_decisions(owner_id=owner_id, scope=scope, topic=topic)
    except ScopeNotFound as e:
        return _scope_not_found_response(e)
    await _charge(ctx, 12, "decisions")
    return _ok(result, credits_used=12)


@mcp.tool()
async def add_source(
    handle: str,
    source_type: str = "channel",
    sync_range: str = "3m",
    sync_frequency_minutes: int | None = None,
    ctx: Context = None,
) -> str:
    """Connect a Telegram channel, group, or entire folder as a memory source.

    Works with public @usernames/t.me links and private invite links
    (t.me/+hash or t.me/joinchat/hash — the account joins the chat).
    For folders: set source_type="folder" and handle = folder name (use list_folders to see available).
    Adding a folder imports ALL channels in it at once.

    Args:
        handle: Channel @username, t.me link, or private invite link (or folder name when source_type="folder").
        source_type: "channel" for single channel, "folder" to import entire Telegram folder.
        sync_range: How far back to sync the history: 1w, 1m, 3m, 6m, or 1y. Default: 3m.
        sync_frequency_minutes: How often to re-sync for new messages. Default 60, minimum 5.

    Returns:
        Status of the source addition. For folders: list of added and skipped channels.
    """
    owner_id = await _resolve_owner(ctx)
    result = await service.add_source(
        owner_id=owner_id, handle=handle, source_type=source_type, sync_range=sync_range,
        sync_frequency_minutes=sync_frequency_minutes,
    )
    return _ok(result)


@mcp.tool()
async def set_digest_schedule(
    frequency_hours: int,
    scope: str = "all",
    send_hour_utc: int = 8,
    focus: str | None = None,
    ctx: Context = None,
) -> str:
    """Schedule a recurring digest that the bot sends to the user automatically.

    Args:
        frequency_hours: How often to send the digest, in hours (e.g. 2 = every 2 hours, 24 = daily). Minimum 1.
        scope: What to digest — "all", "@username", "folder:Name", or a domain id. Default: "all".
        send_hour_utc: For daily/multi-day cadence (>= 24h), the UTC hour to send at (0-23). Ignored for sub-daily. Default: 8.
        focus: Optional free-text extraction focus applied to every run, e.g. for
            a work chat: "deadlines, agreements, decisions, open questions,
            blockers". Omit for the default news-oriented policy.

    Returns:
        The saved digest schedule.
    """
    owner_id = await _resolve_owner(ctx)
    try:
        result = await service.set_digest_schedule(
            owner_id=owner_id, frequency_hours=frequency_hours,
            scope=scope, send_hour_utc=send_hour_utc, focus=focus,
        )
    except ScopeNotFound as e:
        return _scope_not_found_response(e)
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
async def list_scopes(ctx: Context = None) -> str:
    """List all available scopes for get_digest, search_memory, and other tools.

    Call this FIRST to discover valid scope values before calling get_digest or search_memory.

    Returns:
        Available scopes: "all" for everything, "folder:Name" for channel groups, "@username" for individual channels.
    """
    owner_id = await _resolve_owner(ctx)
    from agent_memory_mcp.db import queries_groups as gq

    domains = await db_q.list_domains(async_engine, owner_id)
    groups = await gq.list_groups(async_engine, owner_id)

    scopes = [{"scope": "all", "label": f"All channels ({len(domains)})", "type": "all"}]

    for g in groups:
        members = await gq.get_group_domains(async_engine, g["id"])
        channels = [f"@{m.get('channel_username', '?')}" for m in members]
        scopes.append({
            "scope": f"folder:{g['name']}",
            "label": f"{g.get('emoji', '')} {g['name']} ({len(members)} channels)",
            "type": "folder",
            "channels": channels,
        })

    for d in domains:
        username = d.get("channel_username")
        if username:
            scopes.append({
                "scope": f"@{username}",
                "label": d.get("display_name") or d.get("channel_name") or username,
                "type": "channel",
                "message_count": d.get("message_count", 0),
            })

    return _ok({"scopes": scopes, "count": len(scopes)})


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
        scope: Source scope — "all", "@username", "folder:Name", or domain_id. Use list_scopes to see available options.

    Returns:
        Structured context package with all relevant information.
    """
    owner_id = await _resolve_owner(ctx)
    try:
        result = await service.get_agent_context(owner_id=owner_id, task=task, scope=scope)
    except ScopeNotFound as e:
        return _scope_not_found_response(e)
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
async def keyword_search(query: str, scope: str | None = None, limit: int = 50, since: str | None = None, topic_id: int | None = None, ctx: Context = None) -> str:
    """BM25 full-text search over messages. Best for exact terms, names, hashtags.

    Uses ParadeDB BM25 index with Russian stemming. Falls back to tsvector, then ILIKE.
    Returns raw message snippets with relevance scores — no LLM processing.

    Args:
        query: Search keywords (exact terms work best: names, hashtags, specific phrases).
        scope: "@channel", "folder:Name", or omit for all sources.
        limit: Max results (default 50).
        since: Time filter: "2d", "1w", "1m", "2026-03-23". Only messages after this date.
        topic_id: Restrict to one supergroup forum topic (see list_topics). Omit for all topics.

    Returns:
        List of matching messages with BM25 scores, dates, and channel info.
    """
    owner_id = await _resolve_owner(ctx)
    domain_ids = await service._resolve_scope(owner_id, scope)
    if not domain_ids:
        return _ok({"results": [], "total": 0})

    since_dt = service._parse_since(since)

    from agent_memory_mcp.db.queries import search_messages_bm25_multi
    rows, total = await search_messages_bm25_multi(async_engine, domain_ids, query, limit=limit, topic_id=topic_id)

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
            "topic_id": r.get("topic_id"),
            "sender": resolve_sender_label_from_row(r),
        }
        for r in rows[:limit]
    ]
    await _charge(ctx, 1, "keyword_search")
    return _ok({"results": results, "total": total, "query": query}, credits_used=1)


@mcp.tool()
async def vector_search(query: str, scope: str | None = None, limit: int = 30, since: str | None = None, topic_id: int | None = None, ctx: Context = None) -> str:
    """Semantic vector search. Finds relevant content by meaning, not just keywords.

    Uses BGE-M3 embeddings (1024-dim) with Milvus hybrid search (dense + sparse BM25).
    Best for conceptual queries: "discussions about visa problems", "opinions on taxes".

    Args:
        query: Natural language query (concepts, topics, questions).
        scope: "@channel", "folder:Name", or omit for all sources.
        limit: Max results (default 30).
        since: Time filter: "2d", "1w", "1m", "2026-03-23".
        topic_id: Restrict to one supergroup forum topic (see list_topics). Omit for all topics.

    Returns:
        List of semantically similar messages with similarity scores.
    """
    owner_id = await _resolve_owner(ctx)
    channel_ids = await _get_channel_ids(owner_id, scope)
    if not channel_ids:
        return _ok({"results": [], "total": 0})

    from agent_memory_mcp.storage.milvus_client import MilvusStorage
    from agent_memory_mcp.storage.embedding_client import EmbeddingClient

    # Milvus does not store the forum topic_id, so when filtering by topic we
    # over-fetch and post-filter against Postgres rather than reindex the vectors.
    fetch_limit = min(limit * 5, 200) if topic_id is not None else limit

    embedder = EmbeddingClient()
    milvus = MilvusStorage()
    try:
        dense = await embedder.embed_query(query)

        since_dt = service._parse_since(since)
        if since_dt:
            from datetime import datetime, timezone
            hits = milvus.search_temporal(
                dense, channel_ids,
                date_from=int(since_dt.timestamp()),
                date_to=int(datetime.now(timezone.utc).timestamp()),
                limit=fetch_limit,
            )
        else:
            hits = milvus.search_multi_channel(dense, channel_ids, limit=fetch_limit, query_text=query)
    finally:
        milvus.close()
        await embedder.close()

    # Post-filter by topic via a single Postgres lookup of the returned ids.
    if topic_id is not None and hits:
        try:
            hit_ids = [str(h.get("id", "")) for h in hits if h.get("id")]
            rows = await db_q.get_messages_by_ids(async_engine, hit_ids)
            topic_of = {str(r["id"]): r.get("topic_id") for r in rows}
            hits = [h for h in hits if topic_of.get(str(h.get("id", ""))) == topic_id][:limit]
        except Exception:
            log.warning("vector_search_topic_filter_failed", exc_info=True)
            hits = hits[:limit]

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

    from agent_memory_mcp.storage.falkordb_client import FalkorDBStorage, graph_name_for
    from agent_memory_mcp.llm.client import llm_call
    from agent_memory_mcp.config import settings
    import re

    graph = FalkorDBStorage()
    # Confine the whole query to this one tenant's isolated graph. Even if the
    # LLM emits an unfiltered MATCH, it can only see this domain's graph.
    domain_id = domain_ids[0]
    graph_name = graph_name_for(domain_id)

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
                {"role": "system", "content": f"Convert to Cypher for FalkorDB. Graph name: {graph_name}. {schema_hint}\nOnly READ queries (MATCH/RETURN). No CREATE/DELETE/SET."},
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

        rows = await graph.execute_cypher(cypher, domain_id)
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
            "sender": resolve_sender_label_from_row(r),
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


@mcp.tool()
async def fetch_messages(
    scope: str,
    sender: str | None = None,
    topic_id: int | None = None,
    since: str | None = None,
    until: str | None = None,
    cursor: str | None = None,
    limit: int = 200,
    ctx: Context = None,
) -> str:
    """Fetch RAW messages by author / topic / time window — no LLM, no search ranking.

    This is the tool for bulk retrieval like "give me everything <person> wrote in
    this chat" or "all messages in <topic>". Unlike get_digest / get_agent_context
    it does no embedding, clustering or LLM work — it's a single indexed DB query,
    so it stays well inside the timeout. Page through the full set by passing the
    returned `next_cursor` back in `cursor` until it comes back null.

    Args:
        scope: "@channel", "folder:Name", or "all". Use list_scopes to discover.
        sender: Case-insensitive substring of the author's name (e.g. "Кристина").
            Omit for any author.
        topic_id: Restrict to one supergroup forum topic (see list_topics).
        since: Start of window: "7d", "1m", "2026-03-23". Omit for no lower bound.
        until: End of window (exclusive): same formats. Omit for "up to now".
        cursor: Pagination cursor from a previous call's `next_cursor`.
        limit: Page size (default 200, max 1000). Messages come oldest-first.

    Returns:
        JSON with `messages[]` (full content, sender, date, topic_id, url),
        `count`, and `next_cursor` (null when the last page is reached).
    """
    owner_id = await _resolve_owner(ctx)
    try:
        result = await service.fetch_messages(
            owner_id=owner_id, scope=scope, sender=sender, topic_id=topic_id,
            since=since, until=until, cursor=cursor, limit=limit,
        )
    except ScopeNotFound as e:
        return _scope_not_found_response(e)
    await _charge(ctx, 1, "fetch_messages")
    return _ok(result, credits_used=1)


@mcp.tool()
async def list_topics(scope: str | None = None, ctx: Context = None) -> str:
    """List supergroup forum topics in a chat, so you can filter by `topic_id`.

    Telegram supergroups split conversation into topics; messages carry a
    `topic_id`. Call this to discover the topics (id, recovered name, message
    count, date range) before passing `topic_id` to fetch_messages /
    keyword_search / vector_search.

    Args:
        scope: "@channel", "folder:Name", or "all". Omit for all sources.

    Returns:
        JSON with `topics[]` (topic_id, name, message_count, channel, date range).
    """
    owner_id = await _resolve_owner(ctx)
    try:
        result = await service.list_topics(owner_id=owner_id, scope=scope)
    except ScopeNotFound as e:
        return _scope_not_found_response(e)
    return _ok(result)
