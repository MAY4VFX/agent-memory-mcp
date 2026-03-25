"""MCP server tools — mounted as Streamable HTTP inside FastAPI on /mcp.

Auth flow:
1. Claude Code connects → sees OAuth metadata → opens browser
2. User enters API key on auth page
3. Claude Code gets Bearer token (= the API key)
4. All tool calls include Bearer token → we extract owner_id
"""

from __future__ import annotations

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
        "Agent Memory MCP provides Telegram conversation memory for AI agents. "
        "Use search_memory to find information, get_digest for summaries, "
        "get_decisions for extracted decisions, and add_source to connect channels."
    ),
)

# Cache: key_hash → api_key record
_key_cache: dict[str, dict] = {}


async def _resolve_owner(ctx: Context | None) -> int:
    """Extract owner_id from the Bearer token (API key) in MCP request.

    After OAuth flow, Claude Code sends Authorization: Bearer amk_xxx
    on every MCP request. We hash it and look up the owner.
    """
    api_key_raw = None

    # Get Bearer token from HTTP request via FastMCP helper
    try:
        from fastmcp.server.dependencies import get_http_request
        request = get_http_request()
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            api_key_raw = auth_header.removeprefix("Bearer ").strip()
    except Exception:
        pass

    # Fallback to admin
    if not api_key_raw:
        from agent_memory_mcp.config import settings
        return settings.admin_telegram_id

    key_hash = hashlib.sha256(api_key_raw.encode()).hexdigest()

    if key_hash in _key_cache:
        return _key_cache[key_hash]["telegram_id"]

    api_key = await get_api_key_by_hash(async_engine, key_hash)
    if not api_key or not api_key["is_active"]:
        from agent_memory_mcp.config import settings
        return settings.admin_telegram_id

    _key_cache[key_hash] = api_key
    return api_key["telegram_id"]


def _ok(result, credits_used: int = 0) -> str:
    data = result if isinstance(result, dict) else {"data": result}
    if credits_used:
        data["credits_used"] = credits_used
    return json.dumps(data, ensure_ascii=False, default=str)


@mcp.tool()
async def search_memory(query: str, scope: str | None = None, limit: int = 10, ctx: Context = None) -> str:
    """Search Telegram memory by semantic query.

    Args:
        query: What to search for in the memory.
        scope: Optional scope — @channel_username or domain_id. If omitted, searches all sources.
        limit: Maximum number of source references to return (default 10).

    Returns:
        Answer based on memory with source references.
    """
    owner_id = await _resolve_owner(ctx)
    result = await service.search_memory(query=query, owner_id=owner_id, scope=scope, limit=limit)
    return _ok(result, credits_used=3)


@mcp.tool()
async def get_digest(scope: str, period: str = "7d", ctx: Context = None) -> str:
    """Get a digest of Telegram conversations for a period.

    Args:
        scope: Source scope — @channel_username or domain_id.
        period: Time period for the digest: 1d, 3d, 7d, or 30d. Default: 7d.

    Returns:
        Structured digest with key topics and highlights.
    """
    owner_id = await _resolve_owner(ctx)
    result = await service.get_digest(owner_id=owner_id, scope=scope, period=period)
    return _ok(result, credits_used=10)


@mcp.tool()
async def get_decisions(scope: str, topic: str | None = None, ctx: Context = None) -> str:
    """Extract decisions, action items, and open questions from conversations.

    Args:
        scope: Source scope — @channel_username or domain_id.
        topic: Optional topic to filter decisions by.

    Returns:
        List of decisions, action items, and unresolved questions.
    """
    owner_id = await _resolve_owner(ctx)
    result = await service.get_decisions(owner_id=owner_id, scope=scope, topic=topic)
    return _ok(result, credits_used=5)


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
    return _ok(result, credits_used=5)


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
        scope: Source scope — @channel_username or domain_id.

    Returns:
        Structured context package with all relevant information.
    """
    owner_id = await _resolve_owner(ctx)
    result = await service.get_agent_context(owner_id=owner_id, task=task, scope=scope)
    return _ok(result, credits_used=10)
