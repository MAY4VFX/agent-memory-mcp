"""MCP server tools — mounted as Streamable HTTP inside FastAPI on /mcp."""

from __future__ import annotations

import json

import structlog
from fastmcp import FastMCP

from agent_memory_mcp.memory_api import service
from agent_memory_mcp.memory_api.auth import (
    CREDIT_COSTS,
    charge_credits,
    get_api_key_by_hash,
)

log = structlog.get_logger(__name__)

mcp = FastMCP(
    "agent-memory-mcp",
    instructions=(
        "Agent Memory MCP provides Telegram conversation memory for AI agents. "
        "Use search_memory to find information, get_digest for summaries, "
        "get_decisions for extracted decisions, and add_source to connect channels."
    ),
)


# NOTE: Auth for MCP is handled at the HTTP transport level via Bearer token.
# The MCP tools below call service functions directly.
# In production, we'd extract the user from the MCP session context.
# For MVP, we use a hardcoded admin user — to be replaced with proper session auth.


@mcp.tool()
async def search_memory(query: str, scope: str | None = None, limit: int = 10) -> str:
    """Search Telegram memory by semantic query.

    Args:
        query: What to search for in the memory.
        scope: Optional scope — @channel_username or domain_id. If omitted, searches all sources.
        limit: Maximum number of source references to return (default 10).

    Returns:
        Answer based on memory with source references.
    """
    # TODO: extract owner_id from MCP session auth context
    from agent_memory_mcp.config import settings
    result = await service.search_memory(
        query=query, owner_id=settings.admin_telegram_id, scope=scope, limit=limit,
    )
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
async def get_digest(scope: str, period: str = "7d") -> str:
    """Get a digest of Telegram conversations for a period.

    Args:
        scope: Source scope — @channel_username or domain_id.
        period: Time period for the digest: 1d, 3d, 7d, or 30d. Default: 7d.

    Returns:
        Structured digest with key topics and highlights.
    """
    from agent_memory_mcp.config import settings
    result = await service.get_digest(
        owner_id=settings.admin_telegram_id, scope=scope, period=period,
    )
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
async def get_decisions(scope: str, topic: str | None = None) -> str:
    """Extract decisions, action items, and open questions from conversations.

    Args:
        scope: Source scope — @channel_username or domain_id.
        topic: Optional topic to filter decisions by.

    Returns:
        List of decisions, action items, and unresolved questions.
    """
    from agent_memory_mcp.config import settings
    result = await service.get_decisions(
        owner_id=settings.admin_telegram_id, scope=scope, topic=topic,
    )
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
async def add_source(handle: str, source_type: str = "channel", sync_range: str = "3m") -> str:
    """Connect a Telegram channel or group as a memory source.

    Args:
        handle: Channel/group identifier — @username or t.me/link.
        source_type: Type of source: channel, group, or folder.
        sync_range: How far back to sync: 1w, 1m, 3m, 6m, or 1y. Default: 3m.

    Returns:
        Status of the source addition and sync job.
    """
    from agent_memory_mcp.config import settings
    result = await service.add_source(
        owner_id=settings.admin_telegram_id,
        handle=handle, source_type=source_type, sync_range=sync_range,
    )
    return json.dumps(result, ensure_ascii=False, default=str)


@mcp.tool()
async def list_sources() -> str:
    """List all connected memory sources (channels, groups, folders).

    Returns:
        List of sources with sync status and message counts.
    """
    from agent_memory_mcp.config import settings
    sources = await service.list_sources(owner_id=settings.admin_telegram_id)
    return json.dumps({"sources": sources, "count": len(sources)}, ensure_ascii=False, default=str)


@mcp.tool()
async def get_agent_context(task: str, scope: str) -> str:
    """Get a full context package for an agent task.

    Combines search results, digest, graph data, and decisions into
    a comprehensive context that an agent can use to accomplish a task.

    Args:
        task: Description of what the agent needs to accomplish.
        scope: Source scope — @channel_username or domain_id.

    Returns:
        Structured context package with all relevant information.
    """
    from agent_memory_mcp.config import settings
    result = await service.get_agent_context(
        owner_id=settings.admin_telegram_id, task=task, scope=scope,
    )
    return json.dumps(result, ensure_ascii=False, default=str)
