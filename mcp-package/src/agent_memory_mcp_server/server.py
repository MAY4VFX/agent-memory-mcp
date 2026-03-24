"""FastMCP server — thin client that proxies tool calls to Agent Memory REST API."""

from __future__ import annotations

from fastmcp import FastMCP

from agent_memory_mcp_server.client import AgentMemoryClient

mcp = FastMCP(
    "agent-memory-mcp",
    instructions=(
        "Agent Memory MCP provides Telegram conversation memory for AI agents. "
        "Use search_memory to find information, get_digest for summaries, "
        "get_decisions for extracted decisions, and add_source to connect channels. "
        "Requires AGENT_MEMORY_API_KEY and AGENT_MEMORY_URL environment variables."
    ),
)

_client: AgentMemoryClient | None = None


def _get_client() -> AgentMemoryClient:
    global _client
    if _client is None:
        _client = AgentMemoryClient()
    return _client


@mcp.tool()
async def search_memory(query: str, scope: str | None = None, limit: int = 10) -> str:
    """Search Telegram memory by semantic query.

    Args:
        query: What to search for in the memory.
        scope: Optional scope — @channel_username or domain_id.
        limit: Maximum number of sources to return (default 10).
    """
    return await _get_client().search(query, scope, limit)


@mcp.tool()
async def get_digest(scope: str, period: str = "7d") -> str:
    """Get a digest of Telegram conversations for a period.

    Args:
        scope: Source scope — @channel_username or domain_id.
        period: Time period: 1d, 3d, 7d, or 30d.
    """
    return await _get_client().digest(scope, period)


@mcp.tool()
async def get_decisions(scope: str, topic: str | None = None) -> str:
    """Extract decisions, action items, and open questions from conversations.

    Args:
        scope: Source scope — @channel_username or domain_id.
        topic: Optional topic to filter by.
    """
    return await _get_client().decisions(scope, topic)


@mcp.tool()
async def add_source(handle: str, source_type: str = "channel", sync_range: str = "3m") -> str:
    """Connect a Telegram channel or group as a memory source.

    Args:
        handle: Channel identifier — @username or t.me/link.
        source_type: Type: channel, group, or folder.
        sync_range: How far back to sync: 1w, 1m, 3m, 6m, 1y.
    """
    return await _get_client().add_source(handle, source_type, sync_range)


@mcp.tool()
async def list_sources() -> str:
    """List all connected memory sources with sync status."""
    return await _get_client().list_sources()


@mcp.tool()
async def get_agent_context(task: str, scope: str) -> str:
    """Get a full context package for an agent task.

    Args:
        task: Description of what the agent needs to accomplish.
        scope: Source scope — @channel_username or domain_id.
    """
    return await _get_client().context(task, scope)


def main():
    """Entry point for CLI: agent-memory-mcp."""
    mcp.run()


if __name__ == "__main__":
    main()
