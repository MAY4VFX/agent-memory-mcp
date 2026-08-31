"""FastMCP server — thin client that proxies tool calls to Agent Memory REST API."""

from __future__ import annotations

from fastmcp import FastMCP

from agent_memory_mcp_server.client import AgentMemoryClient

mcp = FastMCP(
    "agent-memory-mcp",
    instructions=(
        "Agent Memory MCP provides Telegram conversation memory for AI agents. "
        "Use search_memory to find information, get_digest for summaries, "
        "get_decisions for extracted decisions, add_source to connect channels, "
        "and list_participants for a chat's full membership (not just message authors). "
        "IMPORTANT: Call list_scopes FIRST to discover valid scope values "
        "(\"all\", \"folder:Name\", \"@username\") before using get_digest or search_memory. "
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
        scope: Optional scope — "all", "@username", "folder:Name", or domain_id. Call list_scopes to see available options.
        limit: Maximum number of sources to return (default 10).
    """
    return await _get_client().search(query, scope, limit)


@mcp.tool()
async def get_digest(scope: str, period: str = "7d") -> str:
    """Get a digest of Telegram conversations for a period.

    Args:
        scope: Source scope — "all", "@username", "folder:Name", or domain_id. Call list_scopes to see available options.
        period: Time period: 1d, 3d, 7d, or 30d.
    """
    return await _get_client().digest(scope, period)


@mcp.tool()
async def get_decisions(scope: str, topic: str | None = None) -> str:
    """Extract decisions, action items, and open questions from conversations.

    Args:
        scope: Source scope — "all", "@username", "folder:Name", or domain_id. Call list_scopes to see available options.
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
    """List all connected memory sources.

    Each source carries two INDEPENDENT flags — do not conflate them:
      - `sync_enabled` (+ `next_sync`, `last_synced`) — the source keeps
        ingesting new messages. This is "is synchronization working?".
      - `monitoring` / `observe_layer` — the source counts toward the Observe
        Layer (labels, workload). OFF does NOT mean syncing is off.

    For per-run job detail and errors, call sync_status.
    """
    return await _get_client().list_sources()


@mcp.tool()
async def list_participants(scope: str | None = None) -> str:
    """List everyone in a chat's membership — not just people who wrote.

    Distinct from message authors: this is Telegram's actual member list,
    so silent members show up too, with @ники even for people who never
    posted. Sources where membership could NOT be collected (no admin
    rights on a broadcast channel, FloodWait, a 1:1 dialog, ...) are listed
    under `unavailable_sources` with the reason — never conflate that with
    an empty chat.

    Args:
        scope: "@channel", "folder:Name", or "all". Call list_scopes to see available options.
    """
    return await _get_client().list_participants(scope)


@mcp.tool()
async def list_scopes() -> str:
    """List all available scopes for get_digest, search_memory, and other tools.

    Call this FIRST to discover valid scope values before calling get_digest or search_memory.

    Returns:
        Available scopes: "all" for everything, "folder:Name" for channel groups, "@username" for individual channels.
    """
    return await _get_client().list_scopes()


@mcp.tool()
async def get_agent_context(task: str, scope: str) -> str:
    """Get a full context package for an agent task.

    Args:
        task: Description of what the agent needs to accomplish.
        scope: Source scope — "all", "@username", "folder:Name", or domain_id. Call list_scopes to see available options.
    """
    return await _get_client().context(task, scope)


def main():
    """Entry point for CLI: agent-memory-mcp."""
    mcp.run()


if __name__ == "__main__":
    main()
