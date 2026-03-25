"""Agent context — shared state for the A-RAG agent loop."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from sqlalchemy.ext.asyncio import AsyncEngine

from agent_memory_mcp.models.query import RetrievedChunk
from agent_memory_mcp.storage.embedding_client import EmbeddingClient
from agent_memory_mcp.storage.falkordb_client import FalkorDBStorage
from agent_memory_mcp.storage.milvus_client import MilvusStorage
from agent_memory_mcp.storage.reranker_client import RerankerClient


@dataclass
class ToolCallRecord:
    """Record of a single tool call for tracing."""

    tool_name: str
    arguments: dict
    result_summary: str = ""
    duration_ms: int = 0
    tokens_in_result: int = 0


@dataclass
class AgentContext:
    """Mutable state shared across all tool executions within one agent run."""

    # Storage clients
    engine: AsyncEngine
    milvus: MilvusStorage
    graph: FalkorDBStorage
    embedder: EmbeddingClient
    reranker: RerankerClient

    # Domain info (loaded once per query)
    domain_id: str
    channel_ids: list[int]
    channel_username: str
    schema: dict | None = None

    # A-RAG context tracking
    read_message_ids: set[str] = field(default_factory=set)
    cached_chunks: dict[str, RetrievedChunk] = field(default_factory=dict)
    tokens_used: int = 0

    # Tracing
    tool_calls: list[ToolCallRecord] = field(default_factory=list)
    progress_callback: Callable | None = None
    progress_step: int = 0

    # Time filter
    since_date: "datetime | None" = None

    # Passthrough: if set, agent loop returns this directly (skips re-summarization)
    passthrough_answer: str | None = None
