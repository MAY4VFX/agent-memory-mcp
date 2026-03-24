"""A-RAG orchestrator — entry point replacing the DAG pipeline."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.llm.agent_prompt import build_agent_system_prompt
from agent_memory_mcp.models.query import (
    AGENT_BUDGETS,
    ContextPayloadData,
    QueryAnswer,
    SourceReference,
)
from agent_memory_mcp.pipeline.agent_context import AgentContext
from agent_memory_mcp.pipeline.agent_loop import run_agent_loop
from agent_memory_mcp.pipeline.history import build_history_context
from agent_memory_mcp.storage.embedding_client import EmbeddingClient
from agent_memory_mcp.storage.falkordb_client import FalkorDBStorage
from agent_memory_mcp.storage.milvus_client import MilvusStorage
from agent_memory_mcp.storage.reranker_client import RerankerClient
from agent_memory_mcp.tracing.tracer import flush, trace_observation
from agent_memory_mcp.utils.tokens import count_tokens

log = structlog.get_logger(__name__)


async def run_agent_pipeline(
    query: str,
    user_id: int,
    conversation_id: UUID,
    domain_ids: list[UUID],
    engine: AsyncEngine,
    milvus: MilvusStorage,
    graph: FalkorDBStorage,
    embedder: EmbeddingClient,
    reranker: RerankerClient,
    search_mode: str = "balanced",
    progress_callback: Callable | None = None,
    since_date: datetime | None = None,
) -> tuple[QueryAnswer, ContextPayloadData]:
    """Run the A-RAG agent pipeline.

    Same signature as run_query_pipeline() for drop-in replacement.
    """
    budget = AGENT_BUDGETS.get(search_mode, AGENT_BUDGETS["balanced"])

    with trace_observation(
        as_type="agent", name="agent_pipeline",
        input=query, metadata={"user_id": user_id, "search_mode": search_mode},
    ) as agent_obs:
        trace_id = agent_obs.trace_id if agent_obs else ""

        # --- Resolve domain info ---
        channel_ids: list[int] = []
        channel_username = ""
        domain_id_str = ""
        for did in domain_ids:
            domain = await db_q.get_domain(engine, did)
            if domain:
                channel_ids.append(domain["channel_id"])
                channel_username = channel_username or domain.get("channel_username", "")
                domain_id_str = domain_id_str or str(domain["id"])

        if not domain_id_str:
            log.warning("agent_no_domain", domain_ids=domain_ids)
            answer = QueryAnswer(
                answer="Не найден ни один домен. Подключите канал через /domains.",
                self_rag_decision="agent",
                route="agent",
                langfuse_trace_id=trace_id,
            )
            payload = ContextPayloadData(query=query, route="agent")
            flush()
            return answer, payload

        # --- Load schema ---
        schema = None
        if domain_ids:
            schema = await db_q.get_active_schema(engine, domain_ids[0])

        # --- History ---
        with trace_observation(as_type="span", name="history"):
            history = await build_history_context(engine, conversation_id)

        # Build history messages for agent
        history_messages: list[dict] = []
        if history.has_summary:
            history_messages.append({
                "role": "system",
                "content": f"Резюме предыдущего разговора:\n{history.summary}",
            })
        for msg in history.messages:
            history_messages.append({"role": msg.role, "content": msg.content})

        # --- Build system prompt ---
        system_prompt = build_agent_system_prompt(channel_username, schema)

        # --- Create agent context ---
        ctx = AgentContext(
            engine=engine,
            milvus=milvus,
            graph=graph,
            embedder=embedder,
            reranker=reranker,
            domain_id=domain_id_str,
            channel_ids=channel_ids,
            channel_username=channel_username,
            schema=schema,
            progress_callback=progress_callback,
        )

        # --- Run agent loop ---
        answer_text, tool_calls = await run_agent_loop(
            query=query,
            system_prompt=system_prompt,
            context=ctx,
            budget=budget,
            history_messages=history_messages,
        )

        # --- Build sources from cached chunks ---
        from agent_memory_mcp.utils.links import make_tme_link

        sources: list[SourceReference] = []
        seen_msg_ids: set[int] = set()
        for chunk in ctx.cached_chunks.values():
            tid = chunk.thread_id
            if not tid:
                continue
            parts = tid.rsplit("_", 1)
            if len(parts) == 2:
                try:
                    msg_id = int(parts[1])
                    if msg_id and msg_id not in seen_msg_ids:
                        seen_msg_ids.add(msg_id)
                        url = make_tme_link(channel_username, msg_id)
                        sources.append(SourceReference(
                            channel_username=channel_username,
                            message_id=msg_id,
                            url=url,
                        ))
                except ValueError:
                    pass

        answer = QueryAnswer(
            answer=answer_text.strip(),
            sources=sources[:budget.sources_limit],
            self_rag_decision="agent",
            route="agent",
            chunks_used=len(ctx.cached_chunks),
            crag_iterations=0,
            langfuse_trace_id=trace_id,
        )

        # --- Build payload ---
        payload = ContextPayloadData(
            query=query,
            transform_type="agent",
            transformed_queries=[],
            route="agent",
            chunks=[
                {"id": c.id, "score": c.score, "content": c.content[:500],
                 "relevance": c.relevance.value}
                for c in list(ctx.cached_chunks.values())[:50]
            ],
            graph_context={},
            crag_iterations=0,
            token_count=ctx.tokens_used,
        )

        if agent_obs:
            agent_obs.update_trace(output=answer.answer)
            agent_obs.update(output={
                "tool_calls": len(tool_calls),
                "tokens_used": ctx.tokens_used,
                "chunks_cached": len(ctx.cached_chunks),
                "messages_read": len(ctx.read_message_ids),
            })

    flush()
    return answer, payload
