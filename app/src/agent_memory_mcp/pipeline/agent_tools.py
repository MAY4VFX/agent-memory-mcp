"""A-RAG tool definitions — wrap existing pipeline steps for function calling."""

from __future__ import annotations

import re
import time
from typing import Any

import structlog

from agent_memory_mcp.config import settings
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.llm.client import llm_call
from agent_memory_mcp.llm.query_prompts import TEXT2CYPHER_SYSTEM
from agent_memory_mcp.models.query import CRAGRelevance, RetrievedChunk
from agent_memory_mcp.pipeline.agent_context import AgentContext, ToolCallRecord
from agent_memory_mcp.pipeline.query_pipeline import (
    step_graph_enrich,
    step_graph_retrieve,
    step_keyword_retrieve,
    step_map_reduce,
    step_rerank,
    step_vector_retrieve,
)
from agent_memory_mcp.utils.tokens import count_tokens

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Tool schema helpers
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "keyword_search",
            "description": (
                "BM25 keyword search in PostgreSQL. Best for exact terms, hashtags, "
                "proper nouns. Returns snippets (first 200 chars) and total count."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query (keywords, hashtags, names)",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "semantic_search",
            "description": (
                "Dense+sparse vector search in Milvus. Best for concepts, topics, "
                "semantic similarity. Returns snippets (first 200 chars) and scores."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query (concepts, topics, descriptions)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max results (default 30)",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_messages",
            "description": (
                "Read full content of messages by their IDs. Use AFTER search to get "
                "complete text of relevant snippets. Already-read IDs are skipped."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "message_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of message IDs to read",
                    },
                },
                "required": ["message_ids"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "graph_search",
            "description": (
                "Search the knowledge graph (FalkorDB). Returns entities, relations, "
                "and community summaries. Best for questions about connections between entities."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "What entities/relations to search for",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "rerank_results",
            "description": (
                "Rerank cached search results using a cross-encoder. Use when you "
                "have >10 search results and want to find the most relevant ones."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The query to rerank against",
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Number of top results to return (default 10)",
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_domain_info",
            "description": (
                "Get metadata about the knowledge base domain: detected topic, "
                "entity types, relation types, total message count. "
                "Call first if you need orientation."
            ),
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "graph_query",
            "description": (
                "Query the knowledge graph with natural language. Converts your question "
                "to a Cypher query and executes it against FalkorDB. Use for: counting "
                "entities, finding specific relations, complex graph patterns, listing "
                "entities of a type."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "Natural language question about entities/relations in the graph",
                    },
                },
                "required": ["question"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "analyze_large_set",
            "description": (
                "Map-reduce analysis for large result sets (>30 BM25 hits). "
                "Extracts relevant info from each batch, then aggregates. "
                "Use for overview/analytical questions with many matching posts."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Analytical question to answer from posts",
                    },
                    "keywords": {
                        "type": "string",
                        "description": "BM25 keywords to fetch posts (space-separated)",
                    },
                    "max_posts": {
                        "type": "integer",
                        "description": "Max posts to analyze (default 200)",
                    },
                },
                "required": ["query", "keywords"],
            },
        },
    },
]


# ---------------------------------------------------------------------------
# Tool executor dispatch
# ---------------------------------------------------------------------------

async def execute_tool(
    tool_name: str,
    arguments: dict[str, Any],
    ctx: AgentContext,
) -> str:
    """Execute a tool and return a result string for the agent."""
    t0 = time.monotonic()
    try:
        result = await _TOOL_FNS[tool_name](arguments, ctx)
    except Exception as exc:
        log.exception("tool_exec_failed", tool=tool_name)
        result = f"Error: {exc}"

    elapsed = int((time.monotonic() - t0) * 1000)
    tokens = count_tokens(result)
    ctx.tokens_used += tokens
    ctx.tool_calls.append(ToolCallRecord(
        tool_name=tool_name,
        arguments=arguments,
        result_summary=result[:200],
        duration_ms=elapsed,
        tokens_in_result=tokens,
    ))

    # Progress callback
    ctx.progress_step += 1
    if ctx.progress_callback:
        try:
            await ctx.progress_callback(ctx.progress_step, 0)
        except Exception:
            pass

    return result


# ---------------------------------------------------------------------------
# Individual tool implementations
# ---------------------------------------------------------------------------

_CYPHER_WRITE_RE = re.compile(
    r"\b(CREATE|DELETE|SET|MERGE|DROP|REMOVE|DETACH|CALL)\b", re.IGNORECASE,
)


async def _graph_expansion(ctx: AgentContext, chunks: list[RetrievedChunk], max_entities: int = 5) -> str:
    """Find entities mentioned in chunks and return their graph relations."""
    if not ctx.domain_id or not ctx.graph:
        return ""
    try:
        domain_entities = await ctx.graph.query_entities(ctx.domain_id)
        if not domain_entities:
            return ""
        entity_map = {e["name"].lower(): e["name"] for e in domain_entities}

        mentioned: list[str] = []
        seen: set[str] = set()
        for chunk in chunks[:15]:
            chunk_lower = chunk.content.lower()
            for lower_name, original in entity_map.items():
                if len(lower_name) >= 3 and lower_name in chunk_lower and original not in seen:
                    seen.add(original)
                    mentioned.append(original)

        if not mentioned:
            return ""

        lines: list[str] = []
        for name in mentioned[:max_entities]:
            rels = await ctx.graph.query_entity_relations(name, ctx.domain_id)
            for r in rels[:5]:
                src = r.get("source", "")
                tgt = r.get("target", "")
                rtype = r.get("type", "RELATED_TO")
                lines.append(f"  {src} →[{rtype}]→ {tgt}")

        if not lines:
            return ""
        return "\n\nRelated entities from knowledge graph:\n" + "\n".join(lines)
    except Exception:
        log.debug("graph_expansion_failed", exc_info=True)
        return ""


async def _tool_keyword_search(args: dict, ctx: AgentContext) -> str:
    query = args["query"]
    chunks, total = await step_keyword_retrieve(
        query, ctx.domain_id, ctx.engine,
        channel_id=ctx.channel_ids[0] if ctx.channel_ids else 0,
    )
    # Cache chunks
    for c in chunks:
        ctx.cached_chunks[c.id] = c

    # Return snippets
    lines = [f"BM25 results: {total} total, showing {len(chunks)}"]
    for c in chunks[:30]:
        lines.append(f"- [{c.id}] (score={c.score:.2f}) {c.content[:200]}")

    expansion = await _graph_expansion(ctx, chunks)
    if expansion:
        lines.append(expansion)
    return "\n".join(lines)


async def _tool_semantic_search(args: dict, ctx: AgentContext) -> str:
    query = args["query"]
    limit = args.get("limit", 30)
    chunks = await step_vector_retrieve(
        [query], ctx.channel_ids, ctx.milvus, ctx.embedder,
        limit=limit, query_text=query,
    )
    # Cache chunks
    for c in chunks:
        ctx.cached_chunks[c.id] = c

    lines = [f"Vector results: {len(chunks)} returned"]
    for c in chunks[:30]:
        lines.append(f"- [{c.id}] (score={c.score:.3f}) {c.content[:200]}")

    expansion = await _graph_expansion(ctx, chunks)
    if expansion:
        lines.append(expansion)
    return "\n".join(lines)


async def _tool_read_messages(args: dict, ctx: AgentContext) -> str:
    ids = args["message_ids"]
    # Filter already-read
    new_ids = [mid for mid in ids if mid not in ctx.read_message_ids]
    if not new_ids:
        return "All requested messages already read (cached)."

    # Try cache first
    from_cache = []
    need_db = []
    for mid in new_ids:
        if mid in ctx.cached_chunks:
            from_cache.append(ctx.cached_chunks[mid])
            ctx.read_message_ids.add(mid)
        else:
            need_db.append(mid)

    # Fetch remaining from DB
    db_msgs = []
    if need_db:
        db_msgs = await db_q.get_messages_by_ids(ctx.engine, need_db)
        for row in db_msgs:
            mid = str(row["id"])
            ctx.read_message_ids.add(mid)
            ctx.cached_chunks[mid] = RetrievedChunk(
                id=mid,
                content=row.get("content", ""),
                channel_id=ctx.channel_ids[0] if ctx.channel_ids else 0,
                msg_date=int(row["msg_date"].timestamp()) if row.get("msg_date") else 0,
                thread_id=f"{ctx.domain_id}_{row.get('telegram_msg_id', 0)}",
                content_type=row.get("content_type", "text"),
                relevance=CRAGRelevance.high,
            )

    # Build response with full content
    lines = [f"Read {len(from_cache) + len(db_msgs)} messages:"]
    for c in from_cache:
        lines.append(f"\n--- [{c.id}] ---\n{c.content}")
    for row in db_msgs:
        lines.append(f"\n--- [{str(row['id'])}] ---\n{row.get('content', '')}")
    return "\n".join(lines)


async def _tool_graph_search(args: dict, ctx: AgentContext) -> str:
    query = args["query"]
    if not ctx.domain_id:
        return "No domain configured for graph search."

    chunks, graph_ctx = await step_graph_retrieve(
        query, ctx.domain_id, ctx.graph,
        ctx.embedder, ctx.milvus, ctx.channel_ids,
        schema=ctx.schema,
    )
    # Also enrich with community info from cached chunks
    if ctx.cached_chunks:
        cached_list = list(ctx.cached_chunks.values())[:10]
        enrich_ctx = await step_graph_enrich(
            cached_list, ctx.domain_id, ctx.graph, schema=ctx.schema,
        )
        # Merge
        graph_ctx.entities.extend(enrich_ctx.entities)
        graph_ctx.relations.extend(enrich_ctx.relations)
        for s in enrich_ctx.community_summaries:
            if s not in graph_ctx.community_summaries:
                graph_ctx.community_summaries.append(s)

    # Cache any new chunks from graph search
    for c in chunks:
        ctx.cached_chunks[c.id] = c

    lines = [f"Graph: {len(graph_ctx.entities)} entities, {len(graph_ctx.relations)} relations"]
    for e in graph_ctx.entities[:20]:
        lines.append(f"  Entity: {e.get('name', '')} ({e.get('type', '')})")
    for r in graph_ctx.relations[:20]:
        lines.append(f"  {r.get('source', '')} --[{r.get('type', '')}]--> {r.get('target', '')}")
    if graph_ctx.community_summaries:
        lines.append("Communities:")
        for s in graph_ctx.community_summaries[:5]:
            lines.append(f"  - {s[:300]}")
    return "\n".join(lines)


async def _tool_rerank(args: dict, ctx: AgentContext) -> str:
    query = args["query"]
    top_k = args.get("top_k", 10)
    if not ctx.cached_chunks:
        return "No cached chunks to rerank. Run a search first."

    chunks = list(ctx.cached_chunks.values())
    ranked = await step_rerank(query, chunks, ctx.reranker, top_k=top_k)

    lines = [f"Reranked {len(chunks)} → top {len(ranked)}:"]
    for c in ranked:
        lines.append(f"- [{c.id}] (score={c.score:.4f}) {c.content[:200]}")
    return "\n".join(lines)


async def _tool_get_domain_info(args: dict, ctx: AgentContext) -> str:
    parts = [f"Channel: @{ctx.channel_username}"]
    if ctx.schema:
        domain = ctx.schema.get("detected_domain", "unknown")
        parts.append(f"Domain: {domain}")
        et = ctx.schema.get("entity_types") or []
        if et:
            names = [e.get("name", "") for e in et[:20]]
            parts.append(f"Entity types ({len(et)}): {', '.join(names)}")
        rt = ctx.schema.get("relation_types") or []
        if rt:
            names = [r.get("name", "") for r in rt[:15]]
            parts.append(f"Relation types ({len(rt)}): {', '.join(names)}")
    else:
        parts.append("No schema discovered yet.")

    parts.append(f"Domain ID: {ctx.domain_id}")
    parts.append(f"Channels: {ctx.channel_ids}")
    return "\n".join(parts)


async def _tool_analyze_large_set(args: dict, ctx: AgentContext) -> str:
    query = args["query"]
    keywords = args["keywords"]
    max_posts = args.get("max_posts", 200)

    from uuid import UUID
    all_posts, total = await db_q.search_messages_bm25(
        ctx.engine, UUID(ctx.domain_id), keywords,
        limit=max_posts,
    )

    if not all_posts:
        return f"No posts found for keywords: {keywords}"

    # Cap
    posts = all_posts[:max_posts]

    # Get community summaries from graph if available
    community_summaries: list[str] = []
    if ctx.cached_chunks:
        try:
            cached_list = list(ctx.cached_chunks.values())[:5]
            enrich = await step_graph_enrich(
                cached_list, ctx.domain_id, ctx.graph, schema=ctx.schema,
            )
            community_summaries = enrich.community_summaries
        except Exception:
            pass

    result = await step_map_reduce(
        query, posts, schema=ctx.schema,
        community_summaries=community_summaries,
        channel_username=ctx.channel_username,
        progress_callback=ctx.progress_callback,
    )

    answer = f"Analyzed {len(posts)}/{total} posts.\n\n{result.strip()}"
    # Signal agent loop to return this directly (skip re-summarization)
    ctx.passthrough_answer = answer
    return answer


async def _tool_graph_query(args: dict, ctx: AgentContext) -> str:
    question = args["question"]
    if not ctx.domain_id:
        return "No domain configured for graph query."

    # Build schema hint
    entity_types = ""
    relation_types = ""
    if ctx.schema:
        et = ctx.schema.get("entity_types") or []
        entity_types = ", ".join(e.get("name", "") for e in et[:20])
        rt = ctx.schema.get("relation_types") or []
        relation_types = ", ".join(r.get("name", "") for r in rt[:15])

    system = TEXT2CYPHER_SYSTEM.format(
        entity_types=entity_types or "не определены",
        relation_types=relation_types or "не определены",
    )

    # Generate Cypher via tier1
    cypher = await llm_call(
        model=settings.llm_tier1_model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": question},
        ],
        temperature=0.0,
        max_tokens=512,
    )
    cypher = cypher.strip().strip("`").strip()
    # Strip markdown code block if present
    if cypher.startswith("```"):
        cypher = re.sub(r"^```(?:cypher)?\s*\n?", "", cypher)
        cypher = re.sub(r"\n?```\s*$", "", cypher)

    # Safety check
    if _CYPHER_WRITE_RE.search(cypher):
        return f"Rejected: generated Cypher contains write operations.\nCypher: {cypher}"

    log.info("text2cypher", question=question, cypher=cypher)

    # Execute with 1 retry on error
    for attempt in range(2):
        try:
            rows = await ctx.graph.execute_cypher(cypher, {"domain_id": ctx.domain_id})
            break
        except Exception as exc:
            if attempt == 0:
                # Retry: let LLM fix the query
                cypher = await llm_call(
                    model=settings.llm_tier1_model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": question},
                        {"role": "assistant", "content": cypher},
                        {"role": "user", "content": f"Ошибка выполнения: {exc}\nИсправь Cypher-запрос."},
                    ],
                    temperature=0.0,
                    max_tokens=512,
                )
                cypher = cypher.strip().strip("`").strip()
                if cypher.startswith("```"):
                    cypher = re.sub(r"^```(?:cypher)?\s*\n?", "", cypher)
                    cypher = re.sub(r"\n?```\s*$", "", cypher)
                if _CYPHER_WRITE_RE.search(cypher):
                    return f"Rejected: retry Cypher contains write operations.\nCypher: {cypher}"
                log.info("text2cypher_retry", cypher=cypher)
            else:
                return f"Graph query failed: {exc}\nCypher: {cypher}"

    if not rows:
        return f"No results.\nCypher: {cypher}"

    lines = [f"Graph query returned {len(rows)} rows:"]
    for row in rows[:50]:
        parts = [f"{k}={v}" for k, v in row.items()]
        lines.append("  " + ", ".join(parts))
    lines.append(f"\nCypher: {cypher}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_TOOL_FNS: dict = {
    "keyword_search": _tool_keyword_search,
    "semantic_search": _tool_semantic_search,
    "read_messages": _tool_read_messages,
    "graph_search": _tool_graph_search,
    "graph_query": _tool_graph_query,
    "rerank_results": _tool_rerank,
    "get_domain_info": _tool_get_domain_info,
    "analyze_large_set": _tool_analyze_large_set,
}
