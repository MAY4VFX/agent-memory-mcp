"""Query pipeline orchestrator — runs the full query flow."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime
from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.models.query import (
    ContextPayloadData,
    GraphContext,
    HistoryContext,
    QueryAnswer,
    RouteType,
    SEARCH_MODES,
    SelfRAGDecision,
    SourceReference,
)
from agent_memory_mcp.pipeline.history import build_history_context
from agent_memory_mcp.pipeline.query_pipeline import (
    _is_overview_query,
    cascaded_filter,
    extract_hashtags_from_query,
    extract_keywords_simple,
    step_assemble_context,
    step_crag_check,
    step_generate,
    step_graph_enrich_multi,
    step_graph_retrieve_multi,
    step_hashtag_retrieve,
    step_keyword_retrieve,
    step_map_reduce,
    step_rerank,
    step_route,
    step_self_rag,
    step_transform,
    step_vector_retrieve,
)
from agent_memory_mcp.storage.embedding_client import EmbeddingClient
from agent_memory_mcp.storage.falkordb_client import FalkorDBStorage
from agent_memory_mcp.storage.milvus_client import MilvusStorage
from agent_memory_mcp.storage.reranker_client import RerankerClient
from agent_memory_mcp.tracing.tracer import flush, trace_observation

log = structlog.get_logger(__name__)


async def run_query_pipeline(
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
    """Run the full query pipeline.

    Three paths:
    A) OVERVIEW: pre-computed tag summary → instant answer
    B) CASCADED: BM25 → entity filter → map-reduce → generate
    C) STANDARD: retrieve → rerank → CRAG → assemble → generate

    ``search_mode`` controls parallelism: fast / balanced / deep.
    """
    from agent_memory_mcp.config import settings as _s
    if _s.use_agent_pipeline:
        from agent_memory_mcp.pipeline.agent_orchestrator import run_agent_pipeline
        return await run_agent_pipeline(
            query, user_id, conversation_id, domain_ids, engine,
            milvus, graph, embedder, reranker,
            search_mode=search_mode, progress_callback=progress_callback,
            since_date=since_date,
        )

    mode = SEARCH_MODES.get(search_mode, SEARCH_MODES["balanced"])

    with trace_observation(
        as_type="agent", name="query_pipeline",
        input=query, metadata={"user_id": user_id, "search_mode": search_mode},
    ) as agent:
        trace_id = agent.trace_id if agent else ""

        # --- Resolve channel info for sources ---
        channel_ids: list[int] = []
        channel_usernames: dict[int, str] = {}  # channel_id → username
        domain_id_str = ""
        for did in domain_ids:
            domain = await db_q.get_domain(engine, did)
            if domain:
                channel_ids.append(domain["channel_id"])
                uname = domain.get("channel_username", "")
                if uname:
                    channel_usernames[domain["channel_id"]] = uname
                domain_id_str = domain_id_str or str(domain["id"])
        # Primary username for sources (first domain with username)
        channel_username = next(iter(channel_usernames.values()), "")

        # --- Load schema (merge from all domains) ---
        schema = None
        if domain_ids:
            if len(domain_ids) == 1:
                schema = await db_q.get_active_schema(engine, domain_ids[0])
            else:
                schemas = await db_q.get_active_schemas_multi(engine, domain_ids)
                schema = _merge_schemas(schemas) if schemas else None

        # --- History ---
        with trace_observation(as_type="span", name="history"):
            history = await build_history_context(engine, conversation_id)

        # --- Self-RAG ---
        with trace_observation(as_type="tool", name="self_rag") as s:
            self_rag = await step_self_rag(query)
            if s:
                s.update(output={"decision": self_rag.decision.value})

        if self_rag.decision == SelfRAGDecision.direct_answer and self_rag.direct_answer:
            answer = QueryAnswer(
                answer=self_rag.direct_answer,
                self_rag_decision="direct_answer",
                langfuse_trace_id=trace_id,
            )
            if agent:
                agent.update_trace(output=self_rag.direct_answer)
            payload = ContextPayloadData(query=query, route="direct")
            flush()
            return answer, payload

        # --- Extract hashtags from query ---
        query_hashtags = extract_hashtags_from_query(query)

        # --- Check pre-computed summary (for GENERAL overview queries) ---
        tag_summary = None
        if query_hashtags and domain_ids:
            if len(domain_ids) == 1:
                tag_summary = await db_q.get_hashtag_summary(
                    engine, domain_ids[0], query_hashtags[0],
                )
            else:
                summaries = await db_q.get_hashtag_summaries_multi(
                    engine, domain_ids, query_hashtags[0],
                )
                # Combine summaries from multiple domains
                if summaries:
                    fresh = [s for s in summaries if not s.get("is_stale")]
                    if fresh:
                        combined_text = "\n\n".join(s["summary"] for s in fresh if s.get("summary"))
                        tag_summary = {"summary": combined_text, "is_stale": False}

        # --- Transform ---
        with trace_observation(as_type="tool", name="transform") as s:
            transform = await step_transform(query, schema=schema)
            if s:
                s.update(output={"type": transform.transform_type.value, "queries": transform.queries})

        # --- Route (only if mode needs it) ---
        route = None
        if mode.use_router:
            with trace_observation(as_type="tool", name="route") as s:
                route = await step_route(query)
                if s:
                    s.update(output={
                        "route": route.route.value,
                        "keywords": route.keywords,
                        "reasoning": route.reasoning[:200],
                    })

        # --- Parallel Retrieve (BM25 + vector + graph) ---
        with trace_observation(as_type="retriever", name="retrieve") as s:
            all_chunks = []
            graph_ctx = GraphContext()
            keyword_total = 0

            retrieve_tasks = []

            # BM25 keyword retrieve (now with scoring, multi-domain)
            if mode.always_keyword:
                async def _do_keyword():
                    return await step_keyword_retrieve(
                        query, domain_ids, engine,
                        channel_usernames=channel_usernames,
                    )
                retrieve_tasks.append(("keyword", _do_keyword()))

            # Hashtag retrieve (if query has hashtags)
            if query_hashtags and domain_id_str:
                async def _do_hashtag(tag=query_hashtags[0]):
                    return await step_hashtag_retrieve(
                        tag, domain_id_str, engine,
                        channel_id=channel_ids[0] if channel_ids else 0,
                    )
                retrieve_tasks.append(("hashtag", _do_hashtag()))

            # Vector retrieve
            if mode.always_vector:
                async def _do_vector():
                    return await step_vector_retrieve(
                        transform.queries, channel_ids, milvus, embedder,
                        limit=mode.vector_top_k, query_text=query,
                    )
                retrieve_tasks.append(("vector", _do_vector()))

            # Graph retrieve (parallel over domain_ids)
            if mode.always_graph and domain_ids:
                async def _do_graph():
                    return await step_graph_retrieve_multi(
                        query, [str(d) for d in domain_ids], graph,
                        embedder, milvus, channel_ids, schema=schema,
                    )
                retrieve_tasks.append(("graph", _do_graph()))

            # Run all retrieve tasks in parallel
            if retrieve_tasks:
                results = await asyncio.gather(
                    *(task for _, task in retrieve_tasks),
                    return_exceptions=True,
                )
                existing_ids: set[str] = set()
                for (task_name, _), result in zip(retrieve_tasks, results):
                    if isinstance(result, Exception):
                        log.exception("retrieve_task_failed", task=task_name, error=str(result))
                        continue
                    if task_name in ("keyword", "hashtag"):
                        kw_chunks, kw_total = result
                        if task_name == "keyword":
                            keyword_total = kw_total
                        for c in kw_chunks:
                            if c.id not in existing_ids:
                                all_chunks.append(c)
                                existing_ids.add(c.id)
                    elif task_name == "vector":
                        v_chunks = result
                        for c in v_chunks:
                            if c.id not in existing_ids:
                                all_chunks.append(c)
                                existing_ids.add(c.id)
                    elif task_name == "graph":
                        g_chunks, graph_ctx = result
                        for c in g_chunks:
                            if c.id not in existing_ids:
                                all_chunks.append(c)
                                existing_ids.add(c.id)

            chunks = all_chunks
            # Apply group scope date filter (hide data outside list's depth)
            if since_date and chunks:
                cutoff_ts = int(since_date.timestamp())
                chunks = [c for c in chunks if c.msg_date >= cutoff_ts]
            if s:
                s.update(output={"chunks": len(chunks), "keyword_total": keyword_total,
                                 "mode": search_mode,
                                 "schema_domain": schema.get("detected_domain") if schema else None})

        # --- Decision: which path? ---
        pipeline_path = "standard"

        # Determine focused keywords for cascaded path
        # Route keywords (from LLM router) take priority; fallback to local extraction
        _focused_keywords: list[str] = []
        if route and route.keywords:
            _focused_keywords = route.keywords
        if not _focused_keywords and keyword_total > 30:
            # Fallback: extract keywords locally (works for all modes)
            _focused_keywords = extract_keywords_simple(query)

        # PATH A: OVERVIEW → pre-computed summary + vector chunks for sources
        if (tag_summary and not tag_summary.get("is_stale")
                and _is_overview_query(query)):
            pipeline_path = "overview"

            with trace_observation(as_type="generation", name="generate_overview") as s:
                context = tag_summary["summary"]
                answer = await step_generate(
                    query, context, history, channel_username, chunks,
                    model=mode.llm_tier,
                    temperature=mode.temperature,
                    max_tokens=mode.max_answer_tokens,
                    sources_limit=mode.sources_limit,
                    domain_schema=schema,
                )
                answer.self_rag_decision = self_rag.decision.value
                answer.route = "overview"
                answer.langfuse_trace_id = trace_id
                if s:
                    s.update(output={"answer_len": len(answer.answer), "path": "overview"})

        # PATH B: CASCADED FILTER → targeted map-reduce
        # Only enter cascaded when we have focused keywords (not for vague semantic queries)
        elif keyword_total > 30 and _focused_keywords:
            pipeline_path = "cascaded"

            with trace_observation(as_type="chain", name="cascaded_map_reduce") as s:
                _CASCADED_FETCH_LIMIT = 500
                cascaded_query = " ".join(_focused_keywords)
                all_scored_posts, cascaded_total = await db_q.search_messages_bm25_multi(
                    engine, domain_ids, cascaded_query,
                    limit=min(keyword_total, _CASCADED_FETCH_LIMIT),
                )
                # Apply group scope date filter
                if since_date and all_scored_posts:
                    all_scored_posts = [
                        p for p in all_scored_posts
                        if (p.get("msg_date") or datetime.min) >= since_date
                    ]
                log.info("cascaded_bm25_fetch",
                         query=cascaded_query, fetched=len(all_scored_posts),
                         total=cascaded_total)

                # Stage 2: Entity type filter (schema-based)
                filtered = await cascaded_filter(all_scored_posts, query, schema)

                # Cap for map-reduce cost control (posts already sorted by BM25 score)
                _MAX_MAP_REDUCE = 200
                if len(filtered) > _MAX_MAP_REDUCE:
                    filtered = filtered[:_MAX_MAP_REDUCE]

                # Stage 3: Map-reduce on filtered set
                mr_result = await step_map_reduce(
                    query, filtered, schema=schema,
                    community_summaries=graph_ctx.community_summaries,
                    channel_username=channel_username,
                    progress_callback=progress_callback,
                )

                # Keep context for payload token counting
                context = mr_result

                if s:
                    s.update(output={
                        "total_scored": cascaded_total,
                        "after_filter": len(filtered),
                        "path": "cascaded",
                    })

            # Build answer directly from MAP-REDUCE (no double-summarization)
            with trace_observation(as_type="generation", name="generate_cascaded") as s:
                # Build source references from filtered BM25 posts
                sources: list[SourceReference] = []
                seen_ids: set[int] = set()

                # Build domain_id → channel_username map for source links
                _domain_username_map: dict[str, str] = {}
                for did in domain_ids:
                    _d = await db_q.get_domain(engine, did)
                    if _d and _d.get("channel_username"):
                        _domain_username_map[str(_d["id"])] = _d["channel_username"]

                from agent_memory_mcp.utils.links import make_tme_link

                for post in filtered[:mode.sources_limit]:
                    msg_id = post.get("telegram_msg_id", 0)
                    if msg_id and msg_id not in seen_ids:
                        seen_ids.add(msg_id)
                        post_did = str(post.get("domain_id", ""))
                        uname = _domain_username_map.get(post_did, channel_username)
                        topic_id = post.get("topic_id")
                        url = make_tme_link(uname, msg_id, topic_id)
                        sources.append(SourceReference(
                            channel_username=uname,
                            message_id=msg_id,
                            url=url,
                        ))

                answer = QueryAnswer(
                    answer=mr_result.strip(),
                    sources=sources,
                    chunks_used=len(filtered),
                    self_rag_decision=self_rag.decision.value,
                    route="cascaded",
                    crag_iterations=0,
                    langfuse_trace_id=trace_id,
                )
                if s:
                    s.update(output={"answer_len": len(answer.answer), "path": "cascaded"})

        # PATH C: STANDARD RAG → rerank → CRAG → generate
        else:
            pipeline_path = "standard"

            # --- Rerank ---
            crag_iterations = 0
            if chunks and len(chunks) > mode.rerank_top_k:
                with trace_observation(as_type="tool", name="rerank") as s:
                    chunks = await step_rerank(query, chunks, reranker, top_k=mode.rerank_top_k)
                    if s:
                        s.update(output={"chunks_after_rerank": len(chunks)})

            # --- CRAG Loop ---
            for i in range(mode.crag_max_iterations):
                with trace_observation(as_type="evaluator", name=f"crag_{i}") as s:
                    chunks, needs_reformulation = await step_crag_check(query, chunks)
                    crag_iterations += 1
                    if s:
                        s.update(output={"needs_reformulation": needs_reformulation, "chunks": len(chunks)})
                if not needs_reformulation:
                    break
                if i + 1 < len(transform.queries):
                    reform_query = transform.queries[i + 1]
                    more_chunks = await step_vector_retrieve(
                        [reform_query], channel_ids, milvus, embedder,
                        query_text=reform_query,
                    )
                    more_reranked = await step_rerank(reform_query, more_chunks, reranker,
                                                      top_k=mode.rerank_top_k)
                    existing_ids = {c.id for c in chunks}
                    for c in more_reranked:
                        if c.id not in existing_ids:
                            chunks.append(c)
                            existing_ids.add(c.id)

            # --- Graph Enrich (skip if graph was already a primary source) ---
            if not mode.always_graph and domain_ids:
                with trace_observation(as_type="retriever", name="graph_enrich") as s:
                    graph_ctx = await step_graph_enrich_multi(
                        chunks, [str(d) for d in domain_ids], graph, schema=schema,
                    )
                    if s:
                        s.update(output={
                            "entities": len(graph_ctx.entities),
                            "relations": len(graph_ctx.relations),
                            "communities": len(graph_ctx.community_summaries),
                        })

            # --- Assemble Context ---
            with trace_observation(as_type="chain", name="context_assembly") as s:
                context_text = step_assemble_context(
                    chunks, graph_ctx, history,
                    max_tokens=mode.context_max_tokens,
                    keyword_total=keyword_total,
                )
                if s:
                    s.update(output=context_text)

            # --- Generate ---
            with trace_observation(as_type="generation", name="generate") as s:
                answer = await step_generate(
                    query, context_text, history, channel_username, chunks,
                    model=mode.llm_tier,
                    temperature=mode.temperature,
                    max_tokens=mode.max_answer_tokens,
                    sources_limit=mode.sources_limit,
                    domain_schema=schema,
                )
                answer.self_rag_decision = self_rag.decision.value
                answer.route = route.route.value if route else "parallel"
                answer.crag_iterations = crag_iterations
                answer.langfuse_trace_id = trace_id
                if s:
                    s.update(output={"answer_len": len(answer.answer), "sources": len(answer.sources)})

        # --- Update trace with output for Langfuse evaluators ---
        if agent:
            agent.update_trace(output=answer.answer)

        # --- Build payload ---
        payload = ContextPayloadData(
            query=query,
            transform_type=transform.transform_type.value,
            transformed_queries=transform.queries,
            route=pipeline_path,
            chunks=[
                {"id": c.id, "score": c.score, "content": c.content[:500],
                 "relevance": c.relevance.value}
                for c in chunks
            ],
            graph_context=graph_ctx.model_dump(),
            crag_iterations=answer.crag_iterations,
            token_count=count_tokens_payload(context if pipeline_path != "standard" else context_text),
        )

    flush()
    return answer, payload


def count_tokens_payload(text: str) -> int:
    from agent_memory_mcp.utils.tokens import count_tokens
    return count_tokens(text)


def _merge_schemas(schemas: list[dict]) -> dict:
    """Merge multiple domain schemas into one (union entity_types/relation_types)."""
    if len(schemas) == 1:
        return schemas[0]

    seen_et: set[str] = set()
    seen_rt: set[str] = set()
    merged_et: list[dict] = []
    merged_rt: list[dict] = []
    detected_domains: list[str] = []

    for s in schemas:
        dd = s.get("detected_domain", "")
        if dd:
            detected_domains.append(dd)
        for et in (s.get("entity_types") or []):
            name = et.get("name", "")
            if name and name not in seen_et:
                seen_et.add(name)
                merged_et.append(et)
        for rt in (s.get("relation_types") or []):
            name = rt.get("name", "")
            if name and name not in seen_rt:
                seen_rt.add(name)
                merged_rt.append(rt)

    return {
        "detected_domain": " + ".join(detected_domains) if detected_domains else "",
        "entity_types": merged_et,
        "relation_types": merged_rt,
        "schema_json": {},
    }
