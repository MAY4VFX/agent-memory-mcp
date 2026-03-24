"""Query pipeline step functions — async, no Haystack components."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable

import structlog

from agent_memory_mcp.config import settings
from agent_memory_mcp.llm.client import llm_call, llm_call_json
from agent_memory_mcp.llm.query_prompts import (
    CRAG_RELEVANCE_SYSTEM,
    GENERATION_SYSTEM,
    MAP_COLLAPSE_SYSTEM,
    MAP_EXTRACT_SYSTEM,
    MAP_REDUCE_SYSTEM,
    QUERY_ROUTER_SYSTEM,
    QUERY_TRANSFORM_SYSTEM,
    SELF_RAG_SYSTEM,
)
from agent_memory_mcp.models.query import (
    CRAGRelevance,
    GraphContext,
    HistoryContext,
    QueryAnswer,
    QueryTransformType,
    RetrievedChunk,
    RouteResult,
    RouteType,
    SelfRAGDecision,
    SelfRAGResult,
    SourceReference,
    TransformResult,
)
from agent_memory_mcp.storage.embedding_client import EmbeddingClient
from agent_memory_mcp.storage.falkordb_client import FalkorDBStorage
from agent_memory_mcp.storage.milvus_client import MilvusStorage
from agent_memory_mcp.storage.reranker_client import RerankerClient
from agent_memory_mcp.utils.tokens import count_tokens, truncate_to_tokens

log = structlog.get_logger(__name__)

_HASHTAG_RE = re.compile(r"#(\w+)", re.UNICODE)


# ------------------------------------------------------------------ Self-RAG

async def step_self_rag(query: str) -> SelfRAGResult:
    """Decide whether retrieval is needed."""
    try:
        data = await llm_call_json(
            model=settings.llm_tier2_model,
            messages=[
                {"role": "system", "content": SELF_RAG_SYSTEM},
                {"role": "user", "content": query},
            ],
            max_tokens=256,
        )
        result = SelfRAGResult(
            decision=SelfRAGDecision(data.get("decision", "needs_retrieval")),
            reasoning=data.get("reasoning", ""),
            direct_answer=data.get("direct_answer"),
        )
        log.info("self_rag_done", decision=result.decision.value, reasoning=result.reasoning[:100])
        return result
    except Exception:
        log.exception("self_rag_failed")
        return SelfRAGResult(decision=SelfRAGDecision.needs_retrieval)


# ------------------------------------------------------------------ Transform

async def step_transform(query: str, schema: dict | None = None) -> TransformResult:
    """Transform/expand the query. Optionally uses schema for domain context."""
    try:
        schema_ctx = ""
        if schema:
            domain = schema.get("detected_domain", "")
            et_names = [et.get("name", "") for et in (schema.get("entity_types") or [])]
            if domain or et_names:
                schema_ctx = f"\nДомен: {domain}. Типы сущностей: {', '.join(et_names)}."

        prompt = QUERY_TRANSFORM_SYSTEM.format(schema_context=schema_ctx)
        data = await llm_call_json(
            model=settings.llm_tier2_model,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": query},
            ],
            max_tokens=512,
        )
        queries = data.get("queries", [query])
        if not queries:
            queries = [query]
        result = TransformResult(
            transform_type=QueryTransformType(data.get("transform_type", "passthrough")),
            queries=queries,
            original_query=query,
        )
        log.info("transform_done", type=result.transform_type.value, queries=result.queries)
        return result
    except Exception:
        log.exception("transform_failed")
        return TransformResult(
            transform_type=QueryTransformType.passthrough,
            queries=[query],
            original_query=query,
        )


# ------------------------------------------------------------------ Route

async def step_route(query: str) -> RouteResult:
    """Route query to the appropriate retrieval strategy."""
    try:
        data = await llm_call_json(
            model=settings.llm_tier2_model,
            messages=[
                {"role": "system", "content": QUERY_ROUTER_SYSTEM},
                {"role": "user", "content": query},
            ],
            max_tokens=256,
        )
        result = RouteResult(
            route=RouteType(data.get("route", "vector")),
            reasoning=data.get("reasoning", ""),
            keywords=data.get("keywords") or [],
        )
        log.info("route_done", route=result.route.value, keywords=result.keywords, reasoning=result.reasoning[:100])
        return result
    except Exception:
        log.exception("route_failed")
        return RouteResult(route=RouteType.vector)


# ------------------------------------------------------------------ Retrieve

async def step_vector_retrieve(
    queries: list[str],
    channel_ids: list[int],
    milvus: MilvusStorage,
    embedder: EmbeddingClient,
    limit: int | None = None,
    query_text: str = "",
) -> list[RetrievedChunk]:
    """Retrieve chunks via dense vector search (or hybrid if query_text provided)."""
    _limit = limit or settings.query_vector_top_k

    # Embed all queries
    vectors = await embedder.embed_dense(queries)
    log.info("vector_retrieve_start", queries=len(queries), channels=channel_ids,
             hybrid=bool(query_text))

    # Search with each query vector, dedup by ID AND content hash
    seen_ids: set[str] = set()
    seen_content: set[int] = set()  # hash of first 200 chars
    all_chunks: list[RetrievedChunk] = []

    for qi, vector in enumerate(vectors):
        # Pass query_text only for the first query to enable hybrid search
        _qt = query_text if qi == 0 else ""
        hits = milvus.search_multi_channel(vector, channel_ids, limit=_limit, query_text=_qt)
        for hit in hits:
            doc_id = hit.get("id", "")
            content = hit.get("content", "")
            content_hash = hash(content[:200])
            # Dedup by both ID and content
            if doc_id in seen_ids or content_hash in seen_content:
                continue
            seen_ids.add(doc_id)
            seen_content.add(content_hash)
            all_chunks.append(RetrievedChunk(
                id=doc_id,
                content=content,
                score=hit.get("score", 0.0),
                channel_id=hit.get("channel_id", 0),
                msg_date=hit.get("msg_date", 0),
                thread_id=hit.get("thread_id", ""),
                language=hit.get("language", ""),
                content_type=hit.get("content_type", "text"),
            ))
        log.info("vector_retrieve_query", query_idx=qi, hits=len(hits), unique_so_far=len(all_chunks))

    # Sort by score descending
    all_chunks.sort(key=lambda c: c.score, reverse=True)
    result = all_chunks[:_limit]
    log.info("vector_retrieve_done", total_unique=len(all_chunks), returned=len(result))
    return result


async def step_graph_retrieve(
    query: str,
    domain_id: str,
    graph: FalkorDBStorage,
    embedder: EmbeddingClient,
    milvus: MilvusStorage,
    channel_ids: list[int],
    schema: dict | None = None,
) -> tuple[list[RetrievedChunk], GraphContext]:
    """Retrieve via graph — find entities mentioned in query, expand neighbors."""
    # Use vector search to find relevant chunks first, then extract entity names
    vector = await embedder.embed_query(query)
    hits = milvus.search_multi_channel(vector, channel_ids, limit=5)

    # Collect entity names from graph for this domain
    entities = await graph.query_entities(domain_id)

    # Filter by schema entity types if available
    if schema and schema.get("entity_types"):
        allowed_types = {et.get("name", "") for et in schema["entity_types"]}
        entities = [e for e in entities if e.get("type") in allowed_types] or entities

    entity_names = [e["name"] for e in entities]

    # Find entities mentioned in query
    query_lower = query.lower()
    matched_names = [n for n in entity_names if n.lower() in query_lower]

    # Get neighbors for matched entities
    all_neighbors: list[dict] = []
    all_relations: list[dict] = []
    for name in matched_names[:5]:
        neighbors = await graph.query_entity_neighbors(name, domain_id, max_depth=1)
        all_neighbors.extend(neighbors)
        relations = await graph.query_entity_relations(name, domain_id)
        all_relations.extend(relations)

    # Convert hits to chunks
    chunks = [
        RetrievedChunk(
            id=h.get("id", ""),
            content=h.get("content", ""),
            score=h.get("score", 0.0),
            channel_id=h.get("channel_id", 0),
            msg_date=h.get("msg_date", 0),
            thread_id=h.get("thread_id", ""),
        )
        for h in hits
    ]

    graph_ctx = GraphContext(
        entities=[{"name": n.get("name", ""), "type": n.get("type", "")} for n in all_neighbors],
        relations=[
            {"source": r.get("source", ""), "target": r.get("target", ""),
             "type": r.get("type", ""), "evidence": r.get("evidence", "")}
            for r in all_relations
        ],
    )

    return chunks, graph_ctx


# ------------------------------------------------------------------ Keyword (BM25) retrieve

async def step_keyword_retrieve(
    query_text: str,
    domain_ids: "list[UUID] | str",
    engine,
    channel_id: int = 0,
    channel_usernames: dict[int, str] | None = None,
    keywords: list[str] | None = None,
) -> tuple[list[RetrievedChunk], int]:
    """Retrieve messages by BM25 search in PostgreSQL (multi-domain).

    Uses ParadeDB BM25 (primary) or tsvector (fallback).
    Returns (chunks, total_count).
    """
    from uuid import UUID as _UUID
    from agent_memory_mcp.db.queries import search_messages_bm25, search_messages_bm25_multi

    # Normalize domain_ids
    if isinstance(domain_ids, str):
        dids = [_UUID(domain_ids)]
    else:
        dids = [d if isinstance(d, _UUID) else _UUID(str(d)) for d in domain_ids]

    rows, total = await search_messages_bm25_multi(
        engine, dids, query_text,
        limit=settings.query_keyword_max_results,
    )

    _cu = channel_usernames or {}

    chunks: list[RetrievedChunk] = []
    for row in rows:
        score = row.get("bm25_score", 1.0)
        row_channel_id = row.get("channel_id", channel_id) or channel_id
        # Build thread_id with domain_id for source link resolution
        row_domain_id = str(row.get("domain_id", dids[0] if dids else ""))
        msg_id = row.get("telegram_msg_id", 0)
        chunks.append(RetrievedChunk(
            id=str(row.get("id", "")),
            content=row.get("content", ""),
            score=float(score) if score else 1.0,
            channel_id=row_channel_id,
            msg_date=int(row["msg_date"].timestamp()) if row.get("msg_date") else 0,
            thread_id=f"{row_domain_id}_{msg_id}",
            content_type=row.get("content_type", "text"),
            relevance=CRAGRelevance.high,
        ))

    log.info("keyword_retrieve_done", query=query_text[:50], total_matches=total, returned=len(chunks))
    return chunks, total


# ------------------------------------------------------------------ Hashtag retrieve

def extract_hashtags_from_query(query: str) -> list[str]:
    """Extract hashtags from query text."""
    return _HASHTAG_RE.findall(query)


async def step_hashtag_retrieve(
    hashtag: str,
    domain_id: str,
    engine,
    channel_id: int = 0,
    limit: int = 200,
) -> tuple[list[RetrievedChunk], int]:
    """Retrieve messages by exact hashtag JSONB match."""
    from uuid import UUID
    from agent_memory_mcp.db.queries import search_messages_by_hashtag

    rows, total = await search_messages_by_hashtag(
        engine, UUID(domain_id), hashtag, limit=limit,
    )

    chunks: list[RetrievedChunk] = []
    for row in rows:
        chunks.append(RetrievedChunk(
            id=str(row.get("id", "")),
            content=row.get("content", ""),
            score=1.0,
            channel_id=channel_id,
            msg_date=int(row["msg_date"].timestamp()) if row.get("msg_date") else 0,
            thread_id=f"{domain_id}_{row.get('telegram_msg_id', 0)}",
            content_type=row.get("content_type", "text"),
            relevance=CRAGRelevance.high,
        ))

    log.info("hashtag_retrieve_done", hashtag=hashtag, total=total, returned=len(chunks))
    return chunks, total


# ------------------------------------------------------------------ Rerank

async def step_rerank(
    query: str,
    chunks: list[RetrievedChunk],
    reranker: RerankerClient,
    top_k: int | None = None,
) -> list[RetrievedChunk]:
    """Rerank chunks using cross-encoder."""
    _top_k = top_k or settings.query_rerank_top_k
    if not chunks:
        return []

    documents = [c.content for c in chunks]
    log.info("rerank_start", chunks=len(chunks), top_k=_top_k)
    try:
        ranked = await reranker.rerank(query, documents, top_k=_top_k)
        result = []
        for item in ranked:
            idx = item.get("index", 0)
            if idx < len(chunks):
                chunk = chunks[idx].model_copy()
                chunk.score = item.get("score", chunk.score)
                result.append(chunk)
        log.info(
            "rerank_done",
            returned=len(result),
            scores=[round(c.score, 4) for c in result],
        )
        return result
    except Exception:
        log.exception("rerank_failed")
        return chunks[:_top_k]


# ------------------------------------------------------------------ CRAG

async def step_crag_check(
    query: str,
    chunks: list[RetrievedChunk],
) -> tuple[list[RetrievedChunk], bool]:
    """Check chunk relevance via CRAG. Returns (chunks with relevance, needs_reformulation)."""
    if not chunks:
        return [], True

    # Check top-3 chunks for relevance
    checked = list(chunks)
    low_count = 0

    for i, chunk in enumerate(checked[:3]):
        try:
            prompt = CRAG_RELEVANCE_SYSTEM.format(query=query, chunk=chunk.content[:1000])
            data = await llm_call_json(
                model=settings.llm_tier1_model,
                messages=[
                    {"role": "system", "content": prompt},
                ],
                max_tokens=128,
            )
            relevance = CRAGRelevance(data.get("relevance", "medium"))
            checked[i] = chunk.model_copy(update={"relevance": relevance})
            if relevance == CRAGRelevance.low:
                low_count += 1
        except Exception:
            log.exception("crag_check_failed", chunk_idx=i)

    # If majority of top chunks are low relevance, suggest reformulation
    needs_reformulation = low_count >= 2
    # Filter out low-relevance chunks
    filtered = [c for c in checked if c.relevance != CRAGRelevance.low]
    if not filtered:
        filtered = checked  # Keep all if everything is low

    log.info(
        "crag_done",
        checked=min(len(chunks), 3),
        low=low_count,
        filtered=len(filtered),
        needs_reformulation=needs_reformulation,
    )
    return filtered, needs_reformulation


# ------------------------------------------------------------------ Graph enrich

async def step_graph_enrich(
    chunks: list[RetrievedChunk],
    domain_id: str,
    graph: FalkorDBStorage,
    schema: dict | None = None,
) -> GraphContext:
    """Enrich context with graph data based on retrieved chunks."""
    # Extract potential entity names from chunk content
    all_entities: list[dict] = []
    all_relations: list[dict] = []

    # Get entities for this domain
    try:
        domain_entities = await graph.query_entities(domain_id)
        entity_names = {e["name"].lower(): e["name"] for e in domain_entities}

        # Find entities mentioned in chunks
        mentioned: set[str] = set()
        for chunk in chunks[:5]:
            chunk_lower = chunk.content.lower()
            for lower_name, original_name in entity_names.items():
                if lower_name in chunk_lower:
                    mentioned.add(original_name)

        # Get relations and communities for mentioned entities
        seen_communities: set[str] = set()
        community_summaries: list[str] = []
        for name in list(mentioned)[:10]:
            relations = await graph.query_entity_relations(name, domain_id)
            all_relations.extend(relations)
            all_entities.append({"name": name, "type": next(
                (e.get("type", "") for e in domain_entities if e["name"] == name), ""
            )})
            # Fetch community info
            try:
                comms = await graph.query_entity_community(name, domain_id)
                for c in comms:
                    cid = c.get("id", "")
                    if cid and cid not in seen_communities:
                        seen_communities.add(cid)
                        summary = c.get("summary", "")
                        if summary:
                            community_summaries.append(summary)
            except Exception:
                pass
    except Exception:
        log.exception("graph_enrich_failed")
        community_summaries = []

    # Filter relations by schema types if available
    filtered_relations = all_relations
    if schema and schema.get("relation_types"):
        allowed_rels = {rt.get("name", "") for rt in schema["relation_types"]}
        filtered_relations = [r for r in all_relations if r.get("type") in allowed_rels]
        if not filtered_relations:
            filtered_relations = all_relations  # fallback

    return GraphContext(
        entities=all_entities,
        relations=[
            {"source": r.get("source", ""), "target": r.get("target", ""),
             "type": r.get("type", ""), "evidence": r.get("evidence", "")}
            for r in filtered_relations
        ],
        community_summaries=community_summaries,
    )


# ------------------------------------------------------------------ Multi-domain graph wrappers

async def step_graph_retrieve_multi(
    query: str,
    domain_ids: list[str],
    graph: FalkorDBStorage,
    embedder: EmbeddingClient,
    milvus: MilvusStorage,
    channel_ids: list[int],
    schema: dict | None = None,
) -> tuple[list[RetrievedChunk], GraphContext]:
    """Graph retrieve across multiple domains — parallel queries, merge results."""
    if len(domain_ids) == 1:
        return await step_graph_retrieve(
            query, domain_ids[0], graph, embedder, milvus, channel_ids, schema=schema,
        )

    tasks = [
        step_graph_retrieve(query, did, graph, embedder, milvus, channel_ids, schema=schema)
        for did in domain_ids
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    merged_chunks: list[RetrievedChunk] = []
    merged_entities: list[dict] = []
    merged_relations: list[dict] = []
    merged_communities: list[str] = []
    seen_ids: set[str] = set()

    for r in results:
        if isinstance(r, Exception):
            log.exception("graph_retrieve_multi_error", error=str(r))
            continue
        chunks, ctx = r
        for c in chunks:
            if c.id not in seen_ids:
                merged_chunks.append(c)
                seen_ids.add(c.id)
        merged_entities.extend(ctx.entities)
        merged_relations.extend(ctx.relations)
        merged_communities.extend(ctx.community_summaries)

    return merged_chunks, GraphContext(
        entities=merged_entities,
        relations=merged_relations,
        community_summaries=merged_communities,
    )


async def step_graph_enrich_multi(
    chunks: list[RetrievedChunk],
    domain_ids: list[str],
    graph: FalkorDBStorage,
    schema: dict | None = None,
) -> GraphContext:
    """Graph enrichment across multiple domains — parallel queries, merge results."""
    if len(domain_ids) == 1:
        return await step_graph_enrich(chunks, domain_ids[0], graph, schema=schema)

    tasks = [
        step_graph_enrich(chunks, did, graph, schema=schema)
        for did in domain_ids
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    merged_entities: list[dict] = []
    merged_relations: list[dict] = []
    merged_communities: list[str] = []

    for r in results:
        if isinstance(r, Exception):
            log.exception("graph_enrich_multi_error", error=str(r))
            continue
        merged_entities.extend(r.entities)
        merged_relations.extend(r.relations)
        merged_communities.extend(r.community_summaries)

    return GraphContext(
        entities=merged_entities,
        relations=merged_relations,
        community_summaries=merged_communities,
    )


# ------------------------------------------------------------------ Assemble

def step_assemble_context(
    chunks: list[RetrievedChunk],
    graph_ctx: GraphContext,
    history: HistoryContext,
    max_tokens: int | None = None,
    keyword_total: int = 0,
) -> str:
    """Assemble final context string within token budget."""
    _max = max_tokens or settings.query_context_max_tokens
    parts: list[str] = []

    # Add keyword stats if available
    if keyword_total > 0:
        parts.append(
            f"Найдено {keyword_total} постов по данному запросу. "
            f"Ниже представлены {len(chunks)} из них."
        )

    # Add community summaries if available
    if graph_ctx.community_summaries:
        comm_lines = ["Тематические группы:"]
        for s in graph_ctx.community_summaries[:5]:
            comm_lines.append(f"- {s}")
        parts.append("\n".join(comm_lines))

    # Add graph context if available
    if graph_ctx.entities or graph_ctx.relations:
        graph_lines = ["Связи из графа знаний:"]
        for rel in graph_ctx.relations[:30]:
            graph_lines.append(
                f"- {rel.get('source', '')} → {rel.get('type', '')} → {rel.get('target', '')}"
            )
        graph_text = "\n".join(graph_lines)
        parts.append(graph_text)

    # Add chunks
    for i, chunk in enumerate(chunks):
        part = f"[Фрагмент {i + 1}]\n{chunk.content}"
        parts.append(part)

    full = "\n\n".join(parts)
    result = truncate_to_tokens(full, _max)
    log.info(
        "assemble_done",
        chunks=len(chunks),
        graph_relations=len(graph_ctx.relations),
        context_chars=len(result),
        context_tokens=count_tokens(result),
    )
    return result


# ------------------------------------------------------------------ Generate

def extract_keywords_simple(query: str) -> list[str]:
    """Extract keywords from query without LLM — instant, for modes without router."""
    import re as _re

    keywords: list[str] = []
    seen: set[str] = set()

    def _add(word: str) -> None:
        w = word.strip()
        if w and w.lower() not in seen:
            seen.add(w.lower())
            keywords.append(w)

    # Quoted phrases
    for match in _re.findall(r'["\u00ab\u201c]([^"\u00bb\u201d]+)["\u00bb\u201d]', query):
        _add(match)

    # Hashtags
    for match in _re.findall(r"#(\w+)", query):
        _add(match)

    # CamelCase words
    for match in _re.findall(r"\b([A-Z][a-z]+(?:[A-Z][a-z]+)+)\b", query):
        _add(match)

    # Words > 4 chars (likely meaningful nouns)
    for word in _re.findall(r"\b(\w{5,})\b", query):
        _add(word)

    return keywords


async def step_generate(
    query: str,
    context: str,
    history: HistoryContext,
    channel_username: str,
    chunks: list[RetrievedChunk],
    model: str | None = None,
    temperature: float = 0.3,
    max_tokens: int = 2048,
    sources_limit: int = 10,
    domain_schema: dict | None = None,
) -> QueryAnswer:
    """Generate final answer using LLM."""
    _model = model or settings.llm_tier3_model
    messages: list[dict] = [{"role": "system", "content": GENERATION_SYSTEM}]

    # Add domain schema context if available
    if domain_schema:
        domain_name = domain_schema.get("detected_domain", "")
        et_names = [et.get("name", "") for et in (domain_schema.get("entity_types") or [])]
        rt_names = [rt.get("name", "") for rt in (domain_schema.get("relation_types") or [])]
        schema_info = f"Домен: {domain_name}."
        if et_names:
            schema_info += f" Сущности: {', '.join(et_names[:15])}."
        if rt_names:
            schema_info += f" Связи: {', '.join(rt_names[:10])}."
        messages.append({"role": "system", "content": schema_info})

    # Add history summary if present
    if history.has_summary:
        messages.append({
            "role": "system",
            "content": f"Резюме предыдущего разговора:\n{history.summary}",
        })

    # Add history messages
    for msg in history.messages:
        messages.append({"role": msg.role, "content": msg.content})

    # Add context + query
    user_content = f"Контекст:\n{context}\n\nВопрос: {query}"
    messages.append({"role": "user", "content": user_content})

    answer_text = await llm_call(
        model=_model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )

    # Build source references from chunks
    from agent_memory_mcp.utils.links import make_tme_link

    sources: list[SourceReference] = []
    seen_thread_ids: set[str] = set()
    for chunk in chunks:
        tid = chunk.thread_id
        if tid and tid not in seen_thread_ids:
            seen_thread_ids.add(tid)
            # thread_id is "{domain_id}_{telegram_msg_id}" format
            parts = tid.rsplit("_", 1)
            if len(parts) == 2:
                try:
                    msg_id = int(parts[1])
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
        sources=sources[:sources_limit],
        chunks_used=len(chunks),
    )
    log.info(
        "generate_done",
        answer_len=len(answer.answer),
        sources=len(answer.sources),
        context_tokens=count_tokens(context),
    )
    return answer


# ------------------------------------------------------------------ Cascaded filter

async def cascaded_filter(
    posts: list[dict],
    query: str,
    schema: dict | None,
) -> list[dict]:
    """PRISMA-style Stage 2 filter: entity type relevance (schema-based).

    BM25 scoring (Stage 1) is already done in retrieval.
    This applies schema entity type filter on retrieved posts.
    """
    if not schema or not schema.get("entity_types"):
        return posts

    relevant_types = _infer_relevant_types(query, schema["entity_types"])
    if not relevant_types:
        return posts

    # Build set of keywords from relevant entity type names + descriptions
    type_keywords: set[str] = set()
    for et in schema["entity_types"]:
        if et.get("name") in relevant_types:
            type_keywords.add(et["name"].lower())
            # Add keywords from description if available
            desc = et.get("description", "")
            if desc:
                for word in desc.split():
                    if len(word) >= 4:
                        type_keywords.add(word.lower())

    filtered = []
    for post in posts:
        content = (post.get("content") or "").lower()
        if any(kw in content for kw in type_keywords):
            filtered.append(post)

    if not filtered:
        return posts  # Don't filter to empty
    log.info("cascaded_filter_done", input=len(posts), output=len(filtered),
             relevant_types=list(relevant_types))
    return filtered


def _infer_relevant_types(query: str, entity_types: list[dict]) -> set[str]:
    """Infer which entity types are relevant to the query (lightweight, no LLM)."""
    query_lower = query.lower()
    relevant: set[str] = set()
    for et in entity_types:
        name = et.get("name", "")
        # Check if entity type name or its keywords appear in query
        if name.lower() in query_lower:
            relevant.add(name)
        desc = et.get("description", "")
        if desc:
            desc_words = [w for w in desc.lower().split() if len(w) >= 5]
            if any(w in query_lower for w in desc_words):
                relevant.add(name)
    return relevant


def _is_overview_query(query: str) -> bool:
    """Simple heuristic: is query asking for general overview?"""
    overview_words = {
        "проанализируй", "обзор", "обобщи", "тренды", "расскажи",
        "о чём", "о чем", "подведи итог", "суммируй", "summary",
    }
    query_lower = query.lower()
    return any(w in query_lower for w in overview_words)


# ------------------------------------------------------------------ Map-Reduce

_REDUCE_CONTEXT_LIMIT = 12_000  # tokens — collapse if MAP output exceeds this
_COLLAPSE_GROUP_SIZE = 4        # MAP outputs per collapse group
_TOKENS_PER_ITEM = 120          # estimated tokens per item in reduce output
_REDUCE_MAX_FLOOR = 4096        # min reduce max_tokens
_REDUCE_MAX_CEIL = 16384        # max reduce max_tokens (Sonnet 4.5 native max)
_SKIP_MARKER = "[SKIP]"


async def step_map_reduce(
    query: str,
    posts: list[dict],
    schema: dict | None = None,
    community_summaries: list[str] | None = None,
    channel_username: str = "",
    batch_size: int = 15,
    max_concurrent: int = 8,
    progress_callback: Callable | None = None,
) -> str:
    """MAP → (COLLAPSE) → REDUCE pipeline.

    MAP: Tier 2 extracts per batch (parallel).
    COLLAPSE: iterative compression if MAP output exceeds reduce context.
    REDUCE: Tier 3 aggregates with adaptive max_tokens.
    """
    if not posts:
        return "Нет данных для анализа."

    # Build domain context
    domain_context = ""
    if schema:
        domain = schema.get("detected_domain", "")
        et_names = [et.get("name", "") for et in (schema.get("entity_types") or [])]
        if domain:
            domain_context = f" ({domain})"
        if et_names:
            domain_context += f". Типы сущностей: {', '.join(et_names[:10])}"

    # --- MAP phase ---
    batches = [posts[i : i + batch_size] for i in range(0, len(posts), batch_size)]
    total_batches = len(batches)
    semaphore = asyncio.Semaphore(max_concurrent)
    extractions: list[str] = []
    done_count = 0

    async def _map_batch(batch: list[dict], batch_idx: int) -> str:
        nonlocal done_count
        posts_text = "\n\n---\n\n".join(
            f"[Пост {i+1}] {(p.get('content') or '')[:1500]}"
            for i, p in enumerate(batch)
        )
        prompt = MAP_EXTRACT_SYSTEM.format(
            domain_context=domain_context,
            query=query,
            posts=posts_text,
        )
        async with semaphore:
            result = await llm_call(
                model=settings.llm_tier2_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=3072,
            )
            done_count += 1
            if progress_callback:
                try:
                    await progress_callback(done_count, total_batches)
                except Exception:
                    pass
            return result.strip()

    tasks = [_map_batch(batch, i) for i, batch in enumerate(batches)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if isinstance(r, str) and r:
            extractions.append(r)
        elif isinstance(r, Exception):
            log.exception("map_batch_failed", error=str(r))

    if not extractions:
        return "Не удалось извлечь данные из постов."

    # --- Filter SKIP-only extractions ---
    filtered_extractions: list[str] = []
    for ext in extractions:
        lines = [ln.strip() for ln in ext.splitlines() if ln.strip()]
        useful = [ln for ln in lines if _SKIP_MARKER not in ln]
        if useful:
            filtered_extractions.append("\n".join(useful))

    if not filtered_extractions:
        return "Не найдено релевантных данных в постах."

    log.info(
        "map_phase_done",
        total_extractions=len(extractions),
        after_skip_filter=len(filtered_extractions),
    )

    # --- COLLAPSE phase (iterative) ---
    collapsed = filtered_extractions
    collapse_round = 0

    while count_tokens("\n\n".join(collapsed)) > _REDUCE_CONTEXT_LIMIT and len(collapsed) > 1:
        collapse_round += 1
        log.info(
            "collapse_round",
            round=collapse_round,
            inputs=len(collapsed),
            tokens=count_tokens("\n\n".join(collapsed)),
        )

        groups: list[list[str]] = [
            collapsed[i : i + _COLLAPSE_GROUP_SIZE]
            for i in range(0, len(collapsed), _COLLAPSE_GROUP_SIZE)
        ]
        collapse_sem = asyncio.Semaphore(max_concurrent)

        async def _collapse_group(group: list[str]) -> str:
            prompt = MAP_COLLAPSE_SYSTEM.format(
                domain_context=domain_context,
                query=query,
                extractions="\n\n---\n\n".join(group),
            )
            async with collapse_sem:
                return (await llm_call(
                    model=settings.llm_tier2_model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    max_tokens=3072,
                )).strip()

        collapse_results = await asyncio.gather(
            *[_collapse_group(g) for g in groups],
            return_exceptions=True,
        )
        collapsed = [
            r for r in collapse_results
            if isinstance(r, str) and r
        ]
        for r in collapse_results:
            if isinstance(r, Exception):
                log.exception("collapse_failed", error=str(r))

        if collapse_round >= 5:
            log.warning("collapse_max_rounds_reached")
            break

    # --- REDUCE phase ---
    combined = "\n\n".join(collapsed)
    community_context = ""
    if community_summaries:
        community_context = "Тематические группы:\n" + "\n".join(
            f"- {s}" for s in community_summaries[:5]
        )

    # Adaptive max_tokens: scale with number of posts
    adaptive_max = len(posts) * _TOKENS_PER_ITEM
    reduce_max_tokens = max(_REDUCE_MAX_FLOOR, min(adaptive_max, _REDUCE_MAX_CEIL))

    reduce_prompt = MAP_REDUCE_SYSTEM.format(
        total_posts=len(posts),
        channel=channel_username,
        domain_context=domain_context,
        community_context=community_context,
        query=query,
    )

    result = await llm_call(
        model=settings.llm_tier3_model,
        messages=[
            {"role": "system", "content": reduce_prompt},
            {"role": "user", "content": combined},
        ],
        temperature=0.3,
        max_tokens=reduce_max_tokens,
    )

    log.info(
        "map_reduce_done",
        posts=len(posts),
        batches=total_batches,
        extractions_raw=len(extractions),
        extractions_filtered=len(filtered_extractions),
        collapse_rounds=collapse_round,
        reduce_input_tokens=count_tokens(combined),
        reduce_max_tokens=reduce_max_tokens,
        result_len=len(result),
    )
    return result.strip()
