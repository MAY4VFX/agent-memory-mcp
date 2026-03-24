"""Lightweight HTTP API for testing the query pipeline.

POST /api/query  {query, domain_id?, search_mode?}
  → runs pipeline, sends answer to admin via Telegram, returns JSON trace.

GET /api/health  → 200 OK
"""

from __future__ import annotations

import time
from uuid import UUID

import structlog
from aiohttp import web
from aiogram import Bot

from agent_memory_mcp.bot.formatters import format_answer
from agent_memory_mcp.config import settings
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db import queries_conversations as qc
from agent_memory_mcp.db.engine import async_engine
from agent_memory_mcp.pipeline.query_orchestrator import run_query_pipeline
from agent_memory_mcp.storage.embedding_client import EmbeddingClient
from agent_memory_mcp.storage.falkordb_client import FalkorDBStorage
from agent_memory_mcp.storage.milvus_client import MilvusStorage
from agent_memory_mcp.storage.reranker_client import RerankerClient

log = structlog.get_logger(__name__)


async def handle_query(request: web.Request) -> web.Response:
    """Run a query through the pipeline and send result to admin via Telegram."""
    bot: Bot = request.app["bot"]

    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON"}, status=400)

    query = body.get("query", "").strip()
    if not query:
        return web.json_response({"error": "query is required"}, status=400)

    domain_id_str = body.get("domain_id")
    search_mode = body.get("search_mode", "balanced")
    send_telegram = body.get("send_telegram", True)
    user_id = settings.admin_telegram_id

    # Resolve domain
    if not domain_id_str:
        user = await db_q.get_user(async_engine, user_id)
        if user and user.get("active_domain_id"):
            domain_id_str = str(user["active_domain_id"])
        else:
            return web.json_response({"error": "no active domain"}, status=400)

    domain_id = UUID(domain_id_str)
    domain = await db_q.get_domain(async_engine, domain_id)
    if not domain:
        return web.json_response({"error": "domain not found"}, status=404)

    # Create temp conversation
    conv = await qc.create_conversation(
        async_engine, user_id=user_id, domain_id=domain_id,
        title=f"[api-test] {query[:40]}",
    )
    conv_id = conv["id"]

    milvus = MilvusStorage()
    graph = FalkorDBStorage()
    embedder = EmbeddingClient()
    reranker = RerankerClient()

    try:
        t0 = time.monotonic()
        answer, payload = await run_query_pipeline(
            query=query,
            user_id=user_id,
            conversation_id=conv_id,
            domain_ids=[domain_id],
            engine=async_engine,
            milvus=milvus,
            graph=graph,
            embedder=embedder,
            reranker=reranker,
            search_mode=search_mode,
        )
        elapsed = time.monotonic() - t0
    except Exception as exc:
        log.exception("api_query_failed", query=query[:80])
        return web.json_response({"error": str(exc)}, status=500)
    finally:
        milvus.close()
        graph.close()
        await embedder.close()
        await reranker.close()

    # Send to admin via Telegram
    if send_telegram:
        try:
            domain_display = (
                f"@{domain['channel_username']}"
                if domain.get("channel_username") else domain.get("display_name", "")
            )
            header = f"<b>[API Test]</b> <code>{query}</code>\n\n"
            formatted = format_answer(answer, domain_display)
            text = header + formatted
            if len(text) > 4096:
                text = text[:4093] + "..."
            await bot.send_message(user_id, text)
        except Exception:
            log.exception("api_send_telegram_failed")

    # Build JSON response
    chunks_relevance: dict[str, int] = {}
    for c in payload.chunks:
        rel = c.get("relevance", "unknown")
        chunks_relevance[rel] = chunks_relevance.get(rel, 0) + 1

    result = {
        "query": query,
        "trace_id": answer.langfuse_trace_id,
        "self_rag": answer.self_rag_decision,
        "route": answer.route,
        "crag_iterations": answer.crag_iterations,
        "latency_sec": round(elapsed, 2),
        "answer": answer.answer,
        "answer_len": len(answer.answer),
        "sources_count": len(answer.sources),
        "sources": [
            {"msg_id": s.message_id, "url": s.url, "channel": s.channel_username}
            for s in answer.sources[:10]
        ],
        "payload": {
            "transform_type": payload.transform_type,
            "transformed_queries": payload.transformed_queries,
            "route": payload.route,
            "chunks_count": len(payload.chunks),
            "chunks_relevance": chunks_relevance,
            "graph_entities": len(payload.graph_context.get("entities", [])),
            "graph_relations": len(payload.graph_context.get("relations", [])),
            "graph_communities": len(payload.graph_context.get("community_summaries", [])),
            "crag_iterations": payload.crag_iterations,
            "token_count": payload.token_count,
        },
        "domain": {
            "id": str(domain_id),
            "channel_username": domain.get("channel_username", ""),
        },
    }

    # Cleanup temp conversation
    await qc.delete_conversation(async_engine, conv_id)

    return web.json_response(result, dumps=_json_dumps)


async def handle_health(request: web.Request) -> web.Response:
    return web.Response(text="OK")


def create_web_app(bot: Bot) -> web.Application:
    app = web.Application()
    app["bot"] = bot
    app.router.add_post("/api/query", handle_query)
    app.router.add_get("/api/health", handle_health)
    return app


def _json_dumps(obj: object, **kw: object) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False, default=str, **kw)
