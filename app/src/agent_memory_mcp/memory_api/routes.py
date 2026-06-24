"""FastAPI routes for Memory API."""

from __future__ import annotations

from uuid import UUID

import structlog
from fastapi import APIRouter, Depends, Query

from fastapi import HTTPException
from agent_memory_mcp.memory_api import schemas as S
from agent_memory_mcp.memory_api import service
from agent_memory_mcp.memory_api.service import ScopeNotFound
from agent_memory_mcp.memory_api.auth import CREDIT_COSTS, require_credits, verify_api_key

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1")


# --- Free endpoints ---

@router.get("/health", response_model=S.HealthResponse)
async def health():
    return S.HealthResponse()


@router.get("/sources")
async def list_sources(api_key: dict = Depends(verify_api_key)):
    sources = await service.list_sources(api_key["telegram_id"])
    return {"sources": sources, "count": len(sources)}


@router.get("/scopes")
async def list_scopes(api_key: dict = Depends(verify_api_key)):
    """List all available scopes for digest, search, and other tools."""
    from agent_memory_mcp.db import queries as db_q
    from agent_memory_mcp.db import queries_groups as gq
    from agent_memory_mcp.db.engine import async_engine

    owner_id = api_key["telegram_id"]
    domains = await db_q.list_domains(async_engine, owner_id)
    groups = await gq.list_groups(async_engine, owner_id)

    scopes = [{"scope": "all", "label": f"All channels ({len(domains)})", "type": "all"}]
    for g in groups:
        members = await gq.get_group_domains(async_engine, g["id"])
        scopes.append({
            "scope": f"folder:{g['name']}",
            "label": f"{g.get('emoji', '')} {g['name']} ({len(members)} channels)",
            "type": "folder",
            "channels": [f"@{m.get('channel_username', '?')}" for m in members],
        })
    for d in domains:
        if d.get("channel_username"):
            scopes.append({
                "scope": f"@{d['channel_username']}",
                "label": d.get("display_name") or d.get("channel_name") or d["channel_username"],
                "type": "channel",
                "message_count": d.get("message_count", 0),
            })
    return {"scopes": scopes, "count": len(scopes)}


def _parse_iso(value: str) -> "datetime":
    from datetime import datetime
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@router.get("/activity")
async def get_activity(
    since: str = Query(..., description="ISO8601 start, inclusive"),
    until: str | None = Query(None, description="ISO8601 end, exclusive"),
    scope: str | None = Query(None, description="all | @channel | folder:Name | domain UUID"),
    cursor: str | None = Query(None, description="Opaque pagination cursor from a prior call"),
    limit: int = Query(1000, ge=1, le=5000),
    api_key: dict = Depends(verify_api_key),  # Free — raw metadata export, no LLM cost
):
    """Raw communication-activity events (metadata only, no message content) for
    the cross-source workload resolver. Oldest-first, cursor-paginated.

    Each event carries timing, direction, length, chat, and reply identifiers.
    The ``read_at`` / ``project_id`` / ``task_id`` fields are reserved (always
    null today) and filled once the read-listener and label classifier land.
    """
    try:
        since_dt = _parse_iso(since)
        until_dt = _parse_iso(until) if until else None
    except ValueError:
        raise HTTPException(422, "since/until must be ISO8601 datetimes")
    try:
        return await service.get_activity(
            owner_id=api_key["telegram_id"],
            since=since_dt,
            until=until_dt,
            scope=scope,
            cursor=cursor,
            limit=limit,
        )
    except ScopeNotFound as e:
        raise HTTPException(404, {"error": "scope_not_found", "scope": e.scope, "available": e.available})


@router.get("/labels")
async def list_labels(
    type: str | None = Query(None, description="Filter by label type, e.g. project"),
    api_key: dict = Depends(verify_api_key),
):
    """List the owner's labels (project/task/…)."""
    return await service.list_labels(api_key["telegram_id"], type)


@router.post("/labels/classify")
async def classify_labels(api_key: dict = Depends(verify_api_key)):
    """Cluster the owner's chats into project labels (one LLM pass) and persist
    them. Free for now — a single batched call; revisit if abused."""
    return await service.classify_labels(api_key["telegram_id"])


@router.get("/account/balance")
async def get_balance(api_key: dict = Depends(verify_api_key)):
    from sqlalchemy import text
    from agent_memory_mcp.db.engine import async_engine
    async with async_engine.begin() as conn:
        row = await conn.execute(
            text("SELECT points_balance, total_points_spent FROM users WHERE telegram_id = :tid"),
            {"tid": api_key["telegram_id"]},
        )
        user = row.mappings().first()
    return {
        "balance": user["points_balance"] if user else 0,
        "total_spent": user["total_points_spent"] if user else 0,
    }


@router.get("/sync-status")
async def get_sync_status(api_key: dict = Depends(verify_api_key)):
    return await service.sync_status(api_key["telegram_id"])


@router.post("/sources/add")
async def add_source(
    req: S.AddSourceRequest,
    api_key: dict = Depends(verify_api_key),  # Free — don't charge for onboarding
):
    result = await service.add_source(
        owner_id=api_key["telegram_id"],
        handle=req.handle,
        source_type=req.source_type,
        sync_range=req.sync_range,
    )
    return result


@router.delete("/sources/{source_id}")
async def remove_source(source_id: UUID, api_key: dict = Depends(verify_api_key)):
    from agent_memory_mcp.db import queries as db_q
    from agent_memory_mcp.db.engine import async_engine
    domain = await db_q.get_domain(async_engine, source_id)
    if not domain or domain["owner_id"] != api_key["telegram_id"]:
        raise HTTPException(404, "Source not found")
    await db_q.delete_domain(async_engine, source_id)
    return {"status": "deleted", "source_id": str(source_id)}


# --- Paid endpoints (costs from CREDIT_COSTS) ---

@router.post("/memory/search")
async def search_memory(
    req: S.SearchMemoryRequest,
    api_key: dict = Depends(require_credits("memory/search")),
):
    try:
        result = await service.search_memory(
            query=req.query,
            owner_id=api_key["telegram_id"],
            scope=req.scope,
            limit=req.limit,
        )
    except ScopeNotFound as e:
        raise HTTPException(404, {"error": "scope_not_found", "scope": e.scope, "available": e.available})
    return {**result, "points_used": CREDIT_COSTS["memory/search"], "balance": api_key["credits_balance"]}


@router.post("/digest")
async def get_digest(
    req: S.GetDigestRequest,
    api_key: dict = Depends(require_credits("digest")),
):
    """Start digest generation as async job. Returns job_id — poll GET /jobs/{id}."""
    from agent_memory_mcp.memory_api.jobs import create_job
    coro = service.get_digest(
        owner_id=api_key["telegram_id"],
        scope=req.scope,
        period=req.period,
    )
    job_id = create_job(coro, owner_id=api_key["telegram_id"])
    return {"job_id": job_id, "status": "running", "points_used": CREDIT_COSTS["digest"]}


@router.post("/decisions")
async def get_decisions(
    req: S.GetDecisionsRequest,
    api_key: dict = Depends(require_credits("decisions")),
):
    """Start decisions extraction as async job. Returns job_id — poll GET /jobs/{id}."""
    from agent_memory_mcp.memory_api.jobs import create_job
    coro = service.get_decisions(
        owner_id=api_key["telegram_id"],
        scope=req.scope,
        topic=req.topic,
    )
    job_id = create_job(coro, owner_id=api_key["telegram_id"])
    return {"job_id": job_id, "status": "running", "points_used": CREDIT_COSTS["decisions"]}


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str, api_key: dict = Depends(verify_api_key)):
    """Poll job status. Returns result when completed."""
    from agent_memory_mcp.memory_api.jobs import get_job
    from fastapi import HTTPException
    result = get_job(job_id)
    if not result:
        raise HTTPException(404, "Job not found or expired")
    return result


@router.post("/memory/context")
async def get_agent_context(
    req: S.AgentContextRequest,
    api_key: dict = Depends(require_credits("memory/context")),
):
    result = await service.get_agent_context(
        owner_id=api_key["telegram_id"],
        task=req.task,
        scope=req.scope,
    )
    return {**result, "points_used": CREDIT_COSTS["memory/context"], "balance": api_key["credits_balance"]}


@router.post("/analysis/deep")
async def deep_analysis(
    req: S.DeepAnalysisRequest,
    api_key: dict = Depends(require_credits("analysis/deep")),
):
    return {
        "analysis": "Deep analysis not yet implemented",
        "points_used": CREDIT_COSTS["analysis/deep"],
        "balance": api_key["credits_balance"],
    }
