"""FastAPI routes for Memory API."""

from __future__ import annotations

from uuid import UUID

import structlog
from fastapi import APIRouter, Depends

from agent_memory_mcp.memory_api import schemas as S
from agent_memory_mcp.memory_api import service
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
        from fastapi import HTTPException
        raise HTTPException(404, "Source not found")
    await db_q.delete_domain(async_engine, source_id)
    return {"status": "deleted", "source_id": str(source_id)}


# --- Paid endpoints (costs from CREDIT_COSTS) ---

@router.post("/memory/search")
async def search_memory(
    req: S.SearchMemoryRequest,
    api_key: dict = Depends(require_credits("memory/search")),
):
    result = await service.search_memory(
        query=req.query,
        owner_id=api_key["telegram_id"],
        scope=req.scope,
        limit=req.limit,
    )
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
