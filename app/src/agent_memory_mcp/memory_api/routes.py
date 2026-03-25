"""FastAPI routes for Memory API."""

from __future__ import annotations

from uuid import UUID

import structlog
from fastapi import APIRouter, Depends

from agent_memory_mcp.memory_api import schemas as S
from agent_memory_mcp.memory_api import service
from agent_memory_mcp.memory_api.auth import require_credits, verify_api_key

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
    result = await service.sync_status(api_key["telegram_id"])
    return result


# --- Paid endpoints ---

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
    return {
        **result,
        "credits_used": 3,
        "balance_after": api_key["credits_balance"],
    }


@router.post("/sources/add")
async def add_source(
    req: S.AddSourceRequest,
    api_key: dict = Depends(require_credits("sources/add")),
):
    result = await service.add_source(
        owner_id=api_key["telegram_id"],
        handle=req.handle,
        source_type=req.source_type,
        sync_range=req.sync_range,
    )
    return {**result, "credits_used": 5, "balance_after": api_key["credits_balance"]}


@router.post("/sources/sync")
async def sync_source(
    req: S.SyncSourceRequest,
    api_key: dict = Depends(require_credits("sources/sync")),
):
    return {"status": "queued", "source_id": str(req.source_id), "credits_used": 5}


@router.delete("/sources/{source_id}")
async def remove_source(source_id: UUID, api_key: dict = Depends(verify_api_key)):
    return {"status": "deleted", "source_id": str(source_id)}


@router.post("/digest")
async def get_digest(
    req: S.GetDigestRequest,
    api_key: dict = Depends(require_credits("digest")),
):
    result = await service.get_digest(
        owner_id=api_key["telegram_id"],
        scope=req.scope,
        period=req.period,
    )
    return {**result, "credits_used": 10, "balance_after": api_key["credits_balance"]}


@router.post("/decisions")
async def get_decisions(
    req: S.GetDecisionsRequest,
    api_key: dict = Depends(require_credits("decisions")),
):
    result = await service.get_decisions(
        owner_id=api_key["telegram_id"],
        scope=req.scope,
        topic=req.topic,
    )
    return {**result, "credits_used": 5, "balance_after": api_key["credits_balance"]}


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
    return {**result, "credits_used": 10, "balance_after": api_key["credits_balance"]}


@router.post("/analysis/deep")
async def deep_analysis(
    req: S.DeepAnalysisRequest,
    api_key: dict = Depends(require_credits("analysis/deep")),
):
    return {
        "analysis": "Deep analysis not yet implemented",
        "credits_used": 25,
        "balance_after": api_key["credits_balance"],
    }
