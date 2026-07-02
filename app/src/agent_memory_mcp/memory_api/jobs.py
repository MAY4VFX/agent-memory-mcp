"""Async job manager for long-running operations (digest, decisions, etc.).

Backed by the `jobs` table so results survive restarts and are readable across
instances. Ownership is enforced in get_job so job_ids can't leak other users'
results (issue #18 #8/#4).
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, Coroutine

import structlog
from sqlalchemy import text

from agent_memory_mcp.db.engine import async_engine

log = structlog.get_logger(__name__)

# Job TTL: results kept for 1 hour
_JOB_TTL = 3600


async def create_job(coro: Coroutine, owner_id: int = 0) -> str:
    """Persist a job row, launch the coroutine in the background, return job_id."""
    job_id = f"job_{uuid.uuid4().hex[:12]}"
    async with async_engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO jobs (job_id, owner_id, status) VALUES (:id, :oid, 'running')"),
            {"id": job_id, "oid": owner_id},
        )

    async def _run():
        try:
            result = await coro
            async with async_engine.begin() as conn:
                await conn.execute(
                    text("UPDATE jobs SET status='completed', result=CAST(:r AS jsonb), completed_at=now() WHERE job_id=:id"),
                    {"r": json.dumps(result, default=str), "id": job_id},
                )
            log.info("job_completed", job_id=job_id)
        except Exception as e:
            async with async_engine.begin() as conn:
                await conn.execute(
                    text("UPDATE jobs SET status='failed', error=:e, completed_at=now() WHERE job_id=:id"),
                    {"e": str(e)[:500], "id": job_id},
                )
            log.warning("job_failed", job_id=job_id, error=str(e)[:200])

    asyncio.create_task(_run(), name=f"job_{job_id}")
    await _cleanup_old_jobs()
    return job_id


async def get_job(job_id: str, owner_id: int | None = None) -> dict | None:
    """Get job status and result.

    If owner_id is given, the job must belong to that owner — otherwise return
    None (caller surfaces 404), so job_ids can't be used to read others' results.
    """
    async with async_engine.begin() as conn:
        row = (await conn.execute(
            text("""
                SELECT owner_id, status, result, error,
                       EXTRACT(EPOCH FROM (now() - created_at)) AS elapsed
                FROM jobs WHERE job_id = :id
            """),
            {"id": job_id},
        )).mappings().first()
    if not row:
        return None
    if owner_id is not None and row["owner_id"] != owner_id:
        return None

    out: dict[str, Any] = {"job_id": job_id, "status": row["status"]}
    if row["status"] == "completed":
        res = row["result"]
        out["result"] = json.loads(res) if isinstance(res, str) else res
    elif row["status"] == "failed":
        out["error"] = row["error"]
    elif row["status"] == "running":
        out["elapsed_seconds"] = int(row["elapsed"] or 0)
    return out


async def _cleanup_old_jobs() -> None:
    """Remove jobs older than TTL."""
    async with async_engine.begin() as conn:
        await conn.execute(
            text("DELETE FROM jobs WHERE created_at < now() - make_interval(secs => :ttl)"),
            {"ttl": _JOB_TTL},
        )
