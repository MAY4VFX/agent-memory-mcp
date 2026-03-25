"""Async job manager for long-running operations (digest, decisions, etc.)."""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine

import structlog

log = structlog.get_logger(__name__)

# Job TTL: results kept for 1 hour
_JOB_TTL = 3600


@dataclass
class Job:
    id: str
    status: str = "running"  # running | completed | failed
    result: Any = None
    error: str | None = None
    created_at: float = field(default_factory=time.monotonic)
    completed_at: float | None = None


# In-memory store (sufficient for single-instance MVP)
_jobs: dict[str, Job] = {}


def create_job(coro: Coroutine, owner_id: int = 0) -> str:
    """Launch async job, return job_id immediately."""
    job_id = f"job_{uuid.uuid4().hex[:12]}"
    job = Job(id=job_id)
    _jobs[job_id] = job

    async def _run():
        try:
            result = await coro
            job.status = "completed"
            job.result = result
            job.completed_at = time.monotonic()
            log.info("job_completed", job_id=job_id)
        except Exception as e:
            job.status = "failed"
            job.error = str(e)[:500]
            job.completed_at = time.monotonic()
            log.warning("job_failed", job_id=job_id, error=str(e)[:200])

    asyncio.create_task(_run(), name=f"job_{job_id}")
    _cleanup_old_jobs()
    return job_id


def get_job(job_id: str) -> dict | None:
    """Get job status and result."""
    job = _jobs.get(job_id)
    if not job:
        return None
    result = {
        "job_id": job.id,
        "status": job.status,
    }
    if job.status == "completed":
        result["result"] = job.result
    elif job.status == "failed":
        result["error"] = job.error
    elif job.status == "running":
        elapsed = time.monotonic() - job.created_at
        result["elapsed_seconds"] = int(elapsed)
    return result


def _cleanup_old_jobs():
    """Remove jobs older than TTL."""
    now = time.monotonic()
    expired = [jid for jid, j in _jobs.items() if now - j.created_at > _JOB_TTL]
    for jid in expired:
        del _jobs[jid]
