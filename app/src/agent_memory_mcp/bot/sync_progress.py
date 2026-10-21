"""Live sync status for a source: one bot message, edited as the sync moves.

After «Add Source» the user only saw «added, sync starts within 30 seconds»
and then nothing — no way to tell queued from running from stuck (real case:
@tammba sat behind 50 overdue re-syncs). The tracker posts a status message in
the thread the source was added from and edits it through the stages that
sync_jobs records: queued → fetching → processing N messages → done / failed.
"""

from __future__ import annotations

import asyncio
from uuid import UUID

import structlog
from aiogram import Bot, html
from aiogram.exceptions import TelegramBadRequest

from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db.engine import async_engine

log = structlog.get_logger(__name__)

POLL_SECONDS = 5
TIMEOUT_SECONDS = 30 * 60

# Strong refs: a bare create_task() can be garbage-collected mid-run.
_tasks: set[asyncio.Task] = set()


def render_status(label: str, job: dict | None, queue_ahead: int | None = None) -> tuple[str, bool]:
    """(status line, finished?) for a source's latest sync job."""
    name = html.quote(label)
    if job is None or job["status"] == "pending":
        ahead = f" — {queue_ahead} ahead" if queue_ahead else ""
        return f"⏳ <b>{name}</b>: queued for sync{ahead}", False
    if job["status"] == "running":
        fetched = job.get("messages_fetched")
        if fetched is None:
            return f"📥 <b>{name}</b>: downloading messages…", False
        return f"⚙️ <b>{name}</b>: processing {fetched} messages…", False
    if job["status"] == "completed":
        n = job.get("messages_processed") or 0
        e = job.get("entities_extracted") or 0
        if n == 0:
            return f"✅ <b>{name}</b>: up to date — no new messages", True
        return f"✅ <b>{name}</b>: synced — {n} messages, {e} entities", True
    error = html.quote((job.get("error_message") or "unknown error")[:200])
    return f"⚠️ <b>{name}</b>: sync failed — {error}\nWill retry automatically.", True


async def current_status(domain: dict, since=None) -> tuple[str, bool]:
    """Status of a source; jobs created before `since` don't count."""
    label = f"@{domain['channel_username']}" if domain.get("channel_username") else (domain.get("display_name") or "?")
    job = await db_q.get_latest_sync_job(async_engine, domain["id"])
    if job is not None and since is not None and job["created_at"] < since:
        job = None
    ahead = None
    if job is None:
        ahead = await db_q.count_sync_queue_ahead(async_engine, domain)
    return render_status(label, job, ahead)


async def _track(bot: Bot, chat_id: int, thread_id: int | None, domain_id: UUID) -> None:
    domain = await db_q.get_domain(async_engine, domain_id)
    if not domain:
        return
    since = domain["created_at"]
    text, done = await current_status(domain, since)
    msg = await bot.send_message(chat_id, text, message_thread_id=thread_id)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + TIMEOUT_SECONDS
    while not done and loop.time() < deadline:
        await asyncio.sleep(POLL_SECONDS)
        domain = await db_q.get_domain(async_engine, domain_id)
        if not domain:
            text, done = "🗑 Source removed.", True
        else:
            new_text, done = await current_status(domain, since)
            if new_text == text:
                continue
            text = new_text
        try:
            await msg.edit_text(text)
        except TelegramBadRequest as e:
            if "not modified" not in str(e):
                log.warning("sync_progress_edit_failed", domain_id=str(domain_id), error=str(e))
                return
    if not done:
        try:
            await msg.edit_text(text + "\n\nStill going — check 📡 Sources later.")
        except TelegramBadRequest:
            pass


def track_sync(bot: Bot, chat_id: int, thread_id: int | None, domain_id: str | UUID) -> None:
    """Start a background status message for a just-added source."""

    async def run() -> None:
        try:
            await _track(bot, chat_id, thread_id, UUID(str(domain_id)))
        except Exception:
            log.exception("sync_progress_failed", domain_id=str(domain_id))

    task = asyncio.create_task(run())
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
