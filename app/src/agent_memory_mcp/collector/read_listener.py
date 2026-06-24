"""Live read-receipt listener — captures *when* the owner reads inbound messages.

Telegram never exposes a per-message "read at T"; the only way to learn it is to
listen, while online, for UpdateReadHistoryInbox / UpdateReadChannelInbox and
stamp the read time as the update arrives. This feeds the cross-source workload
resolver's time model (response effort = read → send, not publish → send).

Gated by `settings.run_read_listener` (default off): it keeps a long-lived
Telethon connection per active user. The manager re-ensures listeners on a timer
so connections recycled by the collector pool get re-attached.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import structlog
from telethon import events
from telethon.tl.types import (
    PeerChannel,
    PeerChat,
    PeerUser,
    UpdateReadChannelInbox,
    UpdateReadHistoryInbox,
)

from agent_memory_mcp.config import settings
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db.engine import async_engine

log = structlog.get_logger(__name__)


def _bare_peer_id(peer) -> int | None:
    """Bare Telegram id from a Peer, matching how domains.channel_id is stored
    (entity.id / chat.id — unmarked, no -100 prefix)."""
    if isinstance(peer, PeerChannel):
        return peer.channel_id
    if isinstance(peer, PeerChat):
        return peer.chat_id
    if isinstance(peer, PeerUser):
        return peer.user_id
    return None


def attach(client, owner_id: int) -> None:
    """Register read-history handlers on an already-connected client for owner."""

    async def _on_read(event) -> None:
        upd = event.original_update if hasattr(event, "original_update") else event
        try:
            if isinstance(upd, UpdateReadChannelInbox):
                channel_id, max_id = upd.channel_id, upd.max_id
            elif isinstance(upd, UpdateReadHistoryInbox):
                channel_id, max_id = _bare_peer_id(upd.peer), upd.max_id
            else:
                return
            if channel_id is None or not max_id:
                return
            n = await db_q.stamp_read_until(
                async_engine, owner_id, channel_id, max_id,
                datetime.now(timezone.utc),
            )
            if n:
                log.debug("read_stamped", owner_id=owner_id, channel_id=channel_id, n=n)
        except Exception:
            log.warning("read_stamp_failed", owner_id=owner_id, exc_info=True)

    client.add_event_handler(_on_read, events.Raw(UpdateReadChannelInbox))
    client.add_event_handler(_on_read, events.Raw(UpdateReadHistoryInbox))


class ReadListenerManager:
    """Keeps read listeners attached to active users' Telethon clients."""

    def __init__(self, pool) -> None:
        self._pool = pool
        self._attached: dict[int, object] = {}  # telegram_id → client we attached to
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop())
        log.info("read_listener_started")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()

    async def _loop(self) -> None:
        while True:
            try:
                await self._ensure_all()
            except Exception:
                log.warning("read_listener_ensure_failed", exc_info=True)
            await asyncio.sleep(settings.read_listener_refresh_seconds)

    async def _ensure_all(self) -> None:
        for tid in await db_q.list_active_telegram_sessions(async_engine):
            uc = await self._pool.get_collector(tid)  # connects/recycles as needed
            if not uc:
                continue
            # Re-attach if this is a fresh client object (pool recycled it).
            if self._attached.get(tid) is not uc.client:
                attach(uc.client, tid)
                self._attached[tid] = uc.client
                log.info("read_listener_attached", telegram_id=tid)
