"""Dedicated read-receipt listener process.

Runs in its OWN process/event loop (separate container, AMM_ROLE=listener) so the
user-session Telethon clients it keeps always-connected can never starve the
aiogram bot's polling loop — which is exactly what broke when the listener ran
inside the bot process.

It is intentionally light: it only receives UpdateReadHistoryInbox /
UpdateReadChannelInbox and stamps messages.read_at (see collector.read_listener
`attach`). No get_dialogs / get_entity, so nothing blocks.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import structlog
from telethon import TelegramClient
from telethon.sessions import StringSession

from agent_memory_mcp.collector.encryption import decrypt_session
from agent_memory_mcp.collector.read_listener import attach
from agent_memory_mcp.config import settings
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db import queries_groups as gq
from agent_memory_mcp.db.engine import async_engine

log = structlog.get_logger(__name__)

_RESYNC_INTERVAL = 1800  # re-read folder membership every 30 min


def _proxy():
    if not settings.telegram_proxy:
        return None
    from python_socks import ProxyType
    url = settings.telegram_proxy
    host = url.split("://")[1].split(":")[0]
    port = int(url.split(":")[-1])
    return (ProxyType.SOCKS5, host, port)


async def _run_for_user(telegram_id: int) -> None:
    """Connect one user's session, attach read handlers, stay connected."""
    row = await db_q.get_telegram_session(async_engine, telegram_id)
    if not row:
        return
    try:
        session_string = decrypt_session(row["session_data"])
    except Exception:
        log.warning("listener_session_decrypt_failed", telegram_id=telegram_id)
        return

    client = TelegramClient(
        StringSession(session_string),
        settings.telegram_api_id,
        settings.telegram_api_hash,
        proxy=_proxy(),
    )
    try:
        await client.connect()
        if not await client.is_user_authorized():
            log.warning("listener_session_unauthorized", telegram_id=telegram_id)
            await client.disconnect()
            return
        attach(client, telegram_id)
        log.info("listener_user_connected", telegram_id=telegram_id)
        resync = asyncio.create_task(_folder_resync_loop(client, telegram_id))
        try:
            await client.run_until_disconnected()
        finally:
            resync.cancel()
    except Exception:
        log.warning("listener_user_loop_failed", telegram_id=telegram_id, exc_info=True)
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


async def _folder_resync_loop(client, telegram_id: int) -> None:
    """Periodically re-read the user's TG folders and add chats newly added to an
    already-imported folder (membership isn't static after the first import)."""
    from agent_memory_mcp.collector.pool import _UserCollector

    uc = _UserCollector(telegram_id, client)
    await asyncio.sleep(60)  # let the connection settle first
    while True:
        try:
            await _resync_folders(uc, telegram_id)
        except Exception:
            log.warning("folder_resync_failed", telegram_id=telegram_id, exc_info=True)
        await asyncio.sleep(_RESYNC_INTERVAL)


async def _resync_folders(uc, telegram_id: int) -> None:
    folders = {f["id"]: f for f in await uc.get_folders()}
    groups = await gq.list_groups(async_engine, telegram_id)
    domains = await db_q.list_domains(async_engine, telegram_id)
    cid_to_did = {d["channel_id"]: d["id"] for d in domains}
    added = 0
    for g in groups:
        fid = g.get("tg_folder_id")
        folder = folders.get(fid) if fid is not None else None
        if not folder:
            continue
        members = set(await gq.get_group_domain_ids(async_engine, g["id"]))
        for p in folder["peers"]:
            if not p.get("supported"):
                continue
            cid = p["channel_id"]
            did = cid_to_did.get(cid)
            if did is None:
                dom = await db_q.create_domain(
                    async_engine,
                    owner_id=telegram_id,
                    channel_id=cid,
                    channel_username=p.get("username", ""),
                    channel_name=p["title"],
                    sync_depth=g.get("sync_depth") or "3m",
                    sync_frequency_minutes=60,
                    emoji="📁",
                    display_name=p["title"],
                    pinned=False,
                    peer_type=p.get("peer_type", "channel"),
                )
                did = dom["id"]
                cid_to_did[cid] = did
                await db_q.update_domain(async_engine, did, next_sync_at=datetime.now(timezone.utc))
                added += 1
            if did not in members:
                await gq.add_domains_to_group(async_engine, g["id"], [did])
                members.add(did)
    if added:
        log.info("folder_resync_added", telegram_id=telegram_id, added=added)


async def main() -> None:
    log.info("read_listener_service_starting")
    running: dict[int, asyncio.Task] = {}
    while True:
        try:
            tids = await db_q.list_active_telegram_sessions(async_engine)
        except Exception:
            log.warning("listener_list_sessions_failed", exc_info=True)
            tids = []
        for tid in tids:
            task = running.get(tid)
            if task is None or task.done():
                running[tid] = asyncio.create_task(_run_for_user(tid))
                log.info("listener_user_task_spawned", telegram_id=tid)
        await asyncio.sleep(settings.read_listener_refresh_seconds)


if __name__ == "__main__":
    asyncio.run(main())
