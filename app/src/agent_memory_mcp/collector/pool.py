"""Pool of per-user Telethon collectors with session caching."""

from __future__ import annotations

import asyncio
import time

import structlog
from telethon import TelegramClient
from telethon.sessions import StringSession

from agent_memory_mcp.collector.encryption import decrypt_session, encrypt_session, hash_phone
from agent_memory_mcp.config import settings
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db.engine import async_engine

log = structlog.get_logger()

# How long to keep idle collectors alive (seconds)
_COLLECTOR_TTL = 600  # 10 min


class _UserCollector:
    """Wrapper around a per-user TelegramClient."""

    def __init__(self, telegram_id: int, client: TelegramClient) -> None:
        self.telegram_id = telegram_id
        self.client = client
        self.last_used = time.monotonic()

    async def resolve_channel(self, link: str) -> dict:
        """Resolve channel link → {channel_id, title, username}.

        Handles public @usernames/links and private invite links (joining the
        chat if needed) via the shared resolver.
        """
        from agent_memory_mcp.collector.client import resolve_link

        info = await resolve_link(self.client, link)
        self.last_used = time.monotonic()
        return info

    async def list_dialogs(self, limit: int = 300) -> list[dict]:
        """List the chats the user is in (no link/admin needed — read as the user).

        Lets a user add private work chats they're merely a member of, which have
        no @username and no invite link. Each entry flags `supported`: channels,
        supergroups, and basic groups sync; DMs are listed but not ingestable.
        """
        from telethon.tl.types import Channel, Chat, User

        out: list[dict] = []
        async for d in self.client.iter_dialogs(limit=limit):
            e = d.entity
            if isinstance(e, Channel):
                typ = "channel" if getattr(e, "broadcast", False) else "supergroup"
                supported = True
            elif isinstance(e, Chat):
                typ, supported = "group", True
            elif isinstance(e, User):
                typ, supported = "dm", False
            else:
                typ, supported = "other", False
            name = (
                getattr(e, "title", None)
                or " ".join(filter(None, [getattr(e, "first_name", None), getattr(e, "last_name", None)]))
                or (f"@{e.username}" if getattr(e, "username", None) else str(getattr(e, "id", "?")))
            )
            out.append({
                "chat_id": getattr(e, "id", None),
                "title": name,
                "username": getattr(e, "username", "") or "",
                "type": typ,
                "supported": supported,
            })
        self.last_used = time.monotonic()
        return out

    async def resolve_dialog(self, chat_id: int) -> dict:
        """Resolve a dialog the user is a member of → {channel_id, title,
        username, peer_type}.

        Bypasses the username/invite-link requirement: the entity is found among
        the user's own dialogs (access_hash from the live session). Channels,
        supergroups, and basic groups are supported; DMs are not."""
        from telethon.tl.types import Channel, Chat

        async for d in self.client.iter_dialogs():
            e = d.entity
            if getattr(e, "id", None) == chat_id:
                if isinstance(e, Channel):
                    peer_type = "channel"
                elif isinstance(e, Chat):
                    peer_type = "chat"
                else:
                    raise ValueError(
                        "Личные диалоги пока не поддерживаются — только группы, "
                        "супергруппы и каналы."
                    )
                self.last_used = time.monotonic()
                return {
                    "channel_id": e.id,
                    "title": getattr(e, "title", "") or str(e.id),
                    "username": getattr(e, "username", "") or "",
                    "peer_type": peer_type,
                }
        raise ValueError(f"Чат {chat_id} не найден среди твоих диалогов")

    async def get_folders(self) -> list[dict]:
        """Get the user's Telegram folders with ALL their dialogs.

        Read directly from the user's own session (GetDialogFilters) — no folder
        sharing / chatlist invite needed, so chats the user can't share still
        show up. Each peer carries `type` + `supported` (only channels/supergroups
        sync today; basic groups + DMs are listed but flagged unsupported).
        """
        from telethon.tl.functions.messages import GetDialogFiltersRequest
        from telethon.tl.types import Channel, InputPeerChannel, InputPeerChat, InputPeerUser

        try:
            result = await self.client(GetDialogFiltersRequest())
        except Exception:
            log.exception("get_dialog_filters_failed")
            return []

        folders: list[dict] = []
        filters = getattr(result, "filters", result) if not isinstance(result, list) else result
        for f in filters:
            if not hasattr(f, "include_peers"):
                continue
            peers: list[dict] = []
            for peer in (f.include_peers or []):
                if not isinstance(peer, (InputPeerChannel, InputPeerChat, InputPeerUser)):
                    continue
                try:
                    entity = await self.client.get_entity(peer)
                except Exception:
                    continue
                if isinstance(peer, InputPeerChannel):
                    typ = "channel" if getattr(entity, "broadcast", False) else "supergroup"
                    supported, peer_type = True, "channel"
                elif isinstance(peer, InputPeerChat):
                    typ, supported, peer_type = "group", True, "chat"
                else:
                    typ, supported, peer_type = "dm", False, "user"
                name = (
                    getattr(entity, "title", None)
                    or " ".join(filter(None, [getattr(entity, "first_name", None), getattr(entity, "last_name", None)]))
                    or (f"@{entity.username}" if getattr(entity, "username", None) else str(entity.id))
                )
                peers.append({
                    "channel_id": entity.id,  # bare id (matches domains.channel_id)
                    "chat_id": entity.id,
                    "title": name,
                    "username": getattr(entity, "username", "") or "",
                    "type": typ,
                    "peer_type": peer_type,
                    "supported": supported,
                })
            if peers:
                title = f.title
                if not isinstance(title, str):
                    title = getattr(title, "text", None) or str(title)
                folders.append({"id": f.id, "title": title, "peers": peers})

        self.last_used = time.monotonic()
        return folders


class CollectorPool:
    """Manages per-user Telethon clients loaded from encrypted DB sessions."""

    def __init__(self) -> None:
        self._collectors: dict[int, _UserCollector] = {}
        self._locks: dict[int, asyncio.Lock] = {}
        self._cleanup_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start background cleanup of idle collectors."""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop(), name="collector_pool_cleanup")

    def stop(self) -> None:
        if self._cleanup_task:
            self._cleanup_task.cancel()

    async def get_collector(self, telegram_id: int) -> _UserCollector | None:
        """Get or create a Telethon collector for a user.

        Returns None if user has no active session.
        """
        # Fast path: cached
        if telegram_id in self._collectors:
            uc = self._collectors[telegram_id]
            if uc.client.is_connected():
                uc.last_used = time.monotonic()
                await db_q.touch_telegram_session(async_engine, telegram_id)
                return uc
            else:
                # Stale connection, remove
                del self._collectors[telegram_id]

        # Per-user lock to avoid double-creating
        if telegram_id not in self._locks:
            self._locks[telegram_id] = asyncio.Lock()

        async with self._locks[telegram_id]:
            # Re-check after acquiring lock
            if telegram_id in self._collectors:
                return self._collectors[telegram_id]

            # Load session from DB
            session_row = await db_q.get_telegram_session(async_engine, telegram_id)
            if not session_row:
                return None

            try:
                session_string = decrypt_session(session_row["session_data"])
            except Exception:
                log.warning("session_decrypt_failed", telegram_id=telegram_id)
                return None

            # Create Telethon client
            proxy = None
            if settings.telegram_proxy:
                from python_socks import ProxyType
                url = settings.telegram_proxy
                host = url.split("://")[1].split(":")[0]
                port = int(url.split(":")[-1])
                proxy = (ProxyType.SOCKS5, host, port)

            client = TelegramClient(
                StringSession(session_string),
                settings.telegram_api_id,
                settings.telegram_api_hash,
                proxy=proxy,
            )

            try:
                await client.connect()
                if not await client.is_user_authorized():
                    log.warning("session_expired", telegram_id=telegram_id)
                    await db_q.deactivate_telegram_session(async_engine, telegram_id)
                    await client.disconnect()
                    return None
            except Exception:
                log.exception("collector_connect_failed", telegram_id=telegram_id)
                try:
                    await client.disconnect()
                except Exception:
                    pass
                return None

            # Warm the entity cache: get_dialogs() populates access_hashes for
            # all joined chats, so private channels (no @username) resolve via
            # PeerChannel after a restart wiped the in-memory session cache.
            # Without this, the first sync of a private channel post-restart
            # cache-misses and fails every hour. Best-effort — never block connect.
            try:
                await client.get_dialogs()
            except Exception:
                log.warning("collector_warm_dialogs_failed", telegram_id=telegram_id)

            uc = _UserCollector(telegram_id, client)
            self._collectors[telegram_id] = uc
            await db_q.touch_telegram_session(async_engine, telegram_id)
            log.info("collector_pool_connected", telegram_id=telegram_id)
            return uc

    def has_session(self, telegram_id: int) -> bool:
        """Check if user has an active cached collector (fast, no DB)."""
        return telegram_id in self._collectors

    async def save_session(
        self, telegram_id: int, session_string: str, phone: str | None = None,
    ) -> None:
        """Encrypt and save a new Telethon session to DB."""
        encrypted = encrypt_session(session_string)
        ph = hash_phone(phone) if phone else None
        await db_q.save_telegram_session(async_engine, telegram_id, encrypted, ph)
        log.info("session_saved", telegram_id=telegram_id)

    async def remove_session(self, telegram_id: int) -> None:
        """Disconnect and deactivate a user's session."""
        if telegram_id in self._collectors:
            try:
                await self._collectors[telegram_id].client.disconnect()
            except Exception:
                pass
            del self._collectors[telegram_id]
        await db_q.deactivate_telegram_session(async_engine, telegram_id)
        log.info("session_removed", telegram_id=telegram_id)

    async def check_auth(self, telegram_id: int) -> dict:
        """Check if user has an active Telegram session.

        Returns dict with status info for MCP/API consumers.
        """
        session_row = await db_q.get_telegram_session(async_engine, telegram_id)
        if not session_row:
            return {
                "connected": False,
                "message": f"Telegram не подключён. Авторизуйся через @{settings.bot_username}.",
                "bot_url": settings.bot_url,
            }
        return {
            "connected": True,
            "connected_at": str(session_row["connected_at"]),
            "last_used": str(session_row["last_used_at"]) if session_row["last_used_at"] else None,
        }

    async def shutdown(self) -> None:
        """Disconnect all cached collectors."""
        self.stop()
        for uc in self._collectors.values():
            try:
                await uc.client.disconnect()
            except Exception:
                pass
        self._collectors.clear()
        log.info("collector_pool_shutdown")

    async def _cleanup_loop(self) -> None:
        """Periodically disconnect idle collectors."""
        while True:
            await asyncio.sleep(60)
            now = time.monotonic()
            expired = [
                tid for tid, uc in self._collectors.items()
                if now - uc.last_used > _COLLECTOR_TTL
            ]
            for tid in expired:
                try:
                    await self._collectors[tid].client.disconnect()
                except Exception:
                    pass
                del self._collectors[tid]
                log.info("collector_pool_evicted", telegram_id=tid)


# Module-level singleton, initialized in __main__
collector_pool: CollectorPool | None = None
