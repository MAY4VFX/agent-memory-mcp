"""Telethon-based Telegram channel collector."""

from __future__ import annotations

import asyncio
import re
import time

import structlog
from telethon import TelegramClient
from telethon.errors import FloodWaitError, TakeoutInitDelayError, TakeoutInvalidError
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Message, PeerChannel

from agent_memory_mcp.config import settings
from agent_memory_mcp.models.messages import TelegramMessage

log = structlog.get_logger()

_LINK_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/(?:\+)?([a-zA-Z0-9_]+)",
)


def _parse_channel_username(link: str) -> str:
    """Extract username from various Telegram link formats."""
    link = link.strip()
    if link.startswith("@"):
        return link[1:]
    m = _LINK_RE.search(link)
    if m:
        return m.group(1)
    # Assume bare username
    return link


def _content_type(msg: Message) -> str:
    if msg.photo:
        return "photo"
    if msg.video:
        return "video"
    if msg.document:
        return "document"
    if msg.sticker:
        return "sticker"
    if msg.voice:
        return "voice"
    if msg.video_note:
        return "video_note"
    if msg.poll:
        return "poll"
    if msg.geo:
        return "geo"
    return "text"


class TelegramCollector:
    """Collect messages from Telegram channels via Telethon."""

    _FOLDER_CACHE_TTL = 300  # 5 min

    def __init__(self) -> None:
        proxy = None
        if settings.telegram_proxy:
            # Parse socks5://host:port
            from python_socks import ProxyType
            url = settings.telegram_proxy
            host = url.split("://")[1].split(":")[0]
            port = int(url.split(":")[-1])
            proxy = (ProxyType.SOCKS5, host, port)
        self._client = TelegramClient(
            StringSession(settings.telegram_session),
            settings.telegram_api_id,
            settings.telegram_api_hash,
            proxy=proxy,
        )
        self._folder_cache: list[dict] | None = None
        self._folder_cache_ts: float = 0

    async def connect(self) -> None:
        await self._client.connect()
        if not await self._client.is_user_authorized():
            raise RuntimeError(
                "Telethon session is not authorized. "
                "Run `python -m agent_memory_mcp.scripts.auth_telethon` first."
            )
        log.info("telethon_connected")

    async def resolve_channel(self, link: str) -> dict:
        """Resolve a channel link/username to basic info.

        Returns dict with keys: channel_id, title, username.
        """
        username = _parse_channel_username(link)
        entity = await self._client.get_entity(username)
        if not isinstance(entity, Channel):
            raise ValueError(f"'{link}' is not a channel/supergroup")
        return {
            "channel_id": entity.id,
            "title": entity.title,
            "username": entity.username or username,
        }

    async def fetch_messages(
        self,
        channel_id: int,
        limit: int | None = None,
        min_id: int = 0,
        since_date=None,
        channel_username: str | None = None,
        use_takeout: bool = False,
    ) -> list[TelegramMessage]:
        """Fetch messages from a channel.

        Args:
            channel_id: Telegram channel ID.
            limit: Max messages to fetch (None = all available).
            min_id: Fetch messages with ID > min_id (incremental sync).
            since_date: Only fetch messages AFTER this date (newest first,
                stops when hitting messages older than since_date).
            channel_username: Fallback username if PeerChannel lookup fails
                (entity cache lost after container restart).
            use_takeout: Use Telegram takeout session for lower rate limits
                (recommended for initial sync of large chats).

        Returns:
            List of TelegramMessage objects, oldest-first.
        """
        # Try PeerChannel first (fast, uses session cache).
        # Fall back to username resolution if cache is stale.
        try:
            entity = await self._client.get_entity(PeerChannel(channel_id))
        except ValueError:
            if channel_username:
                log.warning(
                    "peer_channel_cache_miss_fallback_username",
                    channel_id=channel_id,
                    username=channel_username,
                )
                entity = await self._client.get_entity(channel_username)
            else:
                raise

        if use_takeout:
            return await self._fetch_with_takeout(
                entity, channel_id, limit, min_id, since_date,
            )
        return await self._paginated_fetch(
            self._client, entity, channel_id, limit, min_id, since_date,
        )

    async def _fetch_with_takeout(
        self, entity, channel_id, limit, min_id, since_date,
    ) -> list[TelegramMessage]:
        """Fetch using takeout session (lower flood limits)."""
        MAX_TAKEOUT_DELAY = 300  # max seconds to wait for takeout init

        for attempt in range(2):
            try:
                async with self._client.takeout(megagroups=True) as takeout:
                    log.info("takeout_session_started", channel_id=channel_id)
                    return await self._paginated_fetch(
                        takeout, entity, channel_id, limit, min_id, since_date,
                        wait_time=0, chunk_pause=0.5,
                    )
            except TakeoutInitDelayError as e:
                if e.seconds > MAX_TAKEOUT_DELAY:
                    log.warning(
                        "takeout_delay_too_long",
                        channel_id=channel_id,
                        seconds=e.seconds,
                    )
                    break  # fall back to regular fetch
                log.info(
                    "takeout_init_delay",
                    channel_id=channel_id,
                    seconds=e.seconds,
                )
                await asyncio.sleep(e.seconds)
            except TakeoutInvalidError:
                log.warning("takeout_invalidated", channel_id=channel_id)
                break  # fall back to regular fetch
            except Exception:
                if attempt == 0:
                    log.exception("takeout_failed_fallback", channel_id=channel_id)
                    break  # fall back to regular fetch
                raise

        log.info("takeout_fallback_regular", channel_id=channel_id)
        return await self._paginated_fetch(
            self._client, entity, channel_id, limit, min_id, since_date,
        )

    async def _paginated_fetch(
        self,
        client,
        entity,
        channel_id: int,
        limit: int | None,
        min_id: int,
        since_date,
        wait_time: float | None = None,
        chunk_pause: float = 1.0,
    ) -> list[TelegramMessage]:
        """Paginated fetch in chunks with retry on errors."""
        CHUNK_SIZE = 3000
        MAX_FLOOD_RETRIES = 5

        results: list[TelegramMessage] = []
        offset_id = 0  # 0 = start from newest
        remaining = limit  # None = unlimited

        iter_kwargs: dict = {}
        if wait_time is not None:
            iter_kwargs["wait_time"] = wait_time

        while True:
            batch_limit = (
                min(CHUNK_SIZE, remaining) if remaining is not None else CHUNK_SIZE
            )
            chunk: list[TelegramMessage] = []
            last_msg_id: int | None = None
            hit_date_limit = False

            # Retry loop for current chunk
            for flood_attempt in range(MAX_FLOOD_RETRIES):
                try:
                    chunk = []
                    async for msg in client.iter_messages(
                        entity,
                        limit=batch_limit,
                        min_id=min_id,
                        offset_id=offset_id,
                        **iter_kwargs,
                    ):
                        if not isinstance(msg, Message):
                            continue

                        if since_date and msg.date and msg.date < since_date:
                            hit_date_limit = True
                            break

                        last_msg_id = msg.id

                        sender_name = None
                        if msg.sender:
                            sender_name = getattr(
                                msg.sender, "title", None
                            ) or getattr(msg.sender, "first_name", None)

                        # Extract topic_id: prefer reply_to_top_id,
                        # fallback to reply_to_msg_id when forum_topic flag is set
                        # (Telethon leaves reply_to_top_id=None for direct replies
                        # to topic root messages)
                        topic_id = None
                        if msg.reply_to:
                            topic_id = getattr(msg.reply_to, "reply_to_top_id", None)
                            if topic_id is None and getattr(msg.reply_to, "forum_topic", False):
                                topic_id = msg.reply_to.reply_to_msg_id

                        chunk.append(
                            TelegramMessage(
                                message_id=msg.id,
                                channel_id=channel_id,
                                sender_id=msg.sender_id,
                                sender_name=sender_name,
                                text=msg.text or "",
                                date=msg.date,
                                reply_to_msg_id=(
                                    msg.reply_to.reply_to_msg_id
                                    if msg.reply_to
                                    else None
                                ),
                                topic_id=topic_id,
                                content_type=_content_type(msg),
                                raw_json=msg.to_dict() if msg.text else None,
                            )
                        )
                    break  # chunk fetched successfully
                except FloodWaitError as e:
                    wait = min(e.seconds + 5, 300)
                    log.warning(
                        "flood_wait",
                        channel_id=channel_id,
                        wait_seconds=e.seconds,
                        attempt=flood_attempt + 1,
                        fetched_so_far=len(results),
                    )
                    await asyncio.sleep(wait)
                except ValueError as e:
                    if "unsuccessful" in str(e) and flood_attempt < MAX_FLOOD_RETRIES - 1:
                        backoff = 15 * (flood_attempt + 1)
                        log.warning(
                            "iter_messages_retry",
                            channel_id=channel_id,
                            error=str(e),
                            backoff=backoff,
                            attempt=flood_attempt + 1,
                        )
                        await asyncio.sleep(backoff)
                    else:
                        raise

            results.extend(chunk)

            if remaining is not None:
                remaining -= len(chunk)

            # Stop conditions
            if hit_date_limit or not chunk or len(chunk) < batch_limit:
                break
            if remaining is not None and remaining <= 0:
                break

            # Advance offset to oldest message in chunk for next iteration
            offset_id = last_msg_id or 0

            log.info(
                "fetch_chunk_done",
                channel_id=channel_id,
                chunk=len(chunk),
                total=len(results),
            )
            await asyncio.sleep(chunk_pause)

        # Return oldest first
        results.reverse()
        log.info(
            "fetch_messages_done",
            channel_id=channel_id,
            count=len(results),
        )
        return results

    async def get_dialog_filters(self, force: bool = False) -> list[dict]:
        """Get user's Telegram folder filters (cached for 5 min).

        Returns list of {id, title, peers: [{channel_id, title, username}]}.
        """
        if (
            not force
            and self._folder_cache is not None
            and time.monotonic() - self._folder_cache_ts < self._FOLDER_CACHE_TTL
        ):
            return self._folder_cache

        from telethon.tl.functions.messages import GetDialogFiltersRequest
        from telethon.tl.types import InputPeerChannel

        try:
            result = await self._client(GetDialogFiltersRequest())
        except Exception:
            log.exception("get_dialog_filters_failed")
            return self._folder_cache or []

        folders: list[dict] = []
        # result may be a DialogFilters object or list depending on Telethon version
        filters = getattr(result, "filters", result) if not isinstance(result, list) else result
        for f in filters:
            # Accept DialogFilter and DialogFilterChatlist (shared folders)
            if not hasattr(f, "include_peers"):
                continue
            peers: list[dict] = []
            for peer in (f.include_peers or []):
                if isinstance(peer, InputPeerChannel):
                    try:
                        entity = await self._client.get_entity(peer)
                        peers.append({
                            "channel_id": entity.id,
                            "title": getattr(entity, "title", ""),
                            "username": getattr(entity, "username", "") or "",
                        })
                    except Exception:
                        log.debug("folder_peer_resolve_failed", peer=peer)
            if peers:
                title = f.title
                if not isinstance(title, str):
                    title = getattr(title, "text", None) or str(title)
                folders.append({
                    "id": f.id,
                    "title": title,
                    "peers": peers,
                })
        self._folder_cache = folders
        self._folder_cache_ts = time.monotonic()
        log.info("dialog_filters_loaded", count=len(folders))
        return folders

    async def disconnect(self) -> None:
        await self._client.disconnect()
        log.info("telethon_disconnected")
