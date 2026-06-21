"""Service layer — bridges Memory API routes to existing pipeline functions."""

from __future__ import annotations

from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db import queries_groups as db_g
from agent_memory_mcp.db.engine import async_engine
from agent_memory_mcp.storage.embedding_client import EmbeddingClient
from agent_memory_mcp.storage.falkordb_client import FalkorDBStorage
from agent_memory_mcp.storage.milvus_client import MilvusStorage
from agent_memory_mcp.storage.reranker_client import RerankerClient

log = structlog.get_logger(__name__)


def _parse_since(since: str | None) -> "datetime | None":
    """Parse 'since' parameter: '2d', '1w', '3m', or ISO date string."""
    if not since:
        return None
    from datetime import datetime, timedelta, timezone
    since = since.strip().lower()
    now = datetime.now(timezone.utc)
    mapping = {
        "1d": timedelta(days=1), "2d": timedelta(days=2), "3d": timedelta(days=3),
        "1w": timedelta(weeks=1), "2w": timedelta(weeks=2),
        "1m": timedelta(days=30), "3m": timedelta(days=90),
    }
    if since in mapping:
        return now - mapping[since]
    # Try ISO date
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(since, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


async def search_memory(
    query: str,
    owner_id: int,
    scope: str | None = None,
    limit: int = 10,
    since: str | None = None,
) -> dict:
    """Search memory using agent pipeline. Returns answer + sources."""
    from agent_memory_mcp.db import queries_conversations as qc
    from agent_memory_mcp.pipeline.agent_orchestrator import run_agent_pipeline

    domain_ids = await _resolve_scope(owner_id, scope)
    if not domain_ids:
        return {"answer": "Нет подключённых источников. Добавь канал через агента.", "sources": []}

    # Parse time filter
    since_dt = _parse_since(since)

    # Augment query with time context for the LLM
    augmented_query = query
    if since_dt:
        from datetime import datetime, timezone
        date_str = since_dt.strftime("%Y-%m-%d")
        augmented_query = f"{query} (only messages after {date_str})"

    domain_id = domain_ids[0]

    conv = await qc.create_conversation(
        async_engine, user_id=owner_id, domain_id=domain_id,
        title=f"[api] {query[:40]}",
    )

    milvus = MilvusStorage()
    graph = FalkorDBStorage()
    embedder = EmbeddingClient()
    reranker = RerankerClient()

    try:
        answer, payload = await run_agent_pipeline(
            query=augmented_query,
            user_id=owner_id,
            conversation_id=conv["id"],
            domain_ids=domain_ids,
            engine=async_engine,
            milvus=milvus,
            graph=graph,
            embedder=embedder,
            reranker=reranker,
            since_date=since_dt,
        )
    finally:
        milvus.close()
        graph.close()
        await embedder.close()
        await reranker.close()
        await qc.delete_conversation(async_engine, conv["id"])

    # Post-filter sources by date if since is specified
    sources = []
    for s in (answer.sources or [])[:limit * 2]:  # fetch more, filter down
        src = {"msg_id": s.message_id, "url": s.url, "channel": s.channel_username}
        if since_dt and hasattr(s, "date") and s.date and s.date < since_dt:
            continue
        sources.append(src)
        if len(sources) >= limit:
            break

    result = {"answer": answer.answer, "sources": sources}
    if since:
        result["filtered_since"] = since
    return result


async def list_sources(owner_id: int) -> list[dict]:
    """List all sources (domains) for a user."""
    domains = await db_q.list_domains(async_engine, owner_id)
    return [
        {
            "id": str(d["id"]),
            "channel_username": d.get("channel_username"),
            "display_name": d.get("display_name"),
            "message_count": d.get("message_count", 0),
            "sync_depth": d.get("sync_depth"),
            "last_synced": str(d["last_synced_at"]) if d.get("last_synced_at") else None,
        }
        for d in domains
    ]


def _clamp_sync_frequency(sync_frequency_minutes: int | None) -> int:
    """Normalize the per-source sync cadence (default 60, floor 5 minutes).

    The scheduler ticks every ~30s, so 5 min is the practical floor; very
    frequent syncing also risks Telegram flood-waits.
    """
    if not sync_frequency_minutes:
        return 60
    return max(5, int(sync_frequency_minutes))


async def add_source(
    owner_id: int,
    handle: str,
    source_type: str = "channel",
    sync_range: str = "3m",
    sync_frequency_minutes: int | None = None,
) -> dict:
    """Add a Telegram source for a user via their Telethon session.

    source_type="folder" adds ALL channels from a Telegram folder at once.
    Use list_folders() first to see available folders.

    sync_frequency_minutes controls how often the source re-syncs (default 60,
    minimum 5).
    """
    from agent_memory_mcp.collector.pool import collector_pool
    from agent_memory_mcp.config import settings

    if not collector_pool:
        return {"status": "error", "message": "Collector pool not initialized"}

    uc = await collector_pool.get_collector(owner_id)
    if not uc:
        return {
            "status": "auth_required",
            "message": f"Telegram не подключён. Авторизуйся через @{settings.bot_username}.",
            "bot_url": settings.bot_url,
        }

    freq = _clamp_sync_frequency(sync_frequency_minutes)

    # --- Folder import: add all channels from a Telegram folder ---
    if source_type == "folder":
        return await _add_folder(owner_id, uc, handle, sync_range, freq)

    # --- Single channel ---
    return await _add_single_channel(owner_id, uc, handle, sync_range, freq)


async def _add_single_channel(
    owner_id: int, uc, handle: str, sync_range: str, sync_frequency_minutes: int = 60,
) -> dict:
    """Add a single channel as a source."""
    try:
        info = await uc.resolve_channel(handle)
    except ValueError as e:
        return {"status": "error", "message": str(e)}
    except Exception as e:
        log.exception("resolve_channel_failed", handle=handle, owner_id=owner_id)
        return {"status": "error", "message": f"Failed to resolve channel: {e}"}

    # Private channels have no username — fall back to title for display.
    label = f"@{info['username']}" if info.get("username") else info["title"]

    existing = await db_q.list_domains(async_engine, owner_id)
    for d in existing:
        if d["channel_id"] == info["channel_id"]:
            return {
                "status": "exists",
                "domain_id": str(d["id"]),
                "channel": label,
                "message": f"{label} already connected.",
            }

    from datetime import datetime, timezone
    domain = await db_q.create_domain(
        async_engine,
        owner_id=owner_id,
        channel_id=info["channel_id"],
        channel_username=info["username"],
        channel_name=info["title"],
        sync_depth=sync_range,
        sync_frequency_minutes=sync_frequency_minutes,
        emoji="📡",
        display_name=info["title"],
        pinned=True,
    )
    await db_q.update_domain(
        async_engine, domain["id"],
        next_sync_at=datetime.now(timezone.utc),
    )

    return {
        "status": "queued",
        "domain_id": str(domain["id"]),
        "channel": label,
        "title": info["title"],
        "sync_range": sync_range,
        "message": f"✅ {label} добавлен. Синхронизация начнётся в течение 30 секунд.",
    }


async def _add_folder(
    owner_id: int, uc, folder_name: str, sync_range: str, sync_frequency_minutes: int = 60,
) -> dict:
    """Add all channels from a Telegram folder."""
    from datetime import datetime, timezone
    from agent_memory_mcp.db import queries_groups as gq

    folders = await uc.get_folders()
    if not folders:
        return {"status": "error", "message": "No folders found. Make sure you have Telegram folders set up."}

    # Find folder by name (case-insensitive) or ID
    folder = None
    for f in folders:
        if f["title"].lower() == folder_name.lower():
            folder = f
            break
        if str(f["id"]) == folder_name:
            folder = f
            break

    if not folder:
        available = ", ".join(f["title"] for f in folders)
        return {
            "status": "error",
            "message": f"Folder '{folder_name}' not found. Available: {available}",
        }

    peers = folder["peers"]
    if not peers:
        return {"status": "error", "message": f"Folder '{folder['title']}' has no channels."}

    # Create a group for this folder
    group = await gq.create_group(
        async_engine,
        owner_id=owner_id,
        name=folder["title"],
        emoji="📁",
        tg_folder_id=folder["id"],
        sync_depth=sync_range,
    )

    # Add each channel
    existing = await db_q.list_domains(async_engine, owner_id)
    existing_cids = {d["channel_id"]: d["id"] for d in existing}

    added = []
    skipped = []
    domain_ids = []

    for peer in peers:
        cid = peer["channel_id"]
        if cid in existing_cids:
            skipped.append(f"@{peer['username']}" if peer.get("username") else peer["title"])
            domain_ids.append(existing_cids[cid])
            continue

        domain = await db_q.create_domain(
            async_engine,
            owner_id=owner_id,
            channel_id=cid,
            channel_username=peer.get("username", ""),
            channel_name=peer["title"],
            sync_depth=sync_range,
            sync_frequency_minutes=sync_frequency_minutes,
            emoji="📁",
            display_name=peer["title"],
            pinned=False,
        )
        await db_q.update_domain(
            async_engine, domain["id"],
            next_sync_at=datetime.now(timezone.utc),
        )
        added.append(f"@{peer['username']}" if peer.get("username") else peer["title"])
        domain_ids.append(domain["id"])

    # Link all domains to group
    await gq.add_domains_to_group(async_engine, group["id"], domain_ids)

    return {
        "status": "queued",
        "folder": folder["title"],
        "group_id": str(group["id"]),
        "added": added,
        "skipped": skipped,
        "total_channels": len(peers),
        "sync_range": sync_range,
        "message": f"✅ Папка '{folder['title']}' — добавлено {len(added)} каналов, пропущено {len(skipped)} (уже были).",
    }


async def list_folders(owner_id: int) -> list[dict]:
    """List user's Telegram folders with channel counts."""
    from agent_memory_mcp.collector.pool import collector_pool

    if not collector_pool:
        return []

    uc = await collector_pool.get_collector(owner_id)
    if not uc:
        return []

    folders = await uc.get_folders()
    return [
        {
            "id": f["id"],
            "title": f["title"],
            "channel_count": len(f["peers"]),
            "channels": [
                f"@{p['username']}" if p.get("username") else p["title"]
                for p in f["peers"]
            ],
        }
        for f in folders
    ]


async def check_telegram_auth(owner_id: int) -> dict:
    """Check if user has an active Telegram session."""
    from agent_memory_mcp.collector.pool import collector_pool
    if not collector_pool:
        return {"connected": False, "message": "Service not ready"}
    return await collector_pool.check_auth(owner_id)


async def sync_status(owner_id: int) -> dict:
    """Get sync status for all user's sources."""
    from sqlalchemy import text as sa_text

    domains_list = await db_q.list_domains(async_engine, owner_id)
    if not domains_list:
        return {"sources": [], "message": "No sources connected."}

    sources = []
    for d in domains_list:
        # Get latest sync job for this domain
        async with async_engine.begin() as conn:
            row = await conn.execute(
                sa_text("""
                    SELECT status, messages_fetched, messages_processed, messages_total,
                           error_message, started_at, completed_at
                    FROM sync_jobs WHERE domain_id = :did
                    ORDER BY created_at DESC LIMIT 1
                """),
                {"did": d["id"]},
            )
            job = row.mappings().first()

        source_info = {
            "domain_id": str(d["id"]),
            "channel": f"@{d.get('channel_username', '')}",
            "display_name": d.get("display_name"),
            "message_count": d.get("message_count", 0),
            "is_active": d.get("is_active", True),
            "last_synced": str(d["last_synced_at"]) if d.get("last_synced_at") else None,
            "next_sync": str(d["next_sync_at"]) if d.get("next_sync_at") else None,
        }

        if job:
            source_info["sync_job"] = {
                "status": job["status"],
                "messages_fetched": job["messages_fetched"],
                "messages_processed": job["messages_processed"],
                "messages_total": job["messages_total"],
                "error": job["error_message"],
                "started_at": str(job["started_at"]) if job["started_at"] else None,
                "completed_at": str(job["completed_at"]) if job["completed_at"] else None,
            }
        else:
            source_info["sync_job"] = {"status": "pending", "message": "Waiting for scheduler pickup"}

        sources.append(source_info)

    return {"sources": sources, "count": len(sources)}


def _split_emoji(header: str) -> tuple[str, str]:
    """Split a topic header like "🔧 Инструменты" into (emoji, label)."""
    header = header.strip()
    if header and ord(header[0]) >= 0x2190:  # emoji / symbol range
        return header[0], header[1:].strip()
    return "", header


def _structure_digest(digest_text: str, msgid_to_meta: dict) -> tuple[list, list, str]:
    """Parse REDUCE digest text into structured topics + resolved links, and
    re-render clean markdown.

    Input format (from REDUCE_DIGEST_SYSTEM):
        **🔧 Инструменты**
        • factual bullet [msg_id: 123]
        ...
        *Дайджест на основе анализа N постов*

    Returns (topics, links, markdown) where each bullet carries
    telegram_msg_ids, source_message_ids (internal UUIDs) and links; bullets
    whose message has no resolvable link get an explicit "url unavailable" note.
    """
    import re
    from agent_memory_mcp.utils.links import make_tme_link

    header_re = re.compile(r"^\*\*(.+?)\*\*\s*$")
    msgid_re = re.compile(r"\[msg_id:\s*(\d+)\]")

    topics: list[dict] = []
    links: list[dict] = []
    md_lines: list[str] = []
    current: dict | None = None

    for raw in digest_text.splitlines():
        stripped = raw.strip()
        if not stripped:
            md_lines.append("")
            continue

        hm = header_re.match(stripped)
        if hm:
            emoji, label = _split_emoji(hm.group(1).strip())
            current = {"emoji": emoji, "label": label, "bullets": []}
            topics.append(current)
            md_lines.append(f"**{hm.group(1).strip()}**")
            continue

        is_bullet = stripped.startswith("•") or (
            stripped.startswith(("-", "*")) and msgid_re.search(stripped)
        )
        if is_bullet:
            text = msgid_re.sub("", stripped).lstrip("•-* ").strip()
            msg_ids = [int(x) for x in msgid_re.findall(stripped)]
            bullet = {
                "text": text,
                "telegram_msg_ids": msg_ids,
                "source_message_ids": [],
                "links": [],
            }
            link_strs: list[str] = []
            for mid in msg_ids:
                meta = msgid_to_meta.get(mid)
                url = ""
                if meta:
                    if meta.get("id"):
                        bullet["source_message_ids"].append(meta["id"])
                    url = make_tme_link(
                        meta.get("username"), mid,
                        meta.get("topic_id"), meta.get("channel_id"),
                    )
                if url:
                    bullet["links"].append({"msg_id": mid, "url": url})
                    links.append({"msg_id": mid, "url": url})
                    link_strs.append(f"→ ({url})")
                else:
                    bullet["links"].append(
                        {"msg_id": mid, "url": None, "note": "private chat, url unavailable"}
                    )
                    link_strs.append(f"→ (source: private chat, msg_id: {mid}, url unavailable)")
            if current is None:
                current = {"emoji": "", "label": "", "bullets": []}
                topics.append(current)
            current["bullets"].append(bullet)
            suffix = (" " + " ".join(link_strs)) if link_strs else ""
            md_lines.append(f"• {text}{suffix}")
            continue

        # Footer / freeform line (e.g. "*Дайджест на основе анализа N постов*")
        md_lines.append(stripped)

    markdown = "\n".join(md_lines).strip()
    return topics, links, markdown


async def get_digest(owner_id: int, scope: str, period: str = "7d") -> dict:
    """Generate a digest via map-reduce clustering."""
    import re
    from datetime import datetime, timedelta, timezone
    from agent_memory_mcp.digest.clustering import cluster_messages, deduplicate, embed_messages
    from agent_memory_mcp.llm.client import llm_call, llm_call_json
    from agent_memory_mcp.llm.digest_prompts import (
        CLUSTER_LABEL_PROMPT, MAP_DIGEST_SYSTEM, REDUCE_DIGEST_SYSTEM,
    )
    from agent_memory_mcp.storage.embedding_client import EmbeddingClient

    domain_ids = await _resolve_scope(owner_id, scope)
    if not domain_ids:
        return {"digest": "No sources connected.", "period": period}

    # Parse period
    period_days = {"1d": 1, "3d": 3, "7d": 7, "14d": 14, "30d": 30}.get(period, 7)
    since = datetime.now(timezone.utc) - timedelta(days=period_days)

    messages = await db_q.get_messages_since(async_engine, domain_ids, since, limit=200)
    if not messages:
        return {"digest": "No messages found for this period.", "period": period, "message_count": 0}

    # Build domain_id → {username, channel_id} maps for links (channel_id is
    # needed for private channels that have no public username).
    domain_username_map: dict[str, str] = {}
    domain_channel_map: dict[str, int] = {}
    for did in domain_ids:
        d = await db_q.get_domain(async_engine, did)
        if not d:
            continue
        if d.get("channel_username"):
            domain_username_map[str(d["id"])] = d["channel_username"]
        if d.get("channel_id"):
            domain_channel_map[str(d["id"])] = d["channel_id"]

    # Embed → deduplicate → cluster
    embedder = EmbeddingClient()
    try:
        messages, embeddings = await embed_messages(messages, embedder)
        messages, embeddings = deduplicate(messages, embeddings)
        clusters = cluster_messages(messages, embeddings)
    except Exception:
        log.warning("digest_pipeline_failed", exc_info=True)
        clusters = []
    finally:
        await embedder.close()

    if not clusters:
        return {"digest": "Not enough messages to generate digest.", "period": period}

    # Map phase: summarize each cluster (same format as runner.py)
    import asyncio
    summaries = []
    for cluster in clusters[:10]:  # max 10 clusters
        cluster_label = f"{getattr(cluster, 'emoji', '📌')} {getattr(cluster, 'label', 'Разное')}"
        posts_text = "\n\n---\n\n".join(
            f"[Пост] channel=@{domain_username_map.get(str(m.get('domain_id', '')), '?')} "
            f"msg_id={m.get('telegram_msg_id', 0)}\n"
            f"{(m.get('content') or '')[:1500]}"
            for m in cluster.messages[:20]
        )
        try:
            summary = await llm_call(
                model="tier1/extraction",
                messages=[
                    {"role": "system", "content": MAP_DIGEST_SYSTEM.format(
                        cluster_label=cluster_label, posts=posts_text,
                    )},
                    {"role": "user", "content": posts_text},
                ],
                temperature=0.2,
                max_tokens=1000,
            )
            summaries.append(summary)
        except Exception:
            log.warning("digest_map_failed", exc_info=True)

    if not summaries:
        return {"digest": "Failed to generate digest.", "period": period}

    # Reduce phase: combine summaries
    combined = "\n\n---\n\n".join(summaries)
    reduce_prompt = REDUCE_DIGEST_SYSTEM.format(total_posts=len(messages))
    try:
        digest_text = await llm_call(
            model="tier3/answer",
            messages=[
                {"role": "system", "content": reduce_prompt},
                {"role": "user", "content": combined},
            ],
            temperature=0.3,
            max_tokens=2000,
        )
    except Exception:
        digest_text = "\n\n".join(summaries)

    # Build telegram_msg_id → message meta for link resolution.
    msgid_to_meta: dict[int, dict] = {}
    for m in messages:
        mid = m.get("telegram_msg_id", 0)
        if not mid:
            continue
        did = str(m.get("domain_id", ""))
        msgid_to_meta[mid] = {
            "id": str(m.get("id", "")),
            "domain_id": did,
            "channel_id": domain_channel_map.get(did),
            "username": domain_username_map.get(did),
            "topic_id": m.get("topic_id"),
        }

    topics, links, markdown = _structure_digest(digest_text, msgid_to_meta)

    return {
        "digest": markdown,
        "topics": topics,
        "links": links,
        "period": period,
        "message_count": len(messages),
        "cluster_count": len(clusters),
    }


async def set_digest_schedule(
    owner_id: int,
    frequency_hours: int,
    scope: str = "all",
    send_hour_utc: int = 8,
) -> dict:
    """Create/update the user's scheduled digest cadence.

    frequency_hours: how often to send (minimum 1; the scheduler ticks hourly).
      For < 24 it fires on any hourly tick; for >= 24 it fires at send_hour_utc.
    scope: "all", "@username", "folder:Name", or a domain UUID.
    """
    from uuid import UUID as _UUID
    from agent_memory_mcp.db import queries_digest as dq
    from agent_memory_mcp.db import queries_groups as gq

    freq = max(1, int(frequency_hours))
    hour = min(23, max(0, int(send_hour_utc)))

    # Resolve scope → (scope_type, scope_id) understood by the digest runner.
    scope_type, scope_id = "all", None
    s = (scope or "all").strip()
    if s and s.lower() != "all":
        if s.lower().startswith("folder:"):
            name = s[7:].strip()
            for g in await gq.list_groups(async_engine, owner_id):
                if g["name"].lower() == name.lower():
                    scope_type, scope_id = "group", g["id"]
                    break
            else:
                raise ScopeNotFound(scope, [g["name"] for g in await gq.list_groups(async_engine, owner_id)])
        else:
            # @username or domain UUID → single domain
            domains = await db_q.list_domains(async_engine, owner_id)
            handle = s.lstrip("@").lower()
            match = None
            for d in domains:
                if str(d["id"]) == s or (d.get("channel_username") or "").lower() == handle:
                    match = d
                    break
            if not match:
                raise ScopeNotFound(scope, [f"@{d['channel_username']}" for d in domains if d.get("channel_username")])
            scope_type, scope_id = "domain", match["id"]

    config = await dq.create_digest_config(
        async_engine,
        user_id=owner_id,
        scope_type=scope_type,
        scope_id=scope_id,
        send_hour_utc=hour,
        frequency_hours=freq,
    )
    return {
        "status": "ok",
        "config_id": str(config["id"]),
        "scope": scope,
        "frequency_hours": freq,
        "send_hour_utc": hour,
        "message": f"Digest scheduled every {freq}h (scope: {scope}).",
    }


async def get_decisions(owner_id: int, scope: str, topic: str | None = None) -> dict:
    """Extract decisions, action items, and open questions from memory."""
    from agent_memory_mcp.decision_pipeline.extractor import extract_decisions

    domain_ids = await _resolve_scope(owner_id, scope)
    if not domain_ids:
        return {"decisions": [], "topic": topic, "message": "No sources connected."}

    items = await extract_decisions(
        engine=async_engine,
        domain_ids=domain_ids,
        topic=topic,
        period_days=30,
    )

    # Group by type
    decisions = [i for i in items if i["type"] == "decision"]
    actions = [i for i in items if i["type"] == "action_item"]
    questions = [i for i in items if i["type"] == "open_question"]

    return {
        "decisions": decisions,
        "action_items": actions,
        "open_questions": questions,
        "total": len(items),
        "topic": topic,
    }


async def get_agent_context(owner_id: int, task: str, scope: str) -> dict:
    """Build a full context package for an agent task.

    Combines search + decisions + digest into one package.
    """
    domain_ids = await _resolve_scope(owner_id, scope)
    if not domain_ids:
        return {"context": "No sources connected.", "task": task}

    # Run search, decisions in parallel
    import asyncio
    search_task = asyncio.create_task(
        search_memory(query=task, owner_id=owner_id, scope=scope, limit=5)
    )
    decisions_task = asyncio.create_task(
        get_decisions(owner_id=owner_id, scope=scope)
    )

    search_result, decisions_result = await asyncio.gather(search_task, decisions_task)

    return {
        "task": task,
        "search": search_result,
        "decisions": decisions_result,
        "source_count": len(domain_ids),
    }


class ScopeNotFound(Exception):
    """Raised when a named scope (folder/channel) cannot be resolved."""

    def __init__(self, scope: str, available: list[str] | None = None):
        self.scope = scope
        self.available = available or []
        super().__init__(f"Scope not found: {scope}")


async def _resolve_scope(owner_id: int, scope: str | None) -> list[UUID]:
    """Resolve a scope string to domain IDs for the owner.

    Supported formats:
      - None / "" / "all" → all domains
      - "@username" → single channel
      - "folder:Name" → all channels in a named group/folder
      - UUID string → single domain by ID

    Raises ScopeNotFound if an explicit scope (folder: or @channel) doesn't match.
    """
    domains = await db_q.list_domains(async_engine, owner_id)
    if not domains:
        return []

    if not scope or scope.strip().lower() == "all":
        return [d["id"] for d in domains]

    # folder:Name → resolve via domain_groups
    if scope.lower().startswith("folder:"):
        folder_name = scope[7:].strip()
        from agent_memory_mcp.db import queries_groups as gq
        groups = await gq.list_groups(async_engine, owner_id)
        for g in groups:
            if g["name"].lower() == folder_name.lower():
                members = await gq.get_group_domains(async_engine, g["id"])
                if members:
                    return [m["id"] for m in members]
        available = [g["name"] for g in groups]
        raise ScopeNotFound(scope, available)

    # @username → single channel
    for d in domains:
        username = d.get("channel_username", "")
        if scope.lstrip("@").lower() == username.lower():
            return [d["id"]]

    # UUID → single domain
    try:
        uid = UUID(scope)
        return [uid]
    except ValueError:
        pass

    # Nothing matched — raise with available options
    available_channels = [f"@{d['channel_username']}" for d in domains if d.get("channel_username")]
    from agent_memory_mcp.db import queries_groups as gq
    groups = await gq.list_groups(async_engine, owner_id)
    available_folders = [f"folder:{g['name']}" for g in groups]
    raise ScopeNotFound(scope, available_folders + available_channels)
