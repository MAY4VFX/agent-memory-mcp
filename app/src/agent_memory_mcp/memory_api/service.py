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


async def search_memory(
    query: str,
    owner_id: int,
    scope: str | None = None,
    limit: int = 10,
) -> dict:
    """Search memory using agent pipeline. Returns answer + sources."""
    from agent_memory_mcp.db import queries_conversations as qc
    from agent_memory_mcp.pipeline.agent_orchestrator import run_agent_pipeline

    # Resolve domain(s) for this owner
    domain_ids = await _resolve_scope(owner_id, scope)
    if not domain_ids:
        return {"answer": "Нет подключённых источников. Добавь канал через агента.", "sources": []}

    domain_id = domain_ids[0]
    domain = await db_q.get_domain(async_engine, domain_id)

    # Create temp conversation
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
            query=query,
            user_id=owner_id,
            conversation_id=conv["id"],
            domain_ids=domain_ids,
            engine=async_engine,
            milvus=milvus,
            graph=graph,
            embedder=embedder,
            reranker=reranker,
        )
    finally:
        milvus.close()
        graph.close()
        await embedder.close()
        await reranker.close()
        await qc.delete_conversation(async_engine, conv["id"])

    sources = [
        {"msg_id": s.message_id, "url": s.url, "channel": s.channel_username}
        for s in (answer.sources or [])[:limit]
    ]
    return {"answer": answer.answer, "sources": sources}


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


async def add_source(
    owner_id: int,
    handle: str,
    source_type: str = "channel",
    sync_range: str = "3m",
) -> dict:
    """Add a Telegram source for a user via their Telethon session.

    source_type="folder" adds ALL channels from a Telegram folder at once.
    Use list_folders() first to see available folders.
    """
    from agent_memory_mcp.collector.pool import collector_pool

    if not collector_pool:
        return {"status": "error", "message": "Collector pool not initialized"}

    uc = await collector_pool.get_collector(owner_id)
    if not uc:
        return {
            "status": "auth_required",
            "message": "Telegram не подключён. Авторизуйся через @AgentMemoryBot.",
            "bot_url": "https://t.me/AgentMemoryBot",
        }

    # --- Folder import: add all channels from a Telegram folder ---
    if source_type == "folder":
        return await _add_folder(owner_id, uc, handle, sync_range)

    # --- Single channel ---
    return await _add_single_channel(owner_id, uc, handle, sync_range)


async def _add_single_channel(owner_id: int, uc, handle: str, sync_range: str) -> dict:
    """Add a single channel as a source."""
    try:
        info = await uc.resolve_channel(handle)
    except ValueError as e:
        return {"status": "error", "message": str(e)}
    except Exception as e:
        log.exception("resolve_channel_failed", handle=handle, owner_id=owner_id)
        return {"status": "error", "message": f"Failed to resolve channel: {e}"}

    existing = await db_q.list_domains(async_engine, owner_id)
    for d in existing:
        if d["channel_id"] == info["channel_id"]:
            return {
                "status": "exists",
                "domain_id": str(d["id"]),
                "channel": f"@{info['username']}",
                "message": f"@{info['username']} already connected.",
            }

    from datetime import datetime, timezone
    domain = await db_q.create_domain(
        async_engine,
        owner_id=owner_id,
        channel_id=info["channel_id"],
        channel_username=info["username"],
        channel_name=info["title"],
        sync_depth=sync_range,
        sync_frequency_minutes=60,
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
        "channel": f"@{info['username']}",
        "title": info["title"],
        "sync_range": sync_range,
        "message": f"✅ @{info['username']} добавлен. Синхронизация начнётся в течение 30 секунд.",
    }


async def _add_folder(owner_id: int, uc, folder_name: str, sync_range: str) -> dict:
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
            sync_frequency_minutes=60,
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


async def get_digest(owner_id: int, scope: str, period: str = "7d") -> dict:
    """Generate a digest via map-reduce clustering."""
    from datetime import datetime, timedelta, timezone
    from agent_memory_mcp.digest.clustering import cluster_messages, deduplicate, embed_messages
    from agent_memory_mcp.llm.client import llm_call, llm_call_json
    from agent_memory_mcp.llm.digest_prompts import MAP_DIGEST_SYSTEM, REDUCE_DIGEST_SYSTEM
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

    # Deduplicate and cluster
    messages = deduplicate(messages)
    embedder = EmbeddingClient()
    try:
        embedded = await embed_messages(messages, embedder)
        clusters = cluster_messages(embedded)
    finally:
        await embedder.close()

    if not clusters:
        return {"digest": "Not enough messages to generate digest.", "period": period}

    # Map phase: summarize each cluster
    import asyncio
    summaries = []
    for cluster in clusters[:10]:  # max 10 clusters
        texts = "\n\n".join(m["content"][:300] for m in cluster.messages[:20] if m.get("content"))
        try:
            summary = await llm_call(
                model="tier1/extraction",
                messages=[
                    {"role": "system", "content": MAP_DIGEST_SYSTEM},
                    {"role": "user", "content": texts},
                ],
                temperature=0.2,
                max_tokens=500,
            )
            summaries.append(summary)
        except Exception:
            log.warning("digest_map_failed", exc_info=True)

    if not summaries:
        return {"digest": "Failed to generate digest.", "period": period}

    # Reduce phase: combine summaries
    combined = "\n\n---\n\n".join(summaries)
    try:
        digest_text = await llm_call(
            model="tier3/answer",
            messages=[
                {"role": "system", "content": REDUCE_DIGEST_SYSTEM},
                {"role": "user", "content": combined},
            ],
            temperature=0.3,
            max_tokens=2000,
        )
    except Exception:
        digest_text = "\n\n".join(summaries)

    return {
        "digest": digest_text,
        "period": period,
        "message_count": len(messages),
        "cluster_count": len(clusters),
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


async def _resolve_scope(owner_id: int, scope: str | None) -> list[UUID]:
    """Resolve a scope string to domain IDs for the owner.

    Supported formats:
      - None / "" → all domains
      - "@username" → single channel
      - "folder:Name" → all channels in a named group/folder
      - UUID string → single domain by ID
    """
    domains = await db_q.list_domains(async_engine, owner_id)
    if not domains:
        return []

    if not scope:
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
        # Folder not found — fall through to all
        return [d["id"] for d in domains]

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

    # Default: all
    return [d["id"] for d in domains]
