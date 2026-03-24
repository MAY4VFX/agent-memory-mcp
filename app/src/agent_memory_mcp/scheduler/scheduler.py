"""Periodic sync scheduler for incremental ingestion."""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone

import structlog

from agent_memory_mcp.config import settings
from agent_memory_mcp.db import queries
from agent_memory_mcp.db.engine import async_engine
from agent_memory_mcp.models.messages import pg_row_to_processed, telegram_to_processed
from agent_memory_mcp.models.schema import DomainSchema

_HASHTAG_RE = re.compile(r"#(\w+)", re.UNICODE)

log = structlog.get_logger()


def _depth_to_date(depth: str) -> datetime | None:
    """Convert sync depth string to cutoff date."""
    now = datetime.now(timezone.utc)
    mapping = {
        "1w": timedelta(weeks=1),
        "1m": timedelta(days=30),
        "3m": timedelta(days=90),
        "6m": timedelta(days=180),
        "1y": timedelta(days=365),
        "3y": timedelta(days=1095),
    }
    delta = mapping.get(depth)
    return (now - delta) if delta else None


class SyncScheduler:
    def __init__(self, collector=None, bot=None) -> None:
        self._running = False
        self._collector = collector  # shared TelegramCollector from __main__
        self._bot = bot  # aiogram Bot for digest sending
        self._last_digest_check_hour: int | None = None
        # Serialize Telethon fetch — only 1 at a time (takeout is 1-per-account)
        self._fetch_semaphore = asyncio.Semaphore(1)
        # Track domains currently being synced to prevent duplicate tasks
        self._syncing_domains: set = set()

    async def _get_collector_for_domain(self, domain: dict):
        """Get a Telethon client for a domain: global collector or user's session from pool."""
        if self._collector:
            return self._collector
        # Fallback: try per-user collector from pool
        from agent_memory_mcp.collector.pool import collector_pool
        if collector_pool:
            uc = await collector_pool.get_collector(domain["owner_id"])
            if uc:
                return uc.client
        return None

    async def start(self) -> None:
        self._running = True
        log.info("scheduler_started", interval=settings.scheduler_check_interval)

        # Wait for bot polling to start before doing any sync work
        await asyncio.sleep(15)

        # Recovery on startup
        try:
            await self._recover_on_startup()
            log.info("scheduler_recovery_done")
        except Exception:
            log.exception("scheduler_recovery_failed")

        log.info("scheduler_entering_loop")
        while self._running:
            try:
                domains = await queries.get_domains_for_sync(async_engine)
                started = 0
                skipped = 0
                # Respect in_flight limit — don't start new tasks if too many running
                max_new = max(0, settings.scheduler_max_concurrent - len(self._syncing_domains))
                for domain in domains[:max_new]:
                    did = domain["id"]
                    if did in self._syncing_domains:
                        skipped += 1
                        continue
                    self._syncing_domains.add(did)
                    asyncio.create_task(self._run_incremental(domain))
                    started += 1
                if domains:
                    log.info(
                        "scheduler_tick",
                        eligible=len(domains),
                        started=started,
                        skipped=skipped,
                        in_flight=len(self._syncing_domains),
                    )

                # Check digests every minute
                await self._check_digests()
            except Exception:
                log.exception("scheduler_error")
            await asyncio.sleep(settings.scheduler_check_interval)

    async def _recover_on_startup(self) -> None:
        """Recover stuck jobs and re-run pipeline for incomplete domains."""
        # 1. Mark stuck 'running' jobs as 'failed'
        recovered = await queries.recover_stuck_sync_jobs(async_engine)
        if recovered:
            log.info("recovered_stuck_jobs", count=recovered)

        # 2. Find domains that have messages but no schema (pipeline never ran)
        domains = await queries.get_domains_needing_pipeline(async_engine)

        # Push next_sync_at for all recovery domains BEFORE starting tasks
        # to prevent the scheduler loop from triggering incremental sync
        for domain in domains:
            await queries.update_domain(
                async_engine, domain["id"],
                next_sync_at=datetime.now(timezone.utc) + timedelta(hours=1),
            )

        for domain in domains:
            log.info("recovery_pipeline_needed", domain_id=str(domain["id"]))
            asyncio.create_task(
                self._run_pipeline_recovery(domain),
                name=f"recovery_{domain['id']}",
            )

    async def _run_pipeline_recovery(self, domain: dict) -> None:
        """Re-run initial pipeline for a domain from PG messages."""
        from agent_memory_mcp.pipeline.pipelines import run_initial_ingestion

        domain_id = domain["id"]
        job = await queries.create_sync_job(async_engine, domain_id, "recovery")
        try:
            await queries.update_sync_job(
                async_engine, job["id"], status="running", started_at=datetime.now(timezone.utc)
            )

            msg_rows = await queries.get_domain_messages(async_engine, domain_id)
            if not msg_rows:
                await queries.update_sync_job(
                    async_engine, job["id"], status="completed",
                    messages_processed=0, completed_at=datetime.now(timezone.utc),
                )
                return

            processed = [pg_row_to_processed(r, domain["channel_id"]) for r in msg_rows]
            stats, schema_result = await run_initial_ingestion(processed, str(domain_id))

            # Save schema
            if schema_result and schema_result.schema:
                schema = schema_result.schema
                await queries.save_channel_schema(
                    async_engine,
                    domain_id,
                    schema_json=schema.model_dump(),
                    detected_domain=schema_result.detected_domain,
                    entity_types=[et.model_dump() for et in schema.entity_types],
                    relation_types=[rt.model_dump() for rt in schema.relation_types],
                )

            await queries.update_sync_job(
                async_engine, job["id"],
                status="completed",
                messages_fetched=len(msg_rows),
                messages_filtered=stats.noise_messages,
                messages_processed=stats.clean_messages,
                entities_extracted=stats.entities_extracted,
                completed_at=datetime.now(timezone.utc),
            )

            # Restore normal sync schedule
            next_sync = datetime.now(timezone.utc) + timedelta(
                minutes=domain.get("sync_frequency_minutes", 60)
            )
            domain_update = dict(
                entity_count=stats.entities_extracted,
                relation_count=stats.relations_extracted,
                next_sync_at=next_sync,
            )
            if stats.domain_type:
                domain_update["domain_type"] = stats.domain_type
            await queries.update_domain(async_engine, domain_id, **domain_update)

            log.info(
                "pipeline_recovery_done",
                domain_id=str(domain_id),
                entities=stats.entities_extracted,
                vectors=stats.vectors_stored,
                duration=stats.duration_sec,
            )
        except Exception as exc:
            log.exception("pipeline_recovery_error", domain_id=str(domain_id))
            await queries.update_sync_job(
                async_engine, job["id"], status="failed", error_message=str(exc)[:500]
            )

    async def _run_incremental(self, domain: dict) -> None:
        """Run incremental sync for a domain."""
        from agent_memory_mcp.pipeline.pipelines import run_incremental_ingestion, run_initial_ingestion

        domain_id = domain["id"]
        log.info("incremental_sync_start", domain_id=str(domain_id))

        collector = await self._get_collector_for_domain(domain)
        if not collector:
            log.error("no_collector", domain_id=str(domain_id), owner_id=domain.get("owner_id"))
            self._syncing_domains.discard(domain_id)
            return

        # Determine if collector is a raw TelegramClient (from pool) or TelegramCollector
        _fetch = getattr(collector, "fetch_messages", None)
        if not _fetch:
            # It's a raw TelegramClient from _UserCollector — wrap fetch calls
            from agent_memory_mcp.collector.client import TelegramCollector as _TC
            _wrap = _TC.__new__(_TC)
            _wrap._client = collector
            _wrap._folder_cache = None
            _wrap._folder_cache_ts = 0
            _fetch = _wrap.fetch_messages
            collector = _wrap

        job = await queries.create_sync_job(async_engine, domain_id, "incremental")
        try:
            await queries.update_sync_job(
                async_engine, job["id"], status="running", started_at=datetime.now(timezone.utc)
            )

            # Fetch new messages (serialized via semaphore)
            min_id = domain.get("last_synced_message_id", 0) or 0
            needs_takeout = min_id == 0 and not domain.get("last_synced_at")
            since_date = None
            if min_id == 0 and domain.get("sync_depth"):
                since_date = _depth_to_date(domain["sync_depth"])
            async with self._fetch_semaphore:
                msgs = await collector.fetch_messages(
                    channel_id=domain["channel_id"],
                    min_id=min_id,
                    since_date=since_date,
                    channel_username=domain.get("channel_username"),
                    use_takeout=needs_takeout,
                )
                if not msgs and min_id == 0 and since_date is not None:
                    log.info(
                        "first_sync_empty_widening",
                        domain_id=str(domain_id),
                        sync_depth=domain.get("sync_depth"),
                    )
                    msgs = await collector.fetch_messages(
                        channel_id=domain["channel_id"],
                        min_id=0,
                        channel_username=domain.get("channel_username"),
                        use_takeout=needs_takeout,
                    )

            await queries.update_sync_job(
                async_engine, job["id"], messages_fetched=len(msgs)
            )

            pipeline_entities = 0
            pipeline_relations = 0
            pipeline_vectors = 0

            if msgs:
                # Store messages in PG (with hashtags)
                all_new_tags: set[str] = set()
                msg_rows = []
                for m in msgs:
                    tags = _HASHTAG_RE.findall(m.text or "")
                    all_new_tags.update(tags)
                    msg_rows.append({
                        "domain_id": domain_id,
                        "telegram_msg_id": m.message_id,
                        "reply_to_msg_id": m.reply_to_msg_id,
                        "topic_id": m.topic_id,
                        "sender_id": m.sender_id,
                        "sender_name": m.sender_name,
                        "content": m.text,
                        "content_type": m.content_type,
                        "hashtags": tags if tags else None,
                        "msg_date": m.date,
                    })
                await queries.bulk_insert_messages(async_engine, msg_rows)

                # Mark affected tag summaries as stale
                if all_new_tags:
                    try:
                        await queries.mark_hashtag_summaries_stale(
                            async_engine, domain_id, list(all_new_tags), len(msgs),
                        )
                    except Exception:
                        log.exception("mark_stale_failed", domain_id=str(domain_id))

                # Run pipeline on new messages
                processed = [telegram_to_processed(m, domain_id) for m in msgs]

                schema_row = await queries.get_active_schema(async_engine, domain_id)
                if schema_row:
                    schema = DomainSchema(**schema_row["schema_json"])
                    stats = await run_incremental_ingestion(
                        processed, str(domain_id), schema
                    )
                else:
                    # No schema yet — run full initial pipeline
                    stats, schema_result = await run_initial_ingestion(
                        processed, str(domain_id)
                    )
                    if schema_result and schema_result.schema:
                        s = schema_result.schema
                        await queries.save_channel_schema(
                            async_engine,
                            domain_id,
                            schema_json=s.model_dump(),
                            detected_domain=schema_result.detected_domain,
                            entity_types=[et.model_dump() for et in s.entity_types],
                            relation_types=[rt.model_dump() for rt in s.relation_types],
                        )

                pipeline_entities = stats.entities_extracted
                pipeline_relations = stats.relations_extracted
                pipeline_vectors = stats.vectors_stored

                # Update tag summaries (non-blocking)
                try:
                    from agent_memory_mcp.pipeline.tag_summarizer import update_tag_summaries
                    await update_tag_summaries(domain_id, async_engine)
                except Exception:
                    log.exception("tag_summaries_error", domain_id=str(domain_id))

            await queries.update_sync_job(
                async_engine,
                job["id"],
                status="completed",
                messages_processed=len(msgs),
                entities_extracted=pipeline_entities,
                completed_at=datetime.now(timezone.utc),
            )

            # Schedule next sync
            next_sync = datetime.now(timezone.utc) + timedelta(
                minutes=domain["sync_frequency_minutes"]
            )
            last_msg_id = max((m.message_id for m in msgs), default=domain.get("last_synced_message_id", 0) or 0)
            new_count = (domain.get("message_count", 0) or 0) + len(msgs)
            new_entities = (domain.get("entity_count", 0) or 0) + pipeline_entities
            new_relations = (domain.get("relation_count", 0) or 0) + pipeline_relations
            domain_update = dict(
                last_synced_message_id=last_msg_id,
                next_sync_at=next_sync,
                message_count=new_count,
                entity_count=new_entities,
                relation_count=new_relations,
            )
            # Only mark as "synced" if we actually got messages or already had some
            # (prevents first-time sync with narrow depth from marking channel as done)
            if msgs or last_msg_id > 0:
                domain_update["last_synced_at"] = datetime.now(timezone.utc)
            await queries.update_domain(async_engine, domain_id, **domain_update)
            log.info(
                "incremental_sync_done",
                domain_id=str(domain_id),
                messages=len(msgs),
                entities=pipeline_entities,
                vectors=pipeline_vectors,
            )
        except Exception as exc:
            log.exception("incremental_sync_error", domain_id=str(domain_id))
            await queries.update_sync_job(
                async_engine, job["id"], status="failed", error_message=str(exc)[:500]
            )
            # Push next_sync_at forward to avoid retry-spam on persistent errors
            backoff_minutes = max(domain.get("sync_frequency_minutes", 60), 10)
            await queries.update_domain(
                async_engine, domain_id,
                next_sync_at=datetime.now(timezone.utc) + timedelta(minutes=backoff_minutes),
            )
        finally:
            self._syncing_domains.discard(domain_id)

    async def _check_digests(self) -> None:
        """Check and run due digests for the current UTC hour."""
        if not self._bot:
            return
        current_hour = datetime.now(timezone.utc).hour
        # Only check once per hour
        if current_hour == self._last_digest_check_hour:
            return
        self._last_digest_check_hour = current_hour

        try:
            from agent_memory_mcp.db import queries_digest as dq
            from agent_memory_mcp.digest.runner import run_digest

            due_configs = await dq.get_due_digests(async_engine, current_hour)
            for config in due_configs:
                asyncio.create_task(
                    run_digest(config, async_engine, self._bot),
                    name=f"digest_{config['user_id']}",
                )
                log.info("digest_scheduled", user_id=config["user_id"], hour=current_hour)
        except Exception:
            log.exception("digest_check_error")

    def stop(self) -> None:
        self._running = False
