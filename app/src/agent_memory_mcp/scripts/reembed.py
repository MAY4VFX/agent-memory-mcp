"""Lightweight re-embed: PG messages → threads → embed → Milvus.

No LLM calls, no entity extraction. Only rebuilds embeddings.
Usage: python -m agent_memory_mcp.scripts.reembed
"""

from __future__ import annotations

import asyncio
import logging
import time

import structlog
from sqlalchemy.ext.asyncio import create_async_engine

# Configure structlog for standalone script
structlog.configure(
    processors=[
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
logging.basicConfig(level=logging.INFO)

from agent_memory_mcp.config import settings
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.models.messages import ProcessedMessage, pg_row_to_processed
from agent_memory_mcp.pipeline.components.embedder import BGEEmbedder
from agent_memory_mcp.pipeline.components.metadata_enricher import MetadataEnricher
from agent_memory_mcp.pipeline.components.noise_filter import NoiseFilter
from agent_memory_mcp.pipeline.components.thread_builder import ThreadBuilder
from agent_memory_mcp.pipeline.pipelines import _build_milvus_documents
from agent_memory_mcp.storage.milvus_client import MilvusStorage

log = structlog.get_logger(__name__)


async def main() -> None:
    t0 = time.time()
    engine = create_async_engine(settings.database_url, echo=False)
    milvus = MilvusStorage()

    # Get all active domains
    from sqlalchemy import select
    from agent_memory_mcp.db.tables import domains

    async with engine.begin() as conn:
        rows = (await conn.execute(
            select(domains).where(domains.c.is_active.is_(True))
        )).mappings().all()
        active_domains = [dict(r) for r in rows]

    log.info("reembed_start", domains=len(active_domains))

    # Drop and recreate Milvus collection for clean state
    from agent_memory_mcp.storage.milvus_client import COLLECTION_NAME
    if milvus._client.has_collection(COLLECTION_NAME):
        milvus._client.drop_collection(COLLECTION_NAME)
    milvus.ensure_collection()
    log.info("reembed_milvus_reset")

    total_threads = 0
    total_vectors = 0

    for domain in active_domains:
        domain_id = domain["id"]
        channel_id = domain["channel_id"]
        log.info("reembed_domain", domain_id=str(domain_id), channel=domain.get("channel_username", ""))

        # Load messages from PG
        msg_rows = await db_q.get_domain_messages(engine, domain_id)
        if not msg_rows:
            log.info("reembed_domain_skip", domain_id=str(domain_id), reason="no messages")
            continue

        # Convert to ProcessedMessage
        processed: list[ProcessedMessage] = [
            pg_row_to_processed(r, channel_id) for r in msg_rows
        ]
        log.info("reembed_messages_loaded", count=len(processed))

        # Noise filter
        nf = NoiseFilter()
        clean = nf.run(messages=processed)["clean"]
        log.info("reembed_noise_filter", clean=len(clean), noise=len(processed) - len(clean))

        # Metadata enricher
        enricher = MetadataEnricher()
        enriched = enricher.run(messages=clean)["messages"]

        # Thread builder
        tb = ThreadBuilder()
        threads = tb.run(messages=enriched, domain_id=str(domain_id))["threads"]
        log.info("reembed_threads", count=len(threads))

        if not threads:
            continue

        # Embed
        texts = [t.combined_text for t in threads if t.combined_text]
        embedder = BGEEmbedder()
        dense_vectors = (await asyncio.to_thread(embedder.run, texts=texts))["dense_vectors"]
        log.info("reembed_embedded", vectors=len(dense_vectors))

        # Build Milvus docs (with corrected thread_id format)
        docs = _build_milvus_documents(threads, dense_vectors)
        total_threads += len(threads)
        total_vectors += len(docs)

        # Upsert to Milvus
        milvus.upsert_documents(docs)
        log.info("reembed_domain_done", domain_id=str(domain_id), vectors=len(docs))

    elapsed = time.time() - t0
    log.info("reembed_complete", threads=total_threads, vectors=total_vectors, elapsed_sec=round(elapsed, 1))
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
