"""Ingestion pipeline orchestration.

Two main entrypoints:
- run_initial_ingestion()  -- full pipeline with schema discovery (first sync)
- run_incremental_ingestion()  -- reuses existing schema (delta syncs)

Components are called manually (not via Haystack Pipeline graph) to allow
full control over async, error handling, and progress reporting.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime

import structlog

from agent_memory_mcp.models.extraction import BaseEntity, BaseRelation
from agent_memory_mcp.models.messages import ProcessedMessage, ThreadGroup
from agent_memory_mcp.models.schema import DomainSchema, SchemaDiscoveryResult
from agent_memory_mcp.pipeline.components.community_detector import run_community_detection
from agent_memory_mcp.pipeline.components.embedder import BGEEmbedder
from agent_memory_mcp.pipeline.components.entity_extractor import EntityExtractor
from agent_memory_mcp.pipeline.components.falkordb_writer import FalkorDBWriter
from agent_memory_mcp.pipeline.components.metadata_enricher import MetadataEnricher
from agent_memory_mcp.pipeline.components.milvus_writer import MilvusWriter
from agent_memory_mcp.pipeline.components.noise_filter import NoiseFilter
from agent_memory_mcp.pipeline.components.schema_discovery import SchemaDiscovery
from agent_memory_mcp.pipeline.components.thread_builder import ThreadBuilder

log = structlog.get_logger(__name__)


@dataclass
class IngestionStats:
    """Statistics from an ingestion run."""

    total_messages: int = 0
    noise_messages: int = 0
    clean_messages: int = 0
    threads_built: int = 0
    entities_extracted: int = 0
    relations_extracted: int = 0
    vectors_stored: int = 0
    graph_nodes_stored: int = 0
    schema_discovered: bool = False
    domain_type: str = ""
    started_at: datetime = field(default_factory=datetime.utcnow)
    finished_at: datetime | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def duration_sec(self) -> float:
        if self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return 0.0


def _build_milvus_documents(
    threads: list[ThreadGroup],
    dense_vectors: list[list[float]],
) -> list[dict]:
    """Assemble Milvus documents from threads and their embeddings."""
    docs: list[dict] = []
    for i, thread in enumerate(threads):
        if i >= len(dense_vectors):
            break
        docs.append({
            "id": str(thread.id),
            "channel_id": thread.messages[0].channel_id if thread.messages else 0,
            "thread_id": f"{thread.domain_id}_{thread.root_message_id}",
            "content": thread.combined_text[:32768],
            "msg_date": int(thread.first_msg_date.timestamp()) if thread.first_msg_date else 0,
            "language": thread.messages[0].language or "unknown" if thread.messages else "unknown",
            "content_type": "thread",
            "dense_vector": dense_vectors[i],
        })
    return docs


async def run_initial_ingestion(
    messages: list[ProcessedMessage],
    domain_id: str,
) -> tuple[IngestionStats, SchemaDiscoveryResult | None]:
    """Full ingestion pipeline with schema discovery (first sync).

    Steps:
    1. NoiseFilter
    2. MetadataEnricher
    3. ThreadBuilder
    4. SchemaDiscovery
    5. EntityExtractor + BGEEmbedder (parallel via asyncio.to_thread)
    6. MilvusWriter + FalkorDBWriter (parallel via asyncio.to_thread)

    Returns (stats, schema_result).
    """
    stats = IngestionStats(total_messages=len(messages))
    schema_result: SchemaDiscoveryResult | None = None

    log.info("initial_ingestion_start", domain_id=domain_id, messages=len(messages))

    # 1. Noise filter
    noise_filter = NoiseFilter()
    nf_out = noise_filter.run(messages=messages)
    clean = nf_out["clean"]
    noise = nf_out["noise"]
    stats.noise_messages = len(noise)
    stats.clean_messages = len(clean)
    log.info("noise_filter_done", clean=len(clean), noise=len(noise))

    if not clean:
        stats.finished_at = datetime.utcnow()
        stats.errors.append("No clean messages after noise filtering")
        return stats, None

    # 2. Metadata enricher
    enricher = MetadataEnricher()
    me_out = enricher.run(messages=clean)
    enriched = me_out["messages"]
    log.info("metadata_enricher_done", count=len(enriched))

    # 3. Thread builder
    thread_builder = ThreadBuilder()
    tb_out = thread_builder.run(messages=enriched, domain_id=domain_id)
    threads: list[ThreadGroup] = tb_out["threads"]
    stats.threads_built = len(threads)
    log.info("thread_builder_done", threads=len(threads))

    if not threads:
        stats.finished_at = datetime.utcnow()
        stats.errors.append("No threads built")
        return stats, None

    # 4. Schema discovery
    try:
        discovery = SchemaDiscovery()
        sd_out = await asyncio.to_thread(
            discovery.run, messages=enriched, domain_id=domain_id
        )
        schema_result = sd_out["result"]
        stats.schema_discovered = True
        stats.domain_type = schema_result.detected_domain
        log.info("schema_discovery_done", domain_type=schema_result.detected_domain)
    except Exception as exc:
        log.error("schema_discovery_failed", error=str(exc))
        stats.errors.append(f"Schema discovery failed: {exc}")
        # Use empty schema as fallback
        schema_result = SchemaDiscoveryResult(
            schema=DomainSchema(domain_type="other"),
            detected_domain="other",
            sample_size=0,
        )

    schema = schema_result.schema

    # 5. EntityExtractor + BGEEmbedder in parallel
    extractor = EntityExtractor()
    embedder = BGEEmbedder()

    texts = [t.combined_text for t in threads if t.combined_text]

    extraction_future = asyncio.to_thread(
        extractor.run, threads=threads, schema=schema, domain_id=domain_id
    )
    embedding_future = asyncio.to_thread(embedder.run, texts=texts)

    results = await asyncio.gather(extraction_future, embedding_future, return_exceptions=True)

    # Process extraction results
    entities: list[BaseEntity] = []
    relations: list[BaseRelation] = []
    if isinstance(results[0], Exception):
        log.error("extraction_failed", error=str(results[0]))
        stats.errors.append(f"Extraction failed: {results[0]}")
    else:
        entities = results[0]["entities"]
        relations = results[0]["relations"]
    stats.entities_extracted = len(entities)
    stats.relations_extracted = len(relations)

    # Process embedding results
    dense_vectors: list[list[float]] = []
    if isinstance(results[1], Exception):
        log.error("embedding_failed", error=str(results[1]))
        stats.errors.append(f"Embedding failed: {results[1]}")
    else:
        dense_vectors = results[1]["dense_vectors"]

    # 6. MilvusWriter + FalkorDBWriter in parallel
    milvus_writer = MilvusWriter()
    falkordb_writer = FalkorDBWriter()

    write_coros = []
    write_labels = []

    if dense_vectors:
        milvus_docs = _build_milvus_documents(threads, dense_vectors)
        write_coros.append(asyncio.to_thread(milvus_writer.run, documents=milvus_docs))
        write_labels.append("milvus")

    if entities or relations:
        write_coros.append(
            asyncio.to_thread(
                falkordb_writer.run,
                entities=entities,
                relations=relations,
                domain_id=domain_id,
            )
        )
        write_labels.append("falkordb")

    if write_coros:
        write_results = await asyncio.gather(*write_coros, return_exceptions=True)
        for label, wr in zip(write_labels, write_results):
            if isinstance(wr, Exception):
                log.error(f"{label}_write_failed", error=str(wr))
                stats.errors.append(f"{label} write failed: {wr}")
            elif label == "milvus":
                stats.vectors_stored = wr.get("count", 0)
            else:
                stats.graph_nodes_stored = wr.get("count", 0)

    # 7. Community detection (non-blocking — failure doesn't stop ingestion)
    if stats.graph_nodes_stored > 0:
        try:
            from agent_memory_mcp.storage.falkordb_client import FalkorDBStorage
            graph_storage = FalkorDBStorage()
            community_count = await run_community_detection(domain_id, graph_storage)
            log.info("community_detection_done", domain_id=domain_id, communities=community_count)
            graph_storage.close()
        except Exception as exc:
            log.error("community_detection_failed", error=str(exc))
            stats.errors.append(f"Community detection failed: {exc}")

    stats.finished_at = datetime.utcnow()
    log.info(
        "initial_ingestion_done",
        domain_id=domain_id,
        duration=stats.duration_sec,
        entities=stats.entities_extracted,
        relations=stats.relations_extracted,
        vectors=stats.vectors_stored,
        errors=len(stats.errors),
    )
    return stats, schema_result


async def run_incremental_ingestion(
    messages: list[ProcessedMessage],
    domain_id: str,
    schema: DomainSchema,
) -> IngestionStats:
    """Incremental ingestion pipeline -- reuses existing schema.

    Same as initial but skips SchemaDiscovery.
    """
    stats = IngestionStats(total_messages=len(messages))

    log.info("incremental_ingestion_start", domain_id=domain_id, messages=len(messages))

    # 1. Noise filter
    noise_filter = NoiseFilter()
    nf_out = noise_filter.run(messages=messages)
    clean = nf_out["clean"]
    stats.noise_messages = len(nf_out["noise"])
    stats.clean_messages = len(clean)

    if not clean:
        stats.finished_at = datetime.utcnow()
        stats.errors.append("No clean messages after noise filtering")
        return stats

    # 2. Metadata enricher
    enricher = MetadataEnricher()
    me_out = enricher.run(messages=clean)
    enriched = me_out["messages"]

    # 3. Thread builder
    thread_builder = ThreadBuilder()
    tb_out = thread_builder.run(messages=enriched, domain_id=domain_id)
    threads: list[ThreadGroup] = tb_out["threads"]
    stats.threads_built = len(threads)

    if not threads:
        stats.finished_at = datetime.utcnow()
        stats.errors.append("No threads built")
        return stats

    # 4. EntityExtractor + BGEEmbedder in parallel
    extractor = EntityExtractor()
    embedder = BGEEmbedder()
    texts = [t.combined_text for t in threads if t.combined_text]

    results = await asyncio.gather(
        asyncio.to_thread(
            extractor.run, threads=threads, schema=schema, domain_id=domain_id
        ),
        asyncio.to_thread(embedder.run, texts=texts),
        return_exceptions=True,
    )

    entities: list[BaseEntity] = []
    relations: list[BaseRelation] = []
    if isinstance(results[0], Exception):
        log.error("extraction_failed", error=str(results[0]))
        stats.errors.append(f"Extraction failed: {results[0]}")
    else:
        entities = results[0]["entities"]
        relations = results[0]["relations"]
    stats.entities_extracted = len(entities)
    stats.relations_extracted = len(relations)

    dense_vectors: list[list[float]] = []
    if isinstance(results[1], Exception):
        log.error("embedding_failed", error=str(results[1]))
        stats.errors.append(f"Embedding failed: {results[1]}")
    else:
        dense_vectors = results[1]["dense_vectors"]

    # 5. MilvusWriter + FalkorDBWriter in parallel
    write_futures = []

    if dense_vectors:
        milvus_docs = _build_milvus_documents(threads, dense_vectors)
        write_futures.append(asyncio.to_thread(MilvusWriter().run, documents=milvus_docs))
    if entities or relations:
        write_futures.append(
            asyncio.to_thread(
                FalkorDBWriter().run,
                entities=entities,
                relations=relations,
                domain_id=domain_id,
            )
        )

    if write_futures:
        write_results = await asyncio.gather(*write_futures, return_exceptions=True)
        for i, wr in enumerate(write_results):
            if isinstance(wr, Exception):
                log.error("write_failed", step=i, error=str(wr))
                stats.errors.append(f"Write step {i} failed: {wr}")
            elif i == 0 and dense_vectors:
                stats.vectors_stored = wr.get("count", 0)
            else:
                stats.graph_nodes_stored = wr.get("count", 0)

    stats.finished_at = datetime.utcnow()
    log.info(
        "incremental_ingestion_done",
        domain_id=domain_id,
        duration=stats.duration_sec,
        entities=stats.entities_extracted,
        vectors=stats.vectors_stored,
    )
    return stats
