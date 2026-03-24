"""Milvus writer component -- upserts documents with embeddings into Milvus."""

from __future__ import annotations

import structlog
from haystack import component

from agent_memory_mcp.storage.milvus_client import MilvusStorage

log = structlog.get_logger(__name__)


@component
class MilvusWriter:
    """Write documents (with vectors) to Milvus collection."""

    @component.output_types(count=int)
    def run(self, documents: list[dict]) -> dict:
        if not documents:
            return {"count": 0}
        storage = MilvusStorage()
        try:
            storage.ensure_collection()
            count = storage.upsert_documents(documents)
            log.info("milvus_writer_done", count=count)
            return {"count": count}
        finally:
            storage.close()
