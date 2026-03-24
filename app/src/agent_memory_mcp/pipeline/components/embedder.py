"""BGE-M3 embedder component -- calls TEI for dense embeddings."""

from __future__ import annotations

import asyncio

import structlog
from haystack import component

from agent_memory_mcp.storage.embedding_client import EmbeddingClient

log = structlog.get_logger(__name__)

# Small batch to avoid CUDA OOM on RTX 2070 (8GB VRAM)
_BATCH_SIZE = 4
_MAX_TEXT_CHARS = 8192  # ~2048 tokens for BGE-M3


@component
class BGEEmbedder:
    """Produce dense embeddings via TEI (BGE-M3)."""

    @component.output_types(dense_vectors=list)
    def run(self, texts: list[str]) -> dict:
        if not texts:
            return {"dense_vectors": []}
        loop = asyncio.new_event_loop()
        try:
            dense = loop.run_until_complete(self._embed(texts))
        finally:
            loop.close()
        return {"dense_vectors": dense}

    async def _embed(self, texts: list[str]) -> list[list[float]]:
        client = EmbeddingClient()
        try:
            all_dense: list[list[float]] = []
            # Truncate long texts to prevent OOM
            truncated = [t[:_MAX_TEXT_CHARS] for t in texts]
            for i in range(0, len(truncated), _BATCH_SIZE):
                batch = truncated[i : i + _BATCH_SIZE]
                dense = await client.embed_dense(batch)
                all_dense.extend(dense)
            log.info("embedder_done", texts=len(texts), dense=len(all_dense))
            return all_dense
        finally:
            await client.close()
