"""Client for BGE-M3 embeddings via TEI (Text Embeddings Inference)."""

from __future__ import annotations

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from agent_memory_mcp.config import settings

log = structlog.get_logger(__name__)


class EmbeddingClient:
    """Client for BGE-M3 embeddings via TEI."""

    def __init__(self, base_url: str | None = None) -> None:
        self._base_url = (base_url or settings.embedding_url).rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base_url, timeout=60.0, trust_env=False,
        )

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, max=10), reraise=True)
    async def embed_dense(self, texts: list[str]) -> list[list[float]]:
        """Get dense embeddings (1024-dim) for a list of texts."""
        resp = await self._client.post(
            "/embed", json={"inputs": texts, "normalize": True, "truncate": True},
        )
        resp.raise_for_status()
        data = resp.json()
        log.debug("embed_dense", count=len(texts), dim=len(data[0]) if data else 0)
        return data

    async def embed_query(self, text: str) -> list[float]:
        """Embed a single query text. Returns 1024-dim vector."""
        vectors = await self.embed_dense([text])
        return vectors[0]

    async def close(self) -> None:
        await self._client.aclose()
