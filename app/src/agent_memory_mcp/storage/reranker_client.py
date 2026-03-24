"""Client for BGE-reranker-v2-m3 via TEI /rerank endpoint."""

from __future__ import annotations

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from agent_memory_mcp.config import settings

log = structlog.get_logger(__name__)


class RerankerClient:
    """Cross-encoder reranker via TEI."""

    def __init__(self, base_url: str | None = None) -> None:
        self._base_url = (base_url or settings.reranker_url).rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base_url, timeout=60.0, trust_env=False,
        )

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, max=10), reraise=True)
    async def rerank(
        self,
        query: str,
        documents: list[str],
        top_k: int | None = None,
    ) -> list[dict]:
        """Rerank documents against query.

        Returns list of {index, score} sorted by score descending.
        """
        if not documents:
            return []
        payload: dict = {"query": query, "texts": documents, "raw_scores": False}
        if top_k is not None:
            payload["truncate"] = True
        resp = await self._client.post("/rerank", json=payload)
        resp.raise_for_status()
        results = resp.json()
        # TEI returns [{index, score}, ...] sorted by score desc
        ranked = sorted(results, key=lambda x: x.get("score", 0), reverse=True)
        if top_k is not None:
            ranked = ranked[:top_k]
        log.debug("rerank", query_len=len(query), docs=len(documents), results=len(ranked))
        return ranked

    async def close(self) -> None:
        await self._client.aclose()
