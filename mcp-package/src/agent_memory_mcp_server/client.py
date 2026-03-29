"""HTTP client for Agent Memory MCP REST API."""

from __future__ import annotations

import json
import os

import httpx


class AgentMemoryClient:
    """Thin HTTP client that forwards requests to the Agent Memory REST API."""

    def __init__(self) -> None:
        self.url = os.environ.get("AGENT_MEMORY_URL", "https://agent.ai-vfx.com").rstrip("/")
        self.key = os.environ.get("AGENT_MEMORY_API_KEY", "")
        if not self.key:
            raise RuntimeError(
                "AGENT_MEMORY_API_KEY environment variable is required. "
                "Get your key at https://t.me/AgentMemoryBot"
            )

    async def _request(self, endpoint: str, method: str = "POST", **kwargs) -> str:
        headers = {"Authorization": f"Bearer {self.key}"}
        async with httpx.AsyncClient(timeout=120) as c:
            if method == "GET":
                resp = await c.get(f"{self.url}/api/v1/{endpoint}", headers=headers)
            else:
                resp = await c.post(
                    f"{self.url}/api/v1/{endpoint}",
                    json={k: v for k, v in kwargs.items() if v is not None},
                    headers=headers,
                )
            if resp.status_code == 402:
                data = resp.json()
                return json.dumps({
                    "error": "insufficient_credits",
                    "balance": data.get("detail", {}).get("balance", 0),
                    "required": data.get("detail", {}).get("required", 0),
                    "topup_url": data.get("detail", {}).get("topup_url", ""),
                    "message": "Top up your credits to continue using Agent Memory.",
                }, ensure_ascii=False)
            resp.raise_for_status()
            return json.dumps(resp.json(), ensure_ascii=False, default=str)

    async def search(self, query: str, scope: str | None = None, limit: int = 10) -> str:
        return await self._request("memory/search", query=query, scope=scope, limit=limit)

    async def digest(self, scope: str, period: str = "7d") -> str:
        return await self._request("digest", scope=scope, period=period)

    async def decisions(self, scope: str, topic: str | None = None) -> str:
        return await self._request("decisions", scope=scope, topic=topic)

    async def add_source(self, handle: str, source_type: str = "channel", sync_range: str = "3m") -> str:
        return await self._request("sources/add", handle=handle, source_type=source_type, sync_range=sync_range)

    async def list_sources(self) -> str:
        return await self._request("sources", method="GET")

    async def list_scopes(self) -> str:
        return await self._request("scopes", method="GET")

    async def context(self, task: str, scope: str) -> str:
        return await self._request("memory/context", task=task, scope=scope)

    async def balance(self) -> str:
        return await self._request("account/balance", method="GET")
