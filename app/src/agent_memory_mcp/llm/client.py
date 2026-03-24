"""LLM client -- calls models through the LiteLLM proxy."""

from __future__ import annotations

import re

import httpx
import orjson
import structlog
from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

from agent_memory_mcp.config import settings

log = structlog.get_logger()

_CODE_BLOCK_RE = re.compile(r"^```(?:json)?\s*\n?(.*?)\n?```\s*$", re.DOTALL)

# Shared client — reuse across all LLM calls in the same event loop.
_client: AsyncOpenAI | None = None


def get_llm_client() -> AsyncOpenAI:
    """Return a shared AsyncOpenAI client pointed at the LiteLLM proxy.

    Reuses a single httpx connection pool to avoid resource exhaustion
    during concurrent MAP-REDUCE calls.
    """
    global _client
    if _client is None:
        _client = AsyncOpenAI(
            base_url=f"{settings.litellm_url}/v1",
            api_key=settings.litellm_api_key,
            http_client=httpx.AsyncClient(
                trust_env=False,
                timeout=httpx.Timeout(connect=10, read=600, write=30, pool=120),
                limits=httpx.Limits(
                    max_connections=30,
                    max_keepalive_connections=15,
                ),
            ),
        )
    return _client


@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=5))
async def llm_call(
    model: str,
    messages: list[dict],
    response_format: dict | None = None,
    temperature: float = 0.1,
    max_tokens: int = 4096,
) -> str:
    """Call an LLM via the LiteLLM proxy. Returns the content string."""
    client = get_llm_client()
    kwargs: dict = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if response_format:
        kwargs["response_format"] = response_format

    response = await client.chat.completions.create(**kwargs)
    choice = response.choices[0]
    content = choice.message.content
    log.debug(
        "llm_call",
        model=model,
        tokens=response.usage.total_tokens if response.usage else 0,
        finish_reason=choice.finish_reason,
    )
    if choice.finish_reason == "length":
        log.warning(
            "llm_response_truncated",
            model=model,
            max_tokens=max_tokens,
            content_len=len(content) if content else 0,
        )
    return content


def _extract_json(content: str) -> str:
    """Extract JSON object from LLM response.

    Handles: raw JSON, markdown code blocks, leading text before JSON.
    """
    content = content.strip()
    # Strip markdown code blocks (```json ... ```)
    m = _CODE_BLOCK_RE.match(content)
    if m:
        content = m.group(1).strip()
    # If still doesn't start with {, find the first { ... last }
    if content and not content.startswith("{"):
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end != -1:
            content = content[start : end + 1]
    return content


async def llm_call_json(model: str, messages: list[dict], **kwargs) -> dict:
    """Call an LLM and parse the response as JSON."""
    content = await llm_call(
        model,
        messages,
        response_format={"type": "json_object"},
        **kwargs,
    )
    cleaned = _extract_json(content)
    try:
        result = orjson.loads(cleaned)
    except orjson.JSONDecodeError as exc:
        log.error(
            "json_parse_failed",
            model=model,
            raw_len=len(content),
            cleaned_len=len(cleaned),
            cleaned_head=cleaned[:200],
            cleaned_tail=cleaned[-200:] if len(cleaned) > 200 else "",
        )
        raise ValueError(f"LLM returned invalid JSON: {exc}") from exc
    if not isinstance(result, dict):
        log.error("json_not_dict", model=model, result_type=type(result).__name__)
        raise ValueError(f"LLM returned {type(result).__name__}, expected dict")
    return result
