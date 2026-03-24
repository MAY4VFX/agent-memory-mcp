"""Extract decisions, action items, and open questions from messages."""

from __future__ import annotations

import asyncio
from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_memory_mcp.config import settings
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.decision_pipeline.prompts import DECISION_EXTRACTION_SYSTEM
from agent_memory_mcp.llm.client import llm_call_json

log = structlog.get_logger(__name__)


async def extract_decisions(
    engine: AsyncEngine,
    domain_ids: list[UUID],
    topic: str | None = None,
    period_days: int = 30,
    batch_size: int = 15,
    max_messages: int = 500,
) -> list[dict]:
    """Extract decisions/actions/questions from recent messages.

    Args:
        engine: DB engine.
        domain_ids: Domains to extract from.
        topic: Optional topic filter.
        period_days: How many days back to look.
        batch_size: Messages per LLM call.
        max_messages: Maximum messages to process.

    Returns:
        List of extracted items with type, content, topic, source_message_ids, confidence.
    """
    all_items: list[dict] = []

    for domain_id in domain_ids:
        messages = await db_q.get_recent_messages(
            engine, domain_id, days=period_days, limit=max_messages,
        )
        if not messages:
            continue

        # Batch messages
        batches = [messages[i:i + batch_size] for i in range(0, len(messages), batch_size)]

        # Process batches concurrently (max 5 at a time)
        sem = asyncio.Semaphore(5)

        async def _process_batch(batch: list[dict]) -> list[dict]:
            async with sem:
                return await _extract_from_batch(batch, topic)

        results = await asyncio.gather(*[_process_batch(b) for b in batches])
        for batch_items in results:
            all_items.extend(batch_items)

    # Deduplicate by content similarity (simple exact match for now)
    seen_contents: set[str] = set()
    unique_items: list[dict] = []
    for item in all_items:
        content_key = item["content"].strip().lower()
        if content_key not in seen_contents:
            seen_contents.add(content_key)
            unique_items.append(item)

    # Sort by confidence
    unique_items.sort(key=lambda x: x.get("confidence", 0), reverse=True)

    return unique_items


async def _extract_from_batch(messages: list[dict], topic: str | None) -> list[dict]:
    """Run LLM extraction on a batch of messages."""
    # Format messages for the prompt
    lines = []
    for msg in messages:
        msg_id = msg.get("id", msg.get("telegram_msg_id", "?"))
        content = msg.get("content", "")[:500]
        lines.append(f"[msg_id:{msg_id}] {content}")
    posts_text = "\n\n".join(lines)

    topic_str = topic or "все темы"
    system = DECISION_EXTRACTION_SYSTEM.format(topic=topic_str)

    user_msg = f"Сообщения для анализа:\n\n{posts_text}"

    try:
        result = await llm_call_json(
            model=settings.llm_tier2_model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.1,
            max_tokens=2000,
        )
        items = result.get("items", [])
        # Validate items
        valid = []
        for item in items:
            if all(k in item for k in ("type", "content")) and item["type"] in (
                "decision", "action_item", "open_question"
            ):
                valid.append(item)
        return valid
    except Exception:
        log.warning("decision_extraction_failed", exc_info=True)
        return []
