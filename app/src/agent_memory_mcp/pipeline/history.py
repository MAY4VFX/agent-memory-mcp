"""Conversation history context builder."""

from __future__ import annotations

from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_memory_mcp.config import settings
from agent_memory_mcp.db import queries_conversations as qc
from agent_memory_mcp.llm.client import llm_call
from agent_memory_mcp.llm.query_prompts import SUMMARIZE_SYSTEM
from agent_memory_mcp.models.query import HistoryContext, HistoryMessage
from agent_memory_mcp.utils.tokens import count_tokens

log = structlog.get_logger(__name__)


async def build_history_context(
    engine: AsyncEngine,
    conversation_id: UUID,
    max_msgs: int | None = None,
    max_tokens: int | None = None,
) -> HistoryContext:
    """Build history context from recent conversation messages.

    Returns messages within the token budget. If messages exceed the budget,
    summarizes older messages using tier2 LLM.
    """
    _max_msgs = max_msgs or settings.query_history_max_messages
    _max_tokens = max_tokens or settings.query_history_max_tokens

    recent = await qc.get_recent_messages(engine, conversation_id, limit=_max_msgs)
    if not recent:
        return HistoryContext()

    # Build messages with token counts
    history_msgs: list[HistoryMessage] = []
    for msg in recent:
        tc = msg.get("token_count") or count_tokens(msg["content"])
        history_msgs.append(HistoryMessage(
            role=msg["role"],
            content=msg["content"],
            token_count=tc,
        ))

    total = sum(m.token_count for m in history_msgs)
    if total <= _max_tokens:
        return HistoryContext(messages=history_msgs, total_tokens=total)

    # Token budget exceeded — summarize older messages, keep recent ones
    keep_msgs: list[HistoryMessage] = []
    keep_tokens = 0
    # Walk from newest to oldest, keep as many as fit
    for msg in reversed(history_msgs):
        if keep_tokens + msg.token_count > _max_tokens // 2:
            break
        keep_msgs.insert(0, msg)
        keep_tokens += msg.token_count

    # Summarize the rest
    older = history_msgs[: len(history_msgs) - len(keep_msgs)]
    if older:
        summary_text = await _summarize_messages(older)
        summary_tokens = count_tokens(summary_text)
        return HistoryContext(
            messages=keep_msgs,
            total_tokens=keep_tokens + summary_tokens,
            has_summary=True,
            summary=summary_text,
        )

    return HistoryContext(messages=keep_msgs, total_tokens=keep_tokens)


async def _summarize_messages(messages: list[HistoryMessage]) -> str:
    """Summarize a list of messages using tier2 LLM."""
    text = "\n".join(f"{m.role}: {m.content}" for m in messages)
    try:
        result = await llm_call(
            model=settings.llm_tier2_model,
            messages=[
                {"role": "system", "content": SUMMARIZE_SYSTEM},
                {"role": "user", "content": text},
            ],
            max_tokens=512,
            temperature=0.1,
        )
        return result.strip()
    except Exception:
        log.exception("history_summarize_failed")
        # Fallback: truncate older messages
        return text[:500] + "..."
