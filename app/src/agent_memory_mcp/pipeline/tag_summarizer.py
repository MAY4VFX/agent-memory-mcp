"""Pre-computed tag summaries for overview queries."""

from __future__ import annotations

from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_memory_mcp.config import settings
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.llm.client import llm_call
from agent_memory_mcp.llm.query_prompts import TAG_SUMMARY_SYSTEM

log = structlog.get_logger(__name__)

TAG_SUMMARY_THRESHOLD = 100  # Min posts to generate summary


async def update_tag_summaries(
    domain_id: UUID,
    engine: AsyncEngine,
) -> int:
    """Generate/update summaries for tags with >100 posts.

    Called after sync. Samples 50 posts per tag (not all).
    Returns number of summaries generated/updated.
    """
    hashtag_counts = await db_q.count_posts_per_hashtag(engine, domain_id)
    if not hashtag_counts:
        return 0

    updated = 0
    for hashtag, count in hashtag_counts.items():
        if count < TAG_SUMMARY_THRESHOLD:
            continue

        existing = await db_q.get_hashtag_summary(engine, domain_id, hashtag)
        if existing and not existing["is_stale"]:
            continue

        try:
            summary = await _generate_tag_summary(
                domain_id, hashtag, count, engine,
            )
            await db_q.save_hashtag_summary(
                engine, domain_id, hashtag, summary, count,
            )
            updated += 1
            log.info(
                "tag_summary_generated",
                domain_id=str(domain_id),
                hashtag=hashtag,
                post_count=count,
            )
        except Exception:
            log.exception(
                "tag_summary_failed",
                domain_id=str(domain_id),
                hashtag=hashtag,
            )

    return updated


async def _generate_tag_summary(
    domain_id: UUID,
    hashtag: str,
    post_count: int,
    engine: AsyncEngine,
) -> str:
    """Generate a summary for a hashtag using sampled posts."""
    samples = await db_q.sample_messages_by_hashtag(
        engine, domain_id, hashtag, sample_size=50,
    )
    if not samples:
        return f"Хештег #{hashtag} используется {post_count} раз."

    # Format posts for prompt
    posts_text = "\n\n---\n\n".join(
        f"[{s.get('msg_date', '')}] {(s.get('content', '') or '')[:500]}"
        for s in samples
    )

    prompt = TAG_SUMMARY_SYSTEM.format(
        hashtag=hashtag,
        post_count=post_count,
        posts=posts_text,
    )

    summary = await llm_call(
        model=settings.llm_tier2_model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=1024,
    )
    return summary.strip()
