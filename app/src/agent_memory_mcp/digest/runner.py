"""Daily digest runner — generates and sends digest to user."""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone

import structlog
from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, LinkPreviewOptions
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_memory_mcp.config import settings
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db import queries_digest as dq
from agent_memory_mcp.db import queries_groups as gq
from agent_memory_mcp.digest.clustering import Cluster, cluster_messages, deduplicate, embed_messages
from agent_memory_mcp.llm.client import llm_call, llm_call_json
from agent_memory_mcp.llm.digest_prompts import (
    CLUSTER_LABEL_PROMPT,
    MAP_DIGEST_SYSTEM,
    REDUCE_DIGEST_SYSTEM,
)
from agent_memory_mcp.storage.embedding_client import EmbeddingClient
from agent_memory_mcp.tracing.tracer import flush, trace_observation

log = structlog.get_logger(__name__)

_DIGEST_MAX_MESSAGES = 200
_DIGEST_HASHTAGS = {"дайджест", "digest", "дайджестчата", "дайджестдня", "weekly", "weeklydigest"}
_NO_PREVIEW = LinkPreviewOptions(is_disabled=True)


async def run_digest(
    config: dict,
    engine: AsyncEngine,
    bot: Bot,
    preview: bool = False,
) -> None:
    """Run a single digest: resolve scope → fetch messages → map-reduce → send."""
    user_id = config["user_id"]
    config_id = config["id"]

    run = await dq.create_digest_run(engine, config_id, user_id)
    run_id = run["id"]

    with trace_observation(
        as_type="agent", name="daily_digest",
        input=f"digest for user {user_id}",
        metadata={"config_id": str(config_id)},
    ) as agent:
        trace_id = agent.trace_id if agent else ""

        try:
            # Resolve scope
            scope_type = config.get("scope_type", "all")
            scope_id = config.get("scope_id")

            if scope_type == "group" and scope_id:
                group_domains = await gq.get_group_domains(engine, scope_id)
                domain_ids = [d["id"] for d in group_domains]
            elif scope_type == "domain" and scope_id:
                domain_ids = [scope_id]
            else:
                all_domains = await db_q.list_domains(engine, user_id)
                domain_ids = [d["id"] for d in all_domains]

            if not domain_ids:
                await dq.update_digest_run(
                    engine, run_id,
                    status="completed", digest_text="Нет каналов для дайджеста.",
                    domain_count=0, message_count=0,
                    completed_at=datetime.now(timezone.utc),
                )
                return

            # Fetch messages from last 24h
            since = datetime.now(timezone.utc) - timedelta(hours=24)
            all_messages = await db_q.get_messages_since(engine, domain_ids, since, limit=5000)

            if not all_messages:
                await dq.update_digest_run(
                    engine, run_id,
                    status="completed",
                    digest_text="За последние 24 часа новых постов не было.",
                    domain_count=len(domain_ids), message_count=0,
                    completed_at=datetime.now(timezone.utc),
                )
                return

            # Filter out digest posts from other channels
            filtered = _filter_digest_posts(all_messages)

            # Engagement scoring: prioritize posts with replies
            scored = _score_messages(filtered)
            top_messages = scored[:_DIGEST_MAX_MESSAGES]

            # Build domain_id → username map for links
            domain_username_map: dict[str, str] = {}
            for did in domain_ids:
                d = await db_q.get_domain(engine, did)
                if d and d.get("channel_username"):
                    domain_username_map[str(d["id"])] = d["channel_username"]

            # --- Cluster-based pipeline ---
            # 1. Embed
            emb_client = EmbeddingClient()
            try:
                emb_msgs, embeddings = await embed_messages(top_messages, emb_client)
            finally:
                await emb_client.close()

            if not emb_msgs:
                await dq.update_digest_run(
                    engine, run_id,
                    status="completed",
                    digest_text="Не удалось получить эмбеддинги постов.",
                    domain_count=len(domain_ids), message_count=len(all_messages),
                    completed_at=datetime.now(timezone.utc),
                )
                return

            # 2. Dedup
            deduped_msgs, deduped_embs = deduplicate(emb_msgs, embeddings)

            # 3. Cluster
            clusters = cluster_messages(deduped_msgs, deduped_embs)

            log.info(
                "digest_clustering",
                total=len(filtered), embedded=len(emb_msgs),
                deduped=len(deduped_msgs), clusters=len(clusters),
                removed_dupes=len(emb_msgs) - len(deduped_msgs),
            )

            # 4. Label clusters (parallel, Tier1)
            labeled = await _label_clusters(clusters, domain_username_map)

            # 5. MAP per-cluster (parallel, Tier2)
            cluster_summaries = await _map_clusters(labeled, domain_username_map)

            if not cluster_summaries:
                await dq.update_digest_run(
                    engine, run_id,
                    status="completed",
                    digest_text="Не удалось извлечь данные из постов.",
                    domain_count=len(domain_ids), message_count=len(all_messages),
                    completed_at=datetime.now(timezone.utc),
                )
                return

            # 6. REDUCE (single call, Tier3)
            combined = "\n\n".join(cluster_summaries)
            reduce_prompt = REDUCE_DIGEST_SYSTEM.format(total_posts=len(deduped_msgs))
            digest_raw = await llm_call(
                model=settings.llm_tier3_model,
                messages=[
                    {"role": "system", "content": reduce_prompt},
                    {"role": "user", "content": combined},
                ],
                temperature=0.3,
                max_tokens=4096,
            )

            # Format digest with t.me links (→ arrows, no [ссылка])
            digest_html = _format_digest_html(
                digest_raw.strip(), deduped_msgs, domain_username_map,
            )

            # Get previous digest for navigation link
            prev_run = await dq.get_last_completed_run(engine, config_id)

            # Save run
            await dq.update_digest_run(
                engine, run_id,
                status="completed",
                digest_text=digest_html,
                domain_count=len(domain_ids),
                message_count=len(all_messages),
                langfuse_trace_id=trace_id,
                completed_at=datetime.now(timezone.utc),
            )

            # Update last_sent_at (skip for preview so it doesn't block scheduled send)
            if not preview:
                await dq.update_digest_config(
                    engine, config_id, last_sent_at=datetime.now(timezone.utc),
                )

            # Build inline keyboard with previous digest link
            prev_kb = None
            if prev_run and prev_run.get("completed_at"):
                prev_date = prev_run["completed_at"].strftime("%d.%m")
                prev_kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(
                        text=f"📋 Предыдущий дайджест ({prev_date})",
                        callback_data=f"digest:prev:{prev_run['id']}",
                    ),
                ]])

            # Send to user
            await _send_digest(bot, user_id, digest_html, reply_markup=prev_kb)
            log.info(
                "digest_sent",
                user_id=user_id, messages=len(all_messages),
                domains=len(domain_ids),
            )

        except Exception as exc:
            log.exception("digest_run_failed", user_id=user_id)
            await dq.update_digest_run(
                engine, run_id,
                status="failed",
                error_message=str(exc)[:500],
                completed_at=datetime.now(timezone.utc),
            )

    flush()


async def _label_clusters(
    clusters: list[Cluster],
    domain_map: dict[str, str],
) -> list[Cluster]:
    """Label each cluster with emoji + theme name via Tier1 LLM."""
    semaphore = asyncio.Semaphore(8)

    async def _label_one(cluster: Cluster) -> Cluster:
        # Already labeled (e.g. "Разное")
        if cluster.label:
            return cluster

        # Pick 3-5 representative posts (longest content)
        sorted_msgs = sorted(
            cluster.messages,
            key=lambda m: len(m.get("content") or ""),
            reverse=True,
        )
        reps = sorted_msgs[:5]
        posts_text = "\n\n---\n\n".join(
            f"[Пост] channel={domain_map.get(str(p.get('domain_id', '')), '?')}\n"
            f"{(p.get('content') or '')[:500]}"
            for p in reps
        )
        prompt = CLUSTER_LABEL_PROMPT.format(posts=posts_text)

        async with semaphore:
            try:
                result = await llm_call_json(
                    model=settings.llm_tier1_model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    max_tokens=128,
                )
                cluster.emoji = result.get("emoji", "📌")
                cluster.label = result.get("label", "Разное")
            except Exception:
                log.warning("cluster_label_failed", cluster_size=len(cluster.messages))
                cluster.emoji = "📌"
                cluster.label = "Разное"
        return cluster

    results = await asyncio.gather(
        *[_label_one(c) for c in clusters],
        return_exceptions=True,
    )
    labeled: list[Cluster] = []
    for r in results:
        if isinstance(r, Cluster):
            labeled.append(r)
        elif isinstance(r, Exception):
            log.exception("cluster_label_error", error=str(r))
    return labeled


async def _map_clusters(
    clusters: list[Cluster],
    domain_map: dict[str, str],
) -> list[str]:
    """MAP phase: summarize each cluster via Tier2 LLM."""
    semaphore = asyncio.Semaphore(8)

    async def _map_one(cluster: Cluster) -> str:
        cluster_label = f"{cluster.emoji} {cluster.label}"
        posts_text = "\n\n---\n\n".join(
            f"[Пост] channel={domain_map.get(str(p.get('domain_id', '')), '?')} "
            f"msg_id={p.get('telegram_msg_id', 0)}\n"
            f"{(p.get('content') or '')[:1500]}"
            for p in cluster.messages
        )
        prompt = MAP_DIGEST_SYSTEM.format(
            cluster_label=cluster_label, posts=posts_text,
        )
        async with semaphore:
            result = await llm_call(
                model=settings.llm_tier2_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=3072,
            )
        return f"[Кластер: {cluster_label}]\n{result.strip()}"

    results = await asyncio.gather(
        *[_map_one(c) for c in clusters],
        return_exceptions=True,
    )
    summaries: list[str] = []
    for r in results:
        if isinstance(r, str) and r:
            summaries.append(r)
        elif isinstance(r, Exception):
            log.exception("digest_map_failed", error=str(r))
    return summaries


def _filter_digest_posts(messages: list[dict]) -> list[dict]:
    """Remove messages that are themselves digests from other channels."""
    result = []
    for m in messages:
        hashtags = m.get("hashtags") or []
        # Check hashtags
        if any(h.lower() in _DIGEST_HASHTAGS for h in hashtags):
            continue
        # Check content for digest patterns
        content = (m.get("content") or "").lower()
        if "#дайджест" in content or "#digest" in content:
            continue
        result.append(m)
    return result


def _score_messages(messages: list[dict]) -> list[dict]:
    """Score messages by engagement (replies, thread depth)."""
    # Count replies per message
    reply_counts: dict[int, int] = {}
    for m in messages:
        reply_to = m.get("reply_to_msg_id")
        if reply_to:
            reply_counts[reply_to] = reply_counts.get(reply_to, 0) + 1

    scored = []
    for m in messages:
        msg_id = m.get("telegram_msg_id", 0)
        replies = reply_counts.get(msg_id, 0)
        content_len = len(m.get("content") or "")
        # Score: replies × 3 + content length factor
        score = replies * 3 + min(content_len / 100, 5)
        scored.append((score, m))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [m for _, m in scored]


def _format_digest_html(
    digest_text: str,
    messages: list[dict],
    domain_username_map: dict[str, str],
) -> str:
    """Replace [msg_id: N] with clickable → arrows, format HTML."""
    import html as html_mod

    text = html_mod.escape(digest_text)
    # Restore bold markers
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)

    # Build msg_id → url map
    from agent_memory_mcp.utils.links import make_tme_link

    msg_url_map: dict[int, str] = {}
    for m in messages:
        msg_id = m.get("telegram_msg_id", 0)
        domain_id = str(m.get("domain_id", ""))
        username = domain_username_map.get(domain_id, "")
        if username and msg_id:
            topic_id = m.get("topic_id")
            msg_url_map[msg_id] = make_tme_link(username, msg_id, topic_id)

    # Replace [msg_id: 123] with clickable → arrow
    def _replace_ref(match):
        mid = int(match.group(1))
        url = msg_url_map.get(mid)
        if url:
            return f' <a href="{url}">→</a>'
        return ""

    text = re.sub(r'\[msg_id:\s*(\d+)\]', _replace_ref, text)

    # Clean up markdown headers that LLM might still produce
    text = re.sub(r'^#{1,3}\s+', '', text, flags=re.MULTILINE)

    return text.strip()


async def _send_digest(
    bot: Bot,
    user_id: int,
    html_text: str,
    reply_markup=None,
) -> None:
    """Send digest as HTML message(s) without link preview, splitting if needed."""
    MAX_LEN = 4096
    if len(html_text) <= MAX_LEN:
        await bot.send_message(
            user_id, html_text,
            link_preview_options=_NO_PREVIEW,
            reply_markup=reply_markup,
        )
    else:
        # Split at double newlines
        parts = html_text.split("\n\n")
        current = ""
        for part in parts:
            if len(current) + len(part) + 2 > MAX_LEN:
                if current:
                    await bot.send_message(
                        user_id, current,
                        link_preview_options=_NO_PREVIEW,
                    )
                current = part
            else:
                current = current + "\n\n" + part if current else part
        if current:
            # Last chunk gets the reply_markup (prev digest button)
            await bot.send_message(
                user_id, current,
                link_preview_options=_NO_PREVIEW,
                reply_markup=reply_markup,
            )
