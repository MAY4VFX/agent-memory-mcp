"""Classify the owner's chats into project labels.

One LLM call clusters all of a user's chats into work projects, so the same
project name is reused consistently across chats (a chat title often already
*is* the project name — the cheap signal — with message snippets as a tie-break).
Results are persisted as `labels` (type=project) + `domain_labels`, which the
/api/v1/activity export then stamps onto each message as `project_id`.

Content is read only here, inside the service; it never leaves via the export.
"""

from __future__ import annotations

import structlog

from agent_memory_mcp.config import settings
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db.engine import async_engine
from agent_memory_mcp.llm.client import llm_call_json

log = structlog.get_logger(__name__)

_SYSTEM = """Ты классифицируешь рабочие чаты пользователя по проектам.

На входе список чатов: id, название и несколько последних сообщений.
Часто название чата УЖЕ содержит название проекта — это основной сигнал,
сообщения нужны только для уточнения или когда название неинформативно.

Сгруппируй чаты по проектам так, чтобы один и тот же проект назывался
ОДИНАКОВО во всех чатах (нормализуй варианты написания: "Зенит"/"Zenith" →
один проект). Личные/нерабочие чаты помечай project_name = "" (пусто).

Верни строго JSON:
{"assignments": [
  {"domain_id": "<id>", "project_name": "<проект или пусто>",
   "confidence": 0.0-1.0, "source": "title|content"}
]}
source = "title" если проект ясен из названия, "content" если потребовались сообщения."""


def _title(d: dict) -> str:
    return (
        d.get("display_name")
        or d.get("channel_name")
        or d.get("channel_username")
        or "(без названия)"
    )


async def classify_owner_chats(
    owner_id: int,
    snippets_per_chat: int = 6,
    snippet_chars: int = 120,
) -> dict:
    """Cluster the owner's chats into project labels and persist them.

    Returns {"assigned": [{domain_id, project_name, confidence}], "count": n}.
    """
    domains = await db_q.list_domains(async_engine, owner_id)
    if not domains:
        return {"assigned": [], "count": 0}

    # Build the prompt: title + a few short recent snippets per chat.
    blocks = []
    for d in domains:
        recent = await db_q.get_recent_messages(
            async_engine, d["id"], days=60, limit=snippets_per_chat
        )
        snips = [
            (m.get("content") or "").strip().replace("\n", " ")[:snippet_chars]
            for m in recent
            if (m.get("content") or "").strip()
        ]
        snip_text = " | ".join(snips) if snips else "(нет текстовых сообщений)"
        blocks.append(f'domain_id: {d["id"]}\nназвание: {_title(d)}\nпримеры: {snip_text}')
    user_msg = "Чаты:\n\n" + "\n\n".join(blocks)

    assignments: list[dict] = []
    try:
        result = await llm_call_json(
            model=settings.llm_tier2_model,
            messages=[
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.0,
            max_tokens=2000,
        )
        assignments = result.get("assignments", []) or []
    except Exception:
        log.warning("label_classify_llm_failed", owner_id=owner_id, exc_info=True)
        # Fallback: chat title becomes the project (low confidence).
        assignments = [
            {"domain_id": str(d["id"]), "project_name": _title(d),
             "confidence": 0.3, "source": "title"}
            for d in domains
        ]

    valid_ids = {str(d["id"]): d["id"] for d in domains}
    assigned: list[dict] = []
    for a in assignments:
        did = valid_ids.get(str(a.get("domain_id")))
        name = (a.get("project_name") or "").strip()
        if not did or not name:
            continue  # unmatched id or personal/non-work chat
        conf = a.get("confidence")
        try:
            conf = float(conf) if conf is not None else None
        except (TypeError, ValueError):
            conf = None
        source = a.get("source") if a.get("source") in ("title", "content") else None
        label_id = await db_q.upsert_label(async_engine, owner_id, "project", name)
        await db_q.set_domain_label(async_engine, did, label_id, conf, source)
        assigned.append({"domain_id": str(did), "project_name": name, "confidence": conf})

    log.info("label_classify_done", owner_id=owner_id, assigned=len(assigned))
    return {"assigned": assigned, "count": len(assigned)}
