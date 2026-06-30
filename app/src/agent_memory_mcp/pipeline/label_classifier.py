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
from agent_memory_mcp.db import queries_labels as db_ql
from agent_memory_mcp.db.engine import async_engine
from agent_memory_mcp.llm.client import llm_call_json

log = structlog.get_logger(__name__)

_SYSTEM = """Ты классифицируешь рабочие чаты пользователя по проектам.

На входе список чатов: id, название и несколько последних сообщений.
Часто название чата УЖЕ содержит название проекта — это основной сигнал,
сообщения уточняют.

Классифицируй КАЖДЫЙ чат — ни один не оставляй без метки:
- Рабочий чат → is_work=true, label = название проекта. Сгруппируй так, чтобы
  один проект назывался ОДИНАКОВО во всех чатах (нормализуй: "Зенит"/"Zenith"
  → один проект).
- Нерабочий/личный → is_work=false, label = короткая осмысленная категория
  (имя человека для ЛС, тема для канала, или "Личное"). Тоже НЕ пусто.

КОСВЕННЫЙ ПРИЗНАК для личных диалогов (ЛС): если указано, что собеседник также
участвует в рабочих групповых чатах какого-то проекта — это рабочий контакт, и
ЛС с ним почти наверняка РАБОЧИЙ и относится к ТОМУ ЖЕ проекту (is_work=true,
label = тот же проект). Используй это вместо того, чтобы по умолчанию считать
1:1 диалоги личными.

Верни строго JSON:
{"assignments": [
  {"domain_id": "<id>", "label": "<непусто>", "is_work": true|false,
   "confidence": 0.0-1.0, "source": "title|content"}
]}
source = "title" если ясно из названия, "content" если потребовались сообщения."""


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

    # Cross-reference: which domains each person (sender_id) is active in. A DM's
    # partner has user id == the DM domain's channel_id; if that id also sends in
    # a group chat, the DM is a work contact for that chat's project.
    from collections import defaultdict
    domain_by_id = {d["id"]: d for d in domains}
    sender_domains: dict = defaultdict(set)
    for sid, did in await db_ql.get_sender_domains(async_engine, list(domain_by_id)):
        sender_domains[sid].add(did)

    # One windowed query for all chats' recent snippets (avoids N+1 per domain).
    recent_by_domain = await db_q.get_recent_messages_for_domains(
        async_engine, list(domain_by_id), days=60, per_domain_limit=snippets_per_chat
    )

    # Build the prompt: title + a few short recent snippets per chat.
    blocks = []
    for d in domains:
        recent = recent_by_domain.get(d["id"], [])
        snips = [
            (m.get("content") or "").strip().replace("\n", " ")[:snippet_chars]
            for m in recent
            if (m.get("content") or "").strip()
        ]
        snip_text = " | ".join(snips) if snips else "(нет текстовых сообщений)"
        block = f'domain_id: {d["id"]}\nназвание: {_title(d)}\nпримеры: {snip_text}'
        # Indirect signal for DMs: where else this person participates.
        if d.get("peer_type") == "user":
            co = sender_domains.get(d.get("channel_id"), set()) - {d["id"]}
            co_names = [_title(domain_by_id[x]) for x in co if x in domain_by_id]
            if co_names:
                block += "\nсобеседник также пишет в: " + ", ".join(co_names[:6])
        blocks.append(block)
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
        # Fallback: chat title becomes a work project (low confidence).
        assignments = [
            {"domain_id": str(d["id"]), "label": _title(d), "is_work": True,
             "confidence": 0.3, "source": "title"}
            for d in domains
        ]

    valid_ids = {str(d["id"]): d["id"] for d in domains}
    assigned: list[dict] = []
    for a in assignments:
        did = valid_ids.get(str(a.get("domain_id")))
        name = (a.get("label") or a.get("project_name") or "").strip()
        if not did or not name:
            continue  # unmatched id only — every chat should carry a label
        conf = a.get("confidence")
        try:
            conf = float(conf) if conf is not None else None
        except (TypeError, ValueError):
            conf = None
        is_work = bool(a.get("is_work", True))
        label_type = "project" if is_work else "personal"
        source = a.get("source") if a.get("source") in ("title", "content") else None
        label_id = await db_ql.upsert_label(async_engine, owner_id, label_type, name)
        await db_ql.set_domain_project_label(async_engine, did, label_id, conf, source)
        assigned.append({"domain_id": str(did), "label": name, "is_work": is_work, "confidence": conf})

    log.info("label_classify_done", owner_id=owner_id, assigned=len(assigned))
    return {"assigned": assigned, "count": len(assigned)}
