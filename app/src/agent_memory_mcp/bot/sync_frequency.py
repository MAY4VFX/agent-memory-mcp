"""Sync frequency in the bot: shown on source and folder cards, editable there.

domains.sync_frequency_minutes / domain_groups.sync_frequency_minutes existed
(MCP add_source, folder import) but the bot never showed or changed them.
A change reschedules right away from the last sync, so «every 15 min» doesn't
wait out the old hourly slot.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from aiogram import F, Router
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db import queries_groups as gq
from agent_memory_mcp.db.engine import async_engine

router = Router()

# 15 min floor: 3 syncs run at once across all users' sources, so a lower
# cadence on many chats just backs up the queue.
FREQ_CHOICES = (15, 30, 60, 180, 360, 720, 1440)
DEFAULT_FREQ = 60


def fmt_freq(minutes: int | None) -> str:
    m = minutes or DEFAULT_FREQ
    if m < 60:
        return f"{m} min"
    if m % 60 == 0:
        return f"{m // 60} h"
    return f"{m // 60} h {m % 60} min"


def fmt_next(next_sync_at: datetime | None, now: datetime | None = None) -> str:
    if next_sync_at is None:
        return "—"
    now = now or datetime.now(timezone.utc)
    secs = (next_sync_at - now).total_seconds()
    if secs <= 60:
        return "soon"
    mins = int(secs // 60)
    if mins < 60:
        return f"in {mins} min"
    return f"in {mins // 60} h {mins % 60} min" if mins % 60 else f"in {mins // 60} h"


def folder_freq(group: dict, members: list[dict]) -> str:
    """The folder's cadence; «mixed» if members were changed one by one."""
    values = {m.get("sync_frequency_minutes") or DEFAULT_FREQ for m in members}
    if len(values) > 1:
        return "mixed"
    if values:
        return fmt_freq(values.pop())
    return fmt_freq(group.get("sync_frequency_minutes"))


def picker_kb(set_prefix: str, target_id: str, current: int | None, back_cb: str) -> InlineKeyboardMarkup:
    def btn(m: int) -> InlineKeyboardButton:
        mark = "✅ " if m == (current or DEFAULT_FREQ) else ""
        return InlineKeyboardButton(text=f"{mark}{fmt_freq(m)}", callback_data=f"{set_prefix}:{target_id}:{m}")

    rows = [[btn(m) for m in FREQ_CHOICES[:4]], [btn(m) for m in FREQ_CHOICES[4:]]]
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _parse_minutes(raw: str) -> int | None:
    try:
        m = int(raw)
    except ValueError:
        return None
    return m if m in FREQ_CHOICES else None


# ---------------- Source ----------------

@router.callback_query(F.data.startswith("sfreq:"))
async def cb_source_freq(callback: CallbackQuery) -> None:
    source_id = callback.data.split(":", 1)[1]
    domain = await db_q.get_domain(async_engine, UUID(source_id))
    if not domain or domain["owner_id"] != callback.from_user.id:
        await callback.answer("Source not found.", show_alert=True)
        return
    name = f"@{domain['channel_username']}" if domain.get("channel_username") else domain.get("display_name", "?")
    await callback.message.edit_text(
        f"⏱ <b>{name}</b> — sync every:",
        reply_markup=picker_kb("sfreq_set", source_id, domain.get("sync_frequency_minutes"), f"src:view:{source_id}"),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("sfreq_set:"))
async def cb_source_freq_set(callback: CallbackQuery) -> None:
    from agent_memory_mcp.bot.handlers.forum import _render_source_view

    _, source_id, raw = callback.data.split(":")
    minutes = _parse_minutes(raw)
    domain = await db_q.get_domain(async_engine, UUID(source_id))
    if minutes is None or not domain or domain["owner_id"] != callback.from_user.id:
        await callback.answer("Source not found.", show_alert=True)
        return
    await db_q.update_domain(async_engine, UUID(source_id), sync_frequency_minutes=minutes)
    await db_q.reschedule_from_last_sync(async_engine, [UUID(source_id)])
    await _render_source_view(callback, source_id)


# ---------------- Folder ----------------

@router.callback_query(F.data.startswith("ffreq:"))
async def cb_folder_freq(callback: CallbackQuery) -> None:
    group_id = callback.data.split(":", 1)[1]
    group = await gq.get_group(async_engine, UUID(group_id))
    if not group or group["owner_id"] != callback.from_user.id:
        await callback.answer("Folder not found.", show_alert=True)
        return
    members = await gq.get_group_domains(async_engine, UUID(group_id))
    current = folder_freq(group, members)
    current_min = None if current == "mixed" else (group.get("sync_frequency_minutes") or (members[0].get("sync_frequency_minutes") if members else None))
    await callback.message.edit_text(
        f"⏱ <b>{group['name']}</b> — sync every (all {len(members)} chats):",
        reply_markup=picker_kb("ffreq_set", group_id, current_min, f"src:folder:{group_id}"),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ffreq_set:"))
async def cb_folder_freq_set(callback: CallbackQuery) -> None:
    from agent_memory_mcp.bot.handlers.forum import _render_folder

    _, group_id, raw = callback.data.split(":")
    minutes = _parse_minutes(raw)
    group = await gq.get_group(async_engine, UUID(group_id))
    if minutes is None or not group or group["owner_id"] != callback.from_user.id:
        await callback.answer("Folder not found.", show_alert=True)
        return
    await gq.set_group_sync_frequency(async_engine, UUID(group_id), minutes, apply_to_members=True)
    members = await gq.get_group_domains(async_engine, UUID(group_id))
    await db_q.reschedule_from_last_sync(async_engine, [m["id"] for m in members])
    await _render_folder(callback, group_id)
