"""Callback query handlers for domain management."""

from __future__ import annotations

import re
from uuid import UUID

import structlog
from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from agent_memory_mcp.bot.keyboards import (
    confirm_delete_kb,
    domain_actions_kb,
    domain_edit_kb,
    domain_list_kb,
    edit_depth_kb,
    edit_emoji_kb,
    edit_freq_kb,
    main_menu_kb,
    search_mode_kb,
    settings_kb,
)
from agent_memory_mcp.bot.states import SettingsStates
from agent_memory_mcp.config import is_allowed_user, settings as app_settings
from agent_memory_mcp.db import queries
from agent_memory_mcp.db import queries_groups as gq
from agent_memory_mcp.db.engine import async_engine

log = structlog.get_logger()

router = Router()

_HASHTAG_RE = re.compile(r"#(\w+)", re.UNICODE)

_PERIOD_LABELS: dict[str, str] = {
    "1w": "1 неделя",
    "1m": "1 месяц",
    "3m": "3 месяца",
    "6m": "6 месяцев",
    "1y": "1 год",
    "3y": "3 года",
    "all": "Все сообщения",
}


# ---- Domain view / actions ----


@router.callback_query(F.data.startswith("domain:view:"))
async def on_domain_view(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    domain_id = callback.data.split(":", 2)[2]
    domain = await queries.get_domain(async_engine, UUID(domain_id))
    if not domain:
        await callback.answer("\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d.", show_alert=True)
        return
    # Set as active domain
    await queries.update_user_active_domain(async_engine, callback.from_user.id, UUID(domain_id))

    entities = domain.get("entity_count") or 0
    synced = domain["last_synced_at"]
    synced_text = synced.strftime("%d.%m %H:%M") if synced else "\u043d\u0435\u0442"
    await callback.message.edit_text(
        f"{domain['emoji']} <b>{domain['display_name']}</b>\n"
        f"@{domain['channel_username']}\n\n"
        f"\u0421\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0439: {domain['message_count']}\n"
        f"\u0421\u0443\u0449\u043d\u043e\u0441\u0442\u0435\u0439: {entities}\n"
        f"\u0413\u043b\u0443\u0431\u0438\u043d\u0430: {_PERIOD_LABELS.get(domain['sync_depth'], domain['sync_depth'])}\n"
        f"\u0427\u0430\u0441\u0442\u043e\u0442\u0430: \u043a\u0430\u0436\u0434\u044b\u0435 {domain['sync_frequency_minutes']} \u043c\u0438\u043d\n"
        f"\u041f\u043e\u0441\u043b\u0435\u0434\u043d\u044f\u044f \u0441\u0438\u043d\u0445\u0440\u043e\u043d\u0438\u0437\u0430\u0446\u0438\u044f: {synced_text}",
        reply_markup=domain_actions_kb(domain_id),
    )
    await state.set_state(SettingsStates.editing_domain)
    await state.update_data(current_domain_id=domain_id)
    await callback.answer()


@router.callback_query(F.data == "domain:add")
async def on_domain_add(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    from agent_memory_mcp.bot.keyboards import add_sources_kb

    await state.clear()
    await callback.message.edit_text(
        "➕ <b>Добавить источник</b>\n\nВыберите способ:",
        reply_markup=add_sources_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "domain:back")
async def on_domain_back(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("\u0414\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d.", show_alert=True)
        return
    from agent_memory_mcp.bot.keyboards import manage_kb

    user_id = callback.from_user.id
    domains = await queries.list_domains(async_engine, user_id)
    groups = await gq.list_groups(async_engine, user_id)
    for g in groups:
        g["member_count"] = len(await gq.get_group_domain_ids(async_engine, g["id"]))
    grouped_ids = await gq.get_exclusively_grouped_domain_ids(async_engine, user_id)
    orphan_domains = [d for d in domains if d["id"] not in grouped_ids]
    await callback.message.edit_text(
        "\u270f\ufe0f <b>\u0420\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435</b>\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u0438\u043b\u0438 \u0441\u043f\u0438\u0441\u043e\u043a \u0434\u043b\u044f \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438:",
        reply_markup=manage_kb(orphan_domains, groups),
    )
    await state.set_state(SettingsStates.managing_domains)
    await callback.answer()


# ---- Delete flow ----


@router.callback_query(F.data.startswith("domain:delete:"))
async def on_domain_delete(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    domain_id = callback.data.split(":", 2)[2]
    domain = await queries.get_domain(async_engine, UUID(domain_id))
    if not domain:
        await callback.answer("\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d.", show_alert=True)
        return
    await callback.message.edit_text(
        f"\u0423\u0434\u0430\u043b\u0438\u0442\u044c \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a {domain['emoji']} <b>{domain['display_name']}</b>?\n"
        "\u0412\u0441\u0435 \u0434\u0430\u043d\u043d\u044b\u0435 \u0431\u0443\u0434\u0443\u0442 \u043f\u043e\u0442\u0435\u0440\u044f\u043d\u044b.",
        reply_markup=confirm_delete_kb(domain_id),
    )
    await state.set_state(SettingsStates.confirming_delete)
    await callback.answer()


@router.callback_query(F.data.startswith("confirm_delete:"))
async def on_confirm_delete(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    domain_id = callback.data.split(":", 1)[1]
    await queries.delete_domain(async_engine, UUID(domain_id))
    await callback.message.edit_text("\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u0443\u0434\u0430\u043b\u0451\u043d.")
    await state.clear()
    await callback.answer()


@router.callback_query(F.data == "cancel_delete")
async def on_cancel_delete(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("\u0414\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d.", show_alert=True)
        return
    from agent_memory_mcp.bot.keyboards import manage_kb

    user_id = callback.from_user.id
    domains = await queries.list_domains(async_engine, user_id)
    groups = await gq.list_groups(async_engine, user_id)
    for g in groups:
        g["member_count"] = len(await gq.get_group_domain_ids(async_engine, g["id"]))
    grouped_ids = await gq.get_exclusively_grouped_domain_ids(async_engine, user_id)
    orphan_domains = [d for d in domains if d["id"] not in grouped_ids]
    await callback.message.edit_text(
        "\u270f\ufe0f <b>\u0420\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435</b>\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u0438\u043b\u0438 \u0441\u043f\u0438\u0441\u043e\u043a \u0434\u043b\u044f \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438:",
        reply_markup=manage_kb(orphan_domains, groups),
    )
    await state.set_state(SettingsStates.managing_domains)
    await callback.answer()


# ---- Edit (placeholder for future) ----


@router.callback_query(F.data.startswith("domain:edit:"))
async def on_domain_edit(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("\u0414\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d.", show_alert=True)
        return
    domain_id = callback.data.split(":", 2)[2]
    domain = await queries.get_domain(async_engine, UUID(domain_id))
    if not domain:
        await callback.answer("\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d.", show_alert=True)
        return
    await callback.message.edit_text(
        f"{domain['emoji']} <b>{domain['display_name']}</b>\n\n"
        f"\u0427\u0430\u0441\u0442\u043e\u0442\u0430: \u043a\u0430\u0436\u0434\u044b\u0435 {domain['sync_frequency_minutes']} \u043c\u0438\u043d\n"
        f"\u0413\u043b\u0443\u0431\u0438\u043d\u0430: {_PERIOD_LABELS.get(domain['sync_depth'], domain['sync_depth'])}\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0447\u0442\u043e \u0438\u0437\u043c\u0435\u043d\u0438\u0442\u044c:",
        reply_markup=domain_edit_kb(domain_id, monitoring=bool(domain.get("monitoring"))),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("dmon:"))
async def on_domain_monitoring_toggle(callback: CallbackQuery) -> None:
    """Тумблер «учитывать источник в Observe Layer» (domains.monitoring)."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    domain_id = callback.data.split(":", 1)[1]
    domain = await queries.get_domain(async_engine, UUID(domain_id))
    if not domain or domain["owner_id"] != callback.from_user.id:
        await callback.answer("Источник не найден.", show_alert=True)
        return
    new_state = not bool(domain.get("monitoring"))
    await queries.update_domain(async_engine, UUID(domain_id), monitoring=new_state)
    await callback.message.edit_reply_markup(
        reply_markup=domain_edit_kb(domain_id, monitoring=new_state)
    )
    await callback.answer(
        "Источник учитывается в Observe Layer" if new_state
        else "Источник исключён из Observe Layer"
    )


@router.callback_query(F.data.startswith("dedit:freq:"))
async def on_dedit_freq(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    domain_id = callback.data.split(":")[2]
    await callback.message.edit_text(
        "\u23f1 \u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0447\u0430\u0441\u0442\u043e\u0442\u0443 \u0441\u0438\u043d\u0445\u0440\u043e\u043d\u0438\u0437\u0430\u0446\u0438\u0438:",
        reply_markup=edit_freq_kb(domain_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("efreq:"))
async def on_efreq(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    parts = callback.data.split(":")
    domain_id = parts[1]
    freq = int(parts[2])
    await queries.update_domain(async_engine, UUID(domain_id), sync_frequency_minutes=freq)
    domain = await queries.get_domain(async_engine, UUID(domain_id))
    await callback.answer(f"\u2705 \u0427\u0430\u0441\u0442\u043e\u0442\u0430: {freq} \u043c\u0438\u043d")
    await callback.message.edit_text(
        f"{domain['emoji']} <b>{domain['display_name']}</b>\n\n"
        f"\u0427\u0430\u0441\u0442\u043e\u0442\u0430: \u043a\u0430\u0436\u0434\u044b\u0435 {freq} \u043c\u0438\u043d\n"
        f"\u0413\u043b\u0443\u0431\u0438\u043d\u0430: {_PERIOD_LABELS.get(domain['sync_depth'], domain['sync_depth'])}\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0447\u0442\u043e \u0438\u0437\u043c\u0435\u043d\u0438\u0442\u044c:",
        reply_markup=domain_edit_kb(domain_id),
    )


@router.callback_query(F.data.startswith("dedit:depth:"))
async def on_dedit_depth(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    domain_id = callback.data.split(":")[2]
    await callback.message.edit_text(
        "\U0001f4c5 \u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0433\u043b\u0443\u0431\u0438\u043d\u0443 \u0441\u0438\u043d\u0445\u0440\u043e\u043d\u0438\u0437\u0430\u0446\u0438\u0438:",
        reply_markup=edit_depth_kb(domain_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("edepth:"))
async def on_edepth(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    parts = callback.data.split(":")
    domain_id = parts[1]
    depth_key = parts[2]
    await queries.update_domain(async_engine, UUID(domain_id), sync_depth=depth_key)
    domain = await queries.get_domain(async_engine, UUID(domain_id))
    label = _PERIOD_LABELS.get(depth_key, depth_key)
    await callback.answer(f"\u2705 \u0413\u043b\u0443\u0431\u0438\u043d\u0430: {label}")
    await callback.message.edit_text(
        f"{domain['emoji']} <b>{domain['display_name']}</b>\n\n"
        f"\u0427\u0430\u0441\u0442\u043e\u0442\u0430: \u043a\u0430\u0436\u0434\u044b\u0435 {domain['sync_frequency_minutes']} \u043c\u0438\u043d\n"
        f"\u0413\u043b\u0443\u0431\u0438\u043d\u0430: {label}\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0447\u0442\u043e \u0438\u0437\u043c\u0435\u043d\u0438\u0442\u044c:",
        reply_markup=domain_edit_kb(domain_id),
    )


@router.callback_query(F.data.startswith("dedit:emoji:"))
async def on_dedit_emoji(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    domain_id = callback.data.split(":")[2]
    await callback.message.edit_text(
        "\U0001f600 \u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u044d\u043c\u043e\u0434\u0437\u0438:",
        reply_markup=edit_emoji_kb(domain_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("eemoji:"))
async def on_eemoji(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    parts = callback.data.split(":")
    domain_id = parts[1]
    new_emoji = parts[2]
    await queries.update_domain(async_engine, UUID(domain_id), emoji=new_emoji)
    domain = await queries.get_domain(async_engine, UUID(domain_id))
    await callback.answer(f"\u2705 \u042d\u043c\u043e\u0434\u0437\u0438: {new_emoji}")
    await callback.message.edit_text(
        f"{new_emoji} <b>{domain['display_name']}</b>\n\n"
        f"\u0427\u0430\u0441\u0442\u043e\u0442\u0430: \u043a\u0430\u0436\u0434\u044b\u0435 {domain['sync_frequency_minutes']} \u043c\u0438\u043d\n"
        f"\u0413\u043b\u0443\u0431\u0438\u043d\u0430: {_PERIOD_LABELS.get(domain['sync_depth'], domain['sync_depth'])}\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0447\u0442\u043e \u0438\u0437\u043c\u0435\u043d\u0438\u0442\u044c:",
        reply_markup=domain_edit_kb(domain_id),
    )


# ---- Settings: search mode ----

_VALID_MODES = {"fast", "balanced", "deep"}


@router.callback_query(F.data == "settings:search_mode")
async def on_settings_search_mode(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    user = await queries.get_user(async_engine, callback.from_user.id)
    current = user.get("detail_level", "balanced") if user else "balanced"
    if current not in _VALID_MODES:
        current = "balanced"
    await callback.message.edit_text(
        "Выберите режим поиска:",
        reply_markup=search_mode_kb(current),
    )
    await callback.answer()


@router.callback_query(F.data == "settings:back")
async def on_settings_back(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    is_admin = callback.from_user and callback.from_user.id == app_settings.admin_telegram_id
    await callback.message.edit_text("Настройки:", reply_markup=settings_kb(is_admin=is_admin))
    await callback.answer()


@router.callback_query(F.data.startswith("mode:"))
async def on_mode_select(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    mode = callback.data.split(":", 1)[1]
    if mode not in _VALID_MODES:
        await callback.answer("Неизвестный режим.", show_alert=True)
        return
    await queries.update_user_search_mode(async_engine, callback.from_user.id, mode)
    await callback.message.edit_reply_markup(reply_markup=search_mode_kb(mode))
    await callback.answer()
