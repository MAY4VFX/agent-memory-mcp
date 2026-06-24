"""Handlers for source lists, TG folder import, and scope switching."""

from __future__ import annotations

from uuid import UUID

import structlog
from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, ReplyKeyboardRemove

from agent_memory_mcp.bot.keyboards import (
    add_sources_kb,
    domain_list_kb,
    folder_list_kb,
    folder_multi_kb,
    group_list_kb,
    list_detail_kb,
    main_menu_kb,
    manage_kb,
    period_kb,
    request_chat_kb,
    source_picker_kb,
    sources_hub_kb,
)
from agent_memory_mcp.bot.states import AddChannelStates, AddSourceStates, GroupStates
from agent_memory_mcp.memory_api import service
from agent_memory_mcp.config import is_allowed_user
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db import queries_groups as gq
from agent_memory_mcp.db.engine import async_engine

log = structlog.get_logger(__name__)
router = Router()


async def _safe_edit(message, text: str, **kwargs) -> None:
    """Edit message, ignoring 'message is not modified' errors."""
    try:
        await message.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise


# ------------------------------------------------------------------ Hub helpers

async def _build_hub(user_id: int) -> tuple[str, ...]:
    """Load data and return (text, keyboard) for sources hub."""
    user = await db_q.get_user(async_engine, user_id)
    domains = await db_q.list_domains(async_engine, user_id)
    groups = await gq.list_groups(async_engine, user_id)

    for g in groups:
        g["member_count"] = len(await gq.get_group_domain_ids(async_engine, g["id"]))

    # Orphan domains = not in any list
    grouped_ids = await gq.get_exclusively_grouped_domain_ids(async_engine, user_id)
    orphan_domains = [d for d in domains if d["id"] not in grouped_ids]

    active_scope = user.get("active_scope_type", "domain") if user else "domain"
    if active_scope == "domain":
        active_scope_id = str(user.get("active_domain_id", "")) if user else ""
    elif active_scope == "group":
        active_scope_id = str(user.get("active_group_id", "")) if user else ""
    else:
        active_scope_id = ""

    scope = await gq.resolve_scope(async_engine, user) if user else None
    scope_text = scope.label if scope else "\u043d\u0435 \u0432\u044b\u0431\u0440\u0430\u043d\u0430"

    text = (
        f"\U0001f4cc \u041f\u043e\u0438\u0441\u043a: {scope_text}\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a, \u0441\u043f\u0438\u0441\u043e\u043a \u0438\u043b\u0438 \u0432\u0441\u0435:"
    )
    kb = sources_hub_kb(orphan_domains, groups, active_scope, active_scope_id, len(domains))
    return text, kb


# ------------------------------------------------------------------ Hub navigation

@router.callback_query(F.data == "noop")
async def noop_handler(callback: CallbackQuery) -> None:
    await callback.answer()


@router.callback_query(F.data == "hub:back")
async def hub_back(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    text, kb = await _build_hub(callback.from_user.id)
    await _safe_edit(callback.message, text, reply_markup=kb)


@router.callback_query(F.data == "hub:sources")
async def hub_sources(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    domains = await db_q.list_domains(async_engine, callback.from_user.id)
    await _safe_edit(
        callback.message,
        "\U0001f4cb \u0412\u0430\u0448\u0438 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438:", reply_markup=domain_list_kb(domains),
    )


@router.callback_query(F.data == "hub:add_sources")
async def hub_add_sources(callback: CallbackQuery) -> None:
    """Show sub-menu explaining how to add sources."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    await _safe_edit(
        callback.message,
        "\u2795 <b>\u0414\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438</b>\n\n"
        "\u041e\u0442\u043f\u0440\u0430\u0432\u044c\u0442\u0435 \u0441\u0441\u044b\u043b\u043a\u0443 \u043d\u0430 \u043a\u0430\u043d\u0430\u043b \u2014 \u043e\u0434\u043d\u0443 \u0438\u043b\u0438 \u043d\u0435\u0441\u043a\u043e\u043b\u044c\u043a\u043e "
        "(\u043f\u043e \u043e\u0434\u043d\u043e\u0439 \u043d\u0430 \u0441\u0442\u0440\u043e\u043a\u0443).\n\n"
        "\u041f\u0440\u0438\u043c\u0435\u0440\u044b:\n"
        "<code>@channel_name</code>\n"
        "<code>https://t.me/channel_name</code>\n\n"
        "\u0415\u0441\u043b\u0438 \u043e\u0442\u043f\u0440\u0430\u0432\u0438\u0442\u044c \u043d\u0435\u0441\u043a\u043e\u043b\u044c\u043a\u043e \u2014 "
        "\u043f\u0440\u0435\u0434\u043b\u043e\u0436\u0438\u043c \u043e\u0431\u044a\u0435\u0434\u0438\u043d\u0438\u0442\u044c \u0432 \u0441\u043f\u0438\u0441\u043e\u043a.\n\n"
        "\u0418\u043b\u0438 \u0438\u043c\u043f\u043e\u0440\u0442\u0438\u0440\u0443\u0439\u0442\u0435 \u0446\u0435\u043b\u0443\u044e \u043f\u0430\u043f\u043a\u0443 \u0438\u0437 Telegram:",
        reply_markup=add_sources_kb(),
    )


# ---------------- Native Telegram chat picker ----------------

@router.callback_query(F.data == "src:native")
async def src_native(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    await callback.message.answer(
        "Tap a button below and pick a chat in Telegram's native selector "
        "(channel or group):",
        reply_markup=request_chat_kb(),
    )


def _bare_chat_id(marked: int) -> int:
    """Bot API marked id → bare entity id (matches domains.channel_id)."""
    s = str(marked)
    if s.startswith("-100"):
        return int(s[4:])
    if s.startswith("-"):
        return int(s[1:])
    return marked


@router.message(F.chat_shared)
async def on_chat_shared(message: Message) -> None:
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        return
    bare = _bare_chat_id(message.chat_shared.chat_id)
    await message.answer("Adding…", reply_markup=ReplyKeyboardRemove())
    res = await service.add_source(
        message.from_user.id, str(bare), source_type="dialog", sync_range="3m"
    )
    await message.answer(res.get("message") or f"Status: {res.get('status')}")


# ---------------- Add by link / @username / id ----------------

@router.callback_query(F.data == "src:bylink")
async def src_bylink(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    await state.set_state(AddSourceStates.waiting_input)
    await _safe_edit(
        callback.message,
        "Send a link, @username, invite link, or numeric chat ID "
        "(one per line):",
    )


@router.message(AddSourceStates.waiting_input)
async def on_source_input(message: Message, state: FSMContext) -> None:
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        return
    await state.clear()
    lines = [l.strip() for l in (message.text or "").splitlines() if l.strip()]
    if not lines:
        await message.answer("Empty.")
        return
    out = []
    for h in lines:
        st = "dialog" if h.lstrip("-").isdigit() else "channel"
        res = await service.add_source(message.from_user.id, h, source_type=st, sync_range="3m")
        out.append(f"• {h}: {res.get('status')}")
    await message.answer("\n".join(out))


# ---------------- Multi-folder import ----------------

@router.callback_query(F.data == "hub:folders_multi")
async def folders_multi(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    folders = await _user_folders(callback.from_user.id)
    if not folders:
        await _safe_edit(callback.message, "No Telegram folders found.")
        return
    await state.update_data(fmulti_folders=folders, fmulti_selected=[])
    await _safe_edit(
        callback.message,
        "Select folders to import (one or several), then «Import selected»:",
        reply_markup=folder_multi_kb(folders, set()),
    )


@router.callback_query(F.data.startswith("fmulti:"))
async def folders_multi_toggle(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    fid = int(callback.data.split(":")[1])
    data = await state.get_data()
    folders = data.get("fmulti_folders") or await _user_folders(callback.from_user.id)
    selected = set(data.get("fmulti_selected") or [])
    selected.symmetric_difference_update({fid})
    await state.update_data(fmulti_folders=folders, fmulti_selected=list(selected))
    await _safe_edit(
        callback.message,
        "Select folders to import (one or several), then «Import selected»:",
        reply_markup=folder_multi_kb(folders, selected),
    )


@router.callback_query(F.data == "fmulti_go")
async def folders_multi_go(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    data = await state.get_data()
    folders = {f["id"]: f for f in (data.get("fmulti_folders") or [])}
    selected = set(data.get("fmulti_selected") or [])
    await state.clear()
    if not selected:
        await _safe_edit(callback.message, "Nothing selected.")
        return
    await _safe_edit(callback.message, "Importing selected folders…")
    out = []
    for fid in selected:
        f = folders.get(fid)
        if not f:
            continue
        res = await service.add_source(
            callback.from_user.id, f["title"], source_type="folder", sync_range="3m"
        )
        out.append(f"📁 {f['title']}: {res.get('status')}")
    await callback.message.answer("Done:\n" + "\n".join(out))


@router.callback_query(F.data.in_({"hub:manage", "hub:lists"}))
async def hub_manage(callback: CallbackQuery) -> None:
    """Unified manage view: individual sources + lists."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    user_id = callback.from_user.id
    domains = await db_q.list_domains(async_engine, user_id)
    groups = await gq.list_groups(async_engine, user_id)
    for g in groups:
        g["member_count"] = len(await gq.get_group_domain_ids(async_engine, g["id"]))
    # Orphan domains = not in any list
    grouped_ids = await gq.get_exclusively_grouped_domain_ids(async_engine, user_id)
    orphan_domains = [d for d in domains if d["id"] not in grouped_ids]
    await _safe_edit(
        callback.message,
        "\u270f\ufe0f <b>\u0420\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435</b>\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u0438\u043b\u0438 \u0441\u043f\u0438\u0441\u043e\u043a \u0434\u043b\u044f \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438:",
        reply_markup=manage_kb(orphan_domains, groups),
    )


async def _user_folders(user_id: int) -> list[dict]:
    """Folders from the user's OWN Telethon session (pool), not the global
    collector — the global one has no session in multi-tenant mode."""
    from agent_memory_mcp.collector.pool import collector_pool
    if not collector_pool:
        return []
    uc = await collector_pool.get_collector(user_id)
    if not uc:
        return []
    return await uc.get_folders()


@router.callback_query(F.data == "hub:folders")
async def hub_folders(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    folders = await _user_folders(callback.from_user.id)
    if not folders:
        await _safe_edit(
            callback.message,
            "\u041d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e TG-\u043f\u0430\u043f\u043e\u043a \u0441 \u043a\u0430\u043d\u0430\u043b\u0430\u043c\u0438.",
        )
        return
    await _safe_edit(
        callback.message,
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u043f\u0430\u043f\u043a\u0443 \u0434\u043b\u044f \u0438\u043c\u043f\u043e\u0440\u0442\u0430:", reply_markup=folder_list_kb(folders),
    )


# ------------------------------------------------------------------ Folder import

@router.callback_query(F.data == "groups:folders")
async def show_folders(callback: CallbackQuery) -> None:
    """Legacy callback — redirect to hub:folders."""
    await hub_folders(callback)


@router.callback_query(F.data.startswith("folder_import:"))
async def import_folder(
    callback: CallbackQuery, state: FSMContext,
) -> None:
    """Store folder data in FSM and ask for sync period."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()

    folder_id = int(callback.data.split(":")[1])
    folders = await _user_folders(callback.from_user.id)
    folder = next((f for f in folders if f["id"] == folder_id), None)
    if not folder:
        await callback.message.edit_text("\u041f\u0430\u043f\u043a\u0430 \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u0430.")
        return

    peers = folder["peers"]
    supported = [p for p in peers if p.get("supported", True)]
    skipped = len(peers) - len(supported)

    # Store only ingestable peers; later creation runs after period/freq.
    await state.update_data(
        folder_import={
            "folder_id": folder_id,
            "folder_title": folder["title"],
            "peers": supported,
        }
    )
    note = f"Folder \"{folder['title']}\": {len(supported)} chat(s) to import"
    if skipped:
        note += f"\n\u26a0\ufe0f skipped {skipped} unsupported"
    if not supported:
        await callback.message.edit_text(note + "\n\nNothing to add.")
        await state.clear()
        return
    await callback.message.edit_text(
        note + "\n\nChoose sync depth:",
        reply_markup=period_kb(),
    )
    await state.set_state(AddChannelStates.choosing_period)


# ------------------------------------------------------------------ List CRUD

@router.callback_query(F.data == "groups:create")
async def start_create_group(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    await callback.message.edit_text("\u0412\u0432\u0435\u0434\u0438\u0442\u0435 \u043d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u043d\u043e\u0432\u043e\u0433\u043e \u0441\u043f\u0438\u0441\u043a\u0430:")
    await state.set_state(GroupStates.entering_name)


@router.message(GroupStates.entering_name)
async def create_group_name(message: Message, state: FSMContext) -> None:
    name = message.text.strip()[:255]
    if not name:
        await message.answer("\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u043d\u0435 \u043c\u043e\u0436\u0435\u0442 \u0431\u044b\u0442\u044c \u043f\u0443\u0441\u0442\u044b\u043c.")
        return
    try:
        group = await gq.create_group(async_engine, message.from_user.id, name)
        await state.clear()
        await message.answer(
            f"\u2705 \u0421\u043f\u0438\u0441\u043e\u043a \"{name}\" \u0441\u043e\u0437\u0434\u0430\u043d.\n"
            "\u0414\u043e\u0431\u0430\u0432\u044c\u0442\u0435 \u0432 \u043d\u0435\u0433\u043e \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438.",
            reply_markup=list_detail_kb(str(group["id"]), []),
        )
    except Exception:
        await state.clear()
        await message.answer("\u0421\u043f\u0438\u0441\u043e\u043a \u0441 \u0442\u0430\u043a\u0438\u043c \u0438\u043c\u0435\u043d\u0435\u043c \u0443\u0436\u0435 \u0441\u0443\u0449\u0435\u0441\u0442\u0432\u0443\u0435\u0442.")


@router.callback_query(F.data == "groups:list")
async def show_groups_list(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    groups = await gq.list_groups(async_engine, callback.from_user.id)
    for g in groups:
        g["member_count"] = len(await gq.get_group_domain_ids(async_engine, g["id"]))
    await callback.message.edit_text(
        "\U0001f4c1 \u0412\u0430\u0448\u0438 \u0441\u043f\u0438\u0441\u043a\u0438:", reply_markup=group_list_kb(groups),
    )


@router.callback_query(F.data.startswith("group:view:"))
async def view_group(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    group_id = callback.data.split(":")[2]
    group = await gq.get_group(async_engine, group_id)
    if not group:
        await callback.message.edit_text("\u0421\u043f\u0438\u0441\u043e\u043a \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d.")
        return
    await state.update_data(current_list_id=group_id)
    group_domains = await gq.get_group_domains(async_engine, group_id)
    text = f"{group['emoji']} <b>{group['name']}</b>\n\n\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438 ({len(group_domains)}):"
    if not group_domains:
        text += "\n  (\u043f\u0443\u0441\u0442\u043e)"
    await callback.message.edit_text(
        text, reply_markup=list_detail_kb(group_id, group_domains),
    )


@router.callback_query(F.data.startswith("group:delete:"))
async def delete_group(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    group_id = callback.data.split(":")[2]

    # Delete all domains that belong to this list
    group_domain_ids = await gq.get_group_domain_ids(async_engine, group_id)
    for did in group_domain_ids:
        try:
            await db_q.delete_domain(async_engine, did)
        except Exception:
            log.exception("delete_group_domain_failed", domain_id=str(did))

    await gq.delete_group(async_engine, group_id)

    # Return to hub
    text, kb = await _build_hub(callback.from_user.id)
    await callback.message.edit_text(
        "\u2705 \u0421\u043f\u0438\u0441\u043e\u043a \u0438 \u0435\u0433\u043e \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438 \u0443\u0434\u0430\u043b\u0435\u043d\u044b.",
        reply_markup=kb,
    )


# ------------------------------------------------------------------ Source picker (add/remove from list)

@router.callback_query(F.data.startswith("grp_rm:"))
async def remove_source_from_list(callback: CallbackQuery, state: FSMContext) -> None:
    """Remove a source from a list and refresh."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    domain_id = callback.data.split(":")[1]
    data = await state.get_data()
    group_id = data.get("current_list_id")
    # Fallback: extract group_id from sibling buttons (group:delete:{gid})
    if not group_id and callback.message and callback.message.reply_markup:
        for row in callback.message.reply_markup.inline_keyboard:
            for btn in row:
                if btn.callback_data and btn.callback_data.startswith("group:delete:"):
                    group_id = btn.callback_data.split(":")[2]
                    break
            if group_id:
                break
    if not group_id:
        return
    await gq.remove_domain_from_group(async_engine, UUID(group_id), UUID(domain_id))
    # Refresh list detail
    group = await gq.get_group(async_engine, group_id)
    if not group:
        return
    group_domains = await gq.get_group_domains(async_engine, group_id)
    text = f"{group['emoji']} <b>{group['name']}</b>\n\n\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438 ({len(group_domains)}):"
    if not group_domains:
        text += "\n  (\u043f\u0443\u0441\u0442\u043e)"
    await callback.message.edit_text(
        text, reply_markup=list_detail_kb(group_id, group_domains),
    )


@router.callback_query(F.data.startswith("grp_pick:"))
async def show_source_picker(callback: CallbackQuery, state: FSMContext) -> None:
    """Show source picker for a list."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    group_id = callback.data.split(":")[1]
    group = await gq.get_group(async_engine, group_id)
    if not group:
        return
    await state.update_data(current_list_id=group_id)
    all_domains = await db_q.list_domains(async_engine, callback.from_user.id)
    group_dids = {str(d) for d in await gq.get_group_domain_ids(async_engine, UUID(group_id))}
    await callback.message.edit_text(
        f"\u0414\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u0432 \"{group['name']}\":",
        reply_markup=source_picker_kb(group_id, all_domains, group_dids),
    )


@router.callback_query(F.data.startswith("grp_toggle:"))
async def toggle_source_in_list(callback: CallbackQuery, state: FSMContext) -> None:
    """Toggle a source in/out of a list."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    domain_id = callback.data.split(":")[1]
    data = await state.get_data()
    group_id = data.get("current_list_id")
    if not group_id:
        return
    group_dids = {str(d) for d in await gq.get_group_domain_ids(async_engine, UUID(group_id))}
    if domain_id in group_dids:
        await gq.remove_domain_from_group(async_engine, UUID(group_id), UUID(domain_id))
    else:
        await gq.add_domains_to_group(async_engine, UUID(group_id), [UUID(domain_id)])
    # Refresh picker
    group = await gq.get_group(async_engine, group_id)
    all_domains = await db_q.list_domains(async_engine, callback.from_user.id)
    group_dids = {str(d) for d in await gq.get_group_domain_ids(async_engine, UUID(group_id))}
    await callback.message.edit_text(
        f"\u0414\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u0432 \"{group['name']}\":",
        reply_markup=source_picker_kb(group_id, all_domains, group_dids),
    )


@router.callback_query(F.data.startswith("grp_pick_done:"))
async def picker_done(callback: CallbackQuery, state: FSMContext) -> None:
    """Done picking sources -> back to list detail."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    group_id = callback.data.split(":")[1]
    await state.update_data(current_list_id=group_id)
    group = await gq.get_group(async_engine, group_id)
    if not group:
        return
    group_domains = await gq.get_group_domains(async_engine, group_id)
    text = f"{group['emoji']} <b>{group['name']}</b>\n\n\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438 ({len(group_domains)}):"
    if not group_domains:
        text += "\n  (\u043f\u0443\u0441\u0442\u043e)"
    await callback.message.edit_text(
        text, reply_markup=list_detail_kb(group_id, group_domains),
    )


# ------------------------------------------------------------------ Scope switching

@router.callback_query(F.data.startswith("scope:channel:"))
async def set_scope_channel(callback: CallbackQuery) -> None:
    """Set scope to a single source."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    did = callback.data.split(":")[2]
    domain = await db_q.get_domain(async_engine, UUID(did))
    if not domain:
        await callback.answer("\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d")
        return
    await db_q.update_user_active_domain(async_engine, callback.from_user.id, UUID(did))
    await gq.update_user_scope(async_engine, callback.from_user.id, "domain")
    await callback.answer(f"\u2705 \u041f\u043e\u0438\u0441\u043a: {domain['display_name']}")
    await callback.message.delete()
    await callback.message.answer(
        f"\u2705 \u041e\u0431\u043b\u0430\u0441\u0442\u044c \u043f\u043e\u0438\u0441\u043a\u0430: {domain['emoji']} {domain['display_name']}",
        reply_markup=main_menu_kb(domain),
    )


@router.callback_query(F.data == "scope:domain")
async def set_scope_domain(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer("\u2705 \u041f\u043e\u0438\u0441\u043a: \u0442\u0435\u043a\u0443\u0449\u0438\u0439 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a")
    await gq.update_user_scope(async_engine, callback.from_user.id, "domain")
    user = await db_q.get_user(async_engine, callback.from_user.id)
    domain = await db_q.get_domain(async_engine, user["active_domain_id"]) if user and user.get("active_domain_id") else None
    await callback.message.delete()
    await callback.message.answer(
        "\u2705 \u041e\u0431\u043b\u0430\u0441\u0442\u044c \u043f\u043e\u0438\u0441\u043a\u0430: \u0442\u0435\u043a\u0443\u0449\u0438\u0439 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a",
        reply_markup=main_menu_kb(domain),
    )


@router.callback_query(F.data.startswith("scope:group:"))
async def set_scope_group(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    group_id = callback.data.split(":")[2]
    group = await gq.get_group(async_engine, group_id)
    if not group:
        await callback.answer("\u0421\u043f\u0438\u0441\u043e\u043a \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d")
        return
    await callback.answer(f"\u2705 \u041f\u043e\u0438\u0441\u043a: {group['name']}")
    await gq.update_user_scope(async_engine, callback.from_user.id, "group", group_id)
    scope = await gq.resolve_scope(async_engine, await db_q.get_user(async_engine, callback.from_user.id))
    await callback.message.delete()
    await callback.message.answer(
        f"\u2705 \u041e\u0431\u043b\u0430\u0441\u0442\u044c \u043f\u043e\u0438\u0441\u043a\u0430: {scope.label}",
        reply_markup=main_menu_kb(scope_label=scope.label),
    )


@router.callback_query(F.data == "scope:all")
async def set_scope_all(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer("\u2705 \u041f\u043e\u0438\u0441\u043a: \u0432\u0441\u0435 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438")
    await gq.update_user_scope(async_engine, callback.from_user.id, "all")
    user = await db_q.get_user(async_engine, callback.from_user.id)
    scope = await gq.resolve_scope(async_engine, user)
    await callback.message.delete()
    await callback.message.answer(
        f"\u2705 \u041e\u0431\u043b\u0430\u0441\u0442\u044c \u043f\u043e\u0438\u0441\u043a\u0430: {scope.label}",
        reply_markup=main_menu_kb(scope_label=scope.label),
    )


# ------------------------------------------------------------------ Pagination

@router.callback_query(F.data.startswith("pg:ld:"))
async def page_list_detail(callback: CallbackQuery, state: FSMContext) -> None:
    """Paginate list detail view."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    # pg:ld:{group_id}:{page}
    parts = callback.data.split(":")
    group_id, page = parts[2], int(parts[3])
    group = await gq.get_group(async_engine, group_id)
    if not group:
        return
    await state.update_data(current_list_id=group_id)
    group_domains = await gq.get_group_domains(async_engine, group_id)
    text = f"{group['emoji']} <b>{group['name']}</b>\n\n\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438 ({len(group_domains)}):"
    if not group_domains:
        text += "\n  (\u043f\u0443\u0441\u0442\u043e)"
    await _safe_edit(
        callback.message, text,
        reply_markup=list_detail_kb(group_id, group_domains, page),
    )


@router.callback_query(F.data.startswith("pg:sp:"))
async def page_source_picker(callback: CallbackQuery, state: FSMContext) -> None:
    """Paginate source picker."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    parts = callback.data.split(":")
    group_id, page = parts[2], int(parts[3])
    group = await gq.get_group(async_engine, group_id)
    if not group:
        return
    await state.update_data(current_list_id=group_id)
    all_domains = await db_q.list_domains(async_engine, callback.from_user.id)
    group_dids = {str(d) for d in await gq.get_group_domain_ids(async_engine, UUID(group_id))}
    await _safe_edit(
        callback.message,
        f"\u0414\u043e\u0431\u0430\u0432\u0438\u0442\u044c \u0432 \"{group['name']}\":",
        reply_markup=source_picker_kb(group_id, all_domains, group_dids, page),
    )


@router.callback_query(F.data.startswith("pg:manage:"))
async def page_manage(callback: CallbackQuery) -> None:
    """Paginate manage view."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    page = int(callback.data.split(":")[2])
    user_id = callback.from_user.id
    domains = await db_q.list_domains(async_engine, user_id)
    groups = await gq.list_groups(async_engine, user_id)
    for g in groups:
        g["member_count"] = len(await gq.get_group_domain_ids(async_engine, g["id"]))
    grouped_ids = await gq.get_exclusively_grouped_domain_ids(async_engine, user_id)
    orphan_domains = [d for d in domains if d["id"] not in grouped_ids]
    await _safe_edit(
        callback.message,
        "\u270f\ufe0f <b>\u0420\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435</b>\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u0438\u043b\u0438 \u0441\u043f\u0438\u0441\u043e\u043a \u0434\u043b\u044f \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438:",
        reply_markup=manage_kb(orphan_domains, groups, page),
    )


@router.callback_query(F.data.startswith("pg:hub:"))
async def page_hub(callback: CallbackQuery) -> None:
    """Paginate sources hub."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    page = int(callback.data.split(":")[2])
    user_id = callback.from_user.id
    user = await db_q.get_user(async_engine, user_id)
    domains = await db_q.list_domains(async_engine, user_id)
    groups = await gq.list_groups(async_engine, user_id)
    for g in groups:
        g["member_count"] = len(await gq.get_group_domain_ids(async_engine, g["id"]))
    grouped_ids = await gq.get_exclusively_grouped_domain_ids(async_engine, user_id)
    orphan_domains = [d for d in domains if d["id"] not in grouped_ids]
    active_scope = user.get("active_scope_type", "domain") if user else "domain"
    if active_scope == "domain":
        active_scope_id = str(user.get("active_domain_id", "")) if user else ""
    elif active_scope == "group":
        active_scope_id = str(user.get("active_group_id", "")) if user else ""
    else:
        active_scope_id = ""
    scope = await gq.resolve_scope(async_engine, user) if user else None
    scope_text = scope.label if scope else "\u043d\u0435 \u0432\u044b\u0431\u0440\u0430\u043d\u0430"
    await _safe_edit(
        callback.message,
        f"\U0001f4cc \u041f\u043e\u0438\u0441\u043a: {scope_text}\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a, \u0441\u043f\u0438\u0441\u043e\u043a \u0438\u043b\u0438 \u0432\u0441\u0435:",
        reply_markup=sources_hub_kb(orphan_domains, groups, active_scope, active_scope_id, len(domains), page),
    )


# ------------------------------------------------------------------ Legacy back

@router.callback_query(F.data.in_({"scope:back", "groups:back"}))
async def scope_back(callback: CallbackQuery) -> None:
    """Legacy back — redirect to hub."""
    await callback.answer()
    try:
        text, kb = await _build_hub(callback.from_user.id)
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        await callback.message.delete()
