"""Handlers for digest configuration and preview."""

from __future__ import annotations

import structlog
from aiogram import F, Router
from aiogram.types import CallbackQuery, Message

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, LinkPreviewOptions

from agent_memory_mcp.bot.keyboards import digest_hour_kb, digest_scope_kb, digest_settings_kb
from agent_memory_mcp.config import is_allowed_user
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db import queries_digest as dq
from agent_memory_mcp.db import queries_groups as gq
from agent_memory_mcp.db.engine import async_engine

log = structlog.get_logger(__name__)
router = Router()


# ------------------------------------------------------------------ Menu entry

@router.message(F.text == "\U0001f4f0 Дайджест")
async def digest_menu(message: Message) -> None:
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        return
    config = await dq.get_user_digest_config(async_engine, message.from_user.id)
    if config:
        config = await _enrich_scope_name(config)
    await message.answer(
        "Настройки дайджеста:",
        reply_markup=digest_settings_kb(config),
    )


@router.message(F.text.startswith("/digest"))
async def digest_command(message: Message) -> None:
    """Handle /digest and /digest preview."""
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        return
    text = message.text.strip()
    if "preview" in text.lower():
        await _run_preview(message)
        return
    config = await dq.get_user_digest_config(async_engine, message.from_user.id)
    if config:
        config = await _enrich_scope_name(config)
    await message.answer(
        "Настройки дайджеста:",
        reply_markup=digest_settings_kb(config),
    )


# ------------------------------------------------------------------ Enable/disable

@router.callback_query(F.data == "digest:enable")
async def enable_digest(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    config = await dq.create_digest_config(async_engine, callback.from_user.id)
    config = await _enrich_scope_name(config)
    await callback.message.edit_text(
        f"\u2705 Дайджест включён. Отправка в {config['send_hour_utc']}:00 UTC ежедневно.",
        reply_markup=digest_settings_kb(config),
    )


@router.callback_query(F.data == "digest:disable")
async def disable_digest(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    config = await dq.get_user_digest_config(async_engine, callback.from_user.id)
    if config:
        await dq.update_digest_config(async_engine, config["id"], is_active=False)
    await callback.message.edit_text(
        "\u23f8 Дайджест отключён.",
        reply_markup=digest_settings_kb(None),
    )


# ------------------------------------------------------------------ Hour selection

@router.callback_query(F.data == "digest:hour")
async def choose_hour(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    await callback.message.edit_text(
        "Выберите час отправки (UTC):", reply_markup=digest_hour_kb(),
    )


@router.callback_query(F.data.startswith("digest:set_hour:"))
async def set_hour(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    hour = int(callback.data.split(":")[2])
    config = await dq.get_user_digest_config(async_engine, callback.from_user.id)
    if config:
        await dq.update_digest_config(async_engine, config["id"], send_hour_utc=hour)
        config["send_hour_utc"] = hour
        config = await _enrich_scope_name(config)
    await callback.answer(f"\u2705 Час: {hour}:00 UTC")
    await callback.message.edit_text(
        f"Дайджест будет отправляться в {hour}:00 UTC.",
        reply_markup=digest_settings_kb(config),
    )


# ------------------------------------------------------------------ Preview

@router.callback_query(F.data == "digest:preview")
async def preview_digest(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    await _run_preview_cb(callback)


async def _run_preview(message: Message) -> None:
    """Generate and send a digest preview."""
    status = await message.answer("\u23f3 Генерирую превью дайджеста...")
    try:
        from agent_memory_mcp.digest.runner import run_digest

        config = await dq.get_user_digest_config(async_engine, message.from_user.id)
        if not config:
            config = await dq.create_digest_config(async_engine, message.from_user.id)

        # Use the bot instance from the message context
        bot = message.bot
        await run_digest(config, async_engine, bot, preview=True)
        await status.delete()
    except Exception:
        log.exception("digest_preview_failed")
        await status.edit_text("Ошибка при генерации дайджеста.")


async def _run_preview_cb(callback: CallbackQuery) -> None:
    """Generate digest preview from callback."""
    await callback.message.edit_text("\u23f3 Генерирую превью дайджеста...")
    try:
        from agent_memory_mcp.digest.runner import run_digest

        config = await dq.get_user_digest_config(async_engine, callback.from_user.id)
        if not config:
            config = await dq.create_digest_config(async_engine, callback.from_user.id)

        bot = callback.bot
        await run_digest(config, async_engine, bot, preview=True)
        await callback.message.delete()
    except Exception:
        log.exception("digest_preview_failed")
        await callback.message.edit_text("Ошибка при генерации дайджеста.")


# ------------------------------------------------------------------ Scope selection

async def _enrich_scope_name(config: dict) -> dict:
    """Add scope_name to config for display."""
    scope_type = config.get("scope_type", "all")
    scope_id = config.get("scope_id")
    if scope_type == "domain" and scope_id:
        domain = await db_q.get_domain(async_engine, scope_id)
        config["scope_name"] = domain["display_name"] if domain else "?"
    elif scope_type == "group" and scope_id:
        group = await gq.get_group(async_engine, scope_id)
        config["scope_name"] = group["name"] if group else "?"
    return config


@router.callback_query(F.data == "digest:scope")
async def choose_scope(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    user_id = callback.from_user.id
    config = await dq.get_user_digest_config(async_engine, user_id)
    domains = await db_q.list_domains(async_engine, user_id)
    groups = await gq.list_groups(async_engine, user_id)
    for g in groups:
        g["member_count"] = len(await gq.get_group_domain_ids(async_engine, g["id"]))
    grouped_ids = await gq.get_exclusively_grouped_domain_ids(async_engine, user_id)
    orphan_domains = [d for d in domains if d["id"] not in grouped_ids]
    scope_type = config.get("scope_type", "all") if config else "all"
    scope_id = str(config["scope_id"]) if config and config.get("scope_id") else ""
    await callback.message.edit_text(
        "Выберите область для дайджеста:",
        reply_markup=digest_scope_kb(orphan_domains, groups, scope_type, scope_id),
    )


@router.callback_query(F.data == "dscope:all")
async def set_scope_all(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer("\u2705 Все источники")
    config = await dq.get_user_digest_config(async_engine, callback.from_user.id)
    if config:
        config = await dq.update_digest_config(
            async_engine, config["id"], scope_type="all", scope_id=None,
        )
        config = await _enrich_scope_name(config)
    await callback.message.edit_text(
        "Дайджест: все источники.",
        reply_markup=digest_settings_kb(config),
    )


@router.callback_query(F.data.startswith("dscope:d:"))
async def set_scope_domain(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    from uuid import UUID
    domain_id = callback.data.split(":", 2)[2]
    domain = await db_q.get_domain(async_engine, UUID(domain_id))
    name = domain["display_name"] if domain else "?"
    await callback.answer(f"\u2705 {name}")
    config = await dq.get_user_digest_config(async_engine, callback.from_user.id)
    if config:
        config = await dq.update_digest_config(
            async_engine, config["id"], scope_type="domain", scope_id=UUID(domain_id),
        )
        config["scope_name"] = name
    await callback.message.edit_text(
        f"Дайджест по источнику: {name}.",
        reply_markup=digest_settings_kb(config),
    )


@router.callback_query(F.data.startswith("dscope:g:"))
async def set_scope_group(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    from uuid import UUID
    group_id = callback.data.split(":", 2)[2]
    group = await gq.get_group(async_engine, UUID(group_id))
    name = group["name"] if group else "?"
    await callback.answer(f"\u2705 {name}")
    config = await dq.get_user_digest_config(async_engine, callback.from_user.id)
    if config:
        config = await dq.update_digest_config(
            async_engine, config["id"], scope_type="group", scope_id=UUID(group_id),
        )
        config["scope_name"] = name
    await callback.message.edit_text(
        f"Дайджест по списку: {name}.",
        reply_markup=digest_settings_kb(config),
    )


@router.callback_query(F.data.startswith("pg:ds:"))
async def page_digest_scope(callback: CallbackQuery) -> None:
    """Paginate digest scope picker."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    page = int(callback.data.split(":")[2])
    user_id = callback.from_user.id
    config = await dq.get_user_digest_config(async_engine, user_id)
    domains = await db_q.list_domains(async_engine, user_id)
    groups = await gq.list_groups(async_engine, user_id)
    for g in groups:
        g["member_count"] = len(await gq.get_group_domain_ids(async_engine, g["id"]))
    grouped_ids = await gq.get_exclusively_grouped_domain_ids(async_engine, user_id)
    orphan_domains = [d for d in domains if d["id"] not in grouped_ids]
    scope_type = config.get("scope_type", "all") if config else "all"
    scope_id = str(config["scope_id"]) if config and config.get("scope_id") else ""
    try:
        await callback.message.edit_text(
            "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u043e\u0431\u043b\u0430\u0441\u0442\u044c \u0434\u043b\u044f \u0434\u0430\u0439\u0434\u0436\u0435\u0441\u0442\u0430:",
            reply_markup=digest_scope_kb(orphan_domains, groups, scope_type, scope_id, page),
        )
    except Exception:
        pass


@router.callback_query(F.data == "digest:scope_back")
async def scope_back(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    config = await dq.get_user_digest_config(async_engine, callback.from_user.id)
    if config:
        config = await _enrich_scope_name(config)
    await callback.message.edit_text(
        "Настройки дайджеста:",
        reply_markup=digest_settings_kb(config),
    )


# ------------------------------------------------------------------ Settings entry

@router.callback_query(F.data == "settings:digest")
async def settings_digest(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    config = await dq.get_user_digest_config(async_engine, callback.from_user.id)
    if config:
        config = await _enrich_scope_name(config)
    await callback.message.edit_text(
        "Настройки дайджеста:",
        reply_markup=digest_settings_kb(config),
    )


# ------------------------------------------------------------------ Previous digest

@router.callback_query(F.data.startswith("digest:prev:"))
async def show_prev_digest(callback: CallbackQuery) -> None:
    """Show a previous digest run by ID."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    from uuid import UUID
    run_id = callback.data.split(":", 2)[2]
    run = await dq.get_digest_run(async_engine, UUID(run_id))
    if not run or not run.get("digest_text"):
        await callback.message.answer("Дайджест не найден.")
        return

    # Find even older digest for chaining
    prev_run = await dq.get_last_completed_run(async_engine, run["config_id"])
    # prev_run is the latest — we need the one BEFORE this run
    if prev_run and str(prev_run["id"]) == run_id:
        # This IS the latest, find the one before it
        prev_run = await _get_run_before(run)

    prev_kb = None
    if prev_run and prev_run.get("completed_at") and str(prev_run["id"]) != run_id:
        prev_date = prev_run["completed_at"].strftime("%d.%m")
        prev_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text=f"\U0001f4cb Предыдущий дайджест ({prev_date})",
                callback_data=f"digest:prev:{prev_run['id']}",
            ),
        ]])

    date_str = run["completed_at"].strftime("%d.%m.%Y %H:%M") if run.get("completed_at") else ""
    header = f"\U0001f4c5 <b>Дайджест от {date_str}</b>\n\n"

    text = header + run["digest_text"]
    # Truncate if too long
    if len(text) > 4096:
        text = text[:4090] + "..."

    await callback.message.answer(
        text,
        link_preview_options=LinkPreviewOptions(is_disabled=True),
        reply_markup=prev_kb,
    )


async def _get_run_before(current_run: dict) -> dict | None:
    """Get the completed run before the given one."""
    from sqlalchemy import select
    from agent_memory_mcp.db.tables import digest_runs

    stmt = (
        select(digest_runs)
        .where(
            digest_runs.c.config_id == current_run["config_id"],
            digest_runs.c.status == "completed",
            digest_runs.c.completed_at < current_run["completed_at"],
        )
        .order_by(digest_runs.c.completed_at.desc())
        .limit(1)
    )
    async with async_engine.begin() as conn:
        row = (await conn.execute(stmt)).mappings().one_or_none()
        return dict(row) if row else None


# ------------------------------------------------------------------ Back

@router.callback_query(F.data == "digest:back")
async def digest_back(callback: CallbackQuery) -> None:
    await callback.answer()
    await callback.message.delete()
