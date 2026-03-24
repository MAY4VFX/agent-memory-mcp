"""Bot native topics mode — topics in private chat with bot.

Bot uses native forum topics (Bot API Dec 2025):
- Messages without thread_id (General/default) → inline keyboard (management)
- Messages in a topic thread → agent pipeline (memory chat dialog)

Enable via @BotFather → bot settings → Topics.
No group/supergroup needed — topics work in private chat.
"""

from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

import structlog

from agent_memory_mcp.config import settings
from agent_memory_mcp.db.engine import async_engine
from agent_memory_mcp.memory_api.auth import create_api_key_for_user, get_api_key_by_hash

log = structlog.get_logger(__name__)

router = Router()


def _main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔑 API Keys", callback_data="menu:keys"),
            InlineKeyboardButton(text="💰 Баланс", callback_data="menu:balance"),
        ],
        [
            InlineKeyboardButton(text="📡 Источники", callback_data="menu:sources"),
            InlineKeyboardButton(text="📊 Usage", callback_data="menu:usage"),
        ],
        [
            InlineKeyboardButton(text="💎 Пополнить", callback_data="menu:topup"),
            InlineKeyboardButton(text="❓ Помощь", callback_data="menu:help"),
        ],
    ])


@router.message(Command("start"))
async def cmd_start(message: Message):
    """Handle /start — create API key, show main menu."""
    user_id = message.from_user.id

    # Check if user already has an API key
    from sqlalchemy import text
    async with async_engine.begin() as conn:
        row = await conn.execute(
            text("SELECT key_prefix, credits_balance FROM api_keys WHERE telegram_id = :tid AND is_active = true LIMIT 1"),
            {"tid": user_id},
        )
        existing = row.mappings().first()

    if existing:
        key_prefix = existing["key_prefix"]
        balance = existing["credits_balance"]
    else:
        # Create first API key with welcome bonus
        full_key, rec = await create_api_key_for_user(
            async_engine, user_id,
            name="default",
            bonus_credits=settings.welcome_bonus_credits,
        )
        key_prefix = rec["key_prefix"]
        balance = rec["credits_balance"]
        # Show full key once
        await message.answer(
            f"🔑 <b>Твой API key (покажу один раз!):</b>\n"
            f"<code>{full_key}</code>\n\n"
            f"Сохрани его — потом покажу только префикс.",
        )

    text = (
        "🧠 <b>Agent Memory MCP</b>\n\n"
        f"API key: <code>{key_prefix}...</code>\n"
        f"Баланс: <b>{balance}</b> кредитов\n\n"
        "Напиши в новом треде, чтобы начать диалог с агентом.\n"
        "Управление — через кнопки ниже ⬇️"
    )
    await message.answer(text, reply_markup=_main_menu_kb())


@router.callback_query(F.data == "menu:main")
async def cb_main_menu(callback: CallbackQuery):
    """Return to main menu."""
    user_id = callback.from_user.id
    from sqlalchemy import text as sql_text
    async with async_engine.begin() as conn:
        row = await conn.execute(
            sql_text("SELECT key_prefix, credits_balance FROM api_keys WHERE telegram_id = :tid AND is_active = true LIMIT 1"),
            {"tid": user_id},
        )
        rec = row.mappings().first()

    key_prefix = rec["key_prefix"] if rec else "—"
    balance = rec["credits_balance"] if rec else 0

    text = (
        "🧠 <b>Agent Memory MCP</b>\n\n"
        f"API key: <code>{key_prefix}...</code>\n"
        f"Баланс: <b>{balance}</b> кредитов"
    )
    await callback.message.edit_text(text, reply_markup=_main_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:sources")
async def cb_sources(callback: CallbackQuery):
    """Show connected sources (read-only, delete only)."""
    from agent_memory_mcp.memory_api.service import list_sources
    sources = await list_sources(callback.from_user.id)

    if not sources:
        text = (
            "📡 <b>Источники</b>\n\n"
            "Пока нет подключённых источников.\n\n"
            "💡 Чтобы добавить, напиши в чате:\n"
            "«Подключи канал @example за 3 месяца»"
        )
    else:
        lines = ["📡 <b>Подключённые источники:</b>\n"]
        for i, s in enumerate(sources, 1):
            name = f"@{s['channel_username']}" if s.get("channel_username") else s.get("display_name", "?")
            count = s.get("message_count", 0)
            depth = s.get("sync_depth") or "?"
            lines.append(f"{i}. {name} — {count} сообщений")
            lines.append(f"   Глубина: {depth}")
        lines.append("\n💡 Добавить: напиши в чате «Подключи канал @...»")
        text = "\n".join(lines)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")],
    ])
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data == "menu:help")
async def cb_help(callback: CallbackQuery):
    """Show help / integration guide."""
    text = (
        "❓ <b>Как подключить Agent Memory MCP</b>\n\n"
        "📎 <b>MCP (Claude Desktop / Cursor):</b>\n"
        "Добавь в конфиг MCP:\n"
        "<code>pip install agent-memory-mcp</code>\n\n"
        "Или hosted URL:\n"
        "<code>https://YOUR_SERVER/mcp</code>\n\n"
        "📎 <b>REST API:</b>\n"
        "<code>Authorization: Bearer YOUR_API_KEY</code>\n"
        "<code>POST /api/v1/memory/search</code>\n\n"
        "📎 <b>MCP tools:</b>\n"
        "• search_memory — поиск по памяти\n"
        "• get_digest — дайджест за период\n"
        "• get_decisions — решения и задачи\n"
        "• add_source — подключить канал\n"
        "• list_sources — список источников\n"
        "• get_agent_context — контекст для агента"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")],
    ])
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()
