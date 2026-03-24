"""Bot native topics mode — topics in private chat with bot.

Bot uses native forum topics (Bot API Dec 2025):
- Messages without thread_id (General/default) → reply keyboard + handlers
- Messages in a topic thread → agent pipeline (memory chat dialog)

Enable via @BotFather → bot settings → Topics.
No group/supergroup needed — topics work in private chat.
"""

from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import (
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

import structlog

from agent_memory_mcp.config import settings
from agent_memory_mcp.db.engine import async_engine
from agent_memory_mcp.memory_api.auth import create_api_key_for_user

log = structlog.get_logger(__name__)

router = Router()


def main_menu_kb() -> ReplyKeyboardMarkup:
    """Persistent reply keyboard at the bottom of the screen."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💰 Баланс"), KeyboardButton(text="📡 Источники")],
            [KeyboardButton(text="🔑 API Keys"), KeyboardButton(text="💎 Пополнить")],
            [KeyboardButton(text="📊 Usage"), KeyboardButton(text="❓ Помощь")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


@router.message(Command("start"))
async def cmd_start(message: Message):
    """Handle /start — create API key, show main menu with reply keyboard."""
    user_id = message.from_user.id

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
        full_key, rec = await create_api_key_for_user(
            async_engine, user_id,
            name="default",
            bonus_credits=settings.welcome_bonus_credits,
        )
        key_prefix = rec["key_prefix"]
        balance = rec["credits_balance"]
        await message.answer(
            f"🔑 <b>Твой API key (покажу один раз!):</b>\n"
            f"<code>{full_key}</code>\n\n"
            f"Сохрани его — потом покажу только префикс.",
        )

    await message.answer(
        "🧠 <b>Agent Memory MCP</b>\n\n"
        f"API key: <code>{key_prefix}...</code>\n"
        f"Баланс: <b>{balance}</b> кредитов\n\n"
        "Создай новый тред, чтобы поговорить с агентом.\n"
        "Управление — кнопки внизу ⬇️",
        reply_markup=main_menu_kb(),
    )


# --- Reply keyboard button handlers ---

@router.message(F.text == "💰 Баланс")
async def btn_balance(message: Message):
    """Show balance and recent transactions."""
    user_id = message.from_user.id
    from sqlalchemy import text
    async with async_engine.begin() as conn:
        row = await conn.execute(
            text("SELECT id, credits_balance, total_credits_used FROM api_keys WHERE telegram_id = :tid AND is_active = true LIMIT 1"),
            {"tid": user_id},
        )
        key = row.mappings().first()
        if not key:
            await message.answer("Нет API ключа. Нажми /start")
            return

        rows = await conn.execute(
            text("""
                SELECT amount, type, endpoint, created_at
                FROM credit_transactions WHERE api_key_id = :kid
                ORDER BY created_at DESC LIMIT 5
            """),
            {"kid": key["id"]},
        )
        txs = rows.mappings().all()

    lines = [
        f"💰 <b>Баланс: {key['credits_balance']} кредитов</b>",
        f"Потрачено всего: {key['total_credits_used']}",
        "",
        "<b>Последние операции:</b>",
    ]
    for tx in txs:
        sign = "+" if tx["amount"] > 0 else ""
        ep = tx.get("endpoint") or tx["type"]
        dt = tx["created_at"].strftime("%d.%m %H:%M") if tx["created_at"] else ""
        lines.append(f" {sign}{tx['amount']}  {ep}  {dt}")
    if not txs:
        lines.append(" Пока нет операций")

    await message.answer("\n".join(lines))


@router.message(F.text == "📡 Источники")
async def btn_sources(message: Message):
    """Show connected sources."""
    from agent_memory_mcp.memory_api.service import list_sources
    sources = await list_sources(message.from_user.id)

    if not sources:
        await message.answer(
            "📡 <b>Источники</b>\n\n"
            "Пока нет подключённых источников.\n\n"
            "💡 Чтобы добавить, напиши в новом треде:\n"
            "«Подключи канал @example за 3 месяца»"
        )
        return

    lines = ["📡 <b>Подключённые источники:</b>\n"]
    for i, s in enumerate(sources, 1):
        name = f"@{s['channel_username']}" if s.get("channel_username") else s.get("display_name", "?")
        count = s.get("message_count", 0)
        depth = s.get("sync_depth") or "?"
        lines.append(f"{i}. {name} — {count} сообщений")
        lines.append(f"   Глубина: {depth}")
    lines.append("\n💡 Добавить: напиши в треде «Подключи канал @...»")
    await message.answer("\n".join(lines))


@router.message(F.text == "🔑 API Keys")
async def btn_keys(message: Message):
    """Show API keys."""
    user_id = message.from_user.id
    from sqlalchemy import text
    async with async_engine.begin() as conn:
        rows = await conn.execute(
            text("""
                SELECT key_prefix, name, credits_balance, is_active, created_at, last_used_at
                FROM api_keys WHERE telegram_id = :tid ORDER BY created_at
            """),
            {"tid": user_id},
        )
        keys = rows.mappings().all()

    if not keys:
        await message.answer("Нет ключей. Нажми /start")
        return

    lines = ["🔑 <b>Твои API ключи:</b>\n"]
    for i, k in enumerate(keys, 1):
        status = "✅" if k["is_active"] else "❌"
        last = k["last_used_at"].strftime("%d.%m %H:%M") if k.get("last_used_at") else "—"
        lines.append(f"{i}. <code>{k['key_prefix']}...</code> ({k['name']}) {status}")
        lines.append(f"   Баланс: {k['credits_balance']} • Последнее: {last}")
    lines.append("\nНовый ключ: /newkey")
    await message.answer("\n".join(lines))


@router.message(Command("newkey"))
async def cmd_newkey(message: Message):
    """Create a new API key."""
    user_id = message.from_user.id
    from sqlalchemy import text
    async with async_engine.begin() as conn:
        row = await conn.execute(
            text("SELECT COUNT(*) FROM api_keys WHERE telegram_id = :tid"),
            {"tid": user_id},
        )
        count = row.scalar()
    if count >= 5:
        await message.answer("Максимум 5 ключей на аккаунт.")
        return

    full_key, rec = await create_api_key_for_user(async_engine, user_id, name=f"key-{count + 1}")
    await message.answer(
        f"🔑 <b>Новый ключ создан:</b>\n\n"
        f"<code>{full_key}</code>\n\n"
        "⚠️ Сохрани — больше не покажу!"
    )


@router.message(F.text == "📊 Usage")
async def btn_usage(message: Message):
    """Show usage statistics."""
    user_id = message.from_user.id
    from sqlalchemy import text
    async with async_engine.begin() as conn:
        row = await conn.execute(
            text("SELECT id FROM api_keys WHERE telegram_id = :tid AND is_active = true LIMIT 1"),
            {"tid": user_id},
        )
        key = row.mappings().first()
        if not key:
            await message.answer("Нет API ключа.")
            return

        rows = await conn.execute(
            text("""
                SELECT endpoint, COUNT(*) as cnt, SUM(ABS(amount)) as total_credits
                FROM credit_transactions
                WHERE api_key_id = :kid AND type = 'usage'
                  AND created_at > now() - interval '24 hours'
                GROUP BY endpoint ORDER BY total_credits DESC
            """),
            {"kid": key["id"]},
        )
        stats = rows.mappings().all()

    lines = ["📊 <b>Статистика за сегодня:</b>\n"]
    total_req = 0
    total_cr = 0
    for s in stats:
        ep = s["endpoint"] or "?"
        lines.append(f"  {ep}: {s['cnt']} запросов ({s['total_credits']} кр.)")
        total_req += s["cnt"]
        total_cr += s["total_credits"]
    if not stats:
        lines.append("  Нет запросов за сегодня")
    else:
        lines.append(f"\n  Итого: {total_req} запросов ({total_cr} кр.)")
    await message.answer("\n".join(lines))


@router.message(F.text == "❓ Помощь")
async def btn_help(message: Message):
    """Show help / integration guide."""
    await message.answer(
        "❓ <b>Как подключить Agent Memory MCP</b>\n\n"
        "📎 <b>MCP (Claude Desktop / Cursor):</b>\n"
        "<code>pip install agent-memory-mcp</code>\n\n"
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
