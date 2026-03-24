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
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
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
            [KeyboardButton(text="💰 Balance"), KeyboardButton(text="📡 Sources")],
            [KeyboardButton(text="🔑 API Keys"), KeyboardButton(text="💎 Top Up")],
            [KeyboardButton(text="📊 Usage"), KeyboardButton(text="❓ Help")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


@router.message(Command("start"))
async def cmd_start(message: Message):
    """Handle /start — welcome + check Telegram auth + offer API key."""
    user_id = message.from_user.id

    from agent_memory_mcp.db import queries as db_q

    from sqlalchemy import text
    async with async_engine.begin() as conn:
        row = await conn.execute(
            text("SELECT key_prefix, credits_balance FROM api_keys WHERE telegram_id = :tid AND is_active = true LIMIT 1"),
            {"tid": user_id},
        )
        existing_key = row.mappings().first()

    # Check Telegram session
    tg_session = await db_q.get_telegram_session(async_engine, user_id)

    await message.answer(
        "🧠 <b>Agent Memory MCP</b>\n\n"
        "Long-term memory for Telegram-native AI agents.\n\n"
        "We turn your chats, channels, and folders into structured "
        "persistent memory that any AI agent can use.\n\n"
        "<b>What it does:</b>\n"
        "• Memory search — find anything across chat history\n"
        "• Digests — key topics and highlights for any period\n"
        "• Decisions — extract decisions, action items, open questions\n"
        "• Context — build knowledge packages for agent tasks\n\n"
        "Use the buttons below to manage your account ⬇️",
        reply_markup=main_menu_kb(),
    )

    # Step 1: Telegram auth — required for everything
    if not tg_session:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📱 Connect Telegram", callback_data="connect_telegram")],
        ])
        await message.answer(
            "⚡️ <b>First step — connect your Telegram account</b>\n\n"
            "This lets the service read your channels and groups.\n"
            "Without it, sources can't be synced.\n\n"
            "It takes 30 seconds: share contact → enter code.",
            reply_markup=kb,
        )
    else:
        await message.answer("📱 Telegram: <b>connected</b> ✅")

    # Step 2: API key
    if existing_key:
        await message.answer(
            f"🔑 API key: <code>{existing_key['key_prefix']}...</code>\n"
            f"💰 Balance: <b>{existing_key['credits_balance']}</b> credits",
        )
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔑 Create API Key", callback_data="create_first_key")],
        ])
        await message.answer(
            "🔑 <b>Create an API key</b> to connect your agents.\n"
            f"You'll get <b>{settings.welcome_bonus_credits}</b> bonus credits.",
            reply_markup=kb,
        )


@router.callback_query(F.data == "create_first_key")
async def cb_create_first_key(callback: CallbackQuery):
    """Create first API key, show it once, then delete the message."""
    user_id = callback.from_user.id

    from sqlalchemy import text
    async with async_engine.begin() as conn:
        row = await conn.execute(
            text("SELECT id FROM api_keys WHERE telegram_id = :tid LIMIT 1"),
            {"tid": user_id},
        )
        if row.first():
            await callback.answer("Key already created!", show_alert=True)
            return

    full_key, rec = await create_api_key_for_user(
        async_engine, user_id,
        name="default",
        bonus_credits=settings.welcome_bonus_credits,
    )

    key_msg = await callback.message.edit_text(
        f"🔑 <b>Your API key:</b>\n\n"
        f"<code>{full_key}</code>\n\n"
        f"Balance: <b>{rec['credits_balance']}</b> credits\n\n"
        "⚠️ <b>Copy and save it!</b> This message will be deleted in 60 seconds.",
    )
    await callback.answer()

    import asyncio
    await asyncio.sleep(60)
    try:
        await key_msg.delete()
    except Exception:
        pass


# --- Reply keyboard button handlers ---

@router.message(F.text == "💰 Balance")
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
            await message.answer("No API key found. Press /start")
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
        f"💰 <b>Balance: {key['credits_balance']} credits</b>",
        f"Total spent: {key['total_credits_used']}",
        "",
        "<b>Recent transactions:</b>",
    ]
    for tx in txs:
        sign = "+" if tx["amount"] > 0 else ""
        ep = tx.get("endpoint") or tx["type"]
        dt = tx["created_at"].strftime("%d.%m %H:%M") if tx["created_at"] else ""
        lines.append(f" {sign}{tx['amount']}  {ep}  {dt}")
    if not txs:
        lines.append(" No transactions yet")

    await message.answer("\n".join(lines))


@router.message(F.text == "📡 Sources")
async def btn_sources(message: Message):
    """Show connected sources."""
    from agent_memory_mcp.db import queries as db_q
    from agent_memory_mcp.memory_api.service import list_sources

    # Check Telegram auth first
    tg_session = await db_q.get_telegram_session(async_engine, message.from_user.id)
    if not tg_session:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📱 Connect Telegram", callback_data="connect_telegram")],
        ])
        await message.answer(
            "📡 <b>Sources</b>\n\n"
            "⚠️ Telegram not connected.\n"
            "Connect first to add and sync sources.",
            reply_markup=kb,
        )
        return

    sources = await list_sources(message.from_user.id)

    if not sources:
        await message.answer(
            "📡 <b>Sources</b>\n\n"
            "No sources connected yet.\n\n"
            "💡 To add one, write in a new thread:\n"
            "\"Connect channel @example for 3 months\"\n"
            "Or use MCP tool: add_source(handle=\"@example\")"
        )
        return

    lines = ["📡 <b>Connected sources:</b>\n"]
    for i, s in enumerate(sources, 1):
        name = f"@{s['channel_username']}" if s.get("channel_username") else s.get("display_name", "?")
        count = s.get("message_count", 0)
        depth = s.get("sync_depth") or "?"
        synced = s.get("last_synced")
        status = f"synced {synced}" if synced else "pending"
        lines.append(f"{i}. {name} — {count} msgs ({status})")
        lines.append(f"   Depth: {depth}")
    lines.append("\n💡 To add: write in a thread \"Connect channel @...\"")
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
        await message.answer("No keys found. Press /start")
        return

    lines = ["🔑 <b>Your API keys:</b>\n"]
    for i, k in enumerate(keys, 1):
        status = "✅" if k["is_active"] else "❌"
        last = k["last_used_at"].strftime("%d.%m %H:%M") if k.get("last_used_at") else "—"
        lines.append(f"{i}. <code>{k['key_prefix']}...</code> ({k['name']}) {status}")
        lines.append(f"   Balance: {k['credits_balance']} • Last used: {last}")
    lines.append("\nNew key: /newkey")
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
        await message.answer("Maximum 5 keys per account.")
        return

    full_key, rec = await create_api_key_for_user(async_engine, user_id, name=f"key-{count + 1}")
    key_msg = await message.answer(
        f"🔑 <b>New key created:</b>\n\n"
        f"<code>{full_key}</code>\n\n"
        "⚠️ <b>Copy and save!</b> This message will be deleted in 60 seconds."
    )

    import asyncio
    await asyncio.sleep(60)
    try:
        await key_msg.delete()
    except Exception:
        pass


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
            await message.answer("No API key found.")
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

    lines = ["📊 <b>Usage today:</b>\n"]
    total_req = 0
    total_cr = 0
    for s in stats:
        ep = s["endpoint"] or "?"
        lines.append(f"  {ep}: {s['cnt']} requests ({s['total_credits']} cr.)")
        total_req += s["cnt"]
        total_cr += s["total_credits"]
    if not stats:
        lines.append("  No requests today")
    else:
        lines.append(f"\n  Total: {total_req} requests ({total_cr} cr.)")
    await message.answer("\n".join(lines))


@router.message(F.text == "❓ Help")
async def btn_help(message: Message):
    """Show help / integration guide."""
    await message.answer(
        "❓ <b>How to connect Agent Memory MCP</b>\n\n"
        "📎 <b>MCP (Claude Desktop / Cursor):</b>\n"
        "<code>pip install agent-memory-mcp</code>\n\n"
        "📎 <b>REST API:</b>\n"
        "<code>Authorization: Bearer YOUR_API_KEY</code>\n"
        "<code>POST /api/v1/memory/search</code>\n\n"
        "📎 <b>Available MCP tools:</b>\n"
        "• search_memory — search across chat history\n"
        "• get_digest — digest for a time period\n"
        "• get_decisions — decisions and action items\n"
        "• add_source — connect a channel\n"
        "• list_sources — list connected sources\n"
        "• get_agent_context — context package for agent tasks"
    )
