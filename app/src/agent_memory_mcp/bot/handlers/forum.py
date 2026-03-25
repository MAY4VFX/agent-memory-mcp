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
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
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


def main_menu_kb(telegram_connected: bool = False) -> ReplyKeyboardMarkup:
    """Persistent reply keyboard. Shows 📱 Connect or 📡 Sources depending on auth."""
    source_btn = KeyboardButton(text="📡 Sources") if telegram_connected else KeyboardButton(text="📱 Connect Telegram")
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💰 Balance"), source_btn],
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

    is_connected = bool(tg_session)

    status_parts = []
    if is_connected:
        status_parts.append("📱 Telegram: <b>connected</b> ✅")
    else:
        status_parts.append(
            "📱 Telegram: <b>not connected</b>\n"
            "Press <b>📱 Connect Telegram</b> below to get started."
        )

    await message.answer(
        "🧠 <b>Agent Memory MCP</b>\n\n"
        "Long-term memory for Telegram-native AI agents.\n\n"
        "We turn your chats, channels, and folders into structured "
        "persistent memory that any AI agent can use.\n\n"
        + "\n".join(status_parts) + "\n\n"
        "Use the buttons below to manage your account ⬇️",
        reply_markup=main_menu_kb(telegram_connected=is_connected),
    )

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
    """Show sources: folders + standalone channels."""
    await _show_sources(message, message.from_user.id)


async def _show_sources(target, user_id: int, edit: bool = False):
    """Top-level: folders as buttons + standalone channels."""
    from agent_memory_mcp.db import queries as db_q
    from agent_memory_mcp.db import queries_groups as gq

    domains = await db_q.list_domains(async_engine, user_id)
    groups = await gq.list_groups(async_engine, user_id)

    # Find which domains belong to groups
    grouped_ids: set = set()
    for g in groups:
        members = await gq.get_group_domains(async_engine, g["id"])
        grouped_ids.update(m["id"] for m in members)

    # Standalone = pinned or not in any group
    standalone = [d for d in domains if d["id"] not in grouped_ids]

    if not domains and not groups:
        text_msg = (
            "📡 <b>Sources</b>\n\n"
            "No sources connected yet.\n"
            "Use MCP: <code>add_source(handle=\"@channel\")</code>"
        )
        await _send_or_edit(target, text_msg, edit=edit)
        return

    buttons = []

    # Folder buttons
    for g in groups:
        members = await gq.get_group_domains(async_engine, g["id"])
        total_msgs = sum(m.get("message_count", 0) for m in members)
        label = f"📁 {g['name']} ({len(members)} ch, {total_msgs} msgs)"
        buttons.append([InlineKeyboardButton(
            text=label,
            callback_data=f"src:folder:{g['id']}",
        )])

    # Standalone channel buttons (rows of 2)
    row = []
    for d in standalone:
        name = f"@{d['channel_username']}" if d.get("channel_username") else d.get("display_name", "?")
        count = d.get("message_count", 0)
        row.append(InlineKeyboardButton(
            text=f"📡 {name} ({count})",
            callback_data=f"src:view:{d['id']}",
        ))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    text_msg = f"📡 <b>Sources</b> ({len(domains)} channels)"
    await _send_or_edit(target, text_msg, kb, edit)


@router.callback_query(F.data.startswith("src:folder:"))
async def cb_source_folder(callback: CallbackQuery):
    """View channels inside a folder."""
    group_id = callback.data.split(":", 2)[2]
    user_id = callback.from_user.id

    from agent_memory_mcp.db import queries_groups as gq
    from uuid import UUID

    group = await gq.get_group(async_engine, UUID(group_id))
    if not group or group["owner_id"] != user_id:
        await callback.answer("Folder not found.", show_alert=True)
        return

    members = await gq.get_group_domains(async_engine, UUID(group_id))

    buttons = []
    row = []
    for d in members:
        name = f"@{d['channel_username']}" if d.get("channel_username") else d.get("display_name", "?")
        count = d.get("message_count", 0)
        row.append(InlineKeyboardButton(
            text=f"📡 {name} ({count})",
            callback_data=f"src:view:{d['id']}",
        ))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([InlineKeyboardButton(text="🗑 Delete Folder", callback_data=f"src:delfolder:{group_id}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Back", callback_data="src:list")])

    total_msgs = sum(m.get("message_count", 0) for m in members)
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    text_msg = (
        f"📁 <b>{group['name']}</b>\n\n"
        f"Channels: {len(members)}\n"
        f"Total messages: {total_msgs}"
    )

    await callback.message.edit_text(text_msg, reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("src:view:"))
async def cb_source_view(callback: CallbackQuery):
    """View a single source — details + delete."""
    source_id = callback.data.split(":", 2)[2]
    user_id = callback.from_user.id

    from agent_memory_mcp.db import queries as db_q
    from uuid import UUID

    domain = await db_q.get_domain(async_engine, UUID(source_id))
    if not domain or domain["owner_id"] != user_id:
        await callback.answer("Source not found.", show_alert=True)
        return

    name = f"@{domain['channel_username']}" if domain.get("channel_username") else domain.get("display_name", "?")
    synced = domain["last_synced_at"].strftime("%d.%m %H:%M") if domain.get("last_synced_at") else "not yet"

    text_msg = (
        f"📡 <b>{name}</b>\n\n"
        f"Messages: <b>{domain.get('message_count', 0)}</b>\n"
        f"Entities: {domain.get('entity_count', 0)}\n"
        f"Depth: {domain.get('sync_depth', '?')}\n"
        f"Last synced: {synced}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Delete", callback_data=f"src:delete:{source_id}")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="src:list")],
    ])

    await callback.message.edit_text(text_msg, reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data == "src:list")
async def cb_source_list(callback: CallbackQuery):
    """Back to top-level source list."""
    await _show_sources(callback.message, callback.from_user.id, edit=True)
    await callback.answer()


@router.callback_query(F.data.startswith("src:delete:"))
async def cb_source_delete(callback: CallbackQuery):
    """Delete a single source."""
    source_id = callback.data.split(":", 2)[2]
    user_id = callback.from_user.id

    from agent_memory_mcp.db import queries as db_q
    from uuid import UUID

    domain = await db_q.get_domain(async_engine, UUID(source_id))
    if not domain or domain["owner_id"] != user_id:
        await callback.answer("Source not found.", show_alert=True)
        return

    await db_q.delete_domain(async_engine, UUID(source_id))
    await callback.answer(f"@{domain.get('channel_username', '?')} deleted.")
    await _show_sources(callback.message, user_id, edit=True)


@router.callback_query(F.data.startswith("src:delfolder:"))
async def cb_source_delete_folder(callback: CallbackQuery):
    """Delete all channels in a folder + the group itself."""
    group_id = callback.data.split(":", 2)[2]
    user_id = callback.from_user.id

    from agent_memory_mcp.db import queries as db_q
    from agent_memory_mcp.db import queries_groups as gq
    from uuid import UUID

    group = await gq.get_group(async_engine, UUID(group_id))
    if not group or group["owner_id"] != user_id:
        await callback.answer("Folder not found.", show_alert=True)
        return

    members = await gq.get_group_domains(async_engine, UUID(group_id))
    for m in members:
        await db_q.delete_domain(async_engine, m["id"])
    await gq.delete_group(async_engine, UUID(group_id))

    await callback.answer(f"Folder '{group['name']}' deleted ({len(members)} channels).")
    await _show_sources(callback.message, user_id, edit=True)


async def _send_or_edit(target, text: str, kb=None, edit: bool = False):
    """Helper: edit message if possible, otherwise send new."""
    if edit and hasattr(target, "edit_text"):
        try:
            await target.edit_text(text, reply_markup=kb)
            return
        except Exception:
            pass
    await target.answer(text, reply_markup=kb)


class KeyStates(StatesGroup):
    waiting_name = State()


@router.message(F.text == "🔑 API Keys")
async def btn_keys(message: Message):
    """Show API keys — just buttons with names."""
    await _show_keys(message, message.from_user.id)


async def _get_active_keys(user_id: int) -> list[dict]:
    from sqlalchemy import text as sa_text
    async with async_engine.begin() as conn:
        rows = await conn.execute(
            sa_text("""
                SELECT id, key_prefix, name, credits_balance, last_used_at
                FROM api_keys WHERE telegram_id = :tid AND is_active = true
                ORDER BY created_at
            """),
            {"tid": user_id},
        )
        return [dict(r) for r in rows.mappings().all()]


async def _show_keys(target, user_id: int, edit: bool = False):
    """Key list: buttons with key names + create button."""
    keys = await _get_active_keys(user_id)

    buttons = []
    # Key buttons in rows of 2
    row = []
    for k in keys:
        row.append(InlineKeyboardButton(
            text=f"🔑 {k['name']}",
            callback_data=f"key:view:{k['id']}",
        ))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    if len(keys) < 20:
        buttons.append([InlineKeyboardButton(text="➕ Create Key", callback_data="key:create")])

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    text_msg = f"🔑 <b>API Keys</b> ({len(keys)})" if keys else "🔑 <b>API Keys</b>\n\nNo keys yet."

    if edit and hasattr(target, "edit_text"):
        try:
            await target.edit_text(text_msg, reply_markup=kb)
        except Exception:
            await target.answer(text_msg, reply_markup=kb)
    else:
        await target.answer(text_msg, reply_markup=kb)


@router.callback_query(F.data.startswith("key:view:"))
async def cb_key_view(callback: CallbackQuery):
    """View a single key — details + delete button."""
    key_id = callback.data.split(":", 2)[2]
    user_id = callback.from_user.id

    from sqlalchemy import text as sa_text
    async with async_engine.begin() as conn:
        row = await conn.execute(
            sa_text("""
                SELECT id, key_prefix, name, credits_balance, total_credits_used,
                       created_at, last_used_at, telegram_id
                FROM api_keys WHERE id = :kid AND is_active = true
            """),
            {"kid": key_id},
        )
        k = row.mappings().first()

    if not k or k["telegram_id"] != user_id:
        await callback.answer("Key not found.", show_alert=True)
        return

    created = k["created_at"].strftime("%d.%m.%Y") if k["created_at"] else "—"
    last_used = k["last_used_at"].strftime("%d.%m %H:%M") if k.get("last_used_at") else "never"

    text_msg = (
        f"🔑 <b>{k['name']}</b>\n\n"
        f"Prefix: <code>{k['key_prefix']}...</code>\n"
        f"Balance: <b>{k['credits_balance']}</b> credits\n"
        f"Spent: {k['total_credits_used']} credits\n"
        f"Created: {created}\n"
        f"Last used: {last_used}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Delete Key", callback_data=f"key:delete:{k['id']}")],
        [InlineKeyboardButton(text="⬅️ Back", callback_data="key:list")],
    ])

    await callback.message.edit_text(text_msg, reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data == "key:list")
async def cb_key_list(callback: CallbackQuery):
    """Back to key list."""
    await _show_keys(callback.message, callback.from_user.id, edit=True)
    await callback.answer()


@router.callback_query(F.data == "key:create")
async def cb_key_create(callback: CallbackQuery, state: FSMContext):
    """Ask for key name."""
    keys = await _get_active_keys(callback.from_user.id)
    if len(keys) >= 20:
        await callback.answer("Maximum 20 keys.", show_alert=True)
        return

    await callback.message.edit_text(
        "🔑 <b>New API Key</b>\n\nEnter a name for this key:",
    )
    await state.set_state(KeyStates.waiting_name)
    await callback.answer()


@router.message(KeyStates.waiting_name, F.text)
async def on_key_name(message: Message, state: FSMContext):
    """Create key with the given name."""
    name = message.text.strip()[:32]
    if not name:
        await message.answer("Name can't be empty. Try again:")
        return

    await state.clear()
    user_id = message.from_user.id

    full_key, rec = await create_api_key_for_user(async_engine, user_id, name=name)

    key_msg = await message.answer(
        f"🔑 <b>{name}</b> created!\n\n"
        f"<code>{full_key}</code>\n\n"
        "⚠️ <b>Copy now!</b> This message deletes in 60 seconds.",
    )

    # Show updated key list
    await _show_keys(message, user_id)

    import asyncio
    await asyncio.sleep(60)
    try:
        await key_msg.delete()
    except Exception:
        pass


@router.callback_query(F.data.startswith("key:delete:"))
async def cb_key_delete(callback: CallbackQuery):
    """Deactivate an API key."""
    key_id = callback.data.split(":", 2)[2]
    user_id = callback.from_user.id

    from sqlalchemy import text as sa_text
    async with async_engine.begin() as conn:
        row = await conn.execute(
            sa_text("SELECT key_prefix, name, telegram_id FROM api_keys WHERE id = :kid"),
            {"kid": key_id},
        )
        key = row.mappings().first()
        if not key or key["telegram_id"] != user_id:
            await callback.answer("Key not found.", show_alert=True)
            return

        await conn.execute(
            sa_text("UPDATE api_keys SET is_active = false WHERE id = :kid"),
            {"kid": key_id},
        )

    await callback.answer(f"'{key['name']}' deleted.")
    await _show_keys(callback.message, user_id, edit=True)


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
