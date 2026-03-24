"""Telegram account authorization via Telethon (multi-user sessions).

Flow:
1. User presses "📱 Connect Telegram" → request_contact button
2. User shares contact → bot gets phone number automatically
3. Telethon sends code to user's Telegram
4. User enters code in chat
5. (Optional) 2FA password
6. Session saved encrypted to DB
"""

from __future__ import annotations

import asyncio

import structlog
from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from telethon import TelegramClient
from telethon.errors import (
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession

from agent_memory_mcp.collector.encryption import hash_phone
from agent_memory_mcp.collector.pool import CollectorPool
from agent_memory_mcp.config import settings
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db.engine import async_engine

log = structlog.get_logger(__name__)

router = Router()


class AuthStates(StatesGroup):
    waiting_contact = State()
    waiting_code = State()
    waiting_2fa = State()


async def _start_auth_flow(message: Message, state: FSMContext) -> None:
    """Common auth flow entry: show contact sharing keyboard."""
    session = await db_q.get_telegram_session(async_engine, message.from_user.id if hasattr(message, 'from_user') else 0)
    if session:
        from agent_memory_mcp.bot.handlers.forum import main_menu_kb
        await message.answer(
            "📱 Telegram already connected! ✅",
            reply_markup=main_menu_kb(telegram_connected=True),
        )
        return

    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Share Contact", request_contact=True)],
            [KeyboardButton(text="❌ Cancel")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await message.answer(
        "📱 <b>Connect your Telegram account</b>\n\n"
        "This lets me read your channels and groups.\n"
        "Press the button below to share your phone number:",
        reply_markup=kb,
    )
    await state.set_state(AuthStates.waiting_contact)


@router.message(F.text == "📱 Connect Telegram")
async def btn_connect_telegram(message: Message, state: FSMContext):
    """Reply keyboard button — start Telegram auth flow."""
    await _start_auth_flow(message, state)


@router.callback_query(F.data == "connect_telegram")
async def cb_connect_telegram(callback: CallbackQuery, state: FSMContext):
    """Inline button — start Telegram auth flow."""
    await _start_auth_flow(callback.message, state)
    await callback.answer()


@router.message(AuthStates.waiting_contact, F.text == "❌ Cancel")
async def cancel_auth(message: Message, state: FSMContext):
    """Cancel auth flow."""
    from agent_memory_mcp.bot.handlers.forum import main_menu_kb
    await state.clear()
    await message.answer("Cancelled.", reply_markup=main_menu_kb())


@router.message(AuthStates.waiting_contact, F.contact)
async def on_contact_shared(message: Message, state: FSMContext):
    """User shared contact — start Telethon auth with phone number."""
    contact = message.contact
    phone = contact.phone_number
    if not phone.startswith("+"):
        phone = f"+{phone}"

    from agent_memory_mcp.bot.handlers.forum import main_menu_kb
    await message.answer(
        "📲 Sending verification code to your Telegram...",
        reply_markup=main_menu_kb(),
    )

    # Create temporary Telethon client for auth
    proxy = None
    if settings.telegram_proxy:
        from python_socks import ProxyType
        url = settings.telegram_proxy
        host = url.split("://")[1].split(":")[0]
        port = int(url.split(":")[-1])
        proxy = (ProxyType.SOCKS5, host, port)

    client = TelegramClient(
        StringSession(),
        settings.telegram_api_id,
        settings.telegram_api_hash,
        proxy=proxy,
    )

    try:
        await client.connect()
        result = await client.send_code_request(phone)
    except Exception as e:
        log.exception("send_code_failed", phone_hash=hash_phone(phone))
        await message.answer(f"❌ Failed to send code: {e}")
        await state.clear()
        try:
            await client.disconnect()
        except Exception:
            pass
        return

    # Store client and phone in FSM for next step
    # We store the session string so we can reconnect if needed
    session_string = client.session.save()
    await state.update_data(
        phone=phone,
        phone_code_hash=result.phone_code_hash,
        session_string=session_string,
    )

    # Disconnect temporary client — we'll reconnect when code arrives
    try:
        await client.disconnect()
    except Exception:
        pass

    await message.answer(
        "✅ Code sent! Check your Telegram notifications.\n\n"
        "⚠️ <b>IMPORTANT:</b> Enter the code <b>in reverse order</b>!\n"
        "Example: if you got <code>12345</code>, type <code>54321</code>\n\n"
        "This prevents Telegram from blocking the login.",
    )
    await state.set_state(AuthStates.waiting_code)


@router.message(AuthStates.waiting_code, F.text)
async def on_code_entered(message: Message, state: FSMContext, collector_pool: CollectorPool):
    """User entered verification code (reversed to avoid Telegram anti-phishing)."""
    raw = message.text.strip().replace(" ", "").replace("-", "")
    if not raw.isdigit():
        await message.answer("Enter the numeric code <b>in reverse order</b>:")
        return

    # Reverse the code back to original
    code = raw[::-1]

    # Delete the message with the code immediately
    try:
        await message.delete()
    except Exception:
        pass

    data = await state.get_data()
    phone = data["phone"]
    phone_code_hash = data["phone_code_hash"]
    session_string = data["session_string"]

    # Reconnect with the session from send_code_request
    proxy = None
    if settings.telegram_proxy:
        from python_socks import ProxyType
        url = settings.telegram_proxy
        host = url.split("://")[1].split(":")[0]
        port = int(url.split(":")[-1])
        proxy = (ProxyType.SOCKS5, host, port)

    client = TelegramClient(
        StringSession(session_string),
        settings.telegram_api_id,
        settings.telegram_api_hash,
        proxy=proxy,
    )

    try:
        await client.connect()
        await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
    except PhoneCodeInvalidError:
        try:
            await client.disconnect()
        except Exception:
            pass
        await message.answer("❌ Invalid code. Try again:")
        return
    except PhoneCodeExpiredError:
        try:
            await client.disconnect()
        except Exception:
            pass
        await state.clear()
        await message.answer("❌ Code expired. Please start over with /connect")
        return
    except SessionPasswordNeededError:
        # 2FA required — save updated session and ask for password
        updated_session = client.session.save()
        await state.update_data(session_string=updated_session)
        try:
            await client.disconnect()
        except Exception:
            pass
        await message.answer(
            "🔐 Two-factor authentication enabled.\n"
            "Enter your 2FA password:"
        )
        await state.set_state(AuthStates.waiting_2fa)
        return
    except Exception as e:
        log.exception("sign_in_failed", telegram_id=message.from_user.id)
        try:
            await client.disconnect()
        except Exception:
            pass
        await state.clear()
        await message.answer(f"❌ Auth failed: {e}")
        return

    # Success — save session
    final_session = client.session.save()
    try:
        await client.disconnect()
    except Exception:
        pass

    await collector_pool.save_session(message.from_user.id, final_session, phone)
    await state.clear()

    from agent_memory_mcp.bot.handlers.forum import main_menu_kb
    await message.answer(
        "✅ <b>Telegram connected!</b>\n\n"
        "Now I can read your channels and groups.\n"
        "Add a source: press 📡 Sources or use MCP add_source tool.",
        reply_markup=main_menu_kb(telegram_connected=True),
    )


@router.message(AuthStates.waiting_2fa, F.text)
async def on_2fa_entered(message: Message, state: FSMContext, collector_pool: CollectorPool):
    """User entered 2FA password."""
    password = message.text.strip()
    data = await state.get_data()
    phone = data["phone"]
    session_string = data["session_string"]

    proxy = None
    if settings.telegram_proxy:
        from python_socks import ProxyType
        url = settings.telegram_proxy
        host = url.split("://")[1].split(":")[0]
        port = int(url.split(":")[-1])
        proxy = (ProxyType.SOCKS5, host, port)

    client = TelegramClient(
        StringSession(session_string),
        settings.telegram_api_id,
        settings.telegram_api_hash,
        proxy=proxy,
    )

    try:
        await client.connect()
        await client.sign_in(password=password)
    except Exception as e:
        try:
            await client.disconnect()
        except Exception:
            pass
        await message.answer(f"❌ Wrong password: {e}\nTry again:")
        return

    final_session = client.session.save()
    try:
        await client.disconnect()
    except Exception:
        pass

    await collector_pool.save_session(message.from_user.id, final_session, phone)
    await state.clear()

    # Delete the password message for security
    try:
        await message.delete()
    except Exception:
        pass

    from agent_memory_mcp.bot.handlers.forum import main_menu_kb
    await message.answer(
        "✅ <b>Telegram connected!</b>\n\n"
        "Now I can read your channels and groups.\n"
        "Add a source: press 📡 Sources or use MCP add_source tool.",
        reply_markup=main_menu_kb(telegram_connected=True),
    )
