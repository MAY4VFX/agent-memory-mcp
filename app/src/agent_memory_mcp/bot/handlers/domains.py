"""Handlers for domain (source) management."""

from __future__ import annotations

import re

import structlog
from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import Message

from agent_memory_mcp.bot.keyboards import add_sources_kb, main_menu_kb, settings_kb
from agent_memory_mcp.bot.states import SettingsStates
from agent_memory_mcp.config import is_allowed_user, settings as app_settings
from agent_memory_mcp.db import queries
from agent_memory_mcp.db.engine import async_engine

log = structlog.get_logger(__name__)
router = Router()

_LINK_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/(?:\+)?([a-zA-Z0-9_]+)"
)

_DOMAIN_EMOJIS = {"\U0001f525", "\U0001f916", "\U0001f3ac", "\U0001f9e0",
                  "\U0001f4a1", "\U0001f4da", "\U0001f3a8", "\U0001f4b0",
                  "\U0001f4c1", "\U0001f30d"}


def _looks_like_channel(text: str | None) -> bool:
    if not text:
        return False
    text = text.strip()
    return text.startswith("@") or bool(_LINK_RE.match(text))


def _is_domain_button(text: str) -> bool:
    """Check if text matches the domain reply-keyboard button."""
    if not text:
        return False
    if text in ("\U0001f4da \u041a\u0430\u043d\u0430\u043b\u044b", "\U0001f4da \u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438"):
        return True
    for e in _DOMAIN_EMOJIS:
        if text.startswith(e + " ") and not _looks_like_channel(text):
            return True
    return False


@router.message(F.text.func(_is_domain_button))
async def domain_button_handler(message: Message, state: FSMContext) -> None:
    """Handle domain reply-keyboard button -> show sources hub."""
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        await message.answer("\u0414\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d.")
        return
    user_domains = await queries.list_domains(async_engine, message.from_user.id)
    if not user_domains:
        await message.answer(
            "\u0423 \u0432\u0430\u0441 \u043d\u0435\u0442 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0451\u043d\u043d\u044b\u0445 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u043e\u0432.\n\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0441\u043f\u043e\u0441\u043e\u0431 \u0434\u043e\u0431\u0430\u0432\u0438\u0442\u044c:",
            reply_markup=add_sources_kb(),
        )
        return

    # Show sources hub
    from agent_memory_mcp.bot.handlers.groups import _build_hub

    text, kb = await _build_hub(message.from_user.id)
    await message.answer(text, reply_markup=kb)
    await state.set_state(SettingsStates.managing_domains)


@router.message(F.text == "\u2699\ufe0f \u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438")
async def settings_menu(message: Message, state: FSMContext) -> None:
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        await message.answer("\u0414\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d.")
        return
    is_admin = message.from_user and message.from_user.id == app_settings.admin_telegram_id
    await message.answer("\u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438:", reply_markup=settings_kb(is_admin=is_admin))
    await state.clear()
