"""Private-chat topics: answer where the user is, one topic per agent dialog.

In a topics-enabled bot chat, reply-keyboard presses arrive in the main thread
(no message_thread_id), but text typed from the «New Chat» view always lands in
a topic Telegram creates for it — a question, and equally the answer to a menu
prompt («@tamba» after «Add by link»). The bot can't stop that, and deleting
the topic strands the user on an empty «No messages here yet» screen, so
every message is answered where it came from.

Menu input flows (key name, auth code, add-source link) are per user, not per
topic, so the answer to a prompt is caught in whichever topic it was typed.
A menu press or /start abandons a pending flow, so a flow never swallows a
later question.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

import structlog
from aiogram import BaseMiddleware, Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, TelegramObject

from agent_memory_mcp.db import queries_conversations as qc
from agent_memory_mcp.db.engine import async_engine

log = structlog.get_logger(__name__)

# Reply keyboard button texts (forum.main_menu_kb + auth flow keyboard).
MENU_TEXTS = {
    "💰 Balance", "📡 Sources", "🔑 API Keys", "💎 Top Up", "📊 Usage", "❓ Help",
    "📱 Connect Telegram", "❌ Cancel",
}


async def drop_legacy_home_topic(bot: Bot, user_id: int, chat_id: int) -> None:
    """Delete the «🏠 Menu» topic an earlier version created; the menu is in the main thread now."""
    home = await qc.get_home_thread_id(async_engine, user_id)
    if home is None:
        return
    try:
        await bot.delete_forum_topic(chat_id=chat_id, message_thread_id=home)
    except TelegramBadRequest:
        log.warning("legacy_home_topic_delete_failed", user_id=user_id, thread_id=home)
    await qc.set_home_thread_id(async_engine, user_id, None)


class MenuFlowMiddleware(BaseMiddleware):
    """Log where each private message came from; a menu press abandons a pending flow."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        if not isinstance(event, Message) or event.chat.type != "private" or not event.from_user:
            return await handler(event, data)
        if _is_topic_service(event):
            # «Topic created» arrives right before the first message of a new
            # topic; a waiting flow took it as (empty) input, answered «Empty.»
            # and let the real «@tammba» go to the agent (messages 1161/1163).
            return None

        log.info(
            "bot_message",
            user_id=event.from_user.id,
            message_id=event.message_id,
            thread_id=event.message_thread_id,
            is_topic=event.is_topic_message,
            flow=data.get("raw_state"),
        )
        return await handler(event, await _drop_stale_flow(event, data))


def _is_topic_service(message: Message) -> bool:
    return any(
        getattr(message, f) is not None
        for f in ("forum_topic_created", "forum_topic_edited", "forum_topic_closed", "forum_topic_reopened")
    )


async def _drop_stale_flow(message: Message, data: dict[str, Any]) -> dict[str, Any]:
    """A menu button or /start abandons a pending input flow.

    Otherwise the next free text after e.g. «➕ Create Key» → «💰 Balance»
    would still be taken as the key name.
    """
    text = (message.text or "").strip()
    state: FSMContext | None = data.get("state")
    if state is None or data.get("raw_state") is None:
        return data
    if text == "❌ Cancel" or not (text in MENU_TEXTS or text.startswith("/start")):
        return data
    await state.clear()
    return {**data, "raw_state": None}
