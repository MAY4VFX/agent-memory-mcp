"""Private-chat topics: menu in the main thread, one topic per agent dialog.

In a topics-enabled bot chat the main thread (no message_thread_id) holds the
reply keyboard and every menu answer. A message written from the topic list
makes Telegram auto-create a topic for it, so /start or a keyboard press can
arrive as the first message of a brand-new topic. Left alone, every press
spawns a junk topic and the answer lands there.

MenuTopicMiddleware moves such menu traffic to the main thread: the junk
topic a menu message opened is deleted and the message is handled as if sent
in the main thread. A menu press inside an existing topic is answered in that
topic — that is where the user is looking. Free text in the main thread opens a dialog topic (forum.py);
free text in a topic is that topic's dialog with the agent.
"""

from __future__ import annotations

import dataclasses
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

# Commands that act on the topic they were sent in, not on the menu.
_TOPIC_LOCAL_COMMANDS = ("/export",)


def is_fresh_topic(message: Message) -> bool:
    """The message is the first one in a topic Telegram just auto-created for it.

    A topic's id is the id of its «topic created» service message, and message
    ids in a private chat are sequential, so the first real message is id + 1.
    """
    tid = message.message_thread_id
    return tid is not None and message.message_id == tid + 1


def is_menu_message(message: Message, raw_state: str | None) -> bool:
    """Belongs to the menu (keyboard, commands, contact/chat sharing, FSM input)."""
    if message.contact or message.chat_shared or message.users_shared:
        return True
    text = (message.text or "").strip()
    if text in MENU_TEXTS:
        return True
    if text.startswith("/"):
        return not text.startswith(_TOPIC_LOCAL_COMMANDS)
    # Free text while a menu flow waits for input (key name, auth code, link…)
    return raw_state is not None


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


class MenuTopicMiddleware(BaseMiddleware):
    """Move menu messages out of the junk topics they opened, into the main thread.

    Needs the dispatcher's FSM strategy USER_IN_TOPIC: a flow started in the
    main thread (key name, auth code, add-source link) must not capture
    questions typed in dialog topics.
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        if not isinstance(event, Message) or event.chat.type != "private" or not event.from_user:
            return await handler(event, data)

        main_state = _main_state(data)
        main_raw = await main_state.get_state() if main_state is not None else None
        menu = is_menu_message(event, data.get("raw_state"))
        if not menu and is_fresh_topic(event) and main_raw is not None:
            # A flow in the main thread waits for input (add-source link, key
            # name, auth code), but the client sent the answer into a new
            # topic (real case: «@novchenkova» after «Add by link» landed in
            # topic 316241 and went to the agent). It is the flow's input.
            menu = True
        log.info(
            "bot_message",
            user_id=event.from_user.id,
            message_id=event.message_id,
            thread_id=event.message_thread_id,
            is_topic=event.is_topic_message,
            menu=menu,
        )
        # A menu press inside an existing topic is answered right there: the
        # user is looking at that topic, and an answer sent to the main thread
        # looks like "the button does nothing" (real case: «📡 Sources» pressed
        # in the friend's «@novchenkova» topic, answers went to the main thread).
        if not menu or not is_fresh_topic(event):
            return await handler(event, await _drop_stale_flow(event, data))

        bot: Bot = data["bot"]
        topic = event.message_thread_id
        try:
            await bot.delete_forum_topic(chat_id=event.chat.id, message_thread_id=topic)
        except TelegramBadRequest:
            log.warning("junk_topic_delete_failed", user_id=event.from_user.id, thread_id=topic)

        log.info("menu_message_rerouted", user_id=event.from_user.id, from_thread=topic)
        rerouted = event.model_copy(
            update={"message_thread_id": None, "is_topic_message": None}
        ).as_(bot)
        # The FSM context was resolved for the topic; flows started now
        # (e.g. 📱 Connect → waiting_contact) must wait in the main thread.
        if main_state is not None:
            data = {**data, "state": main_state, "raw_state": main_raw}
        return await handler(rerouted, await _drop_stale_flow(rerouted, data))


def _main_state(data: dict[str, Any]) -> FSMContext | None:
    """FSM context of the main thread for the same user and chat."""
    state: FSMContext | None = data.get("state")
    if state is None:
        return None
    return FSMContext(storage=state.storage, key=dataclasses.replace(state.key, thread_id=None))


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
