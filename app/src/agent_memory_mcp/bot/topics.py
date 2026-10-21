"""Private-chat topics: one «🏠 Menu» topic for the keyboard, one topic per dialog.

A bot private chat with topics enabled has no General: every message carries
a message_thread_id, and a message the user writes outside an open topic makes
Telegram auto-create a new topic for it (allows_users_to_create_topics). So a
reply-keyboard press or /start sent from the topic list arrives as the first
message of a brand-new topic. Left alone, every press spawns a junk topic and
the answer lands there.

The menu therefore lives in a topic the bot creates itself (users.home_thread_id).
MenuTopicMiddleware moves menu traffic into it: a menu message that opened a
fresh topic gets that topic deleted and is handled as if sent in 🏠 Menu.
Everything else in a topic is a dialog with the agent.
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

HOME_TOPIC_NAME = "🏠 Menu"

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


async def _create_home(bot: Bot, user_id: int, chat_id: int) -> int:
    topic = await bot.create_forum_topic(chat_id=chat_id, name=HOME_TOPIC_NAME)
    await qc.set_home_thread_id(async_engine, user_id, topic.message_thread_id)
    log.info("home_topic_created", user_id=user_id, thread_id=topic.message_thread_id)
    return topic.message_thread_id


async def ensure_home(bot: Bot, user_id: int, chat_id: int) -> int | None:
    """Thread id of the user's «🏠 Menu» topic, creating it if missing.

    None when topics are off for the bot — callers then send without a thread.
    """
    home = await qc.get_home_thread_id(async_engine, user_id)
    if home is not None:
        return home
    try:
        return await _create_home(bot, user_id, chat_id)
    except TelegramBadRequest:
        log.warning("home_topic_unavailable", user_id=user_id)
        return None


async def send_home(bot: Bot, user_id: int, chat_id: int, text: str, **kwargs: Any) -> Message:
    """Send into 🏠 Menu; recreate the topic once if the user deleted it."""
    home = await ensure_home(bot, user_id, chat_id)
    try:
        return await bot.send_message(chat_id, text, message_thread_id=home, **kwargs)
    except TelegramBadRequest as e:
        if home is None or "thread" not in str(e).lower():
            raise
        log.info("home_topic_gone", user_id=user_id, thread_id=home)
        home = await _create_home(bot, user_id, chat_id)
        return await bot.send_message(chat_id, text, message_thread_id=home, **kwargs)


async def is_home_message(message: Message) -> bool:
    """Filter: the message was sent in 🏠 Menu (or with no topic at all)."""
    if message.message_thread_id is None:
        return True
    home = await qc.get_home_thread_id(async_engine, message.from_user.id)
    return message.message_thread_id == home


class MenuTopicMiddleware(BaseMiddleware):
    """Route menu messages to 🏠 Menu and drop the junk topics they opened.

    Needs the dispatcher's FSM strategy USER_IN_TOPIC: a flow started in
    🏠 Menu (key name, auth code, add-source link) must not capture questions
    typed in dialog topics.
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        if not isinstance(event, Message) or event.chat.type != "private" or not event.from_user:
            return await handler(event, data)

        menu = is_menu_message(event, data.get("raw_state"))
        log.info(
            "bot_message",
            user_id=event.from_user.id,
            message_id=event.message_id,
            thread_id=event.message_thread_id,
            is_topic=event.is_topic_message,
            menu=menu,
        )
        if not menu or not is_fresh_topic(event):
            return await handler(event, await _drop_stale_flow(event, data))

        bot: Bot = data["bot"]
        user_id = event.from_user.id
        if await qc.get_home_thread_id(async_engine, user_id) == event.message_thread_id:
            return await handler(event, await _drop_stale_flow(event, data))

        junk = event.message_thread_id
        try:
            await bot.delete_forum_topic(chat_id=event.chat.id, message_thread_id=junk)
        except TelegramBadRequest:
            log.warning("junk_topic_delete_failed", user_id=user_id, thread_id=junk)

        home = await ensure_home(bot, user_id, event.chat.id)
        log.info("menu_message_rerouted", user_id=user_id, from_thread=junk, to_thread=home)
        rerouted = event.model_copy(
            update={"message_thread_id": home, "is_topic_message": home is not None}
        ).as_(bot)
        # The FSM context was resolved for the junk topic; flows started now
        # (e.g. 📱 Connect → waiting_contact) must wait in 🏠 Menu.
        state = data.get("state")
        if state is not None:
            home_state = FSMContext(
                storage=state.storage,
                key=dataclasses.replace(state.key, thread_id=home),
            )
            data = {**data, "state": home_state, "raw_state": await home_state.get_state()}
        return await handler(rerouted, await _drop_stale_flow(rerouted, data))


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
