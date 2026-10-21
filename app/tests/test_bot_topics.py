"""Bot private-chat topics: every message is answered where it came from.

Real stream (Telegram Desktop, topics on): keyboard presses arrive with no
thread; text typed from «New Chat» lands in a topic Telegram creates for it —
also the answer to a menu prompt («@tamba» after «Add by link», message 1156
in topic 1279469). Earlier versions moved such messages to the main thread and
deleted the topic: the user was left on an empty «No messages here yet»
screen, and since aiogram picks the handler before inner middlewares run,
«@tamba» still went to the agent instead of the add-source flow.
"""

import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from aiogram import Dispatcher, F
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Chat, ForumTopicCreated, Message, Update, User

from agent_memory_mcp.bot import topics

USER_ID = 42


class Flow(StatesGroup):
    waiting_input = State()


def _msg(text: str, message_id: int, thread_id: int | None) -> Message:
    return Message(
        message_id=message_id,
        date=datetime.now(timezone.utc),
        chat=Chat(id=USER_ID, type="private"),
        from_user=User(id=USER_ID, is_bot=False, first_name="u"),
        text=text,
        message_thread_id=thread_id,
        is_topic_message=thread_id is not None or None,
    )


def _dispatcher() -> tuple[Dispatcher, dict]:
    """Same wiring as bot.app: default FSM strategy + MenuFlowMiddleware."""
    dp = Dispatcher()
    dp.message.middleware(topics.MenuFlowMiddleware())
    seen: dict = {}

    # Menu buttons first, like bot.app: forum_router precedes the state handlers.
    @dp.message(F.text == "💰 Balance")
    async def balance(message: Message) -> None:
        seen["by"], seen["thread"] = "menu", message.message_thread_id

    # No F.text on purpose: a state handler that takes any message is exactly
    # what swallowed «topic created» in production.
    @dp.message(StateFilter(Flow.waiting_input))
    async def flow_input(message: Message, state: FSMContext) -> None:
        await state.clear()
        seen["by"], seen["thread"], seen["text"] = "flow", message.message_thread_id, message.text

    @dp.message(F.text, F.message_thread_id.is_not(None))
    async def dialog(message: Message) -> None:
        seen["by"], seen["thread"] = "agent", message.message_thread_id

    return dp, seen


def _bot() -> MagicMock:
    bot = MagicMock()
    bot.id = 1
    return bot


async def _set_flow(dp: Dispatcher) -> None:
    key = StorageKey(bot_id=1, chat_id=USER_ID, user_id=USER_ID)
    await dp.fsm.storage.set_state(key, Flow.waiting_input)


class DispatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_prompt_answer_typed_into_new_topic_reaches_the_flow_there(self) -> None:
        dp, seen = _dispatcher()
        await _set_flow(dp)

        await dp.feed_update(_bot(), Update(update_id=1, message=_msg("@tamba", 1156, 1279469)))

        self.assertEqual(seen, {"by": "flow", "thread": 1279469, "text": "@tamba"})

    async def test_topic_created_service_message_does_not_eat_the_flow(self) -> None:
        # Real stream: 1161 «topic created» → flow answered «Empty.» and ended,
        # 1163 «@tammba» then went to the agent.
        dp, seen = _dispatcher()
        await _set_flow(dp)
        created = Message(
            message_id=1161,
            date=datetime.now(timezone.utc),
            chat=Chat(id=USER_ID, type="private"),
            from_user=User(id=USER_ID, is_bot=False, first_name="u"),
            message_thread_id=1279475,
            is_topic_message=True,
            forum_topic_created=ForumTopicCreated(name="@tammba", icon_color=0),
        )

        await dp.feed_update(_bot(), Update(update_id=1, message=created))
        await dp.feed_update(_bot(), Update(update_id=2, message=_msg("@tammba", 1163, 1279475)))

        self.assertEqual(seen, {"by": "flow", "thread": 1279475, "text": "@tammba"})

    async def test_question_in_topic_goes_to_agent_in_that_topic(self) -> None:
        dp, seen = _dispatcher()

        await dp.feed_update(_bot(), Update(update_id=1, message=_msg("что решили?", 1123, 1279436)))

        self.assertEqual(seen, {"by": "agent", "thread": 1279436})

    async def test_button_inside_topic_is_answered_there(self) -> None:
        dp, seen = _dispatcher()

        await dp.feed_update(_bot(), Update(update_id=1, message=_msg("💰 Balance", 1113, 316241)))

        self.assertEqual(seen, {"by": "menu", "thread": 316241})

    async def test_menu_press_abandons_pending_flow(self) -> None:
        dp, seen = _dispatcher()
        await _set_flow(dp)

        await dp.feed_update(_bot(), Update(update_id=1, message=_msg("💰 Balance", 1150, None)))
        await dp.feed_update(_bot(), Update(update_id=2, message=_msg("что решили?", 1160, 1279500)))

        self.assertEqual(seen, {"by": "agent", "thread": 1279500})


class StaleFlowTests(unittest.IsolatedAsyncioTestCase):
    async def _drop(self, text: str, state: str) -> str | None:
        storage = MemoryStorage()
        ctx = FSMContext(storage=storage, key=StorageKey(bot_id=1, chat_id=USER_ID, user_id=USER_ID))
        await ctx.set_state(state)
        data = await topics._drop_stale_flow(_msg(text, 1, None), {"state": ctx, "raw_state": state})
        return data["raw_state"]

    async def test_start_abandons_flow(self) -> None:
        self.assertIsNone(await self._drop("/start", "KeyStates:waiting_name"))

    async def test_cancel_keeps_auth_flow_for_its_handler(self) -> None:
        self.assertEqual(await self._drop("❌ Cancel", "AuthStates:waiting_contact"), "AuthStates:waiting_contact")

    async def test_flow_input_keeps_flow(self) -> None:
        self.assertEqual(await self._drop("@tamba", "AddSourceStates:waiting_input"), "AddSourceStates:waiting_input")


class LegacyHomeTopicTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_deletes_old_menu_topic(self) -> None:
        bot = MagicMock()
        bot.delete_forum_topic = AsyncMock()
        with patch.object(topics.qc, "get_home_thread_id", AsyncMock(return_value=1279382)), \
                patch.object(topics.qc, "set_home_thread_id", AsyncMock()) as set_home:
            await topics.drop_legacy_home_topic(bot, USER_ID, USER_ID)

        bot.delete_forum_topic.assert_awaited_once_with(chat_id=USER_ID, message_thread_id=1279382)
        set_home.assert_awaited_once_with(topics.async_engine, USER_ID, None)


if __name__ == "__main__":
    unittest.main()
