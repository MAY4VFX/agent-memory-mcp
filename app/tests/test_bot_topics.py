"""Bot private-chat topics: menu traffic goes to the main thread, junk topics are deleted.

Telegram auto-creates a topic for a message written from the topic list
(real log: /start arrived as message 1047 in fresh topic 1279379, the next
keyboard presses with no thread). Before this, every such press opened a new
topic with the answer inside, and the keyboard never showed up where the
user was looking.
"""

import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Chat, ForumTopicCreated, Message, User

from agent_memory_mcp.bot import topics

USER_ID = 42
MAIN = None


def _msg(text: str | None, message_id: int, thread_id: int | None, **extra) -> Message:
    return Message(
        message_id=message_id,
        date=datetime.now(timezone.utc),
        chat=Chat(id=USER_ID, type="private"),
        from_user=User(id=USER_ID, is_bot=False, first_name="u"),
        text=text,
        message_thread_id=thread_id,
        is_topic_message=thread_id is not None or None,
        **extra,
    )


def _state(storage: MemoryStorage, thread_id: int | None) -> FSMContext:
    key = StorageKey(bot_id=1, chat_id=USER_ID, user_id=USER_ID, thread_id=thread_id)
    return FSMContext(storage=storage, key=key)


def _topic_created(thread_id: int) -> Message:
    return Message(
        message_id=thread_id,
        date=datetime.now(timezone.utc),
        chat=Chat(id=USER_ID, type="private"),
        from_user=User(id=USER_ID, is_bot=False, first_name="u"),
        message_thread_id=thread_id,
        is_topic_message=True,
        forum_topic_created=ForumTopicCreated(name="x", icon_color=0),
    )


class FreshTopicTests(unittest.TestCase):
    def setUp(self) -> None:
        topics._fresh_topics.clear()

    def test_first_message_after_topic_created(self) -> None:
        # Real ids: message 1141 in topic 1279454 — ids are unrelated.
        topics.note_topic_created(_topic_created(1279454))

        self.assertTrue(topics.take_fresh_topic(_msg("@tamba", 1141, 1279454)))

    def test_only_the_first_message_is_fresh(self) -> None:
        topics.note_topic_created(_topic_created(1279454))
        topics.take_fresh_topic(_msg("@tamba", 1141, 1279454))

        self.assertFalse(topics.take_fresh_topic(_msg("ещё", 1143, 1279454)))

    def test_topic_without_created_event_is_not_fresh(self) -> None:
        self.assertFalse(topics.take_fresh_topic(_msg("📡 Sources", 1113, 316241)))

    def test_no_thread(self) -> None:
        self.assertFalse(topics.take_fresh_topic(_msg("📡 Sources", 1052, None)))


class MenuMessageTests(unittest.TestCase):
    def test_keyboard_button(self) -> None:
        self.assertTrue(topics.is_menu_message(_msg("🔑 API Keys", 1, 1), None))

    def test_start_command(self) -> None:
        self.assertTrue(topics.is_menu_message(_msg("/start", 1, 1), None))

    def test_export_stays_in_topic(self) -> None:
        self.assertFalse(topics.is_menu_message(_msg("/export", 1, 1), None))

    def test_question_is_dialog(self) -> None:
        self.assertFalse(topics.is_menu_message(_msg("что решили по релизу?", 1, 1), None))

    def test_input_for_pending_flow(self) -> None:
        self.assertTrue(topics.is_menu_message(_msg("my-key", 1, 1), "KeyStates:waiting_name"))


class MiddlewareTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.bot = MagicMock()
        self.bot.id = 1
        self.bot.delete_forum_topic = AsyncMock()
        self.storage = MemoryStorage()
        topics._fresh_topics.clear()

    async def _run(self, message: Message, fresh: bool = False):
        if fresh:
            topics.note_topic_created(_topic_created(message.message_thread_id))
        seen = {}

        async def handler(event, data):
            seen["event"], seen["data"] = event, data

        state = _state(self.storage, message.message_thread_id)
        data = {"bot": self.bot, "state": state, "raw_state": await state.get_state()}
        await topics.MenuTopicMiddleware()(handler, message, data)
        return seen["event"], seen["data"]

    async def test_start_from_topic_list_goes_to_main_and_junk_is_deleted(self) -> None:
        event, _ = await self._run(_msg("/start", 1047, 1279379), fresh=True)

        self.bot.delete_forum_topic.assert_awaited_once_with(chat_id=USER_ID, message_thread_id=1279379)
        self.assertIsNone(event.message_thread_id)
        self.assertFalse(event.is_topic_message)

    async def test_button_in_main_thread_is_untouched(self) -> None:
        event, _ = await self._run(_msg("📡 Sources", 1052, MAIN))

        self.bot.delete_forum_topic.assert_not_awaited()
        self.assertIsNone(event.message_thread_id)

    async def test_button_inside_existing_topic_is_answered_there(self) -> None:
        # Real case: «📡 Sources» pressed in the «@novchenkova» topic; answers
        # sent to the main thread looked like the button did nothing.
        event, _ = await self._run(_msg("📡 Sources", 1113, 316241))

        self.bot.delete_forum_topic.assert_not_awaited()
        self.assertEqual(event.message_thread_id, 316241)

    async def test_question_in_new_topic_is_a_dialog(self) -> None:
        event, _ = await self._run(_msg("что решили по релизу?", 1123, 1279436), fresh=True)

        self.bot.delete_forum_topic.assert_not_awaited()
        self.assertEqual(event.message_thread_id, 1279436)

    async def test_export_stays_in_its_topic(self) -> None:
        event, _ = await self._run(_msg("/export", 530, 500))

        self.assertEqual(event.message_thread_id, 500)

    async def test_flow_input_sent_into_new_topic_goes_to_the_flow(self) -> None:
        await _state(self.storage, MAIN).set_state("AddSourceStates:waiting_input")

        event, data = await self._run(_msg("@tamba", 1141, 1279454), fresh=True)

        self.bot.delete_forum_topic.assert_awaited_once_with(chat_id=USER_ID, message_thread_id=1279454)
        self.assertIsNone(event.message_thread_id)
        self.assertEqual(data["raw_state"], "AddSourceStates:waiting_input")

    async def test_question_in_existing_topic_ignores_main_flow(self) -> None:
        await _state(self.storage, MAIN).set_state("AddSourceStates:waiting_input")

        event, _ = await self._run(_msg("что решили по релизу?", 530, 500))

        self.bot.delete_forum_topic.assert_not_awaited()
        self.assertEqual(event.message_thread_id, 500)

    async def test_rerouted_flow_waits_in_main(self) -> None:
        _, data = await self._run(_msg("📱 Connect Telegram", 1127, 1279440), fresh=True)
        await data["state"].set_state("AuthStates:waiting_contact")

        self.assertEqual(await _state(self.storage, MAIN).get_state(), "AuthStates:waiting_contact")
        self.assertIsNone(await _state(self.storage, 1279440).get_state())

    async def test_menu_button_abandons_pending_input(self) -> None:
        await _state(self.storage, MAIN).set_state("KeyStates:waiting_name")

        _, data = await self._run(_msg("💰 Balance", 140, MAIN))

        self.assertIsNone(data["raw_state"])
        self.assertIsNone(await _state(self.storage, MAIN).get_state())

    async def test_cancel_keeps_auth_flow_for_its_handler(self) -> None:
        await _state(self.storage, MAIN).set_state("AuthStates:waiting_contact")

        _, data = await self._run(_msg("❌ Cancel", 140, MAIN))

        self.assertEqual(data["raw_state"], "AuthStates:waiting_contact")


class DispatcherOrderTests(unittest.IsolatedAsyncioTestCase):
    """Polling runs updates as concurrent tasks; «topic created» must still win.

    Real stream: update 996172463 «topic created» (not handled), then 996172464
    «@tamba» in topic 1279454 while «Add by link» waited in the main thread.
    """

    async def test_input_after_topic_created_reaches_the_flow(self) -> None:
        from aiogram import Dispatcher, F
        from aiogram.fsm.strategy import FSMStrategy
        from aiogram.types import Update

        topics._fresh_topics.clear()
        dp = Dispatcher(fsm_strategy=FSMStrategy.USER_IN_TOPIC)
        dp.message.outer_middleware(topics.TopicCreatedMiddleware())
        dp.message.middleware(topics.MenuTopicMiddleware())
        seen = {}

        @dp.message(F.text)
        async def capture(message: Message, raw_state: str | None) -> None:
            seen["thread"], seen["state"] = message.message_thread_id, raw_state

        bot = MagicMock()
        bot.id = 1
        bot.delete_forum_topic = AsyncMock()
        await dp.fsm.storage.set_state(
            StorageKey(bot_id=1, chat_id=USER_ID, user_id=USER_ID), "AddSourceStates:waiting_input"
        )

        created = Update(update_id=996172463, message=_topic_created(1279454))
        text = Update(update_id=996172464, message=_msg("@tamba", 1141, 1279454))
        await asyncio.gather(dp.feed_update(bot, created), dp.feed_update(bot, text))

        self.assertIsNone(seen["thread"])
        self.assertEqual(seen["state"], "AddSourceStates:waiting_input")
        bot.delete_forum_topic.assert_awaited_once_with(chat_id=USER_ID, message_thread_id=1279454)


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
