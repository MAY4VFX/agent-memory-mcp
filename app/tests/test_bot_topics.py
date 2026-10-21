"""Bot private-chat topics: menu traffic goes to 🏠 Menu, junk topics are deleted.

A topics-enabled bot chat has no General, and Telegram auto-creates a topic
for any message written from the topic list. Before this, every reply-keyboard
press opened a new topic with the answer inside, and /start answered into a
General the user never sees — "no buttons after /start".
"""

import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Chat, ForumTopic, Message, User

from agent_memory_mcp.bot import topics

USER_ID = 42
HOME = 100


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


class FreshTopicTests(unittest.TestCase):
    def test_first_message_of_auto_created_topic(self) -> None:
        self.assertTrue(topics.is_fresh_topic(_msg("💰 Balance", 501, 500)))

    def test_later_message_in_topic(self) -> None:
        self.assertFalse(topics.is_fresh_topic(_msg("💰 Balance", 520, 500)))

    def test_no_thread(self) -> None:
        self.assertFalse(topics.is_fresh_topic(_msg("💰 Balance", 501, None)))


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
        self.bot.create_forum_topic = AsyncMock(
            return_value=ForumTopic(message_thread_id=HOME, name="🏠 Menu", icon_color=0)
        )
        self.storage = MemoryStorage()
        self.home = HOME
        self.set_home = AsyncMock()
        patches = [
            patch.object(topics.qc, "get_home_thread_id", AsyncMock(side_effect=lambda *_: self.home)),
            patch.object(topics.qc, "set_home_thread_id", self.set_home),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    async def _run(self, message: Message, thread_for_state: int | None = None):
        seen = {}

        async def handler(event, data):
            seen["event"], seen["data"] = event, data

        state = _state(self.storage, message.message_thread_id if thread_for_state is None else thread_for_state)
        data = {"bot": self.bot, "state": state, "raw_state": await state.get_state()}
        await topics.MenuTopicMiddleware()(handler, message, data)
        return seen["event"], seen["data"]

    async def test_button_from_topic_list_goes_home_and_junk_is_deleted(self) -> None:
        event, _ = await self._run(_msg("💰 Balance", 501, 500))

        self.bot.delete_forum_topic.assert_awaited_once_with(chat_id=USER_ID, message_thread_id=500)
        self.assertEqual(event.message_thread_id, HOME)
        self.assertTrue(event.is_topic_message)

    async def test_first_start_creates_home_topic(self) -> None:
        self.home = None
        event, _ = await self._run(_msg("/start", 501, 500))

        self.bot.create_forum_topic.assert_awaited_once()
        self.set_home.assert_awaited_once_with(topics.async_engine, USER_ID, HOME)
        self.assertEqual(event.message_thread_id, HOME)

    async def test_button_inside_home_is_untouched(self) -> None:
        event, _ = await self._run(_msg("💰 Balance", 130, HOME))

        self.bot.delete_forum_topic.assert_not_awaited()
        self.assertEqual(event.message_thread_id, HOME)

    async def test_question_in_new_topic_is_a_dialog(self) -> None:
        event, _ = await self._run(_msg("что решили по релизу?", 501, 500))

        self.bot.delete_forum_topic.assert_not_awaited()
        self.assertEqual(event.message_thread_id, 500)

    async def test_rerouted_flow_waits_in_home(self) -> None:
        _, data = await self._run(_msg("📱 Connect Telegram", 501, 500))
        await data["state"].set_state("AuthStates:waiting_contact")

        self.assertEqual(await _state(self.storage, HOME).get_state(), "AuthStates:waiting_contact")
        self.assertIsNone(await _state(self.storage, 500).get_state())

    async def test_menu_button_abandons_pending_input(self) -> None:
        await _state(self.storage, HOME).set_state("KeyStates:waiting_name")

        _, data = await self._run(_msg("💰 Balance", 140, HOME))

        self.assertIsNone(data["raw_state"])
        self.assertIsNone(await _state(self.storage, HOME).get_state())

    async def test_cancel_keeps_auth_flow_for_its_handler(self) -> None:
        await _state(self.storage, HOME).set_state("AuthStates:waiting_contact")

        _, data = await self._run(_msg("❌ Cancel", 140, HOME))

        self.assertEqual(data["raw_state"], "AuthStates:waiting_contact")


if __name__ == "__main__":
    unittest.main()
