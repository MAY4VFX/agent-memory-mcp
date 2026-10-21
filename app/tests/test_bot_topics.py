"""Bot private-chat topics: menu traffic goes to the main thread, junk topics are deleted.

Telegram auto-creates a topic for a message written from the topic list
(real log: /start arrived as message 1047 in fresh topic 1279379, the next
keyboard presses with no thread). Before this, every such press opened a new
topic with the answer inside, and the keyboard never showed up where the
user was looking.
"""

import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.base import StorageKey
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Chat, Message, User

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
        self.storage = MemoryStorage()

    async def _run(self, message: Message):
        seen = {}

        async def handler(event, data):
            seen["event"], seen["data"] = event, data

        state = _state(self.storage, message.message_thread_id)
        data = {"bot": self.bot, "state": state, "raw_state": await state.get_state()}
        await topics.MenuTopicMiddleware()(handler, message, data)
        return seen["event"], seen["data"]

    async def test_start_from_topic_list_goes_to_main_and_junk_is_deleted(self) -> None:
        event, _ = await self._run(_msg("/start", 1048, 1047))

        self.bot.delete_forum_topic.assert_awaited_once_with(chat_id=USER_ID, message_thread_id=1047)
        self.assertIsNone(event.message_thread_id)
        self.assertFalse(event.is_topic_message)

    async def test_button_in_main_thread_is_untouched(self) -> None:
        event, _ = await self._run(_msg("📡 Sources", 1052, MAIN))

        self.bot.delete_forum_topic.assert_not_awaited()
        self.assertIsNone(event.message_thread_id)

    async def test_button_inside_dialog_topic_answers_in_main_keeps_topic(self) -> None:
        event, _ = await self._run(_msg("💰 Balance", 530, 500))

        self.bot.delete_forum_topic.assert_not_awaited()
        self.assertIsNone(event.message_thread_id)

    async def test_question_in_new_topic_is_a_dialog(self) -> None:
        event, _ = await self._run(_msg("что решили по релизу?", 501, 500))

        self.bot.delete_forum_topic.assert_not_awaited()
        self.assertEqual(event.message_thread_id, 500)

    async def test_export_stays_in_its_topic(self) -> None:
        event, _ = await self._run(_msg("/export", 530, 500))

        self.assertEqual(event.message_thread_id, 500)

    async def test_flow_input_sent_into_new_topic_goes_to_the_flow(self) -> None:
        await _state(self.storage, MAIN).set_state("AddSourceStates:waiting_input")

        event, data = await self._run(_msg("@novchenkova", 1085, 1084))

        self.bot.delete_forum_topic.assert_awaited_once_with(chat_id=USER_ID, message_thread_id=1084)
        self.assertIsNone(event.message_thread_id)
        self.assertEqual(data["raw_state"], "AddSourceStates:waiting_input")

    async def test_question_in_existing_topic_ignores_main_flow(self) -> None:
        await _state(self.storage, MAIN).set_state("AddSourceStates:waiting_input")

        event, _ = await self._run(_msg("что решили по релизу?", 530, 500))

        self.bot.delete_forum_topic.assert_not_awaited()
        self.assertEqual(event.message_thread_id, 500)

    async def test_rerouted_flow_waits_in_main(self) -> None:
        _, data = await self._run(_msg("📱 Connect Telegram", 501, 500))
        await data["state"].set_state("AuthStates:waiting_contact")

        self.assertEqual(await _state(self.storage, MAIN).get_state(), "AuthStates:waiting_contact")
        self.assertIsNone(await _state(self.storage, 500).get_state())

    async def test_menu_button_abandons_pending_input(self) -> None:
        await _state(self.storage, MAIN).set_state("KeyStates:waiting_name")

        _, data = await self._run(_msg("💰 Balance", 140, MAIN))

        self.assertIsNone(data["raw_state"])
        self.assertIsNone(await _state(self.storage, MAIN).get_state())

    async def test_cancel_keeps_auth_flow_for_its_handler(self) -> None:
        await _state(self.storage, MAIN).set_state("AuthStates:waiting_contact")

        _, data = await self._run(_msg("❌ Cancel", 140, MAIN))

        self.assertEqual(data["raw_state"], "AuthStates:waiting_contact")


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
