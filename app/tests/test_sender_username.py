"""sender_username — issue #27: an agent can't act on "Andrey", it needs an
@ник to actually find or message the person. msg.sender.username is read
right next to sender_name in _paginated_fetch, at zero extra Telegram cost.

Covers:
- extract_sender_info() (collector/client.py): the Telethon-facing extraction,
  using cheap duck-typed stand-ins for Message/sender so the test doesn't
  need a live Telethon client. Mirrors the extract_fwd_info test style.
- telegram_to_processed() / pg_row_to_processed() (models/messages.py): the
  conversions that must carry sender_username through untouched.
- username=None must survive as None, not "" — most people don't have one.
"""

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from agent_memory_mcp.collector.client import extract_sender_info
from agent_memory_mcp.memory_api import service
from agent_memory_mcp.models.messages import (
    TelegramMessage,
    pg_row_to_processed,
    telegram_to_processed,
)


def _msg(sender=None):
    """Minimal stand-in for a Telethon custom.Message — extract_sender_info
    only touches .sender."""
    return SimpleNamespace(sender=sender)


class ExtractSenderInfoTests(unittest.TestCase):
    def test_no_sender_returns_all_none(self) -> None:
        info = extract_sender_info(_msg(sender=None))
        self.assertEqual(info, {"sender_name": None, "sender_username": None})

    def test_user_with_username(self) -> None:
        sender = SimpleNamespace(title=None, first_name="Andrey", username="andrey_k")
        info = extract_sender_info(_msg(sender=sender))
        self.assertEqual(info["sender_name"], "Andrey")
        self.assertEqual(info["sender_username"], "andrey_k")

    def test_user_without_username_is_none_not_empty_string(self) -> None:
        """Most people simply don't have a @ник — that's normal, not missing
        data, so the field must stay None rather than becoming ""."""
        sender = SimpleNamespace(title=None, first_name="Ivan", username=None)
        info = extract_sender_info(_msg(sender=sender))
        self.assertEqual(info["sender_name"], "Ivan")
        self.assertIsNone(info["sender_username"])

    def test_channel_sender_prefers_title(self) -> None:
        sender = SimpleNamespace(title="My Channel", first_name=None, username="mychannel")
        info = extract_sender_info(_msg(sender=sender))
        self.assertEqual(info["sender_name"], "My Channel")
        self.assertEqual(info["sender_username"], "mychannel")


class ConversionCarriesSenderUsernameTests(unittest.TestCase):
    def test_telegram_to_processed_carries_sender_username(self) -> None:
        msg = TelegramMessage(
            message_id=1,
            channel_id=100,
            sender_id=42,
            sender_name="Andrey",
            sender_username="andrey_k",
            text="hi",
            date=datetime(2026, 8, 31, tzinfo=timezone.utc),
        )
        processed = telegram_to_processed(msg, domain_id=uuid4())
        self.assertEqual(processed.sender_username, "andrey_k")

    def test_telegram_to_processed_none_username_stays_none(self) -> None:
        msg = TelegramMessage(
            message_id=2,
            channel_id=100,
            sender_id=43,
            sender_name="Ivan",
            sender_username=None,
            text="hi",
            date=datetime(2026, 8, 31, tzinfo=timezone.utc),
        )
        processed = telegram_to_processed(msg, domain_id=uuid4())
        self.assertIsNone(processed.sender_username)

    def test_pg_row_to_processed_carries_sender_username(self) -> None:
        row = {
            "id": uuid4(),
            "domain_id": uuid4(),
            "telegram_msg_id": 5,
            "sender_id": 42,
            "sender_name": "Andrey",
            "sender_username": "andrey_k",
            "content": "hi",
            "msg_date": datetime(2026, 8, 31, tzinfo=timezone.utc),
        }
        processed = pg_row_to_processed(row, channel_id=100)
        self.assertEqual(processed.sender_username, "andrey_k")

    def test_pg_row_to_processed_missing_column_defaults_to_none(self) -> None:
        """Old rows fetched before this migration have no sender_username key
        at all — must not raise, must default to None."""
        row = {
            "id": uuid4(),
            "domain_id": uuid4(),
            "telegram_msg_id": 6,
            "sender_id": 42,
            "sender_name": "Andrey",
            "content": "hi",
            "msg_date": datetime(2026, 8, 31, tzinfo=timezone.utc),
        }
        processed = pg_row_to_processed(row, channel_id=100)
        self.assertIsNone(processed.sender_username)


class FetchMessagesExposesSenderUsernameTests(unittest.IsolatedAsyncioTestCase):
    """fetch_messages() (memory_api/service.py) must surface sender_username
    so an agent asking "give me everyone's @ники in this chat" can get it
    without falling back to raw SQL."""

    async def test_sender_username_in_output(self) -> None:
        domain_id = uuid4()
        row = {
            "id": uuid4(),
            "domain_id": domain_id,
            "telegram_msg_id": 1,
            "sender_id": 42,
            "sender_name": "Andrey",
            "sender_username": "andrey_k",
            "content": "hi",
            "topic_id": None,
            "msg_date": datetime(2026, 8, 31, tzinfo=timezone.utc),
        }
        domain = {"id": domain_id, "channel_username": "somechat", "channel_id": 999}

        with patch.object(
            service, "_resolve_scope", AsyncMock(return_value=[domain_id]),
        ), patch.object(
            service.db_q, "get_messages_filtered", AsyncMock(return_value=[row]),
        ), patch.object(
            service.db_q, "get_domain", AsyncMock(return_value=domain),
        ):
            result = await service.fetch_messages(owner_id=1, scope="@somechat")

        self.assertEqual(len(result["messages"]), 1)
        self.assertEqual(result["messages"][0]["sender_username"], "andrey_k")

    async def test_sender_username_none_when_absent(self) -> None:
        domain_id = uuid4()
        row = {
            "id": uuid4(),
            "domain_id": domain_id,
            "telegram_msg_id": 2,
            "sender_id": 43,
            "sender_name": "Ivan",
            "sender_username": None,
            "content": "hi",
            "topic_id": None,
            "msg_date": datetime(2026, 8, 31, tzinfo=timezone.utc),
        }
        domain = {"id": domain_id, "channel_username": "somechat", "channel_id": 999}

        with patch.object(
            service, "_resolve_scope", AsyncMock(return_value=[domain_id]),
        ), patch.object(
            service.db_q, "get_messages_filtered", AsyncMock(return_value=[row]),
        ), patch.object(
            service.db_q, "get_domain", AsyncMock(return_value=domain),
        ):
            result = await service.fetch_messages(owner_id=1, scope="@somechat")

        self.assertIsNone(result["messages"][0]["sender_username"])


if __name__ == "__main__":
    unittest.main()
