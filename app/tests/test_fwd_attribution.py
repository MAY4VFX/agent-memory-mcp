"""Forward attribution — issue #24: a forwarded message must keep its
original author/channel through raw_json -> TelegramMessage -> ProcessedMessage,
instead of the agent crediting whoever forwarded it.

Covers:
- extract_fwd_info() (collector/client.py): the Telethon-facing extraction,
  using cheap duck-typed stand-ins for Message/Forward/MessageFwdHeader so
  the test doesn't need a live Telethon client.
- telegram_to_processed() (models/messages.py): the conversion that used to
  silently drop forward attribution.
- resolve_sender_label() / ProcessedMessage.attribution: the context-facing
  label that must show the original source, not the forwarder.
"""

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

from agent_memory_mcp.collector.client import extract_fwd_info
from agent_memory_mcp.models.messages import (
    ProcessedMessage,
    TelegramMessage,
    resolve_sender_label,
    telegram_to_processed,
)


def _msg(fwd_from=None, forward=None):
    """Minimal stand-in for a Telethon custom.Message — extract_fwd_info only
    touches .fwd_from and .forward."""
    return SimpleNamespace(fwd_from=fwd_from, forward=forward)


class ExtractFwdInfoTests(unittest.TestCase):
    def test_no_forward_returns_all_none(self) -> None:
        info = extract_fwd_info(_msg(fwd_from=None, forward=None))
        self.assertEqual(
            info,
            {
                "fwd_from_id": None,
                "fwd_from_name": None,
                "fwd_from_username": None,
                "fwd_date": None,
            },
        )

    def test_forward_from_user(self) -> None:
        fwd_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        fwd_from = SimpleNamespace(from_name=None, post_author=None, date=fwd_date)
        sender = SimpleNamespace(title=None, first_name="Ivan", username="ivan123")
        forward = SimpleNamespace(sender=sender, chat=None, sender_id=111, chat_id=None)

        info = extract_fwd_info(_msg(fwd_from=fwd_from, forward=forward))

        self.assertEqual(info["fwd_from_id"], 111)
        self.assertEqual(info["fwd_from_name"], "Ivan")
        self.assertEqual(info["fwd_from_username"], "ivan123")
        self.assertEqual(info["fwd_date"], fwd_date)

    def test_forward_from_channel(self) -> None:
        fwd_date = datetime(2026, 2, 2, tzinfo=timezone.utc)
        fwd_from = SimpleNamespace(from_name=None, post_author=None, date=fwd_date)
        chat = SimpleNamespace(title="My Channel", first_name=None, username="mychannel")
        forward = SimpleNamespace(sender=None, chat=chat, sender_id=None, chat_id=222)

        info = extract_fwd_info(_msg(fwd_from=fwd_from, forward=forward))

        self.assertEqual(info["fwd_from_id"], 222)
        self.assertEqual(info["fwd_from_name"], "My Channel")
        self.assertEqual(info["fwd_from_username"], "mychannel")
        self.assertEqual(info["fwd_date"], fwd_date)

    def test_forward_from_hidden_account_only_from_name(self) -> None:
        fwd_date = datetime(2026, 3, 3, tzinfo=timezone.utc)
        fwd_from = SimpleNamespace(
            from_name="Hidden Sender", post_author=None, date=fwd_date,
        )
        # Telethon still builds a Forward wrapper even when from_id is None —
        # sender/chat/sender_id/chat_id all stay unresolved.
        forward = SimpleNamespace(sender=None, chat=None, sender_id=None, chat_id=None)

        info = extract_fwd_info(_msg(fwd_from=fwd_from, forward=forward))

        self.assertIsNone(info["fwd_from_id"])
        self.assertEqual(info["fwd_from_name"], "Hidden Sender")
        self.assertIsNone(info["fwd_from_username"])
        self.assertEqual(info["fwd_date"], fwd_date)

    def test_missing_forward_attr_does_not_crash(self) -> None:
        # msg.forward absent entirely (defensive: getattr fallback).
        fwd_date = datetime(2026, 4, 4, tzinfo=timezone.utc)
        fwd_from = SimpleNamespace(
            from_name=None, post_author="Channel Signature", date=fwd_date,
        )
        msg = SimpleNamespace(fwd_from=fwd_from)  # no .forward attribute at all

        info = extract_fwd_info(msg)

        self.assertIsNone(info["fwd_from_id"])
        self.assertEqual(info["fwd_from_name"], "Channel Signature")
        self.assertIsNone(info["fwd_from_username"])


class TelegramToProcessedTests(unittest.TestCase):
    def _base_kwargs(self, **overrides) -> dict:
        kwargs = dict(
            message_id=42,
            channel_id=100,
            sender_id=999,
            sender_name="Forwarder Name",
            text="hello",
            date=datetime(2026, 5, 5, tzinfo=timezone.utc),
        )
        kwargs.update(overrides)
        return kwargs

    def test_regular_message_has_no_fwd_fields(self) -> None:
        msg = TelegramMessage(**self._base_kwargs())
        processed = telegram_to_processed(msg, uuid4())

        self.assertIsNone(processed.fwd_from_id)
        self.assertIsNone(processed.fwd_from_name)
        self.assertIsNone(processed.fwd_from_username)
        self.assertIsNone(processed.fwd_date)
        self.assertEqual(processed.attribution, "Forwarder Name")

    def test_forward_attribution_survives_conversion(self) -> None:
        fwd_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        msg = TelegramMessage(
            **self._base_kwargs(
                fwd_from_id=555,
                fwd_from_name="Original Author",
                fwd_from_username="original",
                fwd_date=fwd_date,
            )
        )
        processed = telegram_to_processed(msg, uuid4())

        self.assertEqual(processed.fwd_from_id, 555)
        self.assertEqual(processed.fwd_from_name, "Original Author")
        self.assertEqual(processed.fwd_from_username, "original")
        self.assertEqual(processed.fwd_date, fwd_date)
        # The agent-facing label must credit the original author, not the
        # forwarder (sender_name="Forwarder Name" must NOT win here).
        self.assertEqual(processed.attribution, "Original Author (@original, переслано)")

    def test_forward_from_hidden_account_survives_conversion(self) -> None:
        msg = TelegramMessage(
            **self._base_kwargs(
                fwd_from_name="Hidden Sender",
                fwd_from_id=None,
                fwd_from_username=None,
            )
        )
        processed = telegram_to_processed(msg, uuid4())

        self.assertEqual(processed.fwd_from_name, "Hidden Sender")
        self.assertIsNone(processed.fwd_from_id)
        self.assertEqual(processed.attribution, "Hidden Sender (переслано)")


class ResolveSenderLabelTests(unittest.TestCase):
    def test_no_forward_falls_back_to_sender(self) -> None:
        self.assertEqual(resolve_sender_label("Alice", None, None), "Alice")

    def test_nothing_known_returns_none(self) -> None:
        self.assertIsNone(resolve_sender_label(None, None, None))

    def test_forward_with_username(self) -> None:
        label = resolve_sender_label("Forwarder", "Original Channel", "origchannel")
        self.assertEqual(label, "Original Channel (@origchannel, переслано)")

    def test_forward_without_username(self) -> None:
        label = resolve_sender_label("Forwarder", "Hidden Sender", None)
        self.assertEqual(label, "Hidden Sender (переслано)")


class ProcessedMessageAttributionEdgeCaseTests(unittest.TestCase):
    def test_forward_with_no_resolvable_name_falls_back_to_sender(self) -> None:
        """Rare edge case: fwd header present but neither from_name nor the
        entity could be resolved (e.g. a since-deleted account) — attribution
        must not silently disappear, so it falls back to sender_name."""
        processed = ProcessedMessage(
            domain_id=uuid4(),
            message_id=1,
            channel_id=100,
            sender_name="Forwarder Name",
            text="hi",
            date=datetime(2026, 6, 6, tzinfo=timezone.utc),
            fwd_from_id=777,
            fwd_from_name=None,
            fwd_from_username=None,
        )
        self.assertEqual(processed.attribution, "Forwarder Name")


if __name__ == "__main__":
    unittest.main()
