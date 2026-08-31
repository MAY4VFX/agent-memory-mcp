"""Chat participants — issue #28: the agent must be able to answer "who's in
this chat", silent members included, and must never mistake a refusal to
enumerate (broadcast channel without admin rights, FloodWait, a 1:1 dialog)
for an empty chat. That exact confusion caused a ~24h incident on
2026-08-30 (see issue #28) — a plain empty list looked identical to "checked,
nobody there".

Covers:
- TelegramCollector.iter_participants() (collector/client.py): status/reason
  contract, using a fake Telethon client so no live Telegram call is made.
- _participant_is_admin(): duck-typed admin/creator detection.
- SyncScheduler._should_sync_participants(): the "first sync, then at most
  once/day" throttle (pure function, no I/O).
- SyncScheduler._sync_participants(): wires the collector result into
  domains.participants_sync_* without ever raising into the caller.
- memory_api.service.list_participants(): scope dedup + the
  unavailable_sources contract that carries the refusal reason through.
"""

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from telethon.errors import ChannelPrivateError, ChatAdminRequiredError, FloodWaitError

from agent_memory_mcp.collector.client import TelegramCollector, _participant_is_admin
from agent_memory_mcp.memory_api import service
from agent_memory_mcp.scheduler.scheduler import SyncScheduler, _should_sync_participants


def _collector_with_fake_client(iter_participants_impl=None, entity="ENTITY"):
    """A TelegramCollector instance backed by a fake Telethon client, with
    _resolve_entity stubbed so no real Telegram lookup happens."""
    c = TelegramCollector.__new__(TelegramCollector)
    fake_client = AsyncMock()
    if iter_participants_impl is not None:
        fake_client.iter_participants = iter_participants_impl
    c._client = fake_client
    c._resolve_entity = AsyncMock(return_value=entity)
    return c


def _user(user_id, username=None, first_name=None, last_name=None, bot=False, participant=None):
    from types import SimpleNamespace
    return SimpleNamespace(
        id=user_id, username=username, first_name=first_name, last_name=last_name,
        bot=bot, participant=participant,
    )


class ChannelParticipantAdmin:
    pass


class ChatParticipantCreator:
    pass


class ParticipantIsAdminTests(unittest.TestCase):
    def test_none_is_not_admin(self) -> None:
        self.assertFalse(_participant_is_admin(None))

    def test_regular_participant_type_is_not_admin(self) -> None:
        class ChannelParticipant:
            pass
        self.assertFalse(_participant_is_admin(ChannelParticipant()))

    def test_admin_participant_types_are_admin(self) -> None:
        self.assertTrue(_participant_is_admin(ChannelParticipantAdmin()))
        self.assertTrue(_participant_is_admin(ChatParticipantCreator()))


class IterParticipantsTests(unittest.IsolatedAsyncioTestCase):
    async def test_user_peer_type_is_not_applicable_without_telegram_call(self) -> None:
        """A 1:1 dialog has no membership concept — must be reported without
        ever touching the Telegram client."""
        c = _collector_with_fake_client()
        result = await c.iter_participants(channel_id=1, peer_type="user")
        self.assertEqual(result["status"], "not_applicable")
        self.assertEqual(result["participants"], [])
        c._resolve_entity.assert_not_called()

    async def test_success_collects_participants_including_one_without_username(self) -> None:
        async def _iter(entity):
            for u in [
                _user(1, username="andrey_k", first_name="Andrey"),
                _user(2, username=None, first_name="Ivan"),  # no @ник — must not be dropped
                _user(3, username="boss", first_name="Boss", participant=ChannelParticipantAdmin()),
            ]:
                yield u

        c = _collector_with_fake_client(iter_participants_impl=_iter)
        result = await c.iter_participants(channel_id=1, peer_type="channel")

        self.assertEqual(result["status"], "ok")
        self.assertIsNone(result["reason"])
        self.assertEqual(len(result["participants"]), 3)

        by_id = {p["user_id"]: p for p in result["participants"]}
        self.assertEqual(by_id[1]["username"], "andrey_k")
        self.assertIsNone(by_id[2]["username"])  # None, not dropped and not ""
        self.assertTrue(by_id[3]["is_admin"])
        self.assertFalse(by_id[1]["is_admin"])

    async def test_admin_required_is_forbidden_not_empty(self) -> None:
        """The core issue #28 requirement: a refusal must not look like an
        empty chat."""
        async def _iter(entity):
            raise ChatAdminRequiredError(request=None)
            yield  # pragma: no cover - unreachable, makes this an async gen

        c = _collector_with_fake_client(iter_participants_impl=_iter)
        result = await c.iter_participants(channel_id=1, peer_type="channel")

        self.assertEqual(result["status"], "forbidden")
        self.assertEqual(result["participants"], [])
        self.assertIsNotNone(result["reason"])
        self.assertIn("админ", result["reason"])

    async def test_channel_private_is_forbidden(self) -> None:
        async def _iter(entity):
            raise ChannelPrivateError(request=None)
            yield  # pragma: no cover

        c = _collector_with_fake_client(iter_participants_impl=_iter)
        result = await c.iter_participants(channel_id=1, peer_type="channel")
        self.assertEqual(result["status"], "forbidden")
        self.assertIsNotNone(result["reason"])

    async def test_flood_wait_is_error_with_seconds_in_reason(self) -> None:
        async def _iter(entity):
            raise FloodWaitError(request=None, capture=30)
            yield  # pragma: no cover

        c = _collector_with_fake_client(iter_participants_impl=_iter)
        result = await c.iter_participants(channel_id=1, peer_type="channel")
        self.assertEqual(result["status"], "error")
        self.assertIn("30", result["reason"])

    async def test_entity_resolution_failure_is_error_not_crash(self) -> None:
        c = _collector_with_fake_client()
        c._resolve_entity = AsyncMock(side_effect=ValueError("no cached entity"))
        result = await c.iter_participants(channel_id=1, peer_type="channel")
        self.assertEqual(result["status"], "error")
        self.assertIsNotNone(result["reason"])


class ShouldSyncParticipantsTests(unittest.TestCase):
    def test_never_synced_channel_syncs_now(self) -> None:
        self.assertTrue(_should_sync_participants(
            peer_type="channel", participants_synced_at=None,
        ))

    def test_recently_synced_channel_is_throttled(self) -> None:
        now = datetime(2026, 8, 31, tzinfo=timezone.utc)
        synced_at = now - timedelta(hours=1)
        self.assertFalse(_should_sync_participants(
            peer_type="channel", participants_synced_at=synced_at, now=now,
        ))

    def test_stale_sync_past_24h_runs_again(self) -> None:
        now = datetime(2026, 8, 31, tzinfo=timezone.utc)
        synced_at = now - timedelta(hours=25)
        self.assertTrue(_should_sync_participants(
            peer_type="channel", participants_synced_at=synced_at, now=now,
        ))

    def test_forbidden_status_is_still_retried_after_24h(self) -> None:
        """A source marked forbidden yesterday (e.g. admin rights not yet
        granted) must be retried, not permanently skipped."""
        now = datetime(2026, 8, 31, tzinfo=timezone.utc)
        synced_at = now - timedelta(hours=25)
        self.assertTrue(_should_sync_participants(
            peer_type="channel",
            participants_synced_at=synced_at,
            participants_sync_status="forbidden",
            now=now,
        ))

    def test_user_dialog_syncs_exactly_once(self) -> None:
        self.assertTrue(_should_sync_participants(
            peer_type="user", participants_synced_at=None, participants_sync_status=None,
        ))
        self.assertFalse(_should_sync_participants(
            peer_type="user",
            participants_synced_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
            participants_sync_status="not_applicable",
        ))


class SyncParticipantsMethodTests(unittest.IsolatedAsyncioTestCase):
    async def test_ok_result_is_upserted_and_recorded(self) -> None:
        domain_id = uuid4()
        domain = {
            "id": domain_id, "channel_id": 555, "peer_type": "channel",
            "channel_username": "chat", "participants_synced_at": None,
            "participants_sync_status": None,
        }
        collector = AsyncMock()
        collector.iter_participants = AsyncMock(return_value={
            "status": "ok",
            "participants": [{"user_id": 1, "username": "a", "first_name": "A",
                               "last_name": None, "is_bot": False, "is_admin": False}],
            "reason": None,
        })
        scheduler = SyncScheduler()

        with patch(
            "agent_memory_mcp.scheduler.scheduler.queries.bulk_upsert_participants",
            AsyncMock(),
        ) as upsert, patch(
            "agent_memory_mcp.scheduler.scheduler.queries.update_domain", AsyncMock(),
        ) as update:
            await scheduler._sync_participants(domain, collector)

        upsert.assert_awaited_once()
        update.assert_awaited_once()
        _, kwargs = update.call_args
        self.assertEqual(kwargs["participants_sync_status"], "ok")
        self.assertIsNone(kwargs["participants_sync_error"])

    async def test_forbidden_result_is_recorded_without_upsert(self) -> None:
        domain_id = uuid4()
        domain = {
            "id": domain_id, "channel_id": 555, "peer_type": "channel",
            "channel_username": "chat", "participants_synced_at": None,
            "participants_sync_status": None,
        }
        collector = AsyncMock()
        collector.iter_participants = AsyncMock(return_value={
            "status": "forbidden", "participants": [], "reason": "нужны права администратора",
        })
        scheduler = SyncScheduler()

        with patch(
            "agent_memory_mcp.scheduler.scheduler.queries.bulk_upsert_participants",
            AsyncMock(),
        ) as upsert, patch(
            "agent_memory_mcp.scheduler.scheduler.queries.update_domain", AsyncMock(),
        ) as update:
            await scheduler._sync_participants(domain, collector)

        upsert.assert_not_called()
        _, kwargs = update.call_args
        self.assertEqual(kwargs["participants_sync_status"], "forbidden")
        self.assertEqual(kwargs["participants_sync_error"], "нужны права администратора")

    async def test_throttled_skips_without_calling_collector(self) -> None:
        domain = {
            "id": uuid4(), "channel_id": 555, "peer_type": "channel",
            "channel_username": "chat",
            "participants_synced_at": datetime.now(timezone.utc),
            "participants_sync_status": "ok",
        }
        collector = AsyncMock()
        scheduler = SyncScheduler()
        await scheduler._sync_participants(domain, collector)
        collector.iter_participants.assert_not_called()

    async def test_collector_exception_does_not_propagate(self) -> None:
        """A participants failure must never abort the caller (message sync)."""
        domain = {
            "id": uuid4(), "channel_id": 555, "peer_type": "channel",
            "channel_username": "chat", "participants_synced_at": None,
            "participants_sync_status": None,
        }
        collector = AsyncMock()
        collector.iter_participants = AsyncMock(side_effect=RuntimeError("boom"))
        scheduler = SyncScheduler()

        with patch(
            "agent_memory_mcp.scheduler.scheduler.queries.update_domain", AsyncMock(),
        ) as update:
            await scheduler._sync_participants(domain, collector)  # must not raise

        _, kwargs = update.call_args
        self.assertEqual(kwargs["participants_sync_status"], "error")


class ListParticipantsServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_dedups_across_chats_and_merges_username(self) -> None:
        d1, d2 = uuid4(), uuid4()
        domains = {
            d1: {"id": d1, "channel_username": "chat1", "participants_sync_status": "ok"},
            d2: {"id": d2, "channel_username": "chat2", "participants_sync_status": "ok"},
        }
        rows_by_domain = {
            d1: [{"user_id": 1, "username": None, "first_name": "Ivan",
                  "last_name": None, "is_bot": False}],
            d2: [{"user_id": 1, "username": "ivan_k", "first_name": "Ivan",
                  "last_name": None, "is_bot": False}],
        }

        async def _get_domain(engine, did):
            return domains[did]

        async def _list_participants(engine, did):
            return rows_by_domain[did]

        with patch.object(
            service, "_resolve_scope", AsyncMock(return_value=[d1, d2]),
        ), patch.object(
            service.db_q, "get_domain", AsyncMock(side_effect=_get_domain),
        ), patch.object(
            service.db_q, "list_participants", AsyncMock(side_effect=_list_participants),
        ):
            result = await service.list_participants(owner_id=1, scope="all")

        self.assertEqual(result["count"], 1)
        person = result["participants"][0]
        self.assertEqual(person["user_id"], 1)
        self.assertEqual(person["username"], "ivan_k")  # merged from chat2
        self.assertEqual(sorted(person["channels"]), ["@chat1", "@chat2"])
        self.assertEqual(result["unavailable_sources"], [])

    async def test_forbidden_source_is_reported_not_silently_empty(self) -> None:
        d1 = uuid4()
        domain = {
            "id": d1, "channel_username": "broadcast",
            "participants_sync_status": "forbidden",
            "participants_sync_error": "нужны права администратора",
        }
        with patch.object(
            service, "_resolve_scope", AsyncMock(return_value=[d1]),
        ), patch.object(
            service.db_q, "get_domain", AsyncMock(return_value=domain),
        ), patch.object(
            service.db_q, "list_participants", AsyncMock(),
        ) as list_p:
            result = await service.list_participants(owner_id=1, scope="@broadcast")

        list_p.assert_not_called()
        self.assertEqual(result["participants"], [])
        self.assertEqual(result["count"], 0)
        self.assertEqual(len(result["unavailable_sources"]), 1)
        self.assertEqual(result["unavailable_sources"][0]["status"], "forbidden")
        self.assertEqual(
            result["unavailable_sources"][0]["reason"], "нужны права администратора",
        )

    async def test_never_synced_source_is_reported_as_not_yet_synced(self) -> None:
        d1 = uuid4()
        domain = {"id": d1, "channel_username": "fresh", "participants_sync_status": None}
        with patch.object(
            service, "_resolve_scope", AsyncMock(return_value=[d1]),
        ), patch.object(
            service.db_q, "get_domain", AsyncMock(return_value=domain),
        ):
            result = await service.list_participants(owner_id=1, scope="@fresh")

        self.assertEqual(result["participants"], [])
        self.assertEqual(result["unavailable_sources"][0]["status"], "not_yet_synced")


if __name__ == "__main__":
    unittest.main()
