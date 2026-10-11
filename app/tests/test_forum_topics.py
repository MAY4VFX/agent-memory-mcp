"""Real forum topic titles (migration 023).

Before: list_topics derived a topic's "name" from the content of its root
message. For a real forum topic the root is a Telegram service message
("topic created") that is never stored, so names came back null (chat «ЦУП 2»,
topics 5/6/7/902), and for the rest the "name" was just the first reply. An
extractor agent couldn't tell that topic 5 and topic 6 are different projects.

Covers:
- TelegramCollector.fetch_forum_topics(): status contract, title parsing,
  not-a-forum short-circuit, FloodWait → error (fake Telethon client).
- queries.upsert_forum_topics(): keyed upsert; a rename rewrites title and
  bumps updated_at, an unchanged row is left alone (compiled SQL check).
- _should_sync_topics(): throttle (pure function).
- SyncScheduler._sync_forum_topics(): stores titles + status, never raises.
- service.list_topics(): prefers the real title, marks name_source.
- service.fetch_messages() / service.get_activity(): topic_name next to topic_id.
"""

import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from sqlalchemy.dialects import postgresql
from telethon.errors import FloodWaitError

from agent_memory_mcp.collector.client import TelegramCollector
from agent_memory_mcp.db import queries
from agent_memory_mcp.memory_api import service
from agent_memory_mcp.scheduler.scheduler import SyncScheduler, _should_sync_topics

CH = 4343416506  # «ЦУП 2»


def _collector(entity, call_impl=None):
    c = TelegramCollector.__new__(TelegramCollector)
    c._client = AsyncMock(side_effect=call_impl) if call_impl else AsyncMock()
    c._resolve_entity = AsyncMock(return_value=entity)
    return c


def _topic(tid, title, top_message=None, closed=False):
    return SimpleNamespace(
        id=tid, title=title, top_message=top_message or tid,
        date=datetime(2026, 9, 1, tzinfo=timezone.utc), closed=closed,
        title_missing=False,
    )


# ---------------------------------------------------------------- collector


class FetchForumTopicsTests(unittest.IsolatedAsyncioTestCase):
    async def test_basic_group_is_not_applicable_without_telegram_call(self) -> None:
        c = _collector(entity=SimpleNamespace(forum=False))
        res = await c.fetch_forum_topics(channel_id=1, peer_type="chat")
        self.assertEqual(res["status"], "not_applicable")
        c._resolve_entity.assert_not_called()
        c._client.assert_not_called()

    async def test_non_forum_supergroup_is_not_applicable(self) -> None:
        c = _collector(entity=SimpleNamespace(forum=False))
        res = await c.fetch_forum_topics(channel_id=1)
        self.assertEqual(res["status"], "not_applicable")
        c._client.assert_not_called()

    async def test_forum_returns_real_titles(self) -> None:
        deleted = SimpleNamespace(id=9)  # ForumTopicDeleted: no title
        page = SimpleNamespace(
            topics=[_topic(5, "Проект Альфа"), _topic(6, "Проект Бета", closed=True), deleted],
            messages=[],
        )
        c = _collector(entity=SimpleNamespace(forum=True), call_impl=AsyncMock(return_value=page))
        res = await c.fetch_forum_topics(channel_id=CH)

        self.assertEqual(res["status"], "ok")
        by_id = {t["topic_id"]: t for t in res["topics"]}
        self.assertEqual(by_id[5]["title"], "Проект Альфа")
        self.assertEqual(by_id[6]["title"], "Проект Бета")
        self.assertTrue(by_id[6]["is_closed"])
        self.assertNotIn(9, by_id)
        self.assertEqual(c._client.await_count, 1)  # short page → no second request

    async def test_flood_wait_is_error_not_empty(self) -> None:
        async def _boom(_req):
            raise FloodWaitError(request=None, capture=3600)

        c = _collector(entity=SimpleNamespace(forum=True), call_impl=_boom)
        res = await c.fetch_forum_topics(channel_id=CH)
        self.assertEqual(res["status"], "error")
        self.assertEqual(res["topics"], [])
        self.assertIn("flood", res["reason"])


# ---------------------------------------------------------------- storage


class _CaptureConn:
    def __init__(self, sink):
        self.sink = sink

    async def execute(self, stmt, params=None):
        self.sink.append((stmt, params))


class _CaptureEngine:
    def __init__(self):
        self.calls = []

    def begin(self):
        engine = self

        class _Ctx:
            async def __aenter__(self_inner):
                return _CaptureConn(engine.calls)

            async def __aexit__(self_inner, *a):
                return False

        return _Ctx()


class UpsertForumTopicsTests(unittest.IsolatedAsyncioTestCase):
    async def test_upsert_keyed_by_chat_and_topic_and_rename_updates_title(self) -> None:
        eng = _CaptureEngine()
        await queries.upsert_forum_topics(eng, CH, [
            {"topic_id": 5, "title": "Проект Альфа (переименован)"},
            {"topic_id": 6, "title": ""},  # empty title is skipped, not stored
        ])
        self.assertEqual(len(eng.calls), 1)
        stmt, rows = eng.calls[0]
        self.assertEqual(rows, [{
            "channel_id": CH, "topic_id": 5,
            "title": "Проект Альфа (переименован)", "is_closed": False,
        }])
        sql = str(stmt.compile(dialect=postgresql.dialect())).lower()
        self.assertIn("insert into forum_topics", sql)
        self.assertIn("on conflict (channel_id, topic_id) do update", sql)
        # Rename → new title + updated_at bump…
        self.assertIn("title = excluded.title", sql)
        self.assertIn("updated_at = now()", sql)
        # …but an unchanged title is left alone (updated_at = last rename).
        self.assertIn("forum_topics.title is distinct from excluded.title", sql)

    async def test_nothing_to_store_does_not_touch_db(self) -> None:
        eng = _CaptureEngine()
        await queries.upsert_forum_topics(eng, CH, [])
        self.assertEqual(eng.calls, [])


# ---------------------------------------------------------------- throttle


class ShouldSyncTopicsTests(unittest.TestCase):
    now = datetime(2026, 10, 11, 12, tzinfo=timezone.utc)

    def _s(self, **kw):
        kw.setdefault("peer_type", "channel")
        kw.setdefault("now", self.now)
        return _should_sync_topics(**kw)

    def test_never_synced_runs_now_backfill(self) -> None:
        self.assertTrue(self._s(synced_at=None, status=None))

    def test_basic_group_and_dm_never(self) -> None:
        self.assertFalse(self._s(peer_type="chat", synced_at=None, status=None))
        self.assertFalse(self._s(peer_type="user", synced_at=None, status=None))

    def test_forum_refreshes_every_6h(self) -> None:
        self.assertFalse(self._s(synced_at=self.now - timedelta(hours=1), status="ok"))
        self.assertTrue(self._s(synced_at=self.now - timedelta(hours=7), status="ok"))

    def test_unknown_topic_shortens_throttle(self) -> None:
        self.assertTrue(self._s(
            synced_at=self.now - timedelta(minutes=20), status="ok", has_unknown_topics=True,
        ))
        self.assertFalse(self._s(
            synced_at=self.now - timedelta(minutes=5), status="ok", has_unknown_topics=True,
        ))

    def test_not_forum_rechecks_daily_and_error_hourly(self) -> None:
        self.assertFalse(self._s(synced_at=self.now - timedelta(hours=7), status="not_applicable"))
        self.assertTrue(self._s(synced_at=self.now - timedelta(hours=25), status="not_applicable"))
        self.assertFalse(self._s(synced_at=self.now - timedelta(minutes=30), status="error"))
        self.assertTrue(self._s(synced_at=self.now - timedelta(hours=2), status="error"))


class SchedulerSyncForumTopicsTests(unittest.IsolatedAsyncioTestCase):
    def _domain(self, **over):
        d = {"id": uuid4(), "channel_id": CH, "peer_type": "channel",
             "channel_username": None, "forum_topics_synced_at": None,
             "forum_topics_sync_status": None}
        d.update(over)
        return d

    async def test_stores_titles_and_status(self) -> None:
        collector = SimpleNamespace(fetch_forum_topics=AsyncMock(return_value={
            "status": "ok", "reason": None,
            "topics": [{"topic_id": 5, "title": "Проект Альфа"}],
        }))
        d = self._domain()
        with patch.object(queries, "upsert_forum_topics", AsyncMock()) as up, \
             patch.object(queries, "update_domain", AsyncMock()) as upd:
            await SyncScheduler()._sync_forum_topics(d, collector, topic_ids={5})
        up.assert_awaited_once()
        self.assertEqual(up.await_args.args[1], CH)
        kw = upd.await_args.kwargs
        self.assertEqual(kw["forum_topics_sync_status"], "ok")
        self.assertIsNotNone(kw["forum_topics_synced_at"])

    async def test_new_topic_triggers_refresh_inside_6h(self) -> None:
        collector = SimpleNamespace(fetch_forum_topics=AsyncMock(return_value={
            "status": "ok", "reason": None, "topics": [],
        }))
        d = self._domain(
            forum_topics_synced_at=datetime.now(timezone.utc) - timedelta(minutes=30),
            forum_topics_sync_status="ok",
        )
        with patch.object(queries, "get_known_topic_ids", AsyncMock(return_value={5})), \
             patch.object(queries, "upsert_forum_topics", AsyncMock()), \
             patch.object(queries, "update_domain", AsyncMock()):
            await SyncScheduler()._sync_forum_topics(d, collector, topic_ids={5})
            collector.fetch_forum_topics.assert_not_awaited()  # all known → throttled
            await SyncScheduler()._sync_forum_topics(d, collector, topic_ids={5, 902})
            collector.fetch_forum_topics.assert_awaited_once()  # 902 is new

    async def test_failure_never_raises(self) -> None:
        collector = SimpleNamespace(fetch_forum_topics=AsyncMock(side_effect=RuntimeError("x")))
        with patch.object(queries, "update_domain", AsyncMock()):
            await SyncScheduler()._sync_forum_topics(self._domain(), collector)


# ---------------------------------------------------------------- read paths


def _dom(did):
    return {"id": did, "channel_id": CH, "channel_username": None,
            "channel_name": "ЦУП 2", "display_name": "ЦУП 2",
            "peer_type": "channel", "monitoring": False}


class ListTopicsTests(unittest.IsolatedAsyncioTestCase):
    async def test_prefers_real_title_and_marks_source(self) -> None:
        did = uuid4()
        now = datetime(2026, 10, 1, tzinfo=timezone.utc)
        rows = [
            # real title known, root message absent (service message) → telegram
            {"topic_id": 5, "domain_id": did, "message_count": 10, "first_date": now,
             "last_date": now, "topic_name": None, "topic_title": "Проект Альфа"},
            # real title wins even over a stored first message
            {"topic_id": 6, "domain_id": did, "message_count": 3, "first_date": now,
             "last_date": now, "topic_name": "привет всем", "topic_title": "Проект Бета"},
            # no title yet → legacy fallback, marked as such
            {"topic_id": 7, "domain_id": did, "message_count": 2, "first_date": now,
             "last_date": now, "topic_name": "первое\nсообщение", "topic_title": None},
            # nothing at all
            {"topic_id": 902, "domain_id": did, "message_count": 1, "first_date": now,
             "last_date": now, "topic_name": None, "topic_title": None},
        ]
        with patch.object(service, "_resolve_scope", AsyncMock(return_value=[did])), \
             patch.object(service.db_q, "get_domain", AsyncMock(return_value=_dom(did))), \
             patch.object(service.db_q, "get_topics", AsyncMock(return_value=rows)):
            out = await service.list_topics(owner_id=1, scope="all")

        by_id = {t["topic_id"]: t for t in out["topics"]}
        self.assertEqual((by_id[5]["name"], by_id[5]["name_source"]), ("Проект Альфа", "telegram"))
        self.assertEqual((by_id[6]["name"], by_id[6]["name_source"]), ("Проект Бета", "telegram"))
        self.assertEqual((by_id[7]["name"], by_id[7]["name_source"]), ("первое сообщение", "first_message"))
        self.assertEqual((by_id[902]["name"], by_id[902]["name_source"]), (None, None))
        self.assertEqual(by_id[5]["channel_id"], CH)


def _msg_row(did, topic_id, tg_id):
    return {
        "id": uuid4(), "domain_id": did, "telegram_msg_id": tg_id,
        "reply_to_msg_id": None, "topic_id": topic_id, "thread_id": None,
        "sender_id": 42, "sender_name": "Andrey", "sender_username": None,
        "content": "текст", "content_type": "text", "char_len": 5,
        "msg_date": datetime(2026, 10, 1, tzinfo=timezone.utc), "read_at": None,
        "fwd_from_id": None, "fwd_from_name": None, "fwd_from_username": None,
    }


class TopicNameInReadPathsTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_messages_carries_topic_name(self) -> None:
        did = uuid4()
        rows = [_msg_row(did, 5, 100), _msg_row(did, 902, 101), _msg_row(did, None, 102)]
        titles = AsyncMock(return_value={(CH, 5): "Проект Альфа"})
        with patch.object(service, "_resolve_scope", AsyncMock(return_value=[did])), \
             patch.object(service.db_q, "get_messages_filtered", AsyncMock(return_value=rows)), \
             patch.object(service.db_q, "get_domain", AsyncMock(return_value=_dom(did))), \
             patch.object(service.db_q, "get_topic_titles", titles):
            out = await service.fetch_messages(owner_id=1, scope="all")

        names = [(m["topic_id"], m["topic_name"]) for m in out["messages"]]
        self.assertEqual(names, [(5, "Проект Альфа"), (902, None), (None, None)])
        self.assertEqual(titles.await_args.args[1], [CH])

    async def test_activity_feed_carries_topic_name(self) -> None:
        did = uuid4()
        rows = [_msg_row(did, 6, 200), _msg_row(did, None, 201)]
        with patch.object(service, "_resolve_scope", AsyncMock(return_value=[did])), \
             patch.object(service.db_q, "list_domains", AsyncMock(return_value=[_dom(did)])), \
             patch.object(service.db_ql, "get_domain_label_map", AsyncMock(return_value={})), \
             patch.object(service.db_q, "get_activity_events", AsyncMock(return_value=rows)), \
             patch.object(service.db_q, "get_topic_titles",
                          AsyncMock(return_value={(CH, 6): "Проект Бета"})):
            out = await service.get_activity(
                owner_id=1, since=datetime(2026, 9, 1, tzinfo=timezone.utc),
            )

        ev = out["events"]
        self.assertEqual((ev[0]["topic_id"], ev[0]["topic_name"]), (6, "Проект Бета"))
        self.assertEqual((ev[1]["topic_id"], ev[1]["topic_name"]), (None, None))
        self.assertEqual(ev[0]["chat_id"], CH)

    async def test_title_lookup_failure_degrades_to_null(self) -> None:
        did = uuid4()
        with patch.object(service, "_resolve_scope", AsyncMock(return_value=[did])), \
             patch.object(service.db_q, "get_messages_filtered",
                          AsyncMock(return_value=[_msg_row(did, 5, 1)])), \
             patch.object(service.db_q, "get_domain", AsyncMock(return_value=_dom(did))), \
             patch.object(service.db_q, "get_topic_titles", AsyncMock(side_effect=RuntimeError())):
            out = await service.fetch_messages(owner_id=1, scope="all")
        self.assertIsNone(out["messages"][0]["topic_name"])


if __name__ == "__main__":
    unittest.main()
