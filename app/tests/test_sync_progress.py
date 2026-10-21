"""Live sync status after «Add Source».

Real case: @tammba was added, the bot said «sync starts within 30 seconds»,
then nothing — the source sat behind 50+ overdue hourly re-syncs (3 run at a
time, queue ordered by next_sync_at) and the user couldn't tell queued from
stuck.
"""

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from sqlalchemy.dialects import postgresql

from agent_memory_mcp.bot import sync_progress as sp
from agent_memory_mcp.db import queries

NOW = datetime.now(timezone.utc)


def _job(status: str, **kw) -> dict:
    return {"status": status, "created_at": NOW, **kw}


class RenderTests(unittest.TestCase):
    def test_queued_shows_place_in_queue(self) -> None:
        text, done = sp.render_status("@tammba", None, queue_ahead=4)
        self.assertIn("queued", text)
        self.assertIn("4 ahead", text)
        self.assertFalse(done)

    def test_downloading(self) -> None:
        text, done = sp.render_status("@tammba", _job("running"))
        self.assertIn("downloading", text)
        self.assertFalse(done)

    def test_running_with_default_zero_is_downloading(self) -> None:
        # Real case: @tammba showed «processing 0 messages» while 3102 were
        # being downloaded (sync_jobs.messages_fetched server_default 0).
        text, done = sp.render_status("@tammba", _job("running", messages_fetched=0))
        self.assertIn("downloading", text)
        self.assertFalse(done)

    def test_processing_shows_count(self) -> None:
        text, done = sp.render_status("@tammba", _job("running", messages_fetched=865))
        self.assertIn("processing 865 messages", text)
        self.assertFalse(done)

    def test_done_shows_totals(self) -> None:
        text, done = sp.render_status(
            "@tammba", _job("completed", messages_processed=865, entities_extracted=402)
        )
        self.assertIn("865 messages, 402 entities", text)
        self.assertTrue(done)

    def test_failed_shows_reason(self) -> None:
        text, done = sp.render_status("@tammba", _job("failed", error_message="FloodWait <30s>"))
        self.assertIn("FloodWait &lt;30s&gt;", text)
        self.assertTrue(done)


class QueueOrderTests(unittest.IsolatedAsyncioTestCase):
    async def test_never_synced_sources_go_first(self) -> None:
        captured = {}

        class Conn:
            async def execute(self, stmt):
                captured["sql"] = str(stmt.compile(dialect=postgresql.dialect()))
                res = MagicMock()
                res.mappings.return_value.all.return_value = []
                return res

        engine = MagicMock()
        engine.begin.return_value.__aenter__ = AsyncMock(return_value=Conn())
        engine.begin.return_value.__aexit__ = AsyncMock(return_value=False)

        await queries.get_domains_for_sync(engine)

        order = captured["sql"].split("ORDER BY")[1]
        self.assertLess(order.index("last_synced_at IS NULL"), order.index("next_sync_at"))


class TrackTests(unittest.IsolatedAsyncioTestCase):
    async def test_message_is_edited_through_stages_until_done(self) -> None:
        did = uuid4()
        domain = {"id": did, "channel_username": "tammba", "created_at": NOW - timedelta(seconds=1),
                  "next_sync_at": NOW, "last_synced_at": None}
        jobs = [
            None,
            _job("running"),
            _job("running", messages_fetched=120),
            _job("completed", messages_processed=120, entities_extracted=30),
        ]
        msg = MagicMock()
        msg.edit_text = AsyncMock()
        bot = MagicMock()
        bot.send_message = AsyncMock(return_value=msg)

        with patch.object(sp.db_q, "get_domain", AsyncMock(return_value=domain)), \
                patch.object(sp.db_q, "get_latest_sync_job", AsyncMock(side_effect=jobs)), \
                patch.object(sp.db_q, "count_sync_queue_ahead", AsyncMock(return_value=2)), \
                patch.object(sp, "POLL_SECONDS", 0):
            await sp._track(bot, 42, 1279475, did)

        first = bot.send_message.await_args
        self.assertIn("2 ahead", first.args[1])
        self.assertEqual(first.kwargs["message_thread_id"], 1279475)
        edits = [c.args[0] for c in msg.edit_text.await_args_list]
        self.assertEqual(len(edits), 3)
        self.assertIn("120 messages, 30 entities", edits[-1])


if __name__ == "__main__":
    unittest.main()
