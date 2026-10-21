"""Sync frequency on source / folder cards: shown and editable from the bot.

The cadence lived in domains/domain_groups.sync_frequency_minutes but the bot
never showed it nor let the user change it.
"""

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from sqlalchemy.dialects import postgresql

from agent_memory_mcp.bot import sync_frequency as sf
from agent_memory_mcp.db import queries

NOW = datetime(2026, 10, 21, 20, 0, tzinfo=timezone.utc)


class FormatTests(unittest.TestCase):
    def test_freq(self) -> None:
        self.assertEqual(sf.fmt_freq(15), "15 min")
        self.assertEqual(sf.fmt_freq(60), "1 h")
        self.assertEqual(sf.fmt_freq(1440), "24 h")
        self.assertEqual(sf.fmt_freq(None), "1 h")

    def test_next(self) -> None:
        self.assertEqual(sf.fmt_next(NOW + timedelta(minutes=23), NOW), "in 23 min")
        self.assertEqual(sf.fmt_next(NOW + timedelta(minutes=150), NOW), "in 2 h 30 min")
        self.assertEqual(sf.fmt_next(NOW - timedelta(minutes=5), NOW), "soon")
        self.assertEqual(sf.fmt_next(None, NOW), "—")

    def test_folder_mixed_when_members_differ(self) -> None:
        self.assertEqual(
            sf.folder_freq({}, [{"sync_frequency_minutes": 60}, {"sync_frequency_minutes": 15}]), "mixed"
        )
        self.assertEqual(sf.folder_freq({}, [{"sync_frequency_minutes": 180}] * 3), "3 h")

    def test_picker_marks_current(self) -> None:
        kb = sf.picker_kb("sfreq_set", "x", 180, "src:view:x")
        texts = [b.text for row in kb.inline_keyboard for b in row]
        self.assertIn("✅ 3 h", texts)
        self.assertEqual(sum(t.startswith("✅") for t in texts), 1)

    def test_only_offered_values_accepted(self) -> None:
        self.assertEqual(sf._parse_minutes("15"), 15)
        self.assertIsNone(sf._parse_minutes("5"))
        self.assertIsNone(sf._parse_minutes("abc"))


class RescheduleSqlTests(unittest.IsolatedAsyncioTestCase):
    async def test_next_sync_from_last_sync_plus_new_frequency(self) -> None:
        captured = {}

        class Conn:
            async def execute(self, stmt):
                captured["sql"] = str(stmt.compile(dialect=postgresql.dialect()))

        engine = MagicMock()
        engine.begin.return_value.__aenter__ = AsyncMock(return_value=Conn())
        engine.begin.return_value.__aexit__ = AsyncMock(return_value=False)

        await queries.reschedule_from_last_sync(engine, [uuid4()])

        sql = captured["sql"]
        self.assertIn("next_sync_at=(domains.last_synced_at + make_interval(", sql)
        self.assertIn("domains.sync_frequency_minutes", sql)
        self.assertIn("domains.last_synced_at IS NOT NULL", sql)


class SourceHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_set_updates_frequency_and_reschedules(self) -> None:
        did = uuid4()
        cb = MagicMock()
        cb.data = f"sfreq_set:{did}:180"
        cb.from_user.id = 42
        domain = {"id": did, "owner_id": 42}
        with patch.object(sf.db_q, "get_domain", AsyncMock(return_value=domain)), \
                patch.object(sf.db_q, "update_domain", AsyncMock()) as upd, \
                patch.object(sf.db_q, "reschedule_from_last_sync", AsyncMock()) as resched, \
                patch("agent_memory_mcp.bot.handlers.forum._render_source_view", AsyncMock()) as render:
            await sf.cb_source_freq_set(cb)

        upd.assert_awaited_once_with(sf.async_engine, did, sync_frequency_minutes=180)
        resched.assert_awaited_once_with(sf.async_engine, [did])
        render.assert_awaited_once()

    async def test_foreign_source_is_refused(self) -> None:
        did = uuid4()
        cb = MagicMock()
        cb.data = f"sfreq_set:{did}:180"
        cb.from_user.id = 42
        cb.answer = AsyncMock()
        with patch.object(sf.db_q, "get_domain", AsyncMock(return_value={"id": did, "owner_id": 7})), \
                patch.object(sf.db_q, "update_domain", AsyncMock()) as upd:
            await sf.cb_source_freq_set(cb)

        upd.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
