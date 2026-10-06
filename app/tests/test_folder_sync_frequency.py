"""Per-folder sync frequency (issue #29).

Chats added from a Telegram folder (listener resync, folder import) take the
group's domain_groups.sync_frequency_minutes; NULL means the default 60.
set_group_sync_frequency() sets the group value and optionally pushes it to all
current members (floor 5). No Postgres needed: DB calls are mocked and the
helper's SQL is captured through a fake engine and compiled for PostgreSQL.
"""

import unittest
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from sqlalchemy.dialects import postgresql

from agent_memory_mcp import listener_main
from agent_memory_mcp.db import queries_groups as gq
from agent_memory_mcp.memory_api import service


def _folder(*chat_ids):
    return {
        "id": 7,
        "title": "BS",
        "peers": [
            {"channel_id": c, "title": f"c{c}", "username": "", "supported": True}
            for c in chat_ids
        ],
    }


def _uc(folder):
    uc = MagicMock()
    uc.get_folders = AsyncMock(return_value=[folder])
    return uc


class ResyncFrequencyTest(unittest.IsolatedAsyncioTestCase):
    async def _run_resync(self, group_freq):
        group = {"id": uuid4(), "tg_folder_id": 7, "sync_depth": "3m",
                 "sync_frequency_minutes": group_freq}
        create = AsyncMock(return_value={"id": uuid4()})
        with patch.object(listener_main.gq, "list_groups", AsyncMock(return_value=[group])), \
             patch.object(listener_main.gq, "get_group_domain_ids", AsyncMock(return_value=[])), \
             patch.object(listener_main.gq, "add_domains_to_group", AsyncMock()), \
             patch.object(listener_main.db_q, "list_domains", AsyncMock(return_value=[])), \
             patch.object(listener_main.db_q, "create_domain", create), \
             patch.object(listener_main.db_q, "update_domain", AsyncMock()):
            await listener_main._resync_folders(_uc(_folder(101)), 41437273)
        return create

    async def test_new_chat_uses_group_frequency(self):
        create = await self._run_resync(5)
        self.assertEqual(create.await_args.kwargs["sync_frequency_minutes"], 5)

    async def test_null_group_frequency_defaults_to_60(self):
        create = await self._run_resync(None)
        self.assertEqual(create.await_args.kwargs["sync_frequency_minutes"], 60)


class AddFolderFrequencyTest(unittest.IsolatedAsyncioTestCase):
    async def _run_add(self, group_freq, arg):
        group = {"id": uuid4(), "tg_folder_id": 7, "sync_frequency_minutes": group_freq}
        create = AsyncMock(return_value={"id": uuid4()})
        with patch.object(service.db_q, "list_domains", AsyncMock(return_value=[])), \
             patch.object(service.db_q, "create_domain", create), \
             patch.object(service.db_q, "update_domain", AsyncMock()), \
             patch("agent_memory_mcp.db.queries_groups.list_groups", AsyncMock(return_value=[group])), \
             patch("agent_memory_mcp.db.queries_groups.add_domains_to_group", AsyncMock()):
            await service._add_folder(1, _uc(_folder(101)), "BS", "3m", arg)
        return create.await_args.kwargs["sync_frequency_minutes"]

    async def test_existing_group_frequency_wins(self):
        self.assertEqual(await self._run_add(10, 60), 10)

    async def test_group_frequency_is_clamped_to_5(self):
        self.assertEqual(await self._run_add(1, 60), 5)

    async def test_null_group_uses_argument(self):
        self.assertEqual(await self._run_add(None, 30), 30)


class _FakeConn:
    def __init__(self, log):
        self.log = log

    async def execute(self, stmt):
        self.log.append(stmt)
        res = MagicMock()
        res.rowcount = 3
        res.mappings.return_value.one_or_none.return_value = {"id": "g", "sync_frequency_minutes": 5}
        return res


class _FakeEngine:
    def __init__(self):
        self.log = []

    @asynccontextmanager
    async def begin(self):
        yield _FakeConn(self.log)


def _sql(stmt):
    c = stmt.compile(dialect=postgresql.dialect())
    return str(c), c.params


class SetGroupFrequencyTest(unittest.IsolatedAsyncioTestCase):
    async def test_updates_group_and_members_with_clamp(self):
        eng = _FakeEngine()
        out = await gq.set_group_sync_frequency(eng, uuid4(), 1, apply_to_members=True)
        self.assertEqual(out["minutes"], 5)
        self.assertEqual(out["members_updated"], 3)
        self.assertEqual(len(eng.log), 2)
        sql_g, p_g = _sql(eng.log[0])
        sql_d, p_d = _sql(eng.log[1])
        self.assertTrue(sql_g.startswith("UPDATE domain_groups SET sync_frequency_minutes"))
        self.assertIn(5, p_g.values())
        self.assertTrue(sql_d.startswith("UPDATE domains SET sync_frequency_minutes"))
        self.assertIn("domain_group_members", sql_d)
        self.assertIn(5, p_d.values())

    async def test_without_apply_only_group_is_updated(self):
        eng = _FakeEngine()
        out = await gq.set_group_sync_frequency(eng, uuid4(), 15, apply_to_members=False)
        self.assertEqual(len(eng.log), 1)
        self.assertEqual(out["minutes"], 15)
        self.assertEqual(out["members_updated"], 0)


if __name__ == "__main__":
    unittest.main()
