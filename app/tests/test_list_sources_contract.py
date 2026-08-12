"""list_sources() is what an agent reads to answer "is this source syncing?".

Regression: the tool's docstring promises "sync status", but the only boolean in
the payload was `monitoring` — which actually means "count this source in the
Observe Layer", not "this source syncs". Agents read `monitoring: false` on a
happily-syncing source and told users synchronization was off.
"""

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from agent_memory_mcp.memory_api import service


def _domain(**over) -> dict:
    """A source that IS syncing fine but is NOT in the Observe Layer."""
    now = datetime.now(timezone.utc)
    d = {
        "id": uuid4(),
        "channel_username": None,
        "channel_name": "⭐ Избранное",
        "display_name": "⭐ Избранное",
        "message_count": 2712,
        "sync_depth": "3m",
        "last_synced_at": now - timedelta(minutes=3),
        "next_sync_at": now + timedelta(minutes=57),
        "sync_frequency_minutes": 60,
        "is_active": True,
        "monitoring": False,
        "peer_type": "user",
    }
    d.update(over)
    return d


class ListSourcesSyncStatusTests(unittest.IsolatedAsyncioTestCase):
    async def _call(self, domain: dict) -> dict:
        with patch.object(
            service.db_q, "list_domains", AsyncMock(return_value=[domain]),
        ):
            out = await service.list_sources(owner_id=1)
        self.assertEqual(len(out), 1)
        return out[0]

    async def test_syncing_source_is_reported_as_syncing(self) -> None:
        """monitoring=False must not be the only signal an agent can read —
        a source that syncs must say so unambiguously."""
        src = await self._call(_domain())

        self.assertTrue(
            src.get("sync_enabled"),
            "a source with is_active=True must report sync_enabled=True",
        )
        self.assertIsNotNone(
            src.get("next_sync"),
            "next_sync must be exposed so an agent can see syncing is scheduled",
        )

    async def test_paused_source_is_distinguishable(self) -> None:
        """The inverse must also hold, else sync_enabled is a constant."""
        src = await self._call(_domain(is_active=False))

        self.assertFalse(src.get("sync_enabled"))

    async def test_monitoring_is_not_named_like_a_sync_flag(self) -> None:
        """`monitoring` is the Observe Layer toggle. Keep it (consumers read it)
        but ship an unambiguous alias so nobody has to guess."""
        src = await self._call(_domain())

        self.assertIn("monitoring", src)  # back-compat, MCP contract
        self.assertFalse(src["monitoring"])
        self.assertFalse(
            src.get("observe_layer"),
            "observe_layer must mirror monitoring under a self-explaining name",
        )
        # The two must not be confusable: sync is on while observe layer is off.
        self.assertTrue(src.get("sync_enabled"))


if __name__ == "__main__":
    unittest.main()
