"""A sync that finds nothing must be distinguishable from a sync that failed.

Both bugs covered here were observed in production on a 225-source account:
ten sources sat at `last_synced_at = NULL` forever, re-opening a takeout
session and re-scanning full history every hour, because "zero messages" was
treated as "first sync not done yet" — and a fetch whose retries ran out
reported zero messages instead of raising.
"""

import unittest
from unittest.mock import AsyncMock, patch

from telethon.errors import FloodWaitError

from agent_memory_mcp.collector.client import TelegramCollector
from agent_memory_mcp.scheduler.scheduler import _should_mark_synced


class ShouldMarkSyncedTests(unittest.TestCase):
    def test_messages_mark_synced(self) -> None:
        self.assertTrue(
            _should_mark_synced(
                has_messages=True, last_msg_id=0, min_id=0, widened=False, since_date=object()
            )
        )

    def test_empty_narrow_first_pass_does_not_mark_synced(self) -> None:
        # Depth cutoff returned nothing and the widened pass has not run yet.
        self.assertFalse(
            _should_mark_synced(
                has_messages=False, last_msg_id=0, min_id=0, widened=False, since_date=object()
            )
        )

    def test_empty_widened_first_pass_marks_synced(self) -> None:
        # Full history was scanned and the source really is empty.
        self.assertTrue(
            _should_mark_synced(
                has_messages=False, last_msg_id=0, min_id=0, widened=True, since_date=object()
            )
        )

    def test_empty_first_pass_without_depth_marks_synced(self) -> None:
        self.assertTrue(
            _should_mark_synced(
                has_messages=False, last_msg_id=0, min_id=0, widened=False, since_date=None
            )
        )

    def test_incremental_run_keeps_existing_sync_state(self) -> None:
        self.assertTrue(
            _should_mark_synced(
                has_messages=False, last_msg_id=4321, min_id=4321, widened=False, since_date=None
            )
        )


class PaginatedFetchRetryTests(unittest.IsolatedAsyncioTestCase):
    def _collector(self) -> TelegramCollector:
        return TelegramCollector.__new__(TelegramCollector)

    async def test_exhausted_flood_retries_raise_instead_of_returning_empty(self) -> None:
        class FloodingClient:
            def iter_messages(self, *_args, **_kwargs):
                raise FloodWaitError(request=None, capture=30)

        with patch("agent_memory_mcp.collector.client.asyncio.sleep", AsyncMock()):
            with self.assertRaises(FloodWaitError):
                await self._collector()._paginated_fetch(
                    FloodingClient(), entity=None, channel_id=1,
                    limit=None, min_id=0, since_date=None,
                )

    async def test_genuinely_empty_channel_returns_empty_list(self) -> None:
        class EmptyClient:
            def iter_messages(self, *_args, **_kwargs):
                async def _gen():
                    return
                    yield  # pragma: no cover — makes this an async generator
                return _gen()

        msgs = await self._collector()._paginated_fetch(
            EmptyClient(), entity=None, channel_id=1,
            limit=None, min_id=0, since_date=None,
        )

        self.assertEqual(msgs, [])


if __name__ == "__main__":
    unittest.main()
