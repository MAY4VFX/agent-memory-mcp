"""A hung Telethon fetch must not hold a scheduler slot forever.

Observed in production: three sync tasks sat in `fetch_messages` for ten
minutes with no timeout, no log line and no error. They held all three
concurrency slots, so every tick reported `eligible=234, started=0,
in_flight=3` — the whole sync pipeline was deadlocked by one stuck call.
"""

import asyncio
import unittest
from unittest.mock import patch

from agent_memory_mcp.scheduler.scheduler import SyncScheduler


class _Collector:
    def __init__(self, delay: float, result=None):
        self._delay = delay
        self._result = result if result is not None else []
        self.calls: list[dict] = []

    async def fetch_messages(self, **kwargs):
        self.calls.append(kwargs)
        await asyncio.sleep(self._delay)
        return self._result


class FetchTimeoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_hung_fetch_raises_instead_of_hanging(self) -> None:
        scheduler = SyncScheduler()
        collector = _Collector(delay=30)

        with patch("agent_memory_mcp.scheduler.scheduler.settings") as s:
            s.scheduler_fetch_timeout = 0.05
            with self.assertRaises(TimeoutError) as ctx:
                await scheduler._fetch_bounded(collector, channel_id=1)

        self.assertIn("0.05", str(ctx.exception))

    async def test_fetch_within_timeout_returns_result(self) -> None:
        scheduler = SyncScheduler()
        collector = _Collector(delay=0, result=["msg"])

        with patch("agent_memory_mcp.scheduler.scheduler.settings") as s:
            s.scheduler_fetch_timeout = 5
            result = await scheduler._fetch_bounded(
                collector, channel_id=7, min_id=3, peer_type="chat"
            )

        self.assertEqual(result, ["msg"])
        self.assertEqual(
            collector.calls, [{"channel_id": 7, "min_id": 3, "peer_type": "chat"}]
        )

    async def test_timeout_cancels_the_underlying_fetch(self) -> None:
        """The slot is only really freed if the hung call is cancelled."""
        scheduler = SyncScheduler()
        started = asyncio.Event()
        cancelled = asyncio.Event()

        class _Hanging:
            async def fetch_messages(self, **_kwargs):
                started.set()
                try:
                    await asyncio.sleep(30)
                except asyncio.CancelledError:
                    cancelled.set()
                    raise

        with patch("agent_memory_mcp.scheduler.scheduler.settings") as s:
            s.scheduler_fetch_timeout = 0.05
            with self.assertRaises(TimeoutError):
                await scheduler._fetch_bounded(_Hanging(), channel_id=1)

        self.assertTrue(started.is_set())
        self.assertTrue(cancelled.is_set())


if __name__ == "__main__":
    unittest.main()
