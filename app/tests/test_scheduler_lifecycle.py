import asyncio
import gc
import unittest
import weakref
from contextlib import suppress
from unittest.mock import AsyncMock
from uuid import uuid4

from agent_memory_mcp.scheduler.scheduler import SyncScheduler


class SyncSchedulerLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_spawned_sync_task_is_retained_and_releases_slot(self) -> None:
        scheduler = SyncScheduler()
        release = asyncio.Event()
        domain_id = uuid4()
        domain = {"id": domain_id}

        async def wait_for_release(_domain: dict) -> None:
            await release.wait()

        scheduler._run_incremental = AsyncMock(side_effect=wait_for_release)

        task = scheduler._spawn_incremental(domain)
        task_ref = weakref.ref(task)
        del task
        gc.collect()
        await asyncio.sleep(0)

        self.assertIsNotNone(task_ref())
        self.assertEqual(scheduler._syncing_domains, {domain_id})
        self.assertEqual(len(scheduler._sync_tasks), 1)

        release.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        self.assertEqual(scheduler._syncing_domains, set())
        self.assertEqual(scheduler._sync_tasks, set())

    async def test_cancel_during_collector_acquire_releases_slot(self) -> None:
        scheduler = SyncScheduler()
        domain_id = uuid4()
        domain = {"id": domain_id, "owner_id": 42}
        scheduler._syncing_domains.add(domain_id)

        async def never_returns(_domain: dict):
            await asyncio.Future()

        scheduler._get_collector_for_domain = never_returns
        task = asyncio.create_task(scheduler._run_incremental(domain))
        await asyncio.sleep(0)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

        self.assertNotIn(domain_id, scheduler._syncing_domains)


if __name__ == "__main__":
    unittest.main()
