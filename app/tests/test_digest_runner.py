import unittest
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from agent_memory_mcp.digest import runner


class DigestRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_stale_digest_is_sent_and_advances_schedule(self) -> None:
        config_id = uuid4()
        domain_id = uuid4()
        config = {
            "id": config_id,
            "user_id": 41437273,
            "scope_type": "domain",
            "scope_id": domain_id,
            "frequency_hours": 24,
        }
        engine = object()
        bot = AsyncMock()

        with (
            patch.object(
                runner,
                "trace_observation",
                return_value=nullcontext(SimpleNamespace(trace_id="trace-id")),
            ),
            patch.object(runner, "flush"),
            patch.object(
                runner.dq,
                "create_digest_run",
                AsyncMock(return_value={"id": uuid4()}),
            ),
            patch.object(runner.dq, "update_digest_run", AsyncMock()) as update_run,
            patch.object(runner.dq, "update_digest_config", AsyncMock()) as update_config,
            patch.object(
                runner.db_q,
                "get_messages_since",
                AsyncMock(return_value=[]),
            ),
            patch.object(
                runner,
                "_stale_sources",
                AsyncMock(return_value=["вайбкодеры"]),
            ),
            patch.object(runner, "_send_digest", AsyncMock()) as send_digest,
        ):
            await runner.run_digest(config, engine, bot)

        update_run.assert_awaited_once()
        update_config.assert_awaited_once()
        send_digest.assert_awaited_once()
        sent_text = send_digest.await_args.args[2]
        self.assertIn("не синхронизировались", sent_text)
        self.assertIn("вайбкодеры", sent_text)


if __name__ == "__main__":
    unittest.main()
