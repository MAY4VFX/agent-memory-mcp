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


class SplitMessageTests(unittest.TestCase):
    def test_oversized_single_paragraph_is_split(self) -> None:
        text = "intro\n\n" + ", ".join(f"канал{i}" for i in range(1000))
        chunks = runner._split_message(text)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(c) <= 4096 for c in chunks))
        self.assertEqual(" ".join(chunks).split(), text.split())

    def test_short_text_is_one_chunk(self) -> None:
        self.assertEqual(runner._split_message("hi"), ["hi"])

    def test_unbreakable_text_is_hard_cut(self) -> None:
        chunks = runner._split_message("x" * 9000)
        self.assertEqual([len(c) for c in chunks], [4096, 4096, 808])

    def test_stale_list_is_capped(self) -> None:
        names = [f"c{i}" for i in range(250)]
        self.assertEqual(runner._format_stale(names[:3]), "c0, c1, c2")
        self.assertTrue(runner._format_stale(names).endswith("и ещё 240"))


if __name__ == "__main__":
    unittest.main()
