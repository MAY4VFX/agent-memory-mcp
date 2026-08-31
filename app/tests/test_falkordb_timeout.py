"""A wedged FalkorDB must fail the job, not hang the whole scheduler.

Observed in production: FalkorDB froze during an I/O storm — process alive,
healthcheck failing for 2635 consecutive probes, `redis-cli PING` never
returning. The graph write at the end of ingestion blocked forever because the
client was built without a socket timeout (redis-py defaults to blocking
indefinitely), so every sync task that reached the graph stage held its
concurrency slot for good. Three of them deadlocked the scheduler.
"""

import unittest
from unittest.mock import patch

from agent_memory_mcp.storage import falkordb_client


class FalkorDBTimeoutTests(unittest.TestCase):
    def _build(self):
        captured: dict = {}

        class _FakeGraph:
            pass

        class _FakeDB:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            def select_graph(self, _name):
                return _FakeGraph()

        with patch.object(falkordb_client, "FalkorDB", _FakeDB):
            falkordb_client.FalkorDBStorage(
                host="graph-host", port=6379, password="secret"
            )
        return captured

    def test_client_is_built_with_socket_timeouts(self) -> None:
        kwargs = self._build()

        self.assertIn("socket_timeout", kwargs)
        self.assertIn("socket_connect_timeout", kwargs)

    def test_timeouts_are_finite_and_positive(self) -> None:
        kwargs = self._build()

        for key in ("socket_timeout", "socket_connect_timeout"):
            value = kwargs[key]
            self.assertIsNotNone(value, f"{key} must not block forever")
            self.assertGreater(value, 0)

    def test_connection_details_still_passed_through(self) -> None:
        kwargs = self._build()

        self.assertEqual(kwargs["host"], "graph-host")
        self.assertEqual(kwargs["port"], 6379)
        self.assertEqual(kwargs["password"], "secret")


if __name__ == "__main__":
    unittest.main()
