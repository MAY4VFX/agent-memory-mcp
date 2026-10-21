"""@username of a person in «Add by link» → their 1:1 chat as a source.

Real case: «@novchenkova» came back as "'@novchenkova' is not a
channel/supergroup" — resolve_link only knew channels, though 1:1 chats are
supported as sources (resolve_dialog, peer_type="user").
"""

import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from telethon.tl.types import User

from agent_memory_mcp.collector.client import LinkIsPersonError, resolve_link
from agent_memory_mcp.memory_api import service


class ResolveLinkTests(unittest.IsolatedAsyncioTestCase):
    async def test_person_username_raises_with_user_id(self) -> None:
        client = MagicMock()
        client.get_entity = AsyncMock(return_value=User(id=777, bot=False, username="novchenkova"))

        with self.assertRaises(LinkIsPersonError) as ctx:
            await resolve_link(client, "@novchenkova")

        self.assertEqual(ctx.exception.user_id, 777)

    async def test_bot_username_is_still_rejected(self) -> None:
        client = MagicMock()
        client.get_entity = AsyncMock(return_value=User(id=5, bot=True, username="somebot"))

        with self.assertRaises(ValueError) as ctx:
            await resolve_link(client, "@somebot")

        self.assertNotIsInstance(ctx.exception, LinkIsPersonError)


class AddSourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_person_is_added_as_dialog(self) -> None:
        uc = MagicMock()
        uc.resolve_channel = AsyncMock(side_effect=LinkIsPersonError("@novchenkova", 777))
        with patch.object(service, "_add_single_dialog", AsyncMock(return_value={"status": "ok"})) as add_dialog:
            res = await service._add_single_channel(1, uc, "@novchenkova", "3m", 60)

        self.assertEqual(res, {"status": "ok"})
        add_dialog.assert_awaited_once_with(1, uc, "777", "3m", 60)


if __name__ == "__main__":
    unittest.main()
