"""Избранное (Saved Messages) is the self-chat (PeerUser(own_id)) and used to show
up in dialog/folder listings under the user's own name — indistinguishable from
any other DM. These tests cover the pool.py rename-and-float-to-top behavior."""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from telethon.tl.types import Chat, InputPeerUser, User

from agent_memory_mcp.collector.pool import _UserCollector


def _dialog(entity):
    return SimpleNamespace(entity=entity)


def _make_user(uid: int, first_name: str, is_self: bool = False, username: str | None = None) -> User:
    return User(id=uid, first_name=first_name, is_self=is_self, username=username)


def _make_chat(cid: int, title: str) -> Chat:
    return Chat(id=cid, title=title, photo=None, participants_count=0, date=None, version=0)


class ListDialogsSavedMessagesTests(unittest.IsolatedAsyncioTestCase):
    async def test_self_dialog_is_relabeled_and_floated_to_top(self) -> None:
        """Избранное shows up mid-list (by recency) with the user's own name —
        it must be renamed "⭐ Избранное" and moved to the front."""
        me = _make_user(111, "Roma", is_self=True)
        other_dm = _make_user(222, "Alice")
        group = _make_chat(333, "Work chat")

        async def fake_iter_dialogs(limit=None):
            for e in (other_dm, group, me):  # self is last by "recency"
                yield _dialog(e)

        client = MagicMock()
        client.iter_dialogs = fake_iter_dialogs

        uc = _UserCollector(telegram_id=1, client=client)
        out = await uc.list_dialogs(limit=300)

        self.assertEqual(len(out), 3)
        self.assertEqual(out[0]["chat_id"], 111)
        self.assertEqual(out[0]["title"], "⭐ Избранное")
        self.assertEqual(out[0]["type"], "dm")
        self.assertTrue(out[0]["supported"])
        # No leaked internal sort key.
        self.assertNotIn("_saved", out[0])
        # Others keep their real names, untouched.
        self.assertEqual({d["title"] for d in out[1:]}, {"Alice", "Work chat"})

    async def test_self_dialog_missing_from_limited_page_is_appended_via_get_me(self) -> None:
        """If Избранное is inactive and falls outside `limit`, it must still be
        offered — fetched once via get_me(), not per-dialog."""
        other_dm = _make_user(222, "Alice")

        async def fake_iter_dialogs(limit=None):
            yield _dialog(other_dm)

        client = MagicMock()
        client.iter_dialogs = fake_iter_dialogs
        client.get_me = AsyncMock(return_value=_make_user(111, "Roma", is_self=True))

        uc = _UserCollector(telegram_id=1, client=client)
        out = await uc.list_dialogs(limit=1)

        client.get_me.assert_awaited_once()
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["chat_id"], 111)
        self.assertEqual(out[0]["title"], "⭐ Избранное")

    async def test_no_self_dialog_when_absent_and_get_me_fails(self) -> None:
        """get_me() failing must not blow up list_dialogs — just skip Избранное."""
        other_dm = _make_user(222, "Alice")

        async def fake_iter_dialogs(limit=None):
            yield _dialog(other_dm)

        client = MagicMock()
        client.iter_dialogs = fake_iter_dialogs
        client.get_me = AsyncMock(side_effect=RuntimeError("disconnected"))

        uc = _UserCollector(telegram_id=1, client=client)
        out = await uc.list_dialogs(limit=1)

        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["title"], "Alice")


class ResolveDialogSavedMessagesTests(unittest.IsolatedAsyncioTestCase):
    async def test_self_dialog_resolves_with_saved_messages_title(self) -> None:
        me = _make_user(111, "Roma", is_self=True)

        async def fake_iter_dialogs():
            yield _dialog(me)

        client = MagicMock()
        client.iter_dialogs = fake_iter_dialogs

        uc = _UserCollector(telegram_id=1, client=client)
        info = await uc.resolve_dialog(111)

        self.assertEqual(info["title"], "⭐ Избранное")
        self.assertEqual(info["peer_type"], "user")


class GetFoldersSavedMessagesTests(unittest.IsolatedAsyncioTestCase):
    async def test_self_peer_in_folder_is_relabeled(self) -> None:
        me = _make_user(111, "Roma", is_self=True)
        other_dm = _make_user(222, "Alice")

        folder_filter = SimpleNamespace(
            id=1,
            title="Personal",
            include_peers=[
                InputPeerUser(user_id=111, access_hash=0),
                InputPeerUser(user_id=222, access_hash=0),
            ],
        )

        client = AsyncMock()
        client.return_value = SimpleNamespace(filters=[folder_filter])
        client.get_dialogs = AsyncMock(return_value=[_dialog(me), _dialog(other_dm)])

        uc = _UserCollector(telegram_id=1, client=client)
        folders = await uc.get_folders()

        self.assertEqual(len(folders), 1)
        titles = {p["chat_id"]: p["title"] for p in folders[0]["peers"]}
        self.assertEqual(titles[111], "⭐ Избранное")
        self.assertEqual(titles[222], "Alice")


if __name__ == "__main__":
    unittest.main()
