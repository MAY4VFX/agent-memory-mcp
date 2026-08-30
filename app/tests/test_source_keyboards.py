"""Source menus must stay inside Telegram's reply-markup size limit.

A folder can hold hundreds of channels (real case: a 200-channel folder), and
one button per channel makes Telegram reject the whole message with
"Bad Request: reply markup is too long" — the user just sees a dead button.
"""

import unittest
from uuid import uuid4

from agent_memory_mcp.bot.keyboards import PAGE_SIZE, folder_view_kb, source_list_kb

# Telegram rejects a serialized reply_markup above ~10 KB.
MAX_MARKUP_BYTES = 10240


def _domains(n: int) -> list[dict]:
    return [
        {
            "id": uuid4(),
            "channel_username": f"some_rather_long_channel_name_{i}",
            "display_name": f"Channel {i}",
            "message_count": i * 7,
        }
        for i in range(n)
    ]


def _markup_size(kb) -> int:
    return len(kb.model_dump_json(exclude_none=True).encode())


class FolderViewKeyboardTests(unittest.TestCase):
    def test_large_folder_fits_telegram_limit(self) -> None:
        kb = folder_view_kb(str(uuid4()), _domains(200))

        self.assertLess(_markup_size(kb), MAX_MARKUP_BYTES)

    def test_large_folder_is_paginated(self) -> None:
        group_id = str(uuid4())
        kb = folder_view_kb(group_id, _domains(200))

        channel_buttons = [
            b for row in kb.inline_keyboard for b in row
            if b.callback_data and b.callback_data.startswith("src:view:")
        ]
        nav = [
            b for row in kb.inline_keyboard for b in row
            if b.callback_data and b.callback_data.startswith(f"pg:fld:{group_id}:")
        ]

        self.assertEqual(len(channel_buttons), PAGE_SIZE)
        self.assertTrue(nav, "expected a pagination row for a 200-channel folder")

    def test_second_page_shows_next_slice(self) -> None:
        members = _domains(200)
        page_1 = folder_view_kb(str(uuid4()), members, page=1)

        shown = [
            b.callback_data for row in page_1.inline_keyboard for b in row
            if b.callback_data and b.callback_data.startswith("src:view:")
        ]

        self.assertEqual(shown[0], f"src:view:{members[PAGE_SIZE]['id']}")

    def test_small_folder_has_no_nav_row(self) -> None:
        kb = folder_view_kb(str(uuid4()), _domains(3))

        nav = [
            b for row in kb.inline_keyboard for b in row
            if b.callback_data and b.callback_data.startswith("pg:fld:")
        ]

        self.assertEqual(nav, [])


class SourceListKeyboardTests(unittest.TestCase):
    def test_many_standalone_channels_fit_telegram_limit(self) -> None:
        kb = source_list_kb([], _domains(225))

        self.assertLess(_markup_size(kb), MAX_MARKUP_BYTES)

    def test_folders_come_before_channels_and_paginate(self) -> None:
        folders = [
            {"id": uuid4(), "name": f"Folder {i}", "member_count": 200, "total_messages": 58284}
            for i in range(3)
        ]
        kb = source_list_kb(folders, _domains(225))

        entries = [
            b.callback_data for row in kb.inline_keyboard for b in row
            if b.callback_data and b.callback_data.startswith(("src:folder:", "src:view:"))
        ]

        self.assertEqual(len(entries), PAGE_SIZE)
        self.assertTrue(entries[0].startswith("src:folder:"))
        self.assertTrue(
            any(b.callback_data == "src:add" for row in kb.inline_keyboard for b in row),
            "Add Source must stay reachable on every page",
        )


if __name__ == "__main__":
    unittest.main()
