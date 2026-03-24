"""One-time Telethon authorization -- run interactively to get a StringSession.

Usage:
    python -m agent_memory_mcp.scripts.auth_telethon
"""

from __future__ import annotations

import asyncio

from telethon import TelegramClient
from telethon.sessions import StringSession


async def main() -> None:
    api_id = int(input("API ID: "))
    api_hash = input("API Hash: ")

    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.start()

    session_string = client.session.save()
    print(f"\nYour session string (set as TELEGRAM_SESSION env var):\n{session_string}")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
