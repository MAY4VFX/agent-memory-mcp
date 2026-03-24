"""Bot middlewares."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject

from agent_memory_mcp.db import queries
from agent_memory_mcp.db.engine import async_engine


class UserMiddleware(BaseMiddleware):
    """Ensure user exists in DB on every update."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if user:
            await queries.upsert_user(async_engine, user.id, user.username)
        return await handler(event, data)
