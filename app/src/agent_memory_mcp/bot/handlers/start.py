"""Handlers for /start and /help commands."""

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from agent_memory_mcp.bot.keyboards import main_menu_kb
from agent_memory_mcp.config import is_allowed_user
from agent_memory_mcp.db import queries
from agent_memory_mcp.db.engine import async_engine

router = Router()


async def _get_active_domain(user_id: int) -> dict | None:
    user = await queries.get_user(async_engine, user_id)
    if user and user.get("active_domain_id"):
        return await queries.get_domain(async_engine, user["active_domain_id"])
    return None


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        await message.answer("Доступ ограничен.")
        return
    domain = await _get_active_domain(message.from_user.id)
    await message.answer(
        "Telegram Knowledge Base\n\n"
        "Подключай каналы, задавай вопросы, получай ответы на основе контента каналов.\n\n"
        "Используй меню ниже для навигации.",
        reply_markup=main_menu_kb(domain),
    )


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        await message.answer("Доступ ограничен.")
        return
    await message.answer(
        "<b>Команды:</b>\n"
        "/start — Главное меню\n"
        "/help — Эта справка\n\n"
        "<b>Меню:</b>\n"
        "\u2795 Новый диалог — начать новый диалог\n"
        "\U0001f4ac Диалоги — история диалогов\n"
        "\u2699\ufe0f Настройки — управление каналами",
    )
