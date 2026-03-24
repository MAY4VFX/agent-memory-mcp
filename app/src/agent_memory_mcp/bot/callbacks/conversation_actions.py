"""Callback handlers for conversation actions."""

from __future__ import annotations

from uuid import UUID

from aiogram import F, Router
from aiogram.types import CallbackQuery

from agent_memory_mcp.bot.keyboards import main_menu_kb
from agent_memory_mcp.config import is_allowed_user
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db import queries_conversations as qc
from agent_memory_mcp.db.engine import async_engine

router = Router()


@router.callback_query(F.data.startswith("conv:resume:"))
async def resume_conversation(callback: CallbackQuery) -> None:
    """Resume a saved conversation."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return

    conv_id_str = callback.data.split(":", 2)[2]
    conv_id = UUID(conv_id_str)

    conv = await qc.get_conversation(async_engine, conv_id)
    if not conv:
        await callback.answer("Диалог не найден.", show_alert=True)
        return

    await qc.set_active_conversation(async_engine, callback.from_user.id, conv_id)

    # Get domain emoji
    domain = None
    if conv.get("domain_id"):
        domain = await db_q.get_domain(async_engine, conv["domain_id"])
    emoji = domain["emoji"] if domain else "\U0001f4ac"

    # Show last message
    last_msg = await qc.get_last_message(async_engine, conv_id)
    title = conv.get("title") or "Без названия"

    if last_msg:
        content = last_msg["content"]
        preview = content[:300] + "..." if len(content) > 300 else content
        text = (
            f"{emoji} <b>{title}</b>\n\n"
            f"Последнее сообщение ({last_msg['role']}):\n"
            f"{preview}\n\n"
            "Продолжайте диалог."
        )
    else:
        text = f"{emoji} <b>{title}</b>\n\nДиалог пуст. Задайте вопрос."

    await callback.message.edit_text(text)
    await callback.answer()


@router.callback_query(F.data.startswith("conv:delete:"))
async def delete_conversation(callback: CallbackQuery) -> None:
    """Delete a conversation."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return

    conv_id_str = callback.data.split(":", 2)[2]
    conv_id = UUID(conv_id_str)

    # Check if this is the active conversation
    active_id = await qc.get_active_conversation_id(async_engine, callback.from_user.id)
    if active_id and str(active_id) == conv_id_str:
        await qc.set_active_conversation(async_engine, callback.from_user.id, None)

    await qc.delete_conversation(async_engine, conv_id)

    # Refresh list
    convs = await qc.list_conversations(async_engine, callback.from_user.id)
    if convs:
        from agent_memory_mcp.bot.keyboards import conversation_list_kb
        await callback.message.edit_text(
            "Диалог удалён.\n\nВаши диалоги:",
            reply_markup=conversation_list_kb(convs),
        )
    else:
        await callback.message.edit_text("Диалог удалён. Диалогов больше нет.")
    await callback.answer()
