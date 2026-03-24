"""Handlers for conversations and query pipeline."""

from __future__ import annotations

import asyncio
from uuid import UUID

import structlog
from aiogram import F, Router
from aiogram.types import BufferedInputFile, Message

from agent_memory_mcp.bot.formatters import format_answer, format_conversation_export
from agent_memory_mcp.bot.keyboards import conversation_list_kb, main_menu_kb
from agent_memory_mcp.config import is_allowed_user
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db import queries_conversations as qc
from agent_memory_mcp.db.engine import async_engine
from agent_memory_mcp.models.query import QueryAnswer
from agent_memory_mcp.pipeline.query_orchestrator import run_query_pipeline
from agent_memory_mcp.storage.embedding_client import EmbeddingClient
from agent_memory_mcp.storage.falkordb_client import FalkorDBStorage
from agent_memory_mcp.storage.milvus_client import MilvusStorage
from agent_memory_mcp.storage.reranker_client import RerankerClient
from agent_memory_mcp.utils.tokens import count_tokens

log = structlog.get_logger(__name__)
router = Router()


# ------------------------------------------------------------------ Menu buttons

@router.message(F.text == "\u2795 Новый диалог")
async def new_conversation(message: Message) -> None:
    """Create a new conversation."""
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        await message.answer("Доступ ограничен.")
        return

    user = await db_q.get_user(async_engine, message.from_user.id)
    if not user:
        await message.answer(
            "Сначала подключите канал через \u2699\ufe0f Настройки.",
            reply_markup=main_menu_kb(),
        )
        return

    from agent_memory_mcp.db import queries_groups as gq
    scope = await gq.resolve_scope(async_engine, user)
    if not scope.domain_ids:
        await message.answer(
            "Сначала подключите канал через \u2699\ufe0f Настройки.",
            reply_markup=main_menu_kb(),
        )
        return
    domain = await db_q.get_domain(async_engine, user["active_domain_id"]) if user.get("active_domain_id") else None

    conv = await qc.create_conversation(
        async_engine,
        user_id=message.from_user.id,
        domain_id=user.get("active_domain_id"),
        scope_type=scope.scope_type,
        group_id=scope.group_id,
    )
    await qc.set_active_conversation(async_engine, message.from_user.id, conv["id"])
    await message.answer(
        "Новый диалог создан. Задайте вопрос.",
        reply_markup=main_menu_kb(domain, scope_label=scope.label),
    )


@router.message(F.text == "\U0001f4ac Диалоги")
async def list_conversations_handler(message: Message) -> None:
    """Show conversation list."""
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        await message.answer("Доступ ограничен.")
        return

    convs = await qc.list_conversations(async_engine, message.from_user.id)
    if not convs:
        await message.answer("У вас пока нет диалогов. Задайте вопрос, чтобы начать.")
        return

    await message.answer(
        "Ваши диалоги:",
        reply_markup=conversation_list_kb(convs),
    )


@router.message(F.text.startswith("/export"))
async def export_conversation(message: Message) -> None:
    """Export current conversation as Markdown file."""
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        await message.answer("Доступ ограничен.")
        return

    conv_id = await qc.get_active_conversation_id(async_engine, message.from_user.id)
    if not conv_id:
        await message.answer("Нет активного диалога для экспорта.")
        return

    conv = await qc.get_conversation(async_engine, conv_id)
    msgs = await qc.get_all_messages(async_engine, conv_id)
    if not msgs:
        await message.answer("Диалог пуст.")
        return

    title = conv.get("title", "export") if conv else "export"
    md_text = format_conversation_export(msgs, title)
    filename = f"conversation_{title[:30].replace(' ', '_')}.md"

    doc = BufferedInputFile(md_text.encode("utf-8"), filename=filename)
    await message.answer_document(doc, caption=f"Экспорт: {title}")


# ------------------------------------------------------------------ Query handler

@router.message(F.text)
async def handle_text_query(message: Message) -> None:
    """Handle any text message as a query to the pipeline."""
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        await message.answer("Доступ ограничен.")
        return

    text = message.text.strip()
    if not text:
        return

    user = await db_q.get_user(async_engine, message.from_user.id)
    if not user:
        await message.answer(
            "Сначала подключите канал через \u2699\ufe0f Настройки.",
            reply_markup=main_menu_kb(),
        )
        return

    # Resolve scope (single domain / group / all)
    from agent_memory_mcp.db import queries_groups as gq
    scope = await gq.resolve_scope(async_engine, user)
    if not scope.domain_ids:
        await message.answer("Нет доступных каналов. Подключите канал через Настройки.")
        return

    domain_id = user.get("active_domain_id")
    domain = await db_q.get_domain(async_engine, domain_id) if domain_id else None

    # Determine search mode from user settings
    search_mode = user.get("detail_level", "balanced")
    if search_mode not in ("fast", "balanced", "deep"):
        search_mode = "balanced"

    # Ensure active conversation exists
    conv_id = await qc.get_active_conversation_id(async_engine, message.from_user.id)
    if not conv_id:
        conv = await qc.create_conversation(
            async_engine,
            user_id=message.from_user.id,
            domain_id=domain_id,
            title=text[:50],
            scope_type=scope.scope_type,
            group_id=scope.group_id,
        )
        conv_id = conv["id"]
        await qc.set_active_conversation(async_engine, message.from_user.id, conv_id)
    else:
        # Auto-title: set title from first message if empty
        conv = await qc.get_conversation(async_engine, conv_id)
        if conv and not conv.get("title"):
            await qc.update_conversation(async_engine, conv_id, title=text[:50])

    # Save user message
    user_token_count = count_tokens(text)
    await qc.add_message(
        async_engine,
        conversation_id=conv_id,
        role="user",
        content=text,
        token_count=user_token_count,
    )

    # Show typing indicator
    typing_msg = await message.answer("\u2699\ufe0f Думаю...")

    try:
        # Initialize storage clients
        milvus = MilvusStorage()
        graph = FalkorDBStorage()
        embedder = EmbeddingClient()
        reranker = RerankerClient()

        # Progress callback for map-reduce
        async def _progress(done: int, total: int) -> None:
            try:
                await typing_msg.edit_text(
                    f"\u2699\ufe0f Анализирую посты... ({done}/{total} батчей)"
                )
            except Exception:
                pass

        try:
            answer, payload = await run_query_pipeline(
                query=text,
                user_id=message.from_user.id,
                conversation_id=conv_id,
                domain_ids=scope.domain_ids,
                engine=async_engine,
                milvus=milvus,
                graph=graph,
                embedder=embedder,
                reranker=reranker,
                search_mode=search_mode,
                progress_callback=_progress,
                since_date=scope.since_date,
            )
        finally:
            milvus.close()
            graph.close()
            await embedder.close()
            await reranker.close()

        # Save context payload
        payload_row = await qc.save_context_payload(
            async_engine,
            conversation_id=conv_id,
            query_text=text,
            payload_json=payload.model_dump(),
            token_count=payload.token_count,
            chunks_count=len(payload.chunks),
            graph_entities_count=len(payload.graph_context.get("entities", [])),
            langfuse_trace_id=answer.langfuse_trace_id,
        )

        # Save assistant message
        answer_token_count = count_tokens(answer.answer)
        await qc.add_message(
            async_engine,
            conversation_id=conv_id,
            role="assistant",
            content=answer.answer,
            token_count=answer_token_count,
            context_payload_id=payload_row["id"],
            langfuse_trace_id=answer.langfuse_trace_id,
        )

        # Format and send answer
        if scope.scope_type != "domain":
            domain_display = scope.label
        elif domain:
            domain_display = f"@{domain['channel_username']}" if domain.get("channel_username") else domain.get("display_name", "")
        else:
            domain_display = ""
        formatted = format_answer(answer, domain_display)

        # Delete typing message and send answer
        await typing_msg.delete()
        # Split long messages (Telegram limit 4096 chars)
        if len(formatted) <= 4096:
            await message.answer(formatted)
        else:
            # Split at line boundaries
            chunks = _split_message(formatted, 4096)
            for chunk in chunks:
                await message.answer(chunk)

    except Exception:
        log.exception("query_pipeline_error", query=text[:100])
        await typing_msg.edit_text(
            "Произошла ошибка при обработке запроса. Попробуйте ещё раз."
        )


def _split_message(text: str, max_len: int = 4096) -> list[str]:
    """Split long text into chunks respecting line boundaries."""
    if len(text) <= max_len:
        return [text]
    chunks: list[str] = []
    while text:
        if len(text) <= max_len:
            chunks.append(text)
            break
        # Find last newline before max_len
        split_at = text.rfind("\n", 0, max_len)
        if split_at == -1:
            split_at = max_len
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    return chunks
