"""Eval handlers — /eval_add, /eval_list, /eval_run, /eval_status (admin-only).

Supports both /eval_* commands and inline callback buttons from settings.
"""

from __future__ import annotations

import asyncio

import structlog
from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

from agent_memory_mcp.bot.keyboards import settings_kb, tests_kb
from agent_memory_mcp.config import settings
from agent_memory_mcp.db import queries as db_q
from agent_memory_mcp.db.engine import async_engine
from agent_memory_mcp.pipeline import eval_datasets, eval_runner

log = structlog.get_logger(__name__)
router = Router()


def _is_admin(message: Message) -> bool:
    return message.from_user and message.from_user.id == settings.admin_telegram_id


def _is_admin_cb(callback: CallbackQuery) -> bool:
    return callback.from_user and callback.from_user.id == settings.admin_telegram_id


class EvalStates(StatesGroup):
    waiting_question = State()
    waiting_answer = State()


# ---- /eval_add ----

@router.message(Command("eval_add"))
async def cmd_eval_add(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    await state.set_state(EvalStates.waiting_question)
    await message.answer("Введите вопрос для golden пары:")


@router.message(EvalStates.waiting_question)
async def eval_receive_question(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    await state.update_data(question=message.text.strip())
    await state.set_state(EvalStates.waiting_answer)
    await message.answer("Теперь введите эталонный ответ:")


@router.message(EvalStates.waiting_answer)
async def eval_receive_answer(message: Message, state: FSMContext) -> None:
    if not _is_admin(message):
        return
    data = await state.get_data()
    question = data.get("question", "")
    expected = message.text.strip()
    await state.clear()

    # Bind golden pair to the active domain
    user = await db_q.get_user(async_engine, message.from_user.id)
    domain_id = str(user["active_domain_id"]) if user and user.get("active_domain_id") else None

    try:
        item = eval_datasets.add_item(question, expected, domain_id=domain_id)
        domain_note = f"\nDomain: <code>{domain_id[:8]}...</code>" if domain_id else "\n(без привязки к домену)"
        await message.answer(
            f"Golden пара добавлена:\n"
            f"<b>Q:</b> {question[:100]}\n"
            f"<b>A:</b> {expected[:100]}\n"
            f"ID: <code>{item['id']}</code>{domain_note}"
        )
    except Exception as e:
        log.exception("eval_add_failed")
        await message.answer(f"Ошибка: {e}")


# ---- /eval_list ----

@router.message(Command("eval_list"))
async def cmd_eval_list(message: Message) -> None:
    if not _is_admin(message):
        return

    items = eval_datasets.list_items()
    if not items:
        await message.answer("Golden датасет пуст. Добавьте пары через /eval_add")
        return

    lines = [f"<b>Golden пары ({len(items)}):</b>\n"]
    for i, it in enumerate(items, 1):
        q = it["question"][:60]
        a = it["expected"][:40]
        lines.append(f"{i}. <b>Q:</b> {q}\n   <b>A:</b> {a}...")

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n..."
    await message.answer(text)


# ---- /eval_run ----

@router.message(Command("eval_run"))
async def cmd_eval_run(message: Message) -> None:
    if not _is_admin(message):
        return

    user = await db_q.get_user(async_engine, message.from_user.id)
    if not user or not user.get("active_domain_id"):
        await message.answer("Сначала выберите активный домен.")
        return

    domain_id = user["active_domain_id"]
    # Try domain-filtered first, fallback to all items (handles legacy domain mismatch)
    items = eval_datasets.list_items(domain_id=str(domain_id))
    if not items:
        items = eval_datasets.list_items(domain_id=None)
    if not items:
        await message.answer("Нет golden пар. Добавьте через /eval_add")
        return

    status_msg = await message.answer(f"Запускаю eval прогон ({len(items)} items)...")

    async def _progress(done: int, total: int) -> None:
        try:
            await status_msg.edit_text(f"Eval прогон: {done}/{total} items...")
        except Exception:
            pass

    # Run in background task
    async def _run() -> None:
        try:
            result = await eval_runner.run_eval_batch(
                domain_id=domain_id,
                user_id=message.from_user.id,
                progress_callback=_progress,
            )
            text = (
                f"Eval прогон завершён!\n\n"
                f"<b>Run:</b> <code>{result['run_name']}</code>\n"
                f"<b>Items:</b> {result['items_count']}\n"
                f"<b>Note:</b> {result['note']}\n\n"
                f"Проверьте Langfuse UI → Datasets → {eval_datasets.DATASET_NAME}"
            )
            await status_msg.edit_text(text)
        except Exception as e:
            log.exception("eval_run_failed")
            await status_msg.edit_text(f"Eval прогон провалился: {e}")

    asyncio.create_task(_run())


# ---- /eval_status ----

def _build_status_text() -> str:
    """Build eval status table text."""
    runs = eval_datasets.list_runs()
    if not runs:
        return "Нет eval прогонов. Запустите /eval_run"

    recent = runs[-5:]
    lines = [f"<b>Eval ({eval_datasets.DATASET_NAME}):</b>\n"]

    def _fmt(v: float | None) -> str:
        return f"{v:.2f}" if v is not None else "-"

    for run in recent:
        scores = eval_datasets.get_run_scores(run["name"])
        name = run["name"][:25]
        lines.append(f"<b>{name}</b>")
        lines.append("<pre>")
        score_rows = [
            ("Faithfulness", scores.get("ragas_faithfulness")),
            ("CtxPrecision", scores.get("ragas_context_precision")),
            ("CtxRecall", scores.get("ragas_context_recall")),
            ("AnswerCorrect", scores.get("ragas_answer_correctness")),
            ("FactCorrect", scores.get("ragas_factual_correctness")),
            ("SemanticSim", scores.get("ragas_semantic_similarity")),
            ("Relevancy", scores.get("ragas_answer_relevancy")),
            ("EntityRecall", scores.get("ragas_context_entity_recall")),
            ("Ret.Precision", scores.get("retrieval_precision")),
            ("Latency", scores.get("latency_sec")),
        ]
        for label, val in score_rows:
            if val is not None:
                lines.append(f"  {label:<14} {_fmt(val):>5}")
        lines.append("</pre>")

    return "\n".join(lines)


@router.message(Command("eval_status"))
async def cmd_eval_status(message: Message) -> None:
    if not _is_admin(message):
        return
    await message.answer(_build_status_text())


# ---- Callback handlers for inline buttons ----


@router.callback_query(F.data == "settings:tests")
async def cb_settings_tests(callback: CallbackQuery) -> None:
    if not _is_admin_cb(callback):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    await callback.message.edit_text("Тестирование:", reply_markup=tests_kb())
    await callback.answer()


@router.callback_query(F.data == "eval:add")
async def cb_eval_add(callback: CallbackQuery, state: FSMContext) -> None:
    if not _is_admin_cb(callback):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    await state.set_state(EvalStates.waiting_question)
    await callback.message.edit_text("Введите вопрос для golden пары:")
    await callback.answer()


@router.callback_query(F.data == "eval:list")
async def cb_eval_list(callback: CallbackQuery) -> None:
    if not _is_admin_cb(callback):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return

    items = eval_datasets.list_items()
    if not items:
        await callback.message.edit_text(
            "Golden датасет пуст. Добавьте пары через кнопку «Добавить пару».",
            reply_markup=tests_kb(),
        )
        await callback.answer()
        return

    lines = [f"<b>Golden пары ({len(items)}):</b>\n"]
    for i, it in enumerate(items, 1):
        q = it["question"][:60]
        a = it["expected"][:40]
        lines.append(f"{i}. <b>Q:</b> {q}\n   <b>A:</b> {a}...")

    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n..."
    await callback.message.edit_text(text, reply_markup=tests_kb())
    await callback.answer()


@router.callback_query(F.data == "eval:run")
async def cb_eval_run(callback: CallbackQuery) -> None:
    if not _is_admin_cb(callback):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return

    user = await db_q.get_user(async_engine, callback.from_user.id)
    if not user or not user.get("active_domain_id"):
        await callback.answer("Сначала выберите активный домен.", show_alert=True)
        return

    domain_id = user["active_domain_id"]
    # Try domain-filtered first, fallback to all items (handles legacy domain mismatch)
    items = eval_datasets.list_items(domain_id=str(domain_id))
    if not items:
        items = eval_datasets.list_items(domain_id=None)
    if not items:
        await callback.answer("Нет golden пар.", show_alert=True)
        return

    await callback.message.edit_text(f"Запускаю eval прогон ({len(items)} items)...")
    await callback.answer()

    status_msg = callback.message

    async def _progress(done: int, total: int) -> None:
        try:
            await status_msg.edit_text(f"Eval прогон: {done}/{total} items...")
        except Exception:
            pass

    async def _run() -> None:
        try:
            result = await eval_runner.run_eval_batch(
                domain_id=domain_id,
                user_id=callback.from_user.id,
                progress_callback=_progress,
            )
            text = (
                f"Eval прогон завершён!\n\n"
                f"<b>Run:</b> <code>{result['run_name']}</code>\n"
                f"<b>Items:</b> {result['items_count']}\n"
                f"<b>Note:</b> {result['note']}"
            )
            await status_msg.edit_text(text, reply_markup=tests_kb())
        except Exception as e:
            log.exception("eval_run_failed")
            await status_msg.edit_text(f"Eval прогон провалился: {e}", reply_markup=tests_kb())

    asyncio.create_task(_run())


@router.callback_query(F.data == "eval:status")
async def cb_eval_status(callback: CallbackQuery) -> None:
    if not _is_admin_cb(callback):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    text = _build_status_text()
    # edit_text может упасть если текст > 4096 — тогда отправляем новым сообщением
    try:
        await callback.message.edit_text(text, reply_markup=tests_kb())
    except Exception:
        await callback.message.answer(text, reply_markup=tests_kb())
    await callback.answer()


@router.callback_query(F.data == "eval:back")
async def cb_eval_back(callback: CallbackQuery) -> None:
    if not _is_admin_cb(callback):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    await callback.message.edit_text("Настройки:", reply_markup=settings_kb(is_admin=True))
    await callback.answer()


# ---- /admin_tags — manual tag summary generation ----

@router.message(Command("admin_tags"))
async def cmd_admin_tags(message: Message) -> None:
    if not _is_admin(message):
        return

    user = await db_q.get_user(async_engine, message.from_user.id)
    if not user or not user.get("active_domain_id"):
        await message.answer("Сначала выберите активный домен.")
        return

    domain_id = user["active_domain_id"]
    status_msg = await message.answer("Генерация tag summaries...")

    async def _run() -> None:
        try:
            from agent_memory_mcp.pipeline.tag_summarizer import update_tag_summaries
            count = await update_tag_summaries(domain_id, async_engine)
            await status_msg.edit_text(f"Tag summaries: {count} сгенерировано/обновлено.")
        except Exception as e:
            log.exception("admin_tags_failed")
            await status_msg.edit_text(f"Ошибка: {e}")

    asyncio.create_task(_run())
