"""Callback query handlers for domain management."""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from uuid import UUID

import structlog
from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from agent_memory_mcp.collector.client import TelegramCollector
from agent_memory_mcp.bot.keyboards import (
    confirm_delete_kb,
    domain_actions_kb,
    domain_edit_kb,
    domain_list_kb,
    edit_depth_kb,
    edit_emoji_kb,
    edit_freq_kb,
    emoji_kb,
    frequency_kb,
    list_detail_kb,
    main_menu_kb,
    search_mode_kb,
    settings_kb,
    skip_list_name_kb,
)
from agent_memory_mcp.bot.states import AddChannelStates, SettingsStates
from agent_memory_mcp.config import is_allowed_user, settings as app_settings
from agent_memory_mcp.db import queries
from agent_memory_mcp.db import queries_groups as gq
from agent_memory_mcp.db.engine import async_engine

log = structlog.get_logger()

router = Router()

_HASHTAG_RE = re.compile(r"#(\w+)", re.UNICODE)

# ---- Period depth mapping ----
_PERIOD_MAP: dict[str, str] = {
    "1w": "1w",
    "1m": "1m",
    "3m": "3m",
    "6m": "6m",
    "1y": "1y",
    "3y": "3y",
    "all": "all",
}

_PERIOD_LABELS: dict[str, str] = {
    "1w": "1 неделя",
    "1m": "1 месяц",
    "3m": "3 месяца",
    "6m": "6 месяцев",
    "1y": "1 год",
    "3y": "3 года",
    "all": "Все сообщения",
}


# ---- Period selection ----


@router.callback_query(F.data.startswith("period:"))
async def on_period(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    period_key = callback.data.split(":", 1)[1]
    if period_key not in _PERIOD_MAP:
        await callback.answer("Неизвестный период.", show_alert=True)
        return
    await state.update_data(sync_depth=_PERIOD_MAP[period_key])
    await callback.message.edit_text(
        f"Глубина: <b>{_PERIOD_LABELS[period_key]}</b>\n\n"
        "Выберите частоту синхронизации:",
        reply_markup=frequency_kb(),
    )
    await state.set_state(AddChannelStates.choosing_frequency)
    await callback.answer()


# ---- Frequency selection ----


@router.callback_query(F.data.startswith("freq:"))
async def on_frequency(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    freq_minutes = int(callback.data.split(":", 1)[1])
    await state.update_data(sync_frequency_minutes=freq_minutes)
    data = await state.get_data()

    # --- Batch-add mode: create all domains at once ---
    if "batch_channels" in data:
        await callback.answer()
        await _finish_batch_add(callback, state, data)
        return

    # --- Folder-import mode: create list + domains ---
    if "folder_import" in data:
        await callback.answer()
        await _finish_folder_import(callback, state, data)
        return

    # --- Single source: ask for emoji ---
    await callback.message.edit_text(
        "Выберите эмодзи для источника:",
        reply_markup=emoji_kb(),
    )
    await state.set_state(AddChannelStates.choosing_emoji)
    await callback.answer()


# ---- Emoji selection → create domain ----


@router.callback_query(F.data.startswith("emoji:"))
async def on_emoji(
    callback: CallbackQuery, state: FSMContext, collector: TelegramCollector
) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    emoji = callback.data.split(":", 1)[1]
    data = await state.get_data()

    channel_name = data.get("channel_name", "")
    channel_username = data.get("channel_username", "")
    channel_id = data.get("channel_id")
    sync_depth = data.get("sync_depth", "1m")
    sync_frequency_minutes = data.get("sync_frequency_minutes", 60)

    # Create domain (pinned=True — added individually, visible outside lists)
    domain = await queries.create_domain(
        async_engine,
        owner_id=callback.from_user.id,
        channel_id=channel_id,
        channel_username=channel_username,
        channel_name=channel_name,
        sync_depth=sync_depth,
        sync_frequency_minutes=sync_frequency_minutes,
        emoji=emoji,
        display_name=f"{channel_name}",
        pinned=True,
    )

    # Set as active domain for the user
    await queries.update_user_active_domain(async_engine, callback.from_user.id, domain["id"])

    # Schedule next sync
    next_sync = datetime.now(timezone.utc) + timedelta(minutes=sync_frequency_minutes)
    await queries.update_domain(
        async_engine,
        domain["id"],
        next_sync_at=next_sync,
    )

    await callback.message.edit_text(
        f"{emoji} \u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a <b>{channel_name}</b> (@{channel_username}) \u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d!\n\n"
        f"\u0413\u043b\u0443\u0431\u0438\u043d\u0430: {_PERIOD_LABELS.get(sync_depth, sync_depth)}\n"
        f"\u0427\u0430\u0441\u0442\u043e\u0442\u0430: \u043a\u0430\u0436\u0434\u044b\u0435 {sync_frequency_minutes} \u043c\u0438\u043d\n\n"
        "\u0417\u0430\u043f\u0443\u0441\u043a\u0430\u044e \u043d\u0430\u0447\u0430\u043b\u044c\u043d\u0443\u044e \u0438\u043d\u0434\u0435\u043a\u0441\u0430\u0446\u0438\u044e...",
    )
    # Update reply keyboard with new active domain
    await callback.message.answer(
        f"\u0410\u043a\u0442\u0438\u0432\u043d\u044b\u0439 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a: {emoji} {channel_name}",
        reply_markup=main_menu_kb(domain),
    )

    # Launch initial sync in background
    asyncio.create_task(
        _run_initial_sync(callback, domain, collector),
        name=f"initial_sync_{domain['id']}",
    )

    await state.clear()
    await callback.answer()


async def _edit_progress(callback: CallbackQuery, text: str) -> None:
    """Edit callback message, ignoring errors."""
    try:
        await callback.message.edit_text(text)
    except Exception:
        pass


async def _run_initial_sync(
    callback: CallbackQuery, domain: dict, collector: TelegramCollector
) -> None:
    """Run initial sync for a newly added domain."""
    from agent_memory_mcp.models.messages import telegram_to_processed
    from agent_memory_mcp.pipeline.pipelines import run_initial_ingestion

    domain_id = domain["id"]
    emoji = domain["emoji"]
    name = domain["channel_name"]
    job = await queries.create_sync_job(async_engine, domain_id, "initial")
    try:
        await queries.update_sync_job(
            async_engine, job["id"], status="running", started_at=datetime.now(timezone.utc)
        )

        # --- Step 1: Fetch messages from Telegram ---
        await _edit_progress(callback, f"{emoji} <b>{name}</b>\n\nЗагрузка сообщений...")
        since_date = _depth_to_date(domain["sync_depth"])
        msgs = await collector.fetch_messages(
            channel_id=domain["channel_id"],
            since_date=since_date,
            channel_username=domain.get("channel_username"),
            use_takeout=True,
        )
        await queries.update_sync_job(async_engine, job["id"], messages_fetched=len(msgs))

        # --- Step 2: Store messages in PG ---
        msg_rows = []
        for m in msgs:
            tags = _HASHTAG_RE.findall(m.text or "")
            msg_rows.append({
                "domain_id": domain_id,
                "telegram_msg_id": m.message_id,
                "reply_to_msg_id": m.reply_to_msg_id,
                "sender_id": m.sender_id,
                "sender_name": m.sender_name,
                "content": m.text,
                "content_type": m.content_type,
                "hashtags": tags if tags else None,
                "msg_date": m.date,
            })
        await queries.bulk_insert_messages(async_engine, msg_rows)
        await _edit_progress(
            callback,
            f"{emoji} <b>{name}</b>\n\n"
            f"Загружено: {len(msgs)} сообщений\n"
            "Запуск pipeline обработки...",
        )

        # --- Step 3: Run pipeline ---
        processed = [telegram_to_processed(m, domain_id) for m in msgs]
        stats, schema_result = await run_initial_ingestion(processed, str(domain_id))

        # --- Step 4: Save schema to PG ---
        if schema_result and schema_result.schema:
            schema = schema_result.schema
            await queries.save_channel_schema(
                async_engine,
                domain_id,
                schema_json=schema.model_dump(),
                detected_domain=schema_result.detected_domain,
                entity_types=[et.model_dump() for et in schema.entity_types],
                relation_types=[rt.model_dump() for rt in schema.relation_types],
            )

        # --- Step 4b: Generate tag summaries ---
        try:
            from agent_memory_mcp.pipeline.tag_summarizer import update_tag_summaries
            tag_count = await update_tag_summaries(domain_id, async_engine)
            if tag_count:
                log.info("tag_summaries_generated", domain_id=str(domain_id), count=tag_count)
        except Exception:
            log.exception("tag_summaries_error", domain_id=str(domain_id))

        # --- Step 5: Update sync job ---
        await queries.update_sync_job(
            async_engine,
            job["id"],
            status="completed",
            messages_processed=len(msgs),
            messages_filtered=stats.noise_messages,
            entities_extracted=stats.entities_extracted,
            completed_at=datetime.now(timezone.utc),
        )

        # --- Step 6: Update domain ---
        last_msg_id = max((m.message_id for m in msgs), default=0)
        domain_update = dict(
            last_synced_at=datetime.now(timezone.utc),
            last_synced_message_id=last_msg_id,
            message_count=len(msgs),
            entity_count=stats.entities_extracted,
            relation_count=stats.relations_extracted,
        )
        if stats.domain_type:
            domain_update["domain_type"] = stats.domain_type
        await queries.update_domain(async_engine, domain_id, **domain_update)

        # --- Step 7: Report ---
        errors_text = f"\nОшибки: {len(stats.errors)}" if stats.errors else ""
        await _edit_progress(
            callback,
            f"{emoji} <b>{name}</b>\n\n"
            f"Сообщений: {len(msgs)}\n"
            f"Шум: {stats.noise_messages}\n"
            f"Тредов: {stats.threads_built}\n"
            f"Сущностей: {stats.entities_extracted}\n"
            f"Связей: {stats.relations_extracted}\n"
            f"Векторов: {stats.vectors_stored}\n"
            f"Домен: {stats.domain_type or '—'}\n"
            f"Время: {stats.duration_sec:.1f}с\n"
            f"Индексация завершена!{errors_text}",
        )

        log.info(
            "initial_sync_done",
            domain_id=str(domain_id),
            messages=len(msgs),
            entities=stats.entities_extracted,
            relations=stats.relations_extracted,
            vectors=stats.vectors_stored,
        )
    except Exception as exc:
        log.exception("initial_sync_error", domain_id=str(domain_id))
        await queries.update_sync_job(
            async_engine, job["id"], status="failed", error_message=str(exc)[:500]
        )
        await _edit_progress(callback, f"Ошибка индексации канала {name}: {exc}")


def _depth_to_date(depth: str) -> datetime | None:
    """Convert sync depth string to offset_date."""
    now = datetime.now(timezone.utc)
    mapping = {
        "1w": timedelta(weeks=1),
        "1m": timedelta(days=30),
        "3m": timedelta(days=90),
        "6m": timedelta(days=180),
        "1y": timedelta(days=365),
        "3y": timedelta(days=1095),
    }
    delta = mapping.get(depth)
    if delta is None:
        return None  # "all" — no date limit
    return now - delta


# ---- Batch / folder finish helpers ----


def _sync_progress_text(
    title: str, domain_rows: list[dict], sync_depth: str, freq: int,
) -> str:
    """Build progress text for sync tracking."""
    synced = [d for d in domain_rows if d.get("last_synced_at")]
    total_msgs = sum(d.get("message_count", 0) or 0 for d in domain_rows)

    lines = [f"\U0001f4e5 <b>{title}</b>"]
    lines.append(
        f"\u0413\u043b\u0443\u0431\u0438\u043d\u0430: {_PERIOD_LABELS.get(sync_depth, sync_depth)} "
        f"| \u0427\u0430\u0441\u0442\u043e\u0442\u0430: {freq} \u043c\u0438\u043d"
    )
    lines.append(f"\u0417\u0430\u0433\u0440\u0443\u0436\u0435\u043d\u043e: {len(synced)}/{len(domain_rows)} "
                 f"| \u0421\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0439: {total_msgs}\n")

    for d in domain_rows:
        name = d["display_name"][:30]
        mc = d.get("message_count", 0) or 0
        if d.get("last_synced_at"):
            lines.append(f"\u2705 {name} — {mc}")
        else:
            lines.append(f"\u23f3 {name}")

    if len(synced) == len(domain_rows):
        lines.append(f"\n\u2705 \u0412\u0441\u0435 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438 \u0437\u0430\u0433\u0440\u0443\u0436\u0435\u043d\u044b! ({total_msgs} \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0439)")
    else:
        lines.append(f"\n\U0001f504 \u041e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u0435 \u043a\u0430\u0436\u0434\u044b\u0435 10 \u0441\u0435\u043a...")

    return "\n".join(lines)


async def _track_sync_progress(
    message, title: str, domain_ids: list, sync_depth: str, freq: int,
    group_id: str | None = None,
) -> None:
    """Background task: update message with sync progress every 10s."""
    MAX_UPDATES = 180  # 30 min max tracking

    prev_text = ""
    for _ in range(MAX_UPDATES):
        await asyncio.sleep(10)
        try:
            rows = []
            for did in domain_ids:
                d = await queries.get_domain(async_engine, did)
                if d:
                    rows.append(d)

            text = _sync_progress_text(title, rows, sync_depth, freq)
            if text != prev_text:
                # Add keyboard when all done
                kb = None
                synced = [d for d in rows if d.get("last_synced_at")]
                if len(synced) == len(rows) and group_id:
                    group_domains = await gq.get_group_domains(async_engine, group_id)
                    kb = list_detail_kb(group_id, group_domains)
                try:
                    await message.edit_text(text, reply_markup=kb)
                except Exception:
                    pass
                prev_text = text

            # Stop when all synced
            if all(d.get("last_synced_at") for d in rows):
                break
        except Exception:
            log.exception("track_sync_progress_error")
            break


async def _finish_batch_add(
    callback: CallbackQuery, state: FSMContext, data: dict,
) -> None:
    """Handle batch add: suggest list name if multiple channels."""
    channels = data["batch_channels"]

    if len(channels) > 1:
        names = ", ".join(r["title"][:20] for r in channels[:5])
        if len(channels) > 5:
            names += f" (+{len(channels) - 5})"
        await callback.message.edit_text(
            f"\U0001f4cb {len(channels)} \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u043e\u0432 \u0433\u043e\u0442\u043e\u0432\u044b \u043a \u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u0438\u044e.\n"
            f"{names}\n\n"
            "\u0412\u0432\u0435\u0434\u0438\u0442\u0435 \u043d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u0441\u043f\u0438\u0441\u043a\u0430 \u0434\u043b\u044f \u044d\u0442\u0438\u0445 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u043e\u0432\n"
            "\u0438\u043b\u0438 \u043d\u0430\u0436\u043c\u0438\u0442\u0435 \u00ab\u0411\u0435\u0437 \u0441\u043f\u0438\u0441\u043a\u0430\u00bb:",
            reply_markup=skip_list_name_kb(),
        )
        await state.set_state(AddChannelStates.naming_batch_list)
        return

    # Single channel — create as orphan immediately
    await _execute_batch_creation(
        callback.message, state, data, callback.from_user.id,
    )


@router.callback_query(F.data == "batch:no_list")
async def on_batch_no_list(callback: CallbackQuery, state: FSMContext) -> None:
    """Skip list creation, add batch sources as orphans."""
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    await callback.answer()
    data = await state.get_data()
    if "batch_channels" not in data:
        await callback.message.edit_text("\u041d\u0435\u0442 \u0434\u0430\u043d\u043d\u044b\u0445 \u0434\u043b\u044f \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f.")
        await state.clear()
        return
    await _execute_batch_creation(
        callback.message, state, data, callback.from_user.id,
    )


@router.message(AddChannelStates.naming_batch_list)
async def on_batch_list_name(message: Message, state: FSMContext) -> None:
    """User entered a list name for batch-added sources."""
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        return
    name = message.text.strip()[:255] if message.text else ""
    if not name:
        await message.answer("\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u043d\u0435 \u043c\u043e\u0436\u0435\u0442 \u0431\u044b\u0442\u044c \u043f\u0443\u0441\u0442\u044b\u043c.")
        return
    data = await state.get_data()
    if "batch_channels" not in data:
        await message.answer("\u041d\u0435\u0442 \u0434\u0430\u043d\u043d\u044b\u0445 \u0434\u043b\u044f \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f.")
        await state.clear()
        return
    progress = await message.answer(
        f"\u23f3 \u0421\u043e\u0437\u0434\u0430\u044e \u0441\u043f\u0438\u0441\u043e\u043a \u00ab{name}\u00bb..."
    )
    await _execute_batch_creation(
        progress, state, data, message.from_user.id, group_name=name,
    )


async def _execute_batch_creation(
    target_msg,
    state: FSMContext,
    data: dict,
    owner_id: int,
    group_name: str | None = None,
) -> None:
    """Create domains (and optionally a list) from batch data."""
    channels = data["batch_channels"]
    sync_depth = data.get("sync_depth", "3m")
    freq = data.get("sync_frequency_minutes", 60)

    group_id_str: str | None = None
    title = f"{len(channels)} \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u043e\u0432"

    if group_name:
        try:
            group = await gq.create_group(async_engine, owner_id=owner_id, name=group_name)
            group_id_str = str(group["id"])
            title = f"\u0421\u043f\u0438\u0441\u043e\u043a \u00ab{group_name}\u00bb"
        except Exception:
            log.exception("batch_create_group_failed")

    try:
        await target_msg.edit_text(
            f"\u23f3 \u0421\u043e\u0437\u0434\u0430\u044e {len(channels)} \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u043e\u0432..."
        )
    except Exception:
        pass

    created_ids = []
    for ch in channels:
        try:
            domain = await queries.create_domain(
                async_engine,
                owner_id=owner_id,
                channel_id=ch["channel_id"],
                channel_username=ch["username"],
                channel_name=ch["title"],
                sync_depth=sync_depth,
                sync_frequency_minutes=freq,
                emoji="\U0001f4da",
                display_name=ch["title"][:50],
                pinned=not group_name,  # individual add → pinned
            )
            await queries.update_domain(
                async_engine, domain["id"],
                next_sync_at=datetime.now(timezone.utc),
            )
            if group_id_str:
                await gq.add_domains_to_group(
                    async_engine, UUID(group_id_str), [domain["id"]],
                )
            created_ids.append(domain["id"])
        except Exception:
            log.exception("batch_add_failed", channel=ch.get("title"))

    await state.clear()

    # Show initial progress and start tracking
    rows = [await queries.get_domain(async_engine, did) for did in created_ids]
    rows = [r for r in rows if r]
    text = _sync_progress_text(title, rows, sync_depth, freq)
    try:
        await target_msg.edit_text(text)
    except Exception:
        pass

    asyncio.create_task(
        _track_sync_progress(
            target_msg, title, created_ids, sync_depth, freq,
            group_id=group_id_str,
        ),
        name="track_batch_sync",
    )


async def _finish_folder_import(
    callback: CallbackQuery, state: FSMContext, data: dict,
) -> None:
    """Create list + domains from folder_import with chosen period/frequency."""
    fi = data["folder_import"]
    sync_depth = data.get("sync_depth", "3m")
    freq = data.get("sync_frequency_minutes", 60)

    folder_title = fi["folder_title"]
    folder_id = fi["folder_id"]
    peers = fi["peers"]

    await callback.message.edit_text(
        f"\u23f3 \u0418\u043c\u043f\u043e\u0440\u0442\u0438\u0440\u0443\u044e \u043f\u0430\u043f\u043a\u0443 \"{folder_title}\"..."
    )

    # Create list
    try:
        group = await gq.create_group(
            async_engine,
            owner_id=callback.from_user.id,
            name=folder_title,
            tg_folder_id=folder_id,
            sync_depth=sync_depth,
        )
    except Exception:
        groups = await gq.list_groups(async_engine, callback.from_user.id)
        existing = next((g for g in groups if g["name"] == folder_title), None)
        if existing:
            group = existing
        else:
            await state.clear()
            await callback.message.edit_text("\u041e\u0448\u0438\u0431\u043a\u0430 \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f \u0441\u043f\u0438\u0441\u043a\u0430.")
            return

    # Create domains and add to list
    created_ids = []
    existing_domains = await queries.list_domains(async_engine, callback.from_user.id)
    existing_cids = {d["channel_id"]: d["id"] for d in existing_domains}

    for peer in peers:
        try:
            if peer["channel_id"] in existing_cids:
                domain_id = existing_cids[peer["channel_id"]]
            else:
                domain = await queries.create_domain(
                    async_engine,
                    owner_id=callback.from_user.id,
                    channel_id=peer["channel_id"],
                    channel_username=peer["username"],
                    channel_name=peer["title"],
                    sync_depth=sync_depth,
                    sync_frequency_minutes=freq,
                    emoji="\U0001f4da",
                    display_name=peer["title"][:50],
                )
                domain_id = domain["id"]
                await queries.update_domain(
                    async_engine, domain_id,
                    next_sync_at=datetime.now(timezone.utc),
                )
                existing_cids[peer["channel_id"]] = domain_id
            await gq.add_domains_to_group(async_engine, group["id"], [domain_id])
            created_ids.append(domain_id)
        except Exception:
            log.exception("folder_import_peer_failed", peer=peer)

    await state.clear()

    # Show initial progress and start tracking
    rows = [await queries.get_domain(async_engine, did) for did in created_ids]
    rows = [r for r in rows if r]
    text = _sync_progress_text(
        f"\u0421\u043f\u0438\u0441\u043e\u043a \"{group['name']}\"",
        rows, sync_depth, freq,
    )
    await callback.message.edit_text(text)

    asyncio.create_task(
        _track_sync_progress(
            callback.message,
            f"\u0421\u043f\u0438\u0441\u043e\u043a \"{group['name']}\"",
            created_ids, sync_depth, freq,
            group_id=str(group["id"]),
        ),
        name=f"track_folder_{group['id']}",
    )


# ---- Domain view / actions ----


@router.callback_query(F.data.startswith("domain:view:"))
async def on_domain_view(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    domain_id = callback.data.split(":", 2)[2]
    domain = await queries.get_domain(async_engine, UUID(domain_id))
    if not domain:
        await callback.answer("\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d.", show_alert=True)
        return
    # Set as active domain
    await queries.update_user_active_domain(async_engine, callback.from_user.id, UUID(domain_id))

    entities = domain.get("entity_count") or 0
    synced = domain["last_synced_at"]
    synced_text = synced.strftime("%d.%m %H:%M") if synced else "\u043d\u0435\u0442"
    await callback.message.edit_text(
        f"{domain['emoji']} <b>{domain['display_name']}</b>\n"
        f"@{domain['channel_username']}\n\n"
        f"\u0421\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0439: {domain['message_count']}\n"
        f"\u0421\u0443\u0449\u043d\u043e\u0441\u0442\u0435\u0439: {entities}\n"
        f"\u0413\u043b\u0443\u0431\u0438\u043d\u0430: {_PERIOD_LABELS.get(domain['sync_depth'], domain['sync_depth'])}\n"
        f"\u0427\u0430\u0441\u0442\u043e\u0442\u0430: \u043a\u0430\u0436\u0434\u044b\u0435 {domain['sync_frequency_minutes']} \u043c\u0438\u043d\n"
        f"\u041f\u043e\u0441\u043b\u0435\u0434\u043d\u044f\u044f \u0441\u0438\u043d\u0445\u0440\u043e\u043d\u0438\u0437\u0430\u0446\u0438\u044f: {synced_text}",
        reply_markup=domain_actions_kb(domain_id),
    )
    await state.set_state(SettingsStates.editing_domain)
    await state.update_data(current_domain_id=domain_id)
    await callback.answer()


@router.callback_query(F.data == "domain:add")
async def on_domain_add(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    await callback.message.edit_text(
        "Отправьте ссылку на канал (@channel или https://t.me/channel)."
    )
    await state.set_state(AddChannelStates.waiting_link)
    await callback.answer()


@router.callback_query(F.data == "domain:back")
async def on_domain_back(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("\u0414\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d.", show_alert=True)
        return
    from agent_memory_mcp.bot.keyboards import manage_kb

    user_id = callback.from_user.id
    domains = await queries.list_domains(async_engine, user_id)
    groups = await gq.list_groups(async_engine, user_id)
    for g in groups:
        g["member_count"] = len(await gq.get_group_domain_ids(async_engine, g["id"]))
    grouped_ids = await gq.get_exclusively_grouped_domain_ids(async_engine, user_id)
    orphan_domains = [d for d in domains if d["id"] not in grouped_ids]
    await callback.message.edit_text(
        "\u270f\ufe0f <b>\u0420\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435</b>\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u0438\u043b\u0438 \u0441\u043f\u0438\u0441\u043e\u043a \u0434\u043b\u044f \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438:",
        reply_markup=manage_kb(orphan_domains, groups),
    )
    await state.set_state(SettingsStates.managing_domains)
    await callback.answer()


# ---- Delete flow ----


@router.callback_query(F.data.startswith("domain:delete:"))
async def on_domain_delete(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    domain_id = callback.data.split(":", 2)[2]
    domain = await queries.get_domain(async_engine, UUID(domain_id))
    if not domain:
        await callback.answer("\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d.", show_alert=True)
        return
    await callback.message.edit_text(
        f"\u0423\u0434\u0430\u043b\u0438\u0442\u044c \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a {domain['emoji']} <b>{domain['display_name']}</b>?\n"
        "\u0412\u0441\u0435 \u0434\u0430\u043d\u043d\u044b\u0435 \u0431\u0443\u0434\u0443\u0442 \u043f\u043e\u0442\u0435\u0440\u044f\u043d\u044b.",
        reply_markup=confirm_delete_kb(domain_id),
    )
    await state.set_state(SettingsStates.confirming_delete)
    await callback.answer()


@router.callback_query(F.data.startswith("confirm_delete:"))
async def on_confirm_delete(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    domain_id = callback.data.split(":", 1)[1]
    await queries.delete_domain(async_engine, UUID(domain_id))
    await callback.message.edit_text("\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u0443\u0434\u0430\u043b\u0451\u043d.")
    await state.clear()
    await callback.answer()


@router.callback_query(F.data == "cancel_delete")
async def on_cancel_delete(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("\u0414\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d.", show_alert=True)
        return
    from agent_memory_mcp.bot.keyboards import manage_kb

    user_id = callback.from_user.id
    domains = await queries.list_domains(async_engine, user_id)
    groups = await gq.list_groups(async_engine, user_id)
    for g in groups:
        g["member_count"] = len(await gq.get_group_domain_ids(async_engine, g["id"]))
    grouped_ids = await gq.get_exclusively_grouped_domain_ids(async_engine, user_id)
    orphan_domains = [d for d in domains if d["id"] not in grouped_ids]
    await callback.message.edit_text(
        "\u270f\ufe0f <b>\u0420\u0435\u0434\u0430\u043a\u0442\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435</b>\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u0438\u043b\u0438 \u0441\u043f\u0438\u0441\u043e\u043a \u0434\u043b\u044f \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438:",
        reply_markup=manage_kb(orphan_domains, groups),
    )
    await state.set_state(SettingsStates.managing_domains)
    await callback.answer()


# ---- Edit (placeholder for future) ----


@router.callback_query(F.data.startswith("domain:edit:"))
async def on_domain_edit(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("\u0414\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d.", show_alert=True)
        return
    domain_id = callback.data.split(":", 2)[2]
    domain = await queries.get_domain(async_engine, UUID(domain_id))
    if not domain:
        await callback.answer("\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d.", show_alert=True)
        return
    await callback.message.edit_text(
        f"{domain['emoji']} <b>{domain['display_name']}</b>\n\n"
        f"\u0427\u0430\u0441\u0442\u043e\u0442\u0430: \u043a\u0430\u0436\u0434\u044b\u0435 {domain['sync_frequency_minutes']} \u043c\u0438\u043d\n"
        f"\u0413\u043b\u0443\u0431\u0438\u043d\u0430: {_PERIOD_LABELS.get(domain['sync_depth'], domain['sync_depth'])}\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0447\u0442\u043e \u0438\u0437\u043c\u0435\u043d\u0438\u0442\u044c:",
        reply_markup=domain_edit_kb(domain_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("dedit:freq:"))
async def on_dedit_freq(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    domain_id = callback.data.split(":")[2]
    await callback.message.edit_text(
        "\u23f1 \u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0447\u0430\u0441\u0442\u043e\u0442\u0443 \u0441\u0438\u043d\u0445\u0440\u043e\u043d\u0438\u0437\u0430\u0446\u0438\u0438:",
        reply_markup=edit_freq_kb(domain_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("efreq:"))
async def on_efreq(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    parts = callback.data.split(":")
    domain_id = parts[1]
    freq = int(parts[2])
    await queries.update_domain(async_engine, UUID(domain_id), sync_frequency_minutes=freq)
    domain = await queries.get_domain(async_engine, UUID(domain_id))
    await callback.answer(f"\u2705 \u0427\u0430\u0441\u0442\u043e\u0442\u0430: {freq} \u043c\u0438\u043d")
    await callback.message.edit_text(
        f"{domain['emoji']} <b>{domain['display_name']}</b>\n\n"
        f"\u0427\u0430\u0441\u0442\u043e\u0442\u0430: \u043a\u0430\u0436\u0434\u044b\u0435 {freq} \u043c\u0438\u043d\n"
        f"\u0413\u043b\u0443\u0431\u0438\u043d\u0430: {_PERIOD_LABELS.get(domain['sync_depth'], domain['sync_depth'])}\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0447\u0442\u043e \u0438\u0437\u043c\u0435\u043d\u0438\u0442\u044c:",
        reply_markup=domain_edit_kb(domain_id),
    )


@router.callback_query(F.data.startswith("dedit:depth:"))
async def on_dedit_depth(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    domain_id = callback.data.split(":")[2]
    await callback.message.edit_text(
        "\U0001f4c5 \u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0433\u043b\u0443\u0431\u0438\u043d\u0443 \u0441\u0438\u043d\u0445\u0440\u043e\u043d\u0438\u0437\u0430\u0446\u0438\u0438:",
        reply_markup=edit_depth_kb(domain_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("edepth:"))
async def on_edepth(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    parts = callback.data.split(":")
    domain_id = parts[1]
    depth_key = parts[2]
    await queries.update_domain(async_engine, UUID(domain_id), sync_depth=depth_key)
    domain = await queries.get_domain(async_engine, UUID(domain_id))
    label = _PERIOD_LABELS.get(depth_key, depth_key)
    await callback.answer(f"\u2705 \u0413\u043b\u0443\u0431\u0438\u043d\u0430: {label}")
    await callback.message.edit_text(
        f"{domain['emoji']} <b>{domain['display_name']}</b>\n\n"
        f"\u0427\u0430\u0441\u0442\u043e\u0442\u0430: \u043a\u0430\u0436\u0434\u044b\u0435 {domain['sync_frequency_minutes']} \u043c\u0438\u043d\n"
        f"\u0413\u043b\u0443\u0431\u0438\u043d\u0430: {label}\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0447\u0442\u043e \u0438\u0437\u043c\u0435\u043d\u0438\u0442\u044c:",
        reply_markup=domain_edit_kb(domain_id),
    )


@router.callback_query(F.data.startswith("dedit:emoji:"))
async def on_dedit_emoji(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    domain_id = callback.data.split(":")[2]
    await callback.message.edit_text(
        "\U0001f600 \u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u044d\u043c\u043e\u0434\u0437\u0438:",
        reply_markup=edit_emoji_kb(domain_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("eemoji:"))
async def on_eemoji(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        return
    parts = callback.data.split(":")
    domain_id = parts[1]
    new_emoji = parts[2]
    await queries.update_domain(async_engine, UUID(domain_id), emoji=new_emoji)
    domain = await queries.get_domain(async_engine, UUID(domain_id))
    await callback.answer(f"\u2705 \u042d\u043c\u043e\u0434\u0437\u0438: {new_emoji}")
    await callback.message.edit_text(
        f"{new_emoji} <b>{domain['display_name']}</b>\n\n"
        f"\u0427\u0430\u0441\u0442\u043e\u0442\u0430: \u043a\u0430\u0436\u0434\u044b\u0435 {domain['sync_frequency_minutes']} \u043c\u0438\u043d\n"
        f"\u0413\u043b\u0443\u0431\u0438\u043d\u0430: {_PERIOD_LABELS.get(domain['sync_depth'], domain['sync_depth'])}\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0447\u0442\u043e \u0438\u0437\u043c\u0435\u043d\u0438\u0442\u044c:",
        reply_markup=domain_edit_kb(domain_id),
    )


# ---- Settings: search mode ----

_VALID_MODES = {"fast", "balanced", "deep"}


@router.callback_query(F.data == "settings:search_mode")
async def on_settings_search_mode(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    user = await queries.get_user(async_engine, callback.from_user.id)
    current = user.get("detail_level", "balanced") if user else "balanced"
    if current not in _VALID_MODES:
        current = "balanced"
    await callback.message.edit_text(
        "Выберите режим поиска:",
        reply_markup=search_mode_kb(current),
    )
    await callback.answer()


@router.callback_query(F.data == "settings:back")
async def on_settings_back(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    is_admin = callback.from_user and callback.from_user.id == app_settings.admin_telegram_id
    await callback.message.edit_text("Настройки:", reply_markup=settings_kb(is_admin=is_admin))
    await callback.answer()


@router.callback_query(F.data.startswith("mode:"))
async def on_mode_select(callback: CallbackQuery) -> None:
    if not is_allowed_user(callback.from_user.id, callback.from_user.username):
        await callback.answer("Доступ ограничен.", show_alert=True)
        return
    mode = callback.data.split(":", 1)[1]
    if mode not in _VALID_MODES:
        await callback.answer("Неизвестный режим.", show_alert=True)
        return
    await queries.update_user_search_mode(async_engine, callback.from_user.id, mode)
    await callback.message.edit_reply_markup(reply_markup=search_mode_kb(mode))
    await callback.answer()
