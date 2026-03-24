"""Handlers for domain (source) management."""

from __future__ import annotations

import re

import structlog
from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import Message

from agent_memory_mcp.bot.keyboards import main_menu_kb, period_kb, settings_kb
from agent_memory_mcp.bot.states import AddChannelStates, SettingsStates
from agent_memory_mcp.collector.client import TelegramCollector
from agent_memory_mcp.config import is_allowed_user, settings as app_settings
from agent_memory_mcp.db import queries
from agent_memory_mcp.db.engine import async_engine

log = structlog.get_logger(__name__)
router = Router()

_LINK_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/(?:\+)?([a-zA-Z0-9_]+)"
)

_DOMAIN_EMOJIS = {"\U0001f525", "\U0001f916", "\U0001f3ac", "\U0001f9e0",
                  "\U0001f4a1", "\U0001f4da", "\U0001f3a8", "\U0001f4b0",
                  "\U0001f4c1", "\U0001f30d"}


def _looks_like_channel(text: str) -> bool:
    text = text.strip()
    return text.startswith("@") or bool(_LINK_RE.match(text))


def _looks_like_channel_list(text: str) -> bool:
    """Check if text contains multiple channel links (one per line)."""
    if not text or "\n" not in text:
        return False
    lines = [line.strip() for line in text.strip().split("\n") if line.strip()]
    channel_lines = [line for line in lines if _looks_like_channel(line)]
    return len(channel_lines) >= 2


def _is_domain_button(text: str) -> bool:
    """Check if text matches the domain reply-keyboard button."""
    if not text:
        return False
    if text in ("\U0001f4da \u041a\u0430\u043d\u0430\u043b\u044b", "\U0001f4da \u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0438"):
        return True
    for e in _DOMAIN_EMOJIS:
        if text.startswith(e + " ") and not _looks_like_channel(text):
            return True
    return False


@router.message(F.text.func(_is_domain_button))
async def domain_button_handler(message: Message, state: FSMContext) -> None:
    """Handle domain reply-keyboard button -> show sources hub."""
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        await message.answer("\u0414\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d.")
        return
    user_domains = await queries.list_domains(async_engine, message.from_user.id)
    if not user_domains:
        await message.answer(
            "\u0423 \u0432\u0430\u0441 \u043d\u0435\u0442 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0451\u043d\u043d\u044b\u0445 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u043e\u0432.\n"
            "\u041e\u0442\u043f\u0440\u0430\u0432\u044c\u0442\u0435 \u0441\u0441\u044b\u043b\u043a\u0443 \u043d\u0430 \u043a\u0430\u043d\u0430\u043b (@channel \u0438\u043b\u0438 https://t.me/channel).",
            reply_markup=main_menu_kb(),
        )
        await state.set_state(AddChannelStates.waiting_link)
        return

    # Show sources hub
    from agent_memory_mcp.bot.handlers.groups import _build_hub

    text, kb = await _build_hub(message.from_user.id)
    await message.answer(text, reply_markup=kb)
    await state.set_state(SettingsStates.managing_domains)


@router.message(F.text == "\u2699\ufe0f \u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438")
async def settings_menu(message: Message, state: FSMContext) -> None:
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        await message.answer("\u0414\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d.")
        return
    is_admin = message.from_user and message.from_user.id == app_settings.admin_telegram_id
    await message.answer("\u041d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0438:", reply_markup=settings_kb(is_admin=is_admin))
    await state.clear()


# Batch-add: multiple channel links (one per line)
@router.message(F.text.func(_looks_like_channel_list))
async def channel_list_received(
    message: Message, state: FSMContext, collector: TelegramCollector
) -> None:
    """Handle multiple channel links -> resolve all, then ask period/frequency."""
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        await message.answer("\u0414\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d.")
        return

    lines = [line.strip() for line in message.text.strip().split("\n") if line.strip()]
    channel_lines = [line for line in lines if _looks_like_channel(line)]

    progress = await message.answer(f"\u23f3 \u0420\u0435\u0437\u043e\u043b\u0432\u043b\u044e {len(channel_lines)} \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u043e\u0432...")

    resolved: list[dict] = []
    skipped = 0
    errors: list[str] = []
    existing_domains = await queries.list_domains(async_engine, message.from_user.id)
    existing_cids = {d["channel_id"] for d in existing_domains}

    for link in channel_lines:
        try:
            info = await collector.resolve_channel(link)
            if info["channel_id"] in existing_cids:
                skipped += 1
                continue
            resolved.append(info)
            existing_cids.add(info["channel_id"])
        except Exception as e:
            errors.append(f"{link}: {e}")

    if not resolved:
        parts = ["\u2139\ufe0f \u041d\u0435\u0442 \u043d\u043e\u0432\u044b\u0445 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u043e\u0432 \u0434\u043b\u044f \u0434\u043e\u0431\u0430\u0432\u043b\u0435\u043d\u0438\u044f."]
        if skipped:
            parts.append(f"\u23e9 \u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u043e (\u0443\u0436\u0435 \u0435\u0441\u0442\u044c): {skipped}")
        if errors:
            parts.append("\u26a0\ufe0f \u041e\u0448\u0438\u0431\u043a\u0438:\n" + "\n".join(errors[:5]))
        await progress.edit_text("\n".join(parts))
        return

    # Store resolved channels in FSM, ask for period
    names = ", ".join(r["title"][:20] for r in resolved[:5])
    if len(resolved) > 5:
        names += f" (+{len(resolved) - 5})"
    err_note = ""
    if skipped:
        err_note += f"\n\u23e9 \u041f\u0440\u043e\u043f\u0443\u0449\u0435\u043d\u043e: {skipped}"
    if errors:
        err_note += f"\n\u26a0\ufe0f \u041e\u0448\u0438\u0431\u043e\u043a: {len(errors)}"

    await state.update_data(batch_channels=resolved)
    await progress.edit_text(
        f"\u2705 \u041d\u0430\u0439\u0434\u0435\u043d\u043e: {len(resolved)} \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u043e\u0432\n"
        f"{names}{err_note}\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0433\u043b\u0443\u0431\u0438\u043d\u0443 \u0441\u0438\u043d\u0445\u0440\u043e\u043d\u0438\u0437\u0430\u0446\u0438\u0438:",
        reply_markup=period_kb(),
    )
    await state.set_state(AddChannelStates.choosing_period)


@router.message(F.text.func(_looks_like_channel))
async def channel_link_received(
    message: Message, state: FSMContext, collector: TelegramCollector
) -> None:
    """Handle channel link sent at any point -> start add flow."""
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        await message.answer("\u0414\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d.")
        return
    await _resolve_and_ask_period(message, state, message.text.strip(), collector)


@router.message(AddChannelStates.waiting_link)
async def waiting_link_handler(
    message: Message, state: FSMContext, collector: TelegramCollector
) -> None:
    if not is_allowed_user(message.from_user.id, message.from_user.username):
        await message.answer("\u0414\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d.")
        return
    text = message.text.strip() if message.text else ""
    if not _looks_like_channel(text):
        await message.answer("\u041e\u0442\u043f\u0440\u0430\u0432\u044c\u0442\u0435 \u0441\u0441\u044b\u043b\u043a\u0443 \u043d\u0430 \u043a\u0430\u043d\u0430\u043b (@channel \u0438\u043b\u0438 https://t.me/channel).")
        return
    await _resolve_and_ask_period(message, state, text, collector)


async def _resolve_and_ask_period(
    message: Message, state: FSMContext, link: str, collector: TelegramCollector
) -> None:
    """Resolve channel and show period selection keyboard."""

    progress = await message.answer(f"\u0418\u0449\u0443 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a {link}...")
    try:
        info = await collector.resolve_channel(link)
    except Exception as exc:
        await progress.edit_text(f"\u041d\u0435 \u0443\u0434\u0430\u043b\u043e\u0441\u044c \u043d\u0430\u0439\u0442\u0438 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a: {exc}")
        await state.clear()
        return

    await state.update_data(
        channel_id=info["channel_id"],
        channel_username=info["username"],
        channel_name=info["title"],
    )
    await progress.edit_text(
        f"\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u043d\u0430\u0439\u0434\u0435\u043d: <b>{info['title']}</b> (@{info['username']})\n\n"
        "\u0412\u044b\u0431\u0435\u0440\u0438\u0442\u0435 \u0433\u043b\u0443\u0431\u0438\u043d\u0443 \u0441\u0438\u043d\u0445\u0440\u043e\u043d\u0438\u0437\u0430\u0446\u0438\u0438:",
        reply_markup=period_kb(),
    )
    await state.set_state(AddChannelStates.choosing_period)
