"""Wallet handlers — top-up flow via TON."""

from __future__ import annotations

import asyncio

from aiogram import F, Router
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

import structlog

from agent_memory_mcp.config import settings
from agent_memory_mcp.db.engine import async_engine
from agent_memory_mcp.ton.payments import build_ton_deeplink, generate_payment_id, process_topup

log = structlog.get_logger(__name__)

router = Router()


@router.message(F.text == "💎 Пополнить")
async def btn_topup(message: Message):
    """Show top-up amount selection (inline buttons for amount choice)."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="0.5 TON → 500 кр.", callback_data="topup:0.5"),
            InlineKeyboardButton(text="1 TON → 1000 кр.", callback_data="topup:1"),
        ],
        [
            InlineKeyboardButton(text="5 TON → 5000 кр.", callback_data="topup:5"),
            InlineKeyboardButton(text="10 TON → 10000 кр.", callback_data="topup:10"),
        ],
    ])
    await message.answer("💎 <b>Пополнение баланса</b>\n\nВыбери сумму:", reply_markup=kb)


@router.callback_query(F.data.startswith("topup:"))
async def cb_topup_amount(callback: CallbackQuery):
    """Generate payment link for selected amount."""
    amount_ton = float(callback.data.split(":")[1])
    credits = int(amount_ton * settings.credits_per_ton)

    payment_id = generate_payment_id()
    deeplink = build_ton_deeplink(amount_ton, payment_id)

    text = (
        f"💎 <b>Пополнение: {amount_ton} TON → {credits} кредитов</b>\n\n"
        f"Отправь <b>{amount_ton} TON</b> на адрес:\n"
        f"<code>{settings.ton_wallet_address}</code>\n\n"
        f"С комментарием: <code>{payment_id}</code>\n\n"
        "⏳ Ожидаю оплату (до 5 мин)..."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оплатить в Tonkeeper", url=deeplink)],
    ])
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()

    # Background payment verification
    user_id = callback.from_user.id
    from sqlalchemy import text as sql_text
    async with async_engine.begin() as conn:
        row = await conn.execute(
            sql_text("SELECT id FROM api_keys WHERE telegram_id = :tid AND is_active = true LIMIT 1"),
            {"tid": user_id},
        )
        key = row.mappings().first()

    if key:
        asyncio.create_task(
            _watch_payment(callback.message, key["id"], amount_ton, payment_id)
        )


async def _watch_payment(message, api_key_id, amount_ton: float, payment_id: str):
    """Background task: watch for payment and notify user."""
    result = await process_topup(
        async_engine, api_key_id, amount_ton, payment_id, timeout_seconds=300,
    )
    if result["status"] == "confirmed":
        text = (
            f"✅ <b>Оплата подтверждена!</b>\n\n"
            f"Зачислено: +{result['credits_added']} кредитов\n"
            f"Баланс: {result['balance']} кредитов\n"
            f"TX: <code>{result['tx_hash'][:16]}...</code>"
        )
    else:
        text = (
            "⏰ <b>Время ожидания истекло</b>\n\n"
            "Платёж не найден. Если ты отправил TON, "
            "подожди немного и проверь баланс."
        )
    try:
        await message.edit_text(text)
    except Exception:
        pass
