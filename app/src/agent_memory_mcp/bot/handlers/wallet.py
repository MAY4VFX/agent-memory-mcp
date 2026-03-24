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


@router.message(F.text == "💎 Top Up")
async def btn_topup(message: Message):
    """Show top-up amount selection."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="0.5 TON → 500 cr.", callback_data="topup:0.5"),
            InlineKeyboardButton(text="1 TON → 1000 cr.", callback_data="topup:1"),
        ],
        [
            InlineKeyboardButton(text="5 TON → 5000 cr.", callback_data="topup:5"),
            InlineKeyboardButton(text="10 TON → 10000 cr.", callback_data="topup:10"),
        ],
    ])
    await message.answer("💎 <b>Top up balance</b>\n\nChoose amount:", reply_markup=kb)


@router.callback_query(F.data.startswith("topup:"))
async def cb_topup_amount(callback: CallbackQuery):
    """Generate payment link for selected amount."""
    amount_ton = float(callback.data.split(":")[1])
    credits = int(amount_ton * settings.credits_per_ton)

    payment_id = generate_payment_id()
    deeplink = build_ton_deeplink(amount_ton, payment_id)

    text = (
        f"💎 <b>Top up: {amount_ton} TON → {credits} credits</b>\n\n"
        f"Send <b>{amount_ton} TON</b> to:\n"
        f"<code>{settings.ton_wallet_address}</code>\n\n"
        f"With comment: <code>{payment_id}</code>\n\n"
        "⏳ Waiting for payment (up to 5 min)..."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Pay with Tonkeeper", url=deeplink)],
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
            f"✅ <b>Payment confirmed!</b>\n\n"
            f"Added: +{result['credits_added']} credits\n"
            f"Balance: {result['balance']} credits\n"
            f"TX: <code>{result['tx_hash'][:16]}...</code>"
        )
    else:
        text = (
            "⏰ <b>Payment timeout</b>\n\n"
            "Payment not found. If you sent TON, "
            "wait a moment and check your balance."
        )
    try:
        await message.edit_text(text)
    except Exception:
        pass
