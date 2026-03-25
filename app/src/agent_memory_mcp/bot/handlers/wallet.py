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

# 1 TON ≈ 330 points (at $3.30/TON, $0.01/point)
_TOPUP_OPTIONS = [
    (0.5, "0.5 TON"),
    (1.0, "1 TON"),
    (3.0, "3 TON"),
    (5.0, "5 TON"),
    (10.0, "10 TON"),
]


@router.message(F.text == "💎 Top Up")
async def btn_topup(message: Message):
    """Show top-up amount selection."""
    ppt = settings.credits_per_ton  # points per TON
    rows = []
    row = []
    for amount, label in _TOPUP_OPTIONS:
        pts = int(amount * ppt)
        row.append(InlineKeyboardButton(
            text=f"{label} → {pts} pts",
            callback_data=f"topup:{amount}",
        ))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await message.answer(
        "💎 <b>Top Up</b>\n\n"
        f"Rate: 1 TON = {ppt} points (~$0.01 per point)\n"
        "Choose amount:",
        reply_markup=kb,
    )


@router.callback_query(F.data.startswith("topup:"))
async def cb_topup_amount(callback: CallbackQuery):
    """Generate payment link for selected amount."""
    amount_ton = float(callback.data.split(":")[1])
    points = int(amount_ton * settings.credits_per_ton)

    if not settings.ton_wallet_address:
        await callback.answer("TON wallet not configured.", show_alert=True)
        return

    payment_id = generate_payment_id()
    deeplink = build_ton_deeplink(amount_ton, payment_id)

    text = (
        f"💎 <b>Top up: {amount_ton} TON → {points} points</b>\n\n"
        f"Send <b>{amount_ton} TON</b> to:\n"
        f"<code>{settings.ton_wallet_address}</code>\n\n"
        f"Comment: <code>{payment_id}</code>\n\n"
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
            f"Added: +{result['credits_added']} points\n"
            f"Balance: {result['balance']} points\n"
            f"TX: <code>{result['tx_hash'][:16]}...</code>"
        )
    else:
        text = (
            "⏰ <b>Payment timeout</b>\n\n"
            "Payment not found within 5 minutes.\n"
            "If you sent TON, check your balance — it may arrive later."
        )
    try:
        await message.edit_text(text)
    except Exception:
        pass
