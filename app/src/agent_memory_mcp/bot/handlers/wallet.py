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
from agent_memory_mcp.ton.payments import (
    build_ton_deeplink, generate_payment_id, get_ton_price_usd, process_topup, ton_to_points,
)

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
    """Show top-up options + purchase history."""
    try:
        ton_price = await get_ton_price_usd()
    except Exception:
        ton_price = 1.30  # fallback

    rows = []
    row = []
    for amount, label in _TOPUP_OPTIONS:
        pts = ton_to_points(amount, ton_price)
        row.append(InlineKeyboardButton(
            text=f"{label} → {pts} pts",
            callback_data=f"topup:{amount}",
        ))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    # Purchase history
    from sqlalchemy import text as sql_text
    async with async_engine.begin() as conn:
        bal_row = await conn.execute(
            sql_text("SELECT points_balance FROM users WHERE telegram_id = :tid"),
            {"tid": message.from_user.id},
        )
        balance = bal_row.scalar() or 0

        hist_rows = await conn.execute(
            sql_text("""
                SELECT amount, balance_after, created_at, ton_tx_hash
                FROM credit_transactions
                WHERE (telegram_id = :tid
                       OR api_key_id IN (SELECT id FROM api_keys WHERE telegram_id = :tid))
                  AND type = 'topup'
                ORDER BY created_at DESC LIMIT 5
            """),
            {"tid": message.from_user.id},
        )
        history = hist_rows.mappings().all()

    lines = [
        f"💎 <b>Top Up</b>\n",
        f"Balance: <b>{balance}</b> points",
        f"TON rate: ${ton_price:.2f} (live)",
        "1 point = $0.01\n",
    ]

    if history:
        lines.append("<b>Recent purchases:</b>")
        for h in history:
            dt = h["created_at"].strftime("%d.%m %H:%M") if h["created_at"] else ""
            tx = f" tx:{h['ton_tx_hash'][:8]}..." if h.get("ton_tx_hash") else ""
            lines.append(f"  +{h['amount']} pts → {h['balance_after']} bal  {dt}{tx}")
        lines.append("")

    lines.append("Choose amount:")

    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await message.answer("\n".join(lines), reply_markup=kb)


@router.callback_query(F.data.startswith("topup:"))
async def cb_topup_amount(callback: CallbackQuery):
    """Generate payment link for selected amount."""
    amount_ton = float(callback.data.split(":")[1])
    ton_price = await get_ton_price_usd()
    points = ton_to_points(amount_ton, ton_price)

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
        chat_id = callback.message.chat.id
        # Persist the in-flight payment so a restart doesn't drop it.
        async with async_engine.begin() as conn:
            await conn.execute(
                sql_text("""
                    INSERT INTO pending_payments (payment_id, api_key_id, chat_id, amount_ton, status)
                    VALUES (:pid, :kid, :cid, :amt, 'pending')
                    ON CONFLICT (payment_id) DO NOTHING
                """),
                {"pid": payment_id, "kid": key["id"], "cid": chat_id, "amt": amount_ton},
            )
        asyncio.create_task(
            _watch_payment(callback.message.bot, chat_id, key["id"], amount_ton, payment_id, message=callback.message)
        )


async def _watch_payment(bot, chat_id: int, api_key_id, amount_ton: float, payment_id: str, message=None):
    """Watch for a TON payment, credit it, persist the outcome, and notify the user.

    Works both for a live top-up (edits the original message) and for a resumed
    one after restart (sends a fresh message, since the old message is gone).
    """
    from sqlalchemy import text as sql_text
    result = await process_topup(
        async_engine, api_key_id, amount_ton, payment_id, timeout_seconds=300,
    )
    confirmed = result["status"] == "confirmed"
    async with async_engine.begin() as conn:
        await conn.execute(
            sql_text("""
                UPDATE pending_payments SET status = :st, tx_hash = :tx, resolved_at = now()
                WHERE payment_id = :pid
            """),
            {"st": "confirmed" if confirmed else "timeout", "tx": result.get("tx_hash"), "pid": payment_id},
        )
    if confirmed:
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
        if message is not None:
            await message.edit_text(text)
        else:
            await bot.send_message(chat_id, text)
    except Exception:
        pass


async def resume_pending_payments(bot) -> None:
    """Re-watch top-ups that were still pending when the process last stopped.

    Called on startup. Rows older than 30 min are considered lost (TonCenter only
    returns recent transactions) and marked timed-out rather than re-polled.
    """
    from sqlalchemy import text as sql_text
    async with async_engine.begin() as conn:
        await conn.execute(sql_text(
            "UPDATE pending_payments SET status='timeout', resolved_at=now() "
            "WHERE status='pending' AND created_at <= now() - make_interval(mins => 30)"
        ))
        rows = (await conn.execute(sql_text(
            "SELECT payment_id, api_key_id, chat_id, amount_ton FROM pending_payments "
            "WHERE status='pending'"
        ))).mappings().all()
    for r in rows:
        asyncio.create_task(
            _watch_payment(bot, r["chat_id"], r["api_key_id"], r["amount_ton"], r["payment_id"])
        )
    if rows:
        log.info("resumed_pending_payments", count=len(rows))
