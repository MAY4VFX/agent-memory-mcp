"""Wallet handlers — balance, top-up, API key management."""

from __future__ import annotations

import asyncio

from aiogram import F, Router
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)

import structlog

from agent_memory_mcp.config import settings
from agent_memory_mcp.db.engine import async_engine
from agent_memory_mcp.memory_api.auth import create_api_key_for_user
from agent_memory_mcp.ton.payments import build_ton_deeplink, generate_payment_id, process_topup

log = structlog.get_logger(__name__)

router = Router()


# --- Balance ---

@router.callback_query(F.data == "menu:balance")
async def cb_balance(callback: CallbackQuery):
    """Show balance and recent transactions."""
    user_id = callback.from_user.id
    from sqlalchemy import text
    async with async_engine.begin() as conn:
        # Get balance
        row = await conn.execute(
            text("SELECT id, credits_balance, total_credits_used FROM api_keys WHERE telegram_id = :tid AND is_active = true LIMIT 1"),
            {"tid": user_id},
        )
        key = row.mappings().first()
        if not key:
            await callback.message.edit_text("Нет API ключа. Нажми /start")
            await callback.answer()
            return

        # Last 5 transactions
        rows = await conn.execute(
            text("""
                SELECT amount, type, endpoint, created_at
                FROM credit_transactions WHERE api_key_id = :kid
                ORDER BY created_at DESC LIMIT 5
            """),
            {"kid": key["id"]},
        )
        txs = rows.mappings().all()

    lines = [
        f"💰 <b>Баланс: {key['credits_balance']} кредитов</b>",
        f"Потрачено всего: {key['total_credits_used']}",
        "",
        "<b>Последние операции:</b>",
    ]
    for tx in txs:
        sign = "+" if tx["amount"] > 0 else ""
        ep = tx.get("endpoint") or tx["type"]
        dt = tx["created_at"].strftime("%d.%m %H:%M") if tx["created_at"] else ""
        lines.append(f" {sign}{tx['amount']}  {ep}  {dt}")
    if not txs:
        lines.append(" Пока нет операций")

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Пополнить", callback_data="menu:topup")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")],
    ])
    await callback.message.edit_text("\n".join(lines), reply_markup=kb)
    await callback.answer()


# --- Top-up ---

@router.callback_query(F.data == "menu:topup")
async def cb_topup_menu(callback: CallbackQuery):
    """Show top-up amount selection."""
    text = "💎 <b>Пополнение баланса</b>\n\nВыбери сумму:"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="0.5 TON → 500 кр.", callback_data="topup:0.5"),
            InlineKeyboardButton(text="1 TON → 1000 кр.", callback_data="topup:1"),
        ],
        [
            InlineKeyboardButton(text="5 TON → 5000 кр.", callback_data="topup:5"),
            InlineKeyboardButton(text="10 TON → 10000 кр.", callback_data="topup:10"),
        ],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:balance")],
    ])
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("topup:"))
async def cb_topup_amount(callback: CallbackQuery):
    """Generate payment link for selected amount."""
    amount_str = callback.data.split(":")[1]
    amount_ton = float(amount_str)
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
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="menu:balance")],
    ])
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()

    # Start background payment verification
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
            "подожди немного и проверь баланс через /start."
        )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ В меню", callback_data="menu:main")],
    ])
    try:
        await message.edit_text(text, reply_markup=kb)
    except Exception:
        pass  # Message may have been deleted


# --- API Keys ---

@router.callback_query(F.data == "menu:keys")
async def cb_keys(callback: CallbackQuery):
    """Show API keys."""
    user_id = callback.from_user.id
    from sqlalchemy import text
    async with async_engine.begin() as conn:
        rows = await conn.execute(
            text("""
                SELECT key_prefix, name, credits_balance, is_active, created_at, last_used_at
                FROM api_keys WHERE telegram_id = :tid ORDER BY created_at
            """),
            {"tid": user_id},
        )
        keys = rows.mappings().all()

    if not keys:
        await callback.message.edit_text("Нет ключей. Нажми /start")
        await callback.answer()
        return

    lines = ["🔑 <b>Твои API ключи:</b>\n"]
    for i, k in enumerate(keys, 1):
        status = "✅" if k["is_active"] else "❌"
        last = k["last_used_at"].strftime("%d.%m %H:%M") if k.get("last_used_at") else "—"
        lines.append(f"{i}. <code>{k['key_prefix']}...</code> ({k['name']}) {status}")
        lines.append(f"   Баланс: {k['credits_balance']} • Последнее: {last}")

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Новый ключ", callback_data="keys:new")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")],
    ])
    await callback.message.edit_text("\n".join(lines), reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data == "keys:new")
async def cb_new_key(callback: CallbackQuery):
    """Create a new API key."""
    user_id = callback.from_user.id

    # Check limit
    from sqlalchemy import text
    async with async_engine.begin() as conn:
        row = await conn.execute(
            text("SELECT COUNT(*) FROM api_keys WHERE telegram_id = :tid"),
            {"tid": user_id},
        )
        count = row.scalar()

    if count >= 5:
        await callback.answer("Максимум 5 ключей на аккаунт", show_alert=True)
        return

    full_key, rec = await create_api_key_for_user(async_engine, user_id, name=f"key-{count + 1}")
    text = (
        f"🔑 <b>Новый ключ создан:</b>\n\n"
        f"<code>{full_key}</code>\n\n"
        "⚠️ Сохрани — больше не покажу!"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ К ключам", callback_data="menu:keys")],
    ])
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()


# --- Usage ---

@router.callback_query(F.data == "menu:usage")
async def cb_usage(callback: CallbackQuery):
    """Show usage statistics."""
    user_id = callback.from_user.id
    from sqlalchemy import text
    async with async_engine.begin() as conn:
        row = await conn.execute(
            text("SELECT id FROM api_keys WHERE telegram_id = :tid AND is_active = true LIMIT 1"),
            {"tid": user_id},
        )
        key = row.mappings().first()
        if not key:
            await callback.message.edit_text("Нет API ключа.")
            await callback.answer()
            return

        rows = await conn.execute(
            text("""
                SELECT endpoint, COUNT(*) as cnt, SUM(ABS(amount)) as total_credits
                FROM credit_transactions
                WHERE api_key_id = :kid AND type = 'usage'
                  AND created_at > now() - interval '24 hours'
                GROUP BY endpoint ORDER BY total_credits DESC
            """),
            {"kid": key["id"]},
        )
        stats = rows.mappings().all()

    lines = ["📊 <b>Статистика за сегодня:</b>\n"]
    total_req = 0
    total_cr = 0
    for s in stats:
        ep = s["endpoint"] or "?"
        lines.append(f"  {ep}: {s['cnt']} запросов ({s['total_credits']} кр.)")
        total_req += s["cnt"]
        total_cr += s["total_credits"]
    if not stats:
        lines.append("  Нет запросов за сегодня")
    else:
        lines.append(f"\n  Итого: {total_req} запросов ({total_cr} кр.)")

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu:main")],
    ])
    await callback.message.edit_text("\n".join(lines), reply_markup=kb)
    await callback.answer()
