"""TON payment processing — top-up credits via TON transfer.

Flow:
1. User requests top-up → backend generates unique comment (payment_id)
2. User sends TON to wallet with that comment
3. Backend polls TonCenter API for incoming TX with matching comment
4. TX found → credits added to API key balance
"""

from __future__ import annotations

import asyncio
import secrets
from uuid import UUID

import httpx
import structlog

from agent_memory_mcp.config import settings
from agent_memory_mcp.memory_api.auth import topup_credits

log = structlog.get_logger(__name__)


def generate_payment_id() -> str:
    """Generate a short unique payment identifier for TX comment."""
    return f"amm_{secrets.token_hex(4)}"


def build_ton_deeplink(amount_ton: float, comment: str) -> str:
    """Build a ton:// deeplink for wallet apps (Tonkeeper, etc.)."""
    nanoton = int(amount_ton * 1e9)
    addr = settings.ton_wallet_address
    return f"ton://transfer/{addr}?amount={nanoton}&text={comment}"


async def wait_for_payment(
    comment: str,
    expected_amount_ton: float,
    timeout_seconds: int = 300,
    poll_interval: int = 5,
) -> str | None:
    """Poll TonCenter API for an incoming TX matching comment.

    Returns TX hash if found, None if timeout.
    """
    expected_nanoton = int(expected_amount_ton * 1e9 * 0.95)  # 5% tolerance
    wallet = settings.ton_wallet_address
    if not wallet:
        log.error("ton_wallet_address_not_set")
        return None

    headers = {}
    if settings.ton_api_key:
        headers["X-API-Key"] = settings.ton_api_key

    deadline = asyncio.get_event_loop().time() + timeout_seconds

    async with httpx.AsyncClient(timeout=30) as client:
        while asyncio.get_event_loop().time() < deadline:
            try:
                resp = await client.get(
                    f"{settings.ton_api_url}/transactions",
                    params={"account": wallet, "limit": 20, "direction": "in"},
                    headers=headers,
                )
                data = resp.json()
                for tx in data.get("transactions", []):
                    in_msg = tx.get("in_msg") or {}
                    amount = int(in_msg.get("value", 0))
                    msg_content = in_msg.get("message_content", {})
                    decoded = msg_content.get("decoded", {})
                    msg_comment = decoded.get("comment", "")

                    if msg_comment == comment and amount >= expected_nanoton:
                        tx_hash = tx.get("hash", "unknown")
                        log.info(
                            "ton_payment_confirmed",
                            comment=comment,
                            amount=amount,
                            tx_hash=tx_hash,
                        )
                        return tx_hash
            except Exception:
                log.warning("ton_poll_error", exc_info=True)

            await asyncio.sleep(poll_interval)

    log.warning("ton_payment_timeout", comment=comment)
    return None


async def process_topup(
    engine,
    api_key_id: UUID,
    amount_ton: float,
    comment: str,
    timeout_seconds: int = 300,
) -> dict:
    """Full top-up flow: wait for payment → add credits.

    Returns status dict.
    """
    credits_amount = int(amount_ton * settings.credits_per_ton)

    tx_hash = await wait_for_payment(
        comment=comment,
        expected_amount_ton=amount_ton,
        timeout_seconds=timeout_seconds,
    )

    if tx_hash:
        new_balance = await topup_credits(engine, api_key_id, credits_amount, tx_hash)
        return {
            "status": "confirmed",
            "credits_added": credits_amount,
            "balance": new_balance,
            "tx_hash": tx_hash,
        }
    else:
        return {
            "status": "timeout",
            "message": "Платёж не найден. Попробуй ещё раз или проверь комментарий.",
        }
