"""Telegram message link helpers."""

from __future__ import annotations


def make_tme_link(username: str, msg_id: int, topic_id: int | None = None) -> str:
    """Build a t.me link, using topic_id for forum supergroups."""
    if not username or not msg_id:
        return ""
    if topic_id:
        return f"https://t.me/{username}/{topic_id}/{msg_id}"
    return f"https://t.me/{username}/{msg_id}"
