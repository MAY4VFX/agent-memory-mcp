"""Telegram message link helpers."""

from __future__ import annotations


def private_internal_id(channel_id: int) -> int:
    """Internal chat id used in t.me/c/<id>/<msg> links.

    Telethon stores the raw channel id (e.g. 2154343300). The bot-API form is
    -100<rawid> (e.g. -1002154343300). t.me/c/ links need the raw id without the
    -100 prefix, so strip it if present; otherwise use the value as-is.
    """
    cid = abs(int(channel_id))
    s = str(cid)
    if s.startswith("100") and len(s) >= 13:
        return int(s[3:])
    return cid


def make_tme_link(
    username: str | None,
    msg_id: int,
    topic_id: int | None = None,
    channel_id: int | None = None,
) -> str:
    """Build a t.me link to a message.

    Public channels use t.me/<username>/<msg>. Private channels/supergroups
    (no username) fall back to t.me/c/<internal_id>/<msg> built from channel_id,
    which only works for members. Returns "" when no link can be built.
    """
    if not msg_id:
        return ""
    if username:
        if topic_id:
            return f"https://t.me/{username}/{topic_id}/{msg_id}"
        return f"https://t.me/{username}/{msg_id}"
    if channel_id:
        internal = private_internal_id(channel_id)
        if topic_id:
            return f"https://t.me/c/{internal}/{topic_id}/{msg_id}"
        return f"https://t.me/c/{internal}/{msg_id}"
    return ""
