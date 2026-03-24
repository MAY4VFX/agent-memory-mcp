"""Noise filter component -- removes non-informative messages."""

from __future__ import annotations

from haystack import component

from agent_memory_mcp.models.messages import ProcessedMessage

# content_type values that are always noise
_NOISE_TYPES = frozenset({"sticker", "geo", "video_note", "dice", "contact", "venue"})

# Service-message markers (content_type set by collector)
_SERVICE_TYPES = frozenset({
    "new_chat_members",
    "left_chat_member",
    "pinned_message",
    "migrate_to_chat_id",
    "migrate_from_chat_id",
})

_MIN_TEXT_LEN = 5


@component
class NoiseFilter:
    """Filter out noise messages (stickers, empty, service, very short)."""

    @component.output_types(clean=list[ProcessedMessage], noise=list[ProcessedMessage])
    def run(self, messages: list[ProcessedMessage]) -> dict:
        clean: list[ProcessedMessage] = []
        noise: list[ProcessedMessage] = []
        for msg in messages:
            if self._is_noise(msg):
                msg.is_noise = True
                noise.append(msg)
            else:
                clean.append(msg)
        return {"clean": clean, "noise": noise}

    @staticmethod
    def _is_noise(msg: ProcessedMessage) -> bool:
        if msg.content_type in _NOISE_TYPES:
            return True
        if msg.content_type in _SERVICE_TYPES:
            return True
        # Polls without text
        if msg.content_type == "poll" and not msg.text:
            return True
        # Voice notes without text (transcription not available)
        if msg.content_type in ("voice", "audio") and not msg.text:
            return True
        # Empty or very short text without reply context
        text = (msg.text or "").strip()
        if len(text) < _MIN_TEXT_LEN:
            return msg.reply_to_msg_id is None  # keep short replies
        return False
