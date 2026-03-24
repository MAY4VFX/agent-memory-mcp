"""Telegram message models for the ingestion pipeline."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, Field

# Namespace for deterministic thread UUIDs
_THREAD_NS = uuid.UUID("b7e3f1a0-2d4c-4e6f-8a1b-c3d5e7f90123")


class TelegramMessage(BaseModel):
    """Raw Telegram message from Telethon."""

    message_id: int
    channel_id: int
    sender_id: int | None = None
    sender_name: str | None = None
    text: str = ""
    date: datetime
    reply_to_msg_id: int | None = None
    topic_id: int | None = None
    content_type: str = "text"
    raw_json: dict | None = None


class ProcessedMessage(BaseModel):
    """Message after noise filtering and metadata enrichment."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    domain_id: uuid.UUID
    message_id: int
    channel_id: int
    sender_id: int | None = None
    sender_name: str | None = None
    text: str = ""
    date: datetime
    reply_to_msg_id: int | None = None
    thread_id: uuid.UUID | None = None
    content_type: str = "text"
    language: str | None = None
    hashtags: list[str] = Field(default_factory=list)
    is_noise: bool = False


def telegram_to_processed(
    msg: "TelegramMessage", domain_id: uuid.UUID, channel_id: int = 0,
) -> "ProcessedMessage":
    """Convert a TelegramMessage to ProcessedMessage."""
    return ProcessedMessage(
        domain_id=domain_id,
        message_id=msg.message_id,
        channel_id=channel_id or msg.channel_id,
        sender_id=msg.sender_id,
        sender_name=msg.sender_name,
        text=msg.text,
        date=msg.date,
        reply_to_msg_id=msg.reply_to_msg_id,
        content_type=msg.content_type,
    )


def pg_row_to_processed(row: dict, channel_id: int) -> "ProcessedMessage":
    """Convert a PG messages row to ProcessedMessage."""
    return ProcessedMessage(
        id=row["id"],
        domain_id=row["domain_id"],
        message_id=row["telegram_msg_id"],
        channel_id=channel_id,
        sender_id=row.get("sender_id"),
        sender_name=row.get("sender_name"),
        text=row.get("content") or "",
        date=row["msg_date"],
        reply_to_msg_id=row.get("reply_to_msg_id"),
        content_type=row.get("content_type", "text"),
        language=row.get("language"),
        hashtags=row.get("hashtags") or [],
        is_noise=row.get("is_noise", False),
    )


class ThreadGroup(BaseModel):
    """Group of messages forming a thread / conversation."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    domain_id: uuid.UUID
    root_message_id: int
    messages: list[ProcessedMessage]
    combined_text: str = ""
    first_msg_date: datetime | None = None
    last_msg_date: datetime | None = None

    def make_deterministic_id(self) -> None:
        """Set id to a deterministic UUID5 based on domain_id + root_message_id.

        This ensures the same thread always gets the same ID,
        so Milvus upserts overwrite instead of duplicating.
        """
        self.id = uuid.uuid5(_THREAD_NS, f"{self.domain_id}_{self.root_message_id}")

    def build_combined_text(self) -> None:
        """Concatenate message texts into combined_text."""
        parts = []
        for msg in sorted(self.messages, key=lambda m: m.date):
            prefix = f"[{msg.sender_name or 'Unknown'}] " if msg.sender_name else ""
            parts.append(f"{prefix}{msg.text}")
        self.combined_text = "\n".join(parts)
        if self.messages:
            dates = [m.date for m in self.messages]
            self.first_msg_date = min(dates)
            self.last_msg_date = max(dates)
