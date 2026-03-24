"""Bot response formatters."""

from __future__ import annotations

import html

from agent_memory_mcp.models.query import QueryAnswer


def _sanitize_for_html(text: str) -> str:
    """Escape HTML entities, then restore safe Telegram HTML tags."""
    text = html.escape(text)
    # Restore bold: **text** → <b>text</b>
    import re
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    # Restore italic: *text* → <i>text</i>  (but not inside <b>)
    text = re.sub(r'(?<!\*)(\*)(?!\*)(.+?)(?<!\*)\1(?!\*)', r'<i>\2</i>', text)
    return text


def format_answer(answer: QueryAnswer, domain_name: str = "") -> str:
    """Format QueryAnswer for Telegram message."""
    parts: list[str] = []

    if domain_name:
        parts.append(f"\U0001f4da Домен: {domain_name}")
        parts.append("")

    parts.append(_sanitize_for_html(answer.answer))

    if answer.sources:
        parts.append("")
        parts.append("Источники:")
        for src in answer.sources:
            if src.url:
                parts.append(f"\u2022 {src.url}")

    return "\n".join(parts)


def format_conversation_export(messages: list[dict], title: str = "") -> str:
    """Format conversation messages as Markdown for export."""
    lines: list[str] = []
    if title:
        lines.append(f"# {title}")
        lines.append("")

    for msg in messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        created = msg.get("created_at", "")
        ts = str(created)[:19] if created else ""

        if role == "user":
            lines.append(f"**[{ts}] User:**")
        else:
            lines.append(f"**[{ts}] Assistant:**")
        lines.append(content)
        lines.append("")

    return "\n".join(lines)
