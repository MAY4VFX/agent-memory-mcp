"""Metadata enricher component -- language detection and hashtag extraction."""

from __future__ import annotations

import re

from haystack import component

from agent_memory_mcp.models.messages import ProcessedMessage

_HASHTAG_RE = re.compile(r"#(\w+)", re.UNICODE)

# Cyrillic Unicode range
_CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")

_CYRILLIC_THRESHOLD = 0.30  # 30% cyrillic chars -> "ru"


@component
class MetadataEnricher:
    """Enriches messages with language detection and hashtag extraction."""

    @component.output_types(messages=list[ProcessedMessage])
    def run(self, messages: list[ProcessedMessage]) -> dict:
        for msg in messages:
            if msg.text:
                msg.language = self._detect_language(msg.text)
                msg.hashtags = self._extract_hashtags(msg.text)
            else:
                msg.language = "unknown"
        return {"messages": messages}

    @staticmethod
    def _detect_language(text: str) -> str:
        """Simple heuristic: cyrillic ratio > threshold -> 'ru', else 'en'."""
        alpha_chars = [c for c in text if c.isalpha()]
        if not alpha_chars:
            return "unknown"
        cyrillic_count = len(_CYRILLIC_RE.findall(text))
        ratio = cyrillic_count / len(alpha_chars)
        return "ru" if ratio >= _CYRILLIC_THRESHOLD else "en"

    @staticmethod
    def _extract_hashtags(text: str) -> list[str]:
        return _HASHTAG_RE.findall(text)
