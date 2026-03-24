"""Token counting and truncation utilities."""

from __future__ import annotations

import tiktoken

_enc: tiktoken.Encoding | None = None


def _get_encoder() -> tiktoken.Encoding:
    global _enc
    if _enc is None:
        _enc = tiktoken.get_encoding("cl100k_base")
    return _enc


def count_tokens(text: str) -> int:
    return len(_get_encoder().encode(text))


def truncate_to_tokens(text: str, max_tokens: int) -> str:
    enc = _get_encoder()
    tokens = enc.encode(text)
    if len(tokens) <= max_tokens:
        return text
    return enc.decode(tokens[:max_tokens])
