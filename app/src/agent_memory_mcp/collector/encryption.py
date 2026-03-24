"""AES encryption for Telethon session strings."""

from __future__ import annotations

import base64
import hashlib

from cryptography.fernet import Fernet

from agent_memory_mcp.config import settings


def _get_fernet() -> Fernet:
    """Derive Fernet key from SESSION_ENCRYPTION_KEY setting."""
    raw = settings.session_encryption_key
    if not raw:
        raise RuntimeError(
            "SESSION_ENCRYPTION_KEY is not set. "
            "Generate one with: python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'"
        )
    # If it's already a valid Fernet key (44 url-safe base64 chars), use directly
    if len(raw) == 44:
        try:
            return Fernet(raw.encode())
        except Exception:
            pass
    # Otherwise derive a key from the raw string
    key = base64.urlsafe_b64encode(hashlib.sha256(raw.encode()).digest())
    return Fernet(key)


def encrypt_session(session_string: str) -> bytes:
    """Encrypt a Telethon StringSession string → bytes for DB storage."""
    return _get_fernet().encrypt(session_string.encode("utf-8"))


def decrypt_session(encrypted: bytes) -> str:
    """Decrypt session bytes from DB → Telethon StringSession string."""
    return _get_fernet().decrypt(encrypted).decode("utf-8")


def hash_phone(phone: str) -> str:
    """Hash phone number for verification (not reversible)."""
    return hashlib.sha256(phone.encode()).hexdigest()
