"""Telegram Knowledge Base — Context Engineering for Telegram channels."""

import logging
import sys

import structlog

# Haystack runs `haystack.logging.configure_logging()` at import time, which
# takes over structlog globally and filters at the ROOT logger's level. That
# level defaults to WARNING, so as soon as anything in the pipeline imported
# haystack, every INFO log of this app disappeared — including the whole sync
# pipeline, which made "why did this source sync nothing?" undiagnosable.
# Set the root level up front so that filter keeps INFO, and mute the
# third-party loggers that root level would otherwise let through.
logging.getLogger().setLevel(logging.INFO)
for _noisy in (
    "telethon",
    "httpx",
    "httpcore",
    "urllib3",
    "asyncio",
    "pymilvus",
    "aiohttp",
    "haystack",
    "langfuse",
    "opentelemetry",
    "transformers",
    "sentence_transformers",
    "filelock",
):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

# Configure structlog to write to stderr (Docker reliably captures stderr).
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(0),
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
)
