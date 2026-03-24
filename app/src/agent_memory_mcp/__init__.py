"""Telegram Knowledge Base — Context Engineering for Telegram channels."""

import sys

import structlog

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
