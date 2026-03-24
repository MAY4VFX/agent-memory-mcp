"""Langfuse tracing helpers — v3 SDK with observation types for Agent Graphs."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator

import structlog
from langfuse import Langfuse

from agent_memory_mcp.config import settings

log = structlog.get_logger()

_langfuse: Langfuse | None = None


def _is_enabled() -> bool:
    return bool(settings.langfuse_public_key and settings.langfuse_secret_key)


def get_langfuse() -> Langfuse | None:
    """Return a singleton Langfuse client, or None if not configured."""
    global _langfuse
    if not _is_enabled():
        return None
    if _langfuse is None:
        _langfuse = Langfuse(
            public_key=settings.langfuse_public_key,
            secret_key=settings.langfuse_secret_key,
            host=settings.langfuse_host,
        )
        log.info("langfuse_initialized", host=settings.langfuse_host)
    return _langfuse


class ObservationHandle:
    """Wrapper around a Langfuse observation (span/agent/tool/etc)."""

    def __init__(self, obs: Any) -> None:
        self._obs = obs

    @property
    def trace_id(self) -> str:
        try:
            return self._obs.trace_id or ""
        except Exception:
            return ""

    @property
    def observation_id(self) -> str:
        try:
            return self._obs.observation_id or ""
        except Exception:
            return ""

    def update(self, **kwargs) -> None:
        try:
            self._obs.update(**kwargs)
        except Exception:
            log.debug("langfuse_obs_update_error")

    def update_trace(self, **kwargs) -> None:
        try:
            self._obs.update_trace(**kwargs)
        except Exception:
            log.debug("langfuse_trace_update_error")

    def score(self, name: str, value: float, comment: str = "") -> None:
        try:
            lf = get_langfuse()
            if lf:
                lf.create_score(
                    trace_id=self.trace_id, name=name, value=value, comment=comment,
                )
        except Exception:
            pass

    def end(self, **kwargs) -> None:
        try:
            self._obs.end(**kwargs)
        except Exception:
            pass


@contextmanager
def trace_observation(
    as_type: str = "span",
    name: str = "",
    input: Any = None,
    metadata: dict | None = None,
    **kwargs,
) -> Generator[ObservationHandle | None, None, None]:
    """Context manager for creating a Langfuse observation.

    as_type can be: "span", "agent", "tool", "generation", "event",
                    "chain", "retriever", "evaluator", "embedding", "guardrail"

    When nested inside another trace_observation, automatically becomes a child.
    """
    lf = get_langfuse()
    if lf is None:
        yield None
        return

    try:
        obs_ctx = lf.start_as_current_observation(
            as_type=as_type,
            name=name,
            input=input,
            metadata=metadata or {},
            **kwargs,
        )
        obs = obs_ctx.__enter__()
        handle = ObservationHandle(obs)
        try:
            yield handle
        finally:
            obs_ctx.__exit__(None, None, None)
    except Exception:
        log.debug("langfuse_observation_error", name=name, as_type=as_type)
        yield None


def get_current_trace_id() -> str:
    """Get the current trace ID if inside an observation context."""
    lf = get_langfuse()
    if lf is None:
        return ""
    try:
        return lf.get_current_trace_id() or ""
    except Exception:
        return ""


def flush() -> None:
    """Flush pending Langfuse events."""
    if _langfuse:
        _langfuse.flush()
