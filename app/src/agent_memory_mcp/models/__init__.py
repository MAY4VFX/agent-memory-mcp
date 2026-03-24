"""Pydantic models for the ingestion pipeline."""

from agent_memory_mcp.models.extraction import BaseEntity, BaseRelation, ExtractionResult
from agent_memory_mcp.models.messages import ProcessedMessage, ThreadGroup, TelegramMessage
from agent_memory_mcp.models.schema import DomainSchema, SchemaDiscoveryResult

__all__ = [
    "BaseEntity",
    "BaseRelation",
    "ExtractionResult",
    "ProcessedMessage",
    "TelegramMessage",
    "ThreadGroup",
    "DomainSchema",
    "SchemaDiscoveryResult",
]
