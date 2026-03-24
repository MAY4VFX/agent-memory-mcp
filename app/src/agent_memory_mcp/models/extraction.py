"""Entity and relation extraction models (SGR Cascade output)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class BaseEntity(BaseModel):
    """Extracted entity from text."""

    name: str
    type: str
    confidence: float = Field(ge=0.0, le=1.0, default=0.8)
    source_quote: str = ""
    domain_id: str = ""


class BaseRelation(BaseModel):
    """Extracted relation between two entities."""

    source: str
    target: str
    type: str
    evidence: str = ""
    confidence: float = Field(ge=0.0, le=1.0, default=0.8)


class ExtractionResult(BaseModel):
    """Result of entity/relation extraction from a batch of messages."""

    entities: list[BaseEntity] = Field(default_factory=list)
    relations: list[BaseRelation] = Field(default_factory=list)
    source_message_ids: list[int] = Field(default_factory=list)
