"""Adaptive SGR schema models for Schema Discovery."""

from __future__ import annotations

from pydantic import BaseModel, Field


class EntityType(BaseModel):
    """Allowed entity type in a domain schema."""

    name: str
    description: str = ""
    examples: list[str] = Field(default_factory=list)


class RelationType(BaseModel):
    """Allowed relation type in a domain schema."""

    name: str
    source_type: str
    target_type: str
    description: str = ""


class DomainSchema(BaseModel):
    """Schema describing entity/relation types for a domain (channel)."""

    domain_type: str = ""
    entity_types: list[EntityType] = Field(default_factory=list)
    relation_types: list[RelationType] = Field(default_factory=list)


class SchemaDiscoveryResult(BaseModel):
    """Output of the schema discovery component."""

    schema: DomainSchema
    detected_domain: str = ""
    sample_size: int = 0
    langfuse_trace_id: str = ""
