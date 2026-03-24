"""Schema discovery component -- calls LLM tier2 to discover domain schema."""

from __future__ import annotations

import asyncio
import random

import structlog
from haystack import component

from agent_memory_mcp.config import settings
from agent_memory_mcp.llm.client import llm_call_json
from agent_memory_mcp.llm.prompts import SCHEMA_DISCOVERY_SYSTEM
from agent_memory_mcp.models.messages import ProcessedMessage
from agent_memory_mcp.models.schema import (
    DomainSchema,
    EntityType,
    RelationType,
    SchemaDiscoveryResult,
)

log = structlog.get_logger(__name__)


@component
class SchemaDiscovery:
    """Discover entity/relation schema from a sample of messages using LLM."""

    @component.output_types(result=SchemaDiscoveryResult)
    def run(self, messages: list[ProcessedMessage], domain_id: str) -> dict:
        loop = asyncio.new_event_loop()
        try:
            result = loop.run_until_complete(self._discover(messages, domain_id))
        finally:
            loop.close()
        return {"result": result}

    async def _discover(
        self, messages: list[ProcessedMessage], domain_id: str
    ) -> SchemaDiscoveryResult:
        sample_size = settings.schema_discovery_sample_size
        # Filter messages with text
        with_text = [m for m in messages if m.text and len(m.text.strip()) >= 10]

        if len(with_text) > sample_size:
            sample = random.sample(with_text, sample_size)
        else:
            sample = with_text

        if not sample:
            log.warning("schema_discovery_no_messages", domain_id=domain_id)
            return SchemaDiscoveryResult(
                schema=DomainSchema(),
                detected_domain="unknown",
                sample_size=0,
            )

        # Format messages for the LLM
        lines: list[str] = []
        for i, msg in enumerate(sample, 1):
            sender = msg.sender_name or "Unknown"
            lines.append(f"[{i}] [{sender}] {msg.text}")
        messages_text = "\n".join(lines)

        user_prompt = (
            f"Вот выборка из {len(sample)} сообщений канала:\n\n{messages_text}"
        )

        log.info("schema_discovery_start", domain_id=domain_id, sample_size=len(sample))

        raw = await llm_call_json(
            model=settings.llm_tier2_model,
            messages=[
                {"role": "system", "content": SCHEMA_DISCOVERY_SYSTEM},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
        )

        # Parse response into DomainSchema
        entity_types = [
            EntityType(
                name=et.get("name", ""),
                description=et.get("description", ""),
                examples=et.get("examples", []),
            )
            for et in raw.get("entity_types", [])
        ]
        relation_types = [
            RelationType(
                name=rt.get("name", ""),
                source_type=rt.get("source_type", ""),
                target_type=rt.get("target_type", ""),
                description=rt.get("description", ""),
            )
            for rt in raw.get("relation_types", [])
        ]

        schema = DomainSchema(
            domain_type=raw.get("domain_type", "other"),
            entity_types=entity_types,
            relation_types=relation_types,
        )

        log.info(
            "schema_discovery_done",
            domain_id=domain_id,
            domain_type=schema.domain_type,
            entity_types=len(entity_types),
            relation_types=len(relation_types),
        )

        return SchemaDiscoveryResult(
            schema=schema,
            detected_domain=schema.domain_type,
            sample_size=len(sample),
        )
