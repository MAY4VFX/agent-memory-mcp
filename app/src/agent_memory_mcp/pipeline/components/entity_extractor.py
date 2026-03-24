"""Entity extractor component -- SGR Cascade via LLM tier1."""

from __future__ import annotations

import asyncio

import structlog
from haystack import component

from agent_memory_mcp.config import settings
from agent_memory_mcp.llm.client import llm_call_json
from agent_memory_mcp.llm.prompts import ENTITY_EXTRACTION_SYSTEM
from agent_memory_mcp.models.extraction import BaseEntity, BaseRelation
from agent_memory_mcp.models.messages import ThreadGroup
from agent_memory_mcp.models.schema import DomainSchema

log = structlog.get_logger(__name__)


@component
class EntityExtractor:
    """Extract entities and relations from threads using SGR Cascade."""

    @component.output_types(entities=list[BaseEntity], relations=list[BaseRelation])
    def run(
        self, threads: list[ThreadGroup], schema: DomainSchema, domain_id: str
    ) -> dict:
        loop = asyncio.new_event_loop()
        try:
            entities, relations = loop.run_until_complete(
                self._extract_all(threads, schema, domain_id)
            )
        finally:
            loop.close()
        return {"entities": entities, "relations": relations}

    async def _extract_all(
        self,
        threads: list[ThreadGroup],
        schema: DomainSchema,
        domain_id: str,
    ) -> tuple[list[BaseEntity], list[BaseRelation]]:
        batch_size = settings.extraction_batch_size
        concurrency = settings.extraction_concurrency
        sem = asyncio.Semaphore(concurrency)

        # Prepare schema strings for the prompt
        entity_type_names = ", ".join(et.name for et in schema.entity_types) or "Any"
        relation_type_names = ", ".join(rt.name for rt in schema.relation_types) or "Any"

        system_prompt = ENTITY_EXTRACTION_SYSTEM.format(
            entity_types=entity_type_names,
            relation_types=relation_type_names,
        )

        # Split threads into batches
        batches: list[list[ThreadGroup]] = []
        for i in range(0, len(threads), batch_size):
            batches.append(threads[i : i + batch_size])

        async def process_batch(batch: list[ThreadGroup]) -> tuple[list[BaseEntity], list[BaseRelation]]:
            async with sem:
                return await self._extract_batch(batch, system_prompt, domain_id)

        tasks = [process_batch(batch) for batch in batches]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_entities: list[BaseEntity] = []
        all_relations: list[BaseRelation] = []

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                log.error("extraction_batch_failed", batch=i, error=str(result))
                continue
            ents, rels = result
            all_entities.extend(ents)
            all_relations.extend(rels)

        log.info(
            "extraction_done",
            domain_id=domain_id,
            entities=len(all_entities),
            relations=len(all_relations),
            batches=len(batches),
        )
        return all_entities, all_relations

    async def _extract_batch(
        self,
        batch: list[ThreadGroup],
        system_prompt: str,
        domain_id: str,
    ) -> tuple[list[BaseEntity], list[BaseRelation]]:
        # Format threads into text, truncating to avoid token overflow
        parts: list[str] = []
        for thread in batch:
            if thread.combined_text:
                parts.append(thread.combined_text[:4000])
            else:
                for msg in thread.messages:
                    if msg.text:
                        parts.append(msg.text[:2000])

        text = "\n---\n".join(parts)
        # Limit total input to keep response within max_tokens
        text = text[:8000]
        user_prompt = f"Извлеки сущности и связи из следующих сообщений:\n\n{text}"

        raw = await llm_call_json(
            model=settings.llm_tier1_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.1,
            max_tokens=16384,
        )

        entities: list[BaseEntity] = []
        for e in raw.get("entities", []):
            entities.append(
                BaseEntity(
                    name=e.get("name", ""),
                    type=e.get("type", ""),
                    confidence=e.get("confidence", 0.8),
                    source_quote=e.get("source_quote", ""),
                    domain_id=domain_id,
                )
            )

        relations: list[BaseRelation] = []
        for r in raw.get("relations", []):
            relations.append(
                BaseRelation(
                    source=r.get("source", ""),
                    target=r.get("target", ""),
                    type=r.get("type", "RELATED_TO"),
                    evidence=r.get("evidence", ""),
                    confidence=r.get("confidence", 0.8),
                )
            )

        return entities, relations
