"""FalkorDB writer component -- writes entities and relations to the graph."""

from __future__ import annotations

import asyncio

import structlog
from haystack import component

from agent_memory_mcp.models.extraction import BaseEntity, BaseRelation
from agent_memory_mcp.storage.falkordb_client import FalkorDBStorage

log = structlog.get_logger(__name__)


@component
class FalkorDBWriter:
    """Write entities and relations to FalkorDB graph."""

    @component.output_types(count=int)
    def run(
        self,
        entities: list[BaseEntity],
        relations: list[BaseRelation],
        domain_id: str,
    ) -> dict:
        if not entities and not relations:
            return {"count": 0}
        loop = asyncio.new_event_loop()
        try:
            count = loop.run_until_complete(
                self._write_all(entities, relations, domain_id)
            )
        finally:
            loop.close()
        return {"count": count}

    async def _write_all(
        self,
        entities: list[BaseEntity],
        relations: list[BaseRelation],
        domain_id: str,
    ) -> int:
        storage = FalkorDBStorage()
        try:
            count = 0
            for entity in entities:
                await storage.merge_entity(
                    {
                        "name": entity.name,
                        "type": entity.type,
                        "confidence": entity.confidence,
                        "source_quote": entity.source_quote,
                        "domain_id": domain_id,
                    }
                )
                count += 1

            for rel in relations:
                await storage.merge_relation(
                    {
                        "source": rel.source,
                        "target": rel.target,
                        "type": rel.type,
                        "evidence": rel.evidence,
                        "confidence": rel.confidence,
                        "domain_id": domain_id,
                    }
                )
                count += 1

            log.info(
                "falkordb_writer_done",
                entities=len(entities),
                relations=len(relations),
                domain_id=domain_id,
            )
            return count
        finally:
            storage.close()
