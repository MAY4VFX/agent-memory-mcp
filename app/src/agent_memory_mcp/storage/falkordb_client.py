"""FalkorDB graph storage for entities and relations."""

from __future__ import annotations

import asyncio

import structlog
from falkordb import FalkorDB

from agent_memory_mcp.config import settings

log = structlog.get_logger(__name__)


def _parse_result(result) -> list[dict]:
    """Parse FalkorDB query result into list of dicts.

    FalkorDB returns header as [[type_code, name], ...] not [name, ...].
    """
    if not result.result_set:
        return []
    keys = [h[1] if isinstance(h, list) else h for h in result.header]
    return [dict(zip(keys, row)) for row in result.result_set]


class FalkorDBStorage:
    """FalkorDB graph storage for entities and relations."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        password: str | None = None,
    ) -> None:
        _host = host or settings.falkordb_host
        _port = port or settings.falkordb_port
        _password = password or settings.falkordb_password
        self._db = FalkorDB(host=_host, port=_port, password=_password)
        self._graph = self._db.select_graph(settings.falkordb_graph)
        log.info("falkordb_connected", host=_host, port=_port, graph=settings.falkordb_graph)

    # ----------------------------------------------------------- sync internals

    def _merge_entity_sync(self, entity: dict) -> None:
        """MERGE an entity node."""
        self._graph.query(
            "MERGE (e:Entity {name: $name, domain_id: $domain_id}) "
            "SET e.type = $type, e.confidence = $confidence, e.source_quote = $source_quote",
            params={
                "name": entity["name"],
                "domain_id": entity["domain_id"],
                "type": entity.get("type", ""),
                "confidence": entity.get("confidence", 1.0),
                "source_quote": entity.get("source_quote", ""),
            },
        )

    def _merge_relation_sync(self, relation: dict) -> None:
        """MERGE a relation between two entities."""
        self._graph.query(
            "MERGE (a:Entity {name: $src_name, domain_id: $domain_id}) "
            "MERGE (b:Entity {name: $tgt_name, domain_id: $domain_id}) "
            "MERGE (a)-[r:RELATION {type: $type}]->(b) "
            "SET r.evidence = $evidence, r.confidence = $confidence",
            params={
                "src_name": relation["source"],
                "tgt_name": relation["target"],
                "domain_id": relation["domain_id"],
                "type": relation.get("type", "RELATED_TO"),
                "evidence": relation.get("evidence", ""),
                "confidence": relation.get("confidence", 1.0),
            },
        )

    def _merge_channel_sync(self, channel_id: int, name: str, domain_type: str = "") -> None:
        """MERGE a Channel node."""
        self._graph.query(
            "MERGE (c:Channel {channel_id: $channel_id}) "
            "SET c.name = $name, c.domain_type = $domain_type",
            params={"channel_id": channel_id, "name": name, "domain_type": domain_type},
        )

    def _query_entities_sync(self, domain_id: str, entity_type: str | None = None) -> list[dict]:
        """Query entities for a domain."""
        if entity_type:
            result = self._graph.query(
                "MATCH (e:Entity {domain_id: $domain_id, type: $type}) "
                "RETURN e.name AS name, e.type AS type, e.confidence AS confidence",
                params={"domain_id": domain_id, "type": entity_type},
            )
        else:
            result = self._graph.query(
                "MATCH (e:Entity {domain_id: $domain_id}) "
                "RETURN e.name AS name, e.type AS type, e.confidence AS confidence",
                params={"domain_id": domain_id},
            )
        return _parse_result(result)

    # ----------------------------------------------------------- community internals

    def _merge_community_sync(
        self, community_id: str, domain_id: str, summary: str, level: int = 0,
    ) -> None:
        """MERGE a Community node."""
        self._graph.query(
            "MERGE (c:Community {community_id: $community_id, domain_id: $domain_id}) "
            "SET c.summary = $summary, c.level = $level",
            params={
                "community_id": community_id,
                "domain_id": domain_id,
                "summary": summary,
                "level": level,
            },
        )

    def _merge_member_of_sync(
        self, entity_name: str, community_id: str, domain_id: str,
    ) -> None:
        """Create MEMBER_OF relation between entity and community."""
        self._graph.query(
            "MATCH (e:Entity {name: $name, domain_id: $domain_id}) "
            "MERGE (c:Community {community_id: $community_id, domain_id: $domain_id}) "
            "MERGE (e)-[:MEMBER_OF]->(c)",
            params={
                "name": entity_name,
                "community_id": community_id,
                "domain_id": domain_id,
            },
        )

    def _query_entity_community_sync(
        self, entity_name: str, domain_id: str,
    ) -> list[dict]:
        """Get communities for an entity."""
        result = self._graph.query(
            "MATCH (e:Entity {name: $name, domain_id: $domain_id})"
            "-[:MEMBER_OF]->(c:Community) "
            "RETURN c.community_id AS id, c.summary AS summary, c.level AS level",
            params={"name": entity_name, "domain_id": domain_id},
        )
        return _parse_result(result)

    def _export_graph_for_community_sync(
        self, domain_id: str,
    ) -> tuple[list[dict], list[dict]]:
        """Export entities and relations for community detection."""
        entities = self._query_entities_sync(domain_id)
        result = self._graph.query(
            "MATCH (a:Entity {domain_id: $domain_id})"
            "-[r:RELATION]-(b:Entity {domain_id: $domain_id}) "
            "RETURN DISTINCT a.name AS source, b.name AS target, r.type AS type",
            params={"domain_id": domain_id},
        )
        relations = _parse_result(result)
        return entities, relations

    def _clear_communities_sync(self, domain_id: str) -> None:
        """Remove all Community nodes and MEMBER_OF relations for a domain."""
        self._graph.query(
            "MATCH (c:Community {domain_id: $domain_id}) DETACH DELETE c",
            params={"domain_id": domain_id},
        )

    # ----------------------------------------------------------- query internals

    def _query_entity_neighbors_sync(
        self, name: str, domain_id: str, max_depth: int = 2,
    ) -> list[dict]:
        """Get entity and its neighbors up to max_depth."""
        result = self._graph.query(
            "MATCH path = (e:Entity {name: $name, domain_id: $domain_id})"
            "-[r:RELATION*1.." + str(max_depth) + "]-(n:Entity) "
            "RETURN DISTINCT n.name AS name, n.type AS type, "
            "n.confidence AS confidence, n.source_quote AS source_quote",
            params={"name": name, "domain_id": domain_id},
        )
        return _parse_result(result)

    def _query_entities_by_names_sync(
        self, names: list[str], domain_id: str,
    ) -> list[dict]:
        """Get entities by a list of names."""
        if not names:
            return []
        result = self._graph.query(
            "MATCH (e:Entity) "
            "WHERE e.domain_id = $domain_id AND e.name IN $names "
            "RETURN e.name AS name, e.type AS type, "
            "e.confidence AS confidence, e.source_quote AS source_quote",
            params={"domain_id": domain_id, "names": names},
        )
        return _parse_result(result)

    def _query_entity_relations_sync(
        self, name: str, domain_id: str,
    ) -> list[dict]:
        """Get all relations for an entity."""
        result = self._graph.query(
            "MATCH (a:Entity {name: $name, domain_id: $domain_id})"
            "-[r:RELATION]-(b:Entity) "
            "RETURN a.name AS source, b.name AS target, "
            "r.type AS type, r.evidence AS evidence, r.confidence AS confidence",
            params={"name": name, "domain_id": domain_id},
        )
        return _parse_result(result)

    def _aggregate_entity_counts_sync(
        self, domain_id: str, entity_type: str | None = None,
    ) -> list[dict]:
        """Count entities grouped by type."""
        if entity_type:
            result = self._graph.query(
                "MATCH (e:Entity {domain_id: $domain_id, type: $type}) "
                "RETURN e.type AS type, count(e) AS count",
                params={"domain_id": domain_id, "type": entity_type},
            )
        else:
            result = self._graph.query(
                "MATCH (e:Entity {domain_id: $domain_id}) "
                "RETURN e.type AS type, count(e) AS count "
                "ORDER BY count DESC",
                params={"domain_id": domain_id},
            )
        return _parse_result(result)

    # ----------------------------------------------------------- async wrappers

    async def merge_entity(self, entity: dict) -> None:
        await asyncio.to_thread(self._merge_entity_sync, entity)

    async def merge_relation(self, relation: dict) -> None:
        await asyncio.to_thread(self._merge_relation_sync, relation)

    async def merge_channel(self, channel_id: int, name: str, domain_type: str = "") -> None:
        await asyncio.to_thread(self._merge_channel_sync, channel_id, name, domain_type)

    async def query_entities(self, domain_id: str, entity_type: str | None = None) -> list[dict]:
        return await asyncio.to_thread(self._query_entities_sync, domain_id, entity_type)

    async def query_entity_neighbors(
        self, name: str, domain_id: str, max_depth: int = 2,
    ) -> list[dict]:
        return await asyncio.to_thread(
            self._query_entity_neighbors_sync, name, domain_id, max_depth,
        )

    async def query_entities_by_names(
        self, names: list[str], domain_id: str,
    ) -> list[dict]:
        return await asyncio.to_thread(
            self._query_entities_by_names_sync, names, domain_id,
        )

    async def query_entity_relations(
        self, name: str, domain_id: str,
    ) -> list[dict]:
        return await asyncio.to_thread(
            self._query_entity_relations_sync, name, domain_id,
        )

    async def aggregate_entity_counts(
        self, domain_id: str, entity_type: str | None = None,
    ) -> list[dict]:
        return await asyncio.to_thread(
            self._aggregate_entity_counts_sync, domain_id, entity_type,
        )

    async def merge_community(
        self, community_id: str, domain_id: str, summary: str, level: int = 0,
    ) -> None:
        await asyncio.to_thread(
            self._merge_community_sync, community_id, domain_id, summary, level,
        )

    async def merge_member_of(
        self, entity_name: str, community_id: str, domain_id: str,
    ) -> None:
        await asyncio.to_thread(
            self._merge_member_of_sync, entity_name, community_id, domain_id,
        )

    async def query_entity_community(
        self, entity_name: str, domain_id: str,
    ) -> list[dict]:
        return await asyncio.to_thread(
            self._query_entity_community_sync, entity_name, domain_id,
        )

    async def export_graph_for_community(
        self, domain_id: str,
    ) -> tuple[list[dict], list[dict]]:
        return await asyncio.to_thread(
            self._export_graph_for_community_sync, domain_id,
        )

    async def clear_communities(self, domain_id: str) -> None:
        await asyncio.to_thread(self._clear_communities_sync, domain_id)

    # ----------------------------------------------------------- raw Cypher execution

    def _execute_cypher_sync(self, cypher: str, params: dict | None = None) -> list[dict]:
        """Execute a raw Cypher query (READ-ONLY)."""
        result = self._graph.query(cypher, params=params or {})
        return _parse_result(result)

    async def execute_cypher(self, cypher: str, params: dict | None = None) -> list[dict]:
        return await asyncio.to_thread(self._execute_cypher_sync, cypher, params)

    # ----------------------------------------------------------- close

    def close(self) -> None:
        try:
            self._db.connection.close()
        except Exception:
            pass
