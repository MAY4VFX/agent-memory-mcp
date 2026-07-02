"""One-time migration: split the single shared FalkorDB graph into per-domain graphs.

Before, all tenants lived in one graph (``agent_memory_mcp``) separated only by a
``domain_id`` property. For tenant isolation we now keep each domain in its own
graph ``agent_memory_mcp_<domain_id>`` (see storage/falkordb_client.py). This
script copies every Entity/RELATION/Community/MEMBER_OF from the old shared graph
into the per-domain graphs, verifies counts, and leaves the old graph in place.

Usage:
    python -m agent_memory_mcp.scripts.migrate_falkordb_per_domain          # dry run
    python -m agent_memory_mcp.scripts.migrate_falkordb_per_domain --apply  # write

After verifying the per-domain graphs, drop the old graph manually:
    redis-cli -h <host> -a <pw> GRAPH.DELETE agent_memory_mcp
"""

from __future__ import annotations

import argparse
import sys

from falkordb import FalkorDB

from agent_memory_mcp.config import settings
from agent_memory_mcp.storage.falkordb_client import (
    FalkorDBStorage,
    _parse_result,
    graph_name_for,
)


def _rows(graph, cypher: str, params: dict | None = None) -> list[dict]:
    return _parse_result(graph.query(cypher, params=params or {}))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually write (default: dry run)")
    args = ap.parse_args()

    db = FalkorDB(
        host=settings.falkordb_host,
        port=settings.falkordb_port,
        password=settings.falkordb_password,
    )
    src = db.select_graph(settings.falkordb_graph)

    domains = [
        r["domain_id"]
        for r in _rows(src, "MATCH (e:Entity) RETURN DISTINCT e.domain_id AS domain_id")
        if r.get("domain_id")
    ]
    print(f"Found {len(domains)} domain(s) in '{settings.falkordb_graph}'")

    storage = FalkorDBStorage() if args.apply else None
    total_ok = True

    for d in domains:
        entities = _rows(
            src,
            "MATCH (e:Entity {domain_id:$d}) "
            "RETURN e.name AS name, e.type AS type, e.confidence AS confidence, "
            "e.source_quote AS source_quote",
            {"d": d},
        )
        relations = _rows(
            src,
            "MATCH (a:Entity {domain_id:$d})-[r:RELATION]->(b:Entity {domain_id:$d}) "
            "RETURN a.name AS source, b.name AS target, r.type AS type, "
            "r.evidence AS evidence, r.confidence AS confidence",
            {"d": d},
        )
        communities = _rows(
            src,
            "MATCH (c:Community {domain_id:$d}) "
            "RETURN c.community_id AS community_id, c.summary AS summary, c.level AS level",
            {"d": d},
        )
        members = _rows(
            src,
            "MATCH (e:Entity {domain_id:$d})-[:MEMBER_OF]->(c:Community {domain_id:$d}) "
            "RETURN e.name AS name, c.community_id AS community_id",
            {"d": d},
        )
        print(
            f"  domain {d} → {graph_name_for(d)}: "
            f"{len(entities)} entities, {len(relations)} relations, "
            f"{len(communities)} communities, {len(members)} memberships"
        )

        if not args.apply:
            continue

        dst = storage._g(d)
        for e in entities:
            dst.query(
                "MERGE (e:Entity {name:$name, domain_id:$d}) "
                "SET e.type=$type, e.confidence=$confidence, e.source_quote=$source_quote",
                params={"name": e["name"], "d": d, "type": e.get("type", ""),
                        "confidence": e.get("confidence", 1.0),
                        "source_quote": e.get("source_quote", "")},
            )
        for r in relations:
            dst.query(
                "MERGE (a:Entity {name:$s, domain_id:$d}) "
                "MERGE (b:Entity {name:$t, domain_id:$d}) "
                "MERGE (a)-[rel:RELATION {type:$type}]->(b) "
                "SET rel.evidence=$evidence, rel.confidence=$confidence",
                params={"s": r["source"], "t": r["target"], "d": d,
                        "type": r.get("type", "RELATED_TO"),
                        "evidence": r.get("evidence", ""),
                        "confidence": r.get("confidence", 1.0)},
            )
        for c in communities:
            dst.query(
                "MERGE (c:Community {community_id:$cid, domain_id:$d}) "
                "SET c.summary=$summary, c.level=$level",
                params={"cid": c["community_id"], "d": d,
                        "summary": c.get("summary", ""), "level": c.get("level", 0)},
            )
        for m in members:
            dst.query(
                "MATCH (e:Entity {name:$name, domain_id:$d}) "
                "MATCH (c:Community {community_id:$cid, domain_id:$d}) "
                "MERGE (e)-[:MEMBER_OF]->(c)",
                params={"name": m["name"], "cid": m["community_id"], "d": d},
            )

        # Verify entity + relation counts round-tripped.
        got_e = _rows(dst, "MATCH (e:Entity) RETURN count(e) AS n")[0]["n"]
        got_r = _rows(dst, "MATCH ()-[r:RELATION]->() RETURN count(r) AS n")[0]["n"]
        ok = got_e == len(entities) and got_r == len(relations)
        total_ok = total_ok and ok
        print(f"    verify: entities {got_e}/{len(entities)}, relations {got_r}/{len(relations)} "
              f"{'OK' if ok else 'MISMATCH'}")

    if not args.apply:
        print("\nDry run — re-run with --apply to write.")
    elif total_ok:
        print("\nMigration complete and verified. Drop the old graph manually when satisfied.")
    else:
        print("\nMigration finished with COUNT MISMATCHES — inspect before dropping the old graph.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
