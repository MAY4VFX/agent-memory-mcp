"""Community detection using igraph Leiden on FalkorDB graph."""

from __future__ import annotations

import structlog
import igraph as ig

from agent_memory_mcp.config import settings
from agent_memory_mcp.llm.client import llm_call
from agent_memory_mcp.storage.falkordb_client import FalkorDBStorage

log = structlog.get_logger(__name__)

MIN_COMMUNITY_SIZE = 3  # skip trivial communities


async def detect_communities(
    domain_id: str,
    graph: FalkorDBStorage,
) -> list[dict]:
    """Detect communities via Leiden algorithm (igraph).

    Returns list of {id, members: list[str], size: int}.
    """
    entities, relations = await graph.export_graph_for_community(domain_id)
    if not entities or not relations:
        log.info("community_detect_skip", reason="no graph data", domain_id=domain_id)
        return []

    # Build igraph
    name_to_idx: dict[str, int] = {}
    names: list[str] = []
    for e in entities:
        n = e["name"]
        if n not in name_to_idx:
            name_to_idx[n] = len(names)
            names.append(n)

    edges: list[tuple[int, int]] = []
    for r in relations:
        src, tgt = r.get("source", ""), r.get("target", "")
        if src in name_to_idx and tgt in name_to_idx:
            edges.append((name_to_idx[src], name_to_idx[tgt]))

    if not edges:
        return []

    G = ig.Graph(n=len(names), edges=edges, directed=False)
    G.vs["name"] = names

    # Leiden community detection
    partition = G.community_leiden(
        objective_function="modularity",
        n_iterations=3,
    )

    result: list[dict] = []
    for i, members_idx in enumerate(partition):
        if len(members_idx) < MIN_COMMUNITY_SIZE:
            continue
        member_names = sorted(G.vs[idx]["name"] for idx in members_idx)
        result.append({
            "id": f"{domain_id[:8]}_c{i}",
            "members": member_names,
            "size": len(member_names),
        })

    log.info(
        "community_detect_done",
        domain_id=domain_id,
        total_communities=len(partition),
        significant=len(result),
        nodes=G.vcount(),
        edges=G.ecount(),
    )
    return result


async def summarize_community(
    community: dict,
    domain_id: str,
    graph: FalkorDBStorage,
) -> str:
    """Generate a short summary for a community using Tier 2 LLM."""
    members = community["members"]

    # Get relations between community members
    relations: list[str] = []
    for name in members[:15]:  # limit to avoid token overflow
        rels = await graph.query_entity_relations(name, domain_id)
        for r in rels:
            if r.get("target") in members:
                relations.append(
                    f"{r.get('source', '')} → {r.get('type', '')} → {r.get('target', '')}"
                )

    members_text = ", ".join(members[:30])
    relations_text = "\n".join(relations[:30]) if relations else "нет связей"

    prompt = (
        f"Ты аналитик знаний. Дана группа связанных сущностей из базы знаний.\n\n"
        f"Участники: {members_text}\n\n"
        f"Связи:\n{relations_text}\n\n"
        f"Напиши краткое описание этой тематической группы (2-3 предложения). "
        f"Что объединяет эти сущности? Какую тему/область они покрывают?"
    )

    summary = await llm_call(
        model=settings.llm_tier2_model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=256,
    )
    return summary.strip()


async def run_community_detection(
    domain_id: str,
    graph: FalkorDBStorage,
) -> int:
    """Full community detection pipeline: detect → summarize → store.

    Returns number of communities created.
    """
    # Clear old communities
    await graph.clear_communities(domain_id)

    communities = await detect_communities(domain_id, graph)
    if not communities:
        return 0

    for community in communities:
        # Summarize
        try:
            summary = await summarize_community(community, domain_id, graph)
        except Exception:
            log.exception("community_summarize_failed", community_id=community["id"])
            summary = f"Группа из {community['size']} сущностей"

        # Store community node
        await graph.merge_community(
            community_id=community["id"],
            domain_id=domain_id,
            summary=summary,
        )

        # Store MEMBER_OF relations
        for member in community["members"]:
            await graph.merge_member_of(member, community["id"], domain_id)

    log.info("community_pipeline_done", domain_id=domain_id, communities=len(communities))
    return len(communities)
