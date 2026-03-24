"""Semantic clustering for digest pipeline: embed → dedup → cluster."""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import structlog

from agent_memory_mcp.storage.embedding_client import EmbeddingClient

log = structlog.get_logger(__name__)

_EMBED_BATCH = 32


@dataclass
class Cluster:
    """A group of semantically related messages."""

    messages: list[dict]
    embeddings: np.ndarray
    centroid_idx: int = 0
    label: str = ""
    emoji: str = "📌"


async def embed_messages(
    messages: list[dict],
    client: EmbeddingClient,
) -> tuple[list[dict], np.ndarray]:
    """Embed messages via BGE-M3 in batches. Skips empty content.

    Returns (filtered_messages, embeddings) — only messages with non-empty content.
    """
    valid: list[tuple[int, str]] = []
    for i, m in enumerate(messages):
        content = (m.get("content") or "").strip()
        if content:
            valid.append((i, content[:2000]))

    if not valid:
        return [], np.empty((0, 1024), dtype=np.float32)

    all_vectors: list[list[float]] = []
    texts = [t for _, t in valid]

    for start in range(0, len(texts), _EMBED_BATCH):
        batch = texts[start : start + _EMBED_BATCH]
        vecs = await client.embed_dense(batch)
        all_vectors.extend(vecs)

    filtered_msgs = [messages[i] for i, _ in valid]
    embeddings = np.array(all_vectors, dtype=np.float32)

    log.debug("embed_messages", total=len(messages), valid=len(filtered_msgs))
    return filtered_msgs, embeddings


def deduplicate(
    messages: list[dict],
    embeddings: np.ndarray,
    threshold: float = 0.92,
) -> tuple[list[dict], np.ndarray]:
    """Remove near-duplicate messages based on cosine similarity.

    For each duplicate pair, keeps the message with higher engagement score.
    """
    if len(messages) <= 1:
        return messages, embeddings

    # Cosine similarity matrix (embeddings already L2-normalized by TEI)
    sim_matrix = embeddings @ embeddings.T

    removed: set[int] = set()
    n = len(messages)

    for i in range(n):
        if i in removed:
            continue
        for j in range(i + 1, n):
            if j in removed:
                continue
            if sim_matrix[i, j] > threshold:
                # Keep the one with higher engagement
                score_i = _engagement_score(messages[i])
                score_j = _engagement_score(messages[j])
                if score_i >= score_j:
                    removed.add(j)
                else:
                    removed.add(i)
                    break  # i is removed, skip rest of j loop

    keep = [idx for idx in range(n) if idx not in removed]
    result_msgs = [messages[idx] for idx in keep]
    result_embs = embeddings[keep]

    log.debug("deduplicate", before=n, after=len(keep), removed=len(removed))
    return result_msgs, result_embs


def cluster_messages(
    messages: list[dict],
    embeddings: np.ndarray,
    min_cluster: int = 3,
    max_cluster: int = 25,
    sim_threshold: float = 0.55,
) -> list[Cluster]:
    """Greedy seed clustering based on cosine similarity.

    1. Sort messages by engagement (desc)
    2. First unassigned = seed of new cluster
    3. All unassigned with sim > threshold to seed → join cluster (up to max_cluster)
    4. Clusters < min_cluster → merge into "Разное"
    """
    if len(messages) == 0:
        return []

    n = len(messages)
    sim_matrix = embeddings @ embeddings.T

    # Sort indices by engagement score descending
    scores = [_engagement_score(m) for m in messages]
    order = sorted(range(n), key=lambda i: scores[i], reverse=True)

    assigned: set[int] = set()
    clusters: list[list[int]] = []

    for seed in order:
        if seed in assigned:
            continue
        cluster_idxs = [seed]
        assigned.add(seed)

        for candidate in order:
            if candidate in assigned:
                continue
            if len(cluster_idxs) >= max_cluster:
                break
            if sim_matrix[seed, candidate] > sim_threshold:
                cluster_idxs.append(candidate)
                assigned.add(candidate)

        clusters.append(cluster_idxs)

    # Merge small clusters into "misc"
    result: list[Cluster] = []
    misc_idxs: list[int] = []

    for idxs in clusters:
        if len(idxs) < min_cluster:
            misc_idxs.extend(idxs)
        else:
            result.append(Cluster(
                messages=[messages[i] for i in idxs],
                embeddings=embeddings[idxs],
                centroid_idx=0,  # seed is first (highest engagement)
            ))

    if misc_idxs:
        result.append(Cluster(
            messages=[messages[i] for i in misc_idxs],
            embeddings=embeddings[misc_idxs],
            centroid_idx=0,
            label="Разное",
            emoji="📌",
        ))

    log.debug(
        "cluster_messages",
        total=n, clusters=len(result),
        sizes=[len(c.messages) for c in result],
    )
    return result


def _engagement_score(msg: dict) -> float:
    """Simple engagement score for ranking."""
    content_len = len(msg.get("content") or "")
    return min(content_len / 100, 5.0)
