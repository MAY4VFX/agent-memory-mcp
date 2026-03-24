"""Export Milvus embeddings to Phoenix as a dataset.

Uses only httpx + pymilvus (already in container). No extra deps needed.

Usage:
    MILVUS_URI=http://milvus:19530 PHOENIX_URL=http://phoenix:6006 \
    python -m agent_memory_mcp.scripts.export_embeddings_to_phoenix

Note: Phoenix embedding cluster visualization (UMAP) requires Inferences mode
which is only available when running Phoenix locally with px.launch_app().
The deployed server stores the dataset for browsing/export.
"""

from __future__ import annotations

import os
import sys

import httpx
from pymilvus import MilvusClient

MILVUS_URI = os.getenv("MILVUS_URI", "http://192.168.2.140:19530")
PHOENIX_URL = os.getenv("PHOENIX_URL", "http://192.168.2.140:6006")
COLLECTION = "telegram_messages"
BATCH_SIZE = 1000
UPLOAD_CHUNK = 500  # Phoenix payload size limit


def fetch_from_milvus() -> list[dict]:
    """Fetch all vectors + metadata from Milvus."""
    print(f"Connecting to Milvus at {MILVUS_URI} ...")
    client = MilvusClient(uri=MILVUS_URI)

    stats = client.get_collection_stats(COLLECTION)
    total = stats.get("row_count", 0)
    print(f"Collection '{COLLECTION}' has {total} vectors")

    if total == 0:
        print("No vectors to export.")
        sys.exit(0)

    all_records: list[dict] = []
    fields = [
        "id", "channel_id", "thread_id", "content",
        "msg_date", "language", "content_type", "dense_vector",
    ]

    iterator = client.query_iterator(
        collection_name=COLLECTION,
        output_fields=fields,
        batch_size=BATCH_SIZE,
    )
    while True:
        batch = iterator.next()
        if not batch:
            break
        all_records.extend(batch)
        print(f"  fetched {len(all_records)}/{total} ...")

    print(f"Total fetched: {len(all_records)} records")
    return all_records


def upload_to_phoenix(records: list[dict]) -> None:
    """Upload records to Phoenix as a dataset via REST API."""
    url = f"{PHOENIX_URL}/v1/datasets/upload"
    total = len(records)
    print(f"Uploading {total} records to Phoenix at {PHOENIX_URL} ...")

    # First chunk creates the dataset, rest append
    for i in range(0, total, UPLOAD_CHUNK):
        chunk = records[i : i + UPLOAD_CHUNK]
        action = "create" if i == 0 else "append"

        inputs = []
        metadata = []
        for r in chunk:
            inputs.append({
                "content": (r.get("content") or "")[:500],
                "id": r.get("id", ""),
            })
            metadata.append({
                "channel_id": r.get("channel_id", 0),
                "thread_id": r.get("thread_id", ""),
                "language": r.get("language", ""),
                "content_type": r.get("content_type", ""),
                "msg_date": r.get("msg_date", 0),
            })

        payload = {
            "action": action,
            "name": "telegram_embeddings",
            "description": f"Milvus embeddings from {COLLECTION} ({total} vectors)",
            "inputs": inputs,
            "metadata": metadata,
        }

        resp = httpx.post(url, json=payload, timeout=60)
        if resp.status_code not in (200, 201):
            print(f"  ERROR uploading chunk {i}: {resp.status_code} {resp.text[:200]}")
            return
        print(f"  uploaded {min(i + UPLOAD_CHUNK, total)}/{total}")

    print(f"\nDataset uploaded: {PHOENIX_URL}/datasets")
    print("Navigate to Datasets -> telegram_embeddings")


def main() -> None:
    records = fetch_from_milvus()
    if not records:
        return
    upload_to_phoenix(records)


if __name__ == "__main__":
    main()
