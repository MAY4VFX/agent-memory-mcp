"""Milvus vector storage for telegram messages."""

from __future__ import annotations

import structlog
from pymilvus import (
    AnnSearchRequest,
    CollectionSchema,
    DataType,
    FieldSchema,
    Function,
    FunctionType,
    MilvusClient,
    RRFRanker,
)

from agent_memory_mcp.config import settings

log = structlog.get_logger(__name__)

COLLECTION_NAME = "telegram_messages"

# Per-process cache of (uri, db) pairs already ensured, so we don't run an admin
# list/create on every MilvusStorage() instantiation (it's created per request).
_ensured_dbs: set[str] = set()


def _ensure_database(uri: str, db: str) -> None:
    """Create the Milvus database if it doesn't exist (idempotent, cached)."""
    key = f"{uri}::{db}"
    if key in _ensured_dbs:
        return
    admin = MilvusClient(uri=uri)
    try:
        if db not in admin.list_databases():
            admin.create_database(db)
    finally:
        admin.close()
    _ensured_dbs.add(key)


class MilvusStorage:
    """Milvus vector storage for telegram messages."""

    def __init__(
        self, host: str | None = None, port: int | None = None, db_name: str | None = None
    ) -> None:
        _host = host or settings.milvus_host
        _port = port or settings.milvus_port
        _db = db_name if db_name is not None else settings.milvus_db
        uri = f"http://{_host}:{_port}"
        # On a shared Milvus, isolate this project in its own database. Empty
        # db => use the server default (back-compat / escape hatch).
        if _db:
            _ensure_database(uri, _db)
            self._client = MilvusClient(uri=uri, db_name=_db)
        else:
            self._client = MilvusClient(uri=uri)
        log.info("milvus_connected", host=_host, port=_port, db=_db or "default")

    # ------------------------------------------------------------------ schema

    def ensure_collection(self) -> None:
        """Create collection with schema + indexes if not exists."""
        if self._client.has_collection(COLLECTION_NAME):
            log.info("milvus_collection_exists", name=COLLECTION_NAME)
            return

        fields = [
            FieldSchema("id", DataType.VARCHAR, max_length=64, is_primary=True),
            FieldSchema("channel_id", DataType.INT64, is_partition_key=True),
            FieldSchema("thread_id", DataType.VARCHAR, max_length=64),
            FieldSchema("content", DataType.VARCHAR, max_length=32768, enable_analyzer=True),
            FieldSchema("msg_date", DataType.INT64),
            FieldSchema("language", DataType.VARCHAR, max_length=8),
            FieldSchema("content_type", DataType.VARCHAR, max_length=32),
            FieldSchema("dense_vector", DataType.FLOAT_VECTOR, dim=settings.embedding_dim),
            FieldSchema("sparse_bm25", DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(fields=fields, enable_dynamic_field=False)

        # BM25 function: auto-generates sparse vectors from content text
        bm25_fn = Function(
            name="bm25",
            input_field_names=["content"],
            output_field_names=["sparse_bm25"],
            function_type=FunctionType.BM25,
        )
        schema.add_function(bm25_fn)

        index_params = self._client.prepare_index_params()
        index_params.add_index(
            field_name="dense_vector",
            index_type="IVF_FLAT",
            metric_type="COSINE",
            params={"nlist": 1024},
        )
        index_params.add_index(
            field_name="sparse_bm25",
            index_type="SPARSE_INVERTED_INDEX",
            metric_type="BM25",
        )

        self._client.create_collection(
            collection_name=COLLECTION_NAME,
            schema=schema,
            index_params=index_params,
        )
        log.info("milvus_collection_created", name=COLLECTION_NAME)

    def migrate_collection(self) -> None:
        """Drop and recreate collection if BM25 sparse field is missing."""
        if self._client.has_collection(COLLECTION_NAME):
            info = self._client.describe_collection(COLLECTION_NAME)
            field_names = [f["name"] for f in info.get("fields", [])]
            if "sparse_bm25" not in field_names:
                log.info("milvus_schema_migration", reason="adding sparse_bm25 for hybrid search")
                self._client.drop_collection(COLLECTION_NAME)
        self.ensure_collection()

    # ------------------------------------------------------------------ write

    def upsert_documents(self, documents: list[dict]) -> int:
        """Upsert documents into collection. Returns count."""
        if not documents:
            return 0
        # Milvus varchar max_length counts BYTES, not chars; Cyrillic is 2 bytes
        # per char, so a char-based slice overflows the 32768-byte limit. Truncate
        # on the UTF-8 byte length (with margin), decoding back on a char boundary.
        for doc in documents:
            c = doc.get("content")
            if c and len(c.encode("utf-8")) > 32000:
                doc["content"] = c.encode("utf-8")[:32000].decode("utf-8", "ignore")
        result = self._client.upsert(collection_name=COLLECTION_NAME, data=documents)
        count = result.get("upsert_count", len(documents))
        log.info("milvus_upsert", count=count)
        return count

    # ------------------------------------------------------------------ search

    def search(
        self,
        dense_vector: list[float],
        channel_id: int | None = None,
        limit: int = 10,
    ) -> list[dict]:
        """Dense vector search."""
        filter_expr = f"channel_id == {channel_id}" if channel_id is not None else ""
        output_fields = ["id", "channel_id", "thread_id", "content", "msg_date", "language", "content_type"]

        hits = self._client.search(
            collection_name=COLLECTION_NAME,
            data=[dense_vector],
            anns_field="dense_vector",
            search_params={"metric_type": "COSINE", "params": {"nprobe": 32}},
            limit=limit,
            filter=filter_expr or None,
            output_fields=output_fields,
        )[0]

        docs: list[dict] = []
        for hit in hits:
            entity = hit.get("entity", hit)
            entity["score"] = hit.get("distance", hit.get("score", 0.0))
            docs.append(entity)
        log.debug("milvus_search", results=len(docs))
        return docs

    # ------------------------------------------------------------------ multi-channel

    def search_multi_channel(
        self,
        dense_vector: list[float],
        channel_ids: list[int],
        limit: int = 20,
        query_text: str = "",
    ) -> list[dict]:
        """Search across multiple channels. Uses hybrid (dense+BM25) if query_text provided."""
        if query_text and settings.hybrid_search_enabled:
            return self.hybrid_search(dense_vector, query_text, channel_ids, limit=limit)

        if not channel_ids:
            return self.search(dense_vector, limit=limit)
        if len(channel_ids) == 1:
            return self.search(dense_vector, channel_id=channel_ids[0], limit=limit)
        id_list = ", ".join(str(cid) for cid in channel_ids)
        filter_expr = f"channel_id in [{id_list}]"
        output_fields = ["id", "channel_id", "thread_id", "content", "msg_date", "language", "content_type"]

        hits = self._client.search(
            collection_name=COLLECTION_NAME,
            data=[dense_vector],
            anns_field="dense_vector",
            search_params={"metric_type": "COSINE", "params": {"nprobe": 32}},
            limit=limit,
            filter=filter_expr,
            output_fields=output_fields,
        )[0]

        docs: list[dict] = []
        for hit in hits:
            entity = hit.get("entity", hit)
            entity["score"] = hit.get("distance", hit.get("score", 0.0))
            docs.append(entity)
        log.debug("milvus_search_multi", channels=len(channel_ids), results=len(docs))
        return docs

    # ------------------------------------------------------------------ hybrid

    def hybrid_search(
        self,
        dense_vector: list[float],
        query_text: str,
        channel_ids: list[int],
        limit: int = 20,
    ) -> list[dict]:
        """Hybrid search: dense COSINE + BM25, combined via RRFRanker."""
        filter_expr = ""
        if channel_ids:
            if len(channel_ids) == 1:
                filter_expr = f"channel_id == {channel_ids[0]}"
            else:
                id_list = ", ".join(str(cid) for cid in channel_ids)
                filter_expr = f"channel_id in [{id_list}]"

        output_fields = ["id", "channel_id", "thread_id", "content", "msg_date", "language", "content_type"]

        # Dense ANN request
        dense_req = AnnSearchRequest(
            data=[dense_vector],
            anns_field="dense_vector",
            param={"metric_type": "COSINE", "params": {"nprobe": 32}},
            limit=limit,
            expr=filter_expr or None,
        )

        # BM25 sparse request (raw text query)
        bm25_req = AnnSearchRequest(
            data=[query_text],
            anns_field="sparse_bm25",
            param={"metric_type": "BM25"},
            limit=limit,
            expr=filter_expr or None,
        )

        hits = self._client.hybrid_search(
            collection_name=COLLECTION_NAME,
            reqs=[dense_req, bm25_req],
            ranker=RRFRanker(),
            limit=limit,
            output_fields=output_fields,
        )[0]

        docs: list[dict] = []
        for hit in hits:
            entity = hit.get("entity", hit)
            entity["score"] = hit.get("distance", hit.get("score", 0.0))
            docs.append(entity)
        log.info("milvus_hybrid_search", channels=len(channel_ids), results=len(docs))
        return docs

    def search_temporal(
        self,
        dense_vector: list[float],
        channel_ids: list[int],
        date_from: int,
        date_to: int,
        limit: int = 20,
    ) -> list[dict]:
        """Dense vector search with temporal filter."""
        parts = [f"msg_date >= {date_from}", f"msg_date <= {date_to}"]
        if channel_ids:
            if len(channel_ids) == 1:
                parts.append(f"channel_id == {channel_ids[0]}")
            else:
                id_list = ", ".join(str(cid) for cid in channel_ids)
                parts.append(f"channel_id in [{id_list}]")
        filter_expr = " and ".join(parts)
        output_fields = ["id", "channel_id", "thread_id", "content", "msg_date", "language", "content_type"]

        hits = self._client.search(
            collection_name=COLLECTION_NAME,
            data=[dense_vector],
            anns_field="dense_vector",
            search_params={"metric_type": "COSINE", "params": {"nprobe": 32}},
            limit=limit,
            filter=filter_expr,
            output_fields=output_fields,
        )[0]

        docs: list[dict] = []
        for hit in hits:
            entity = hit.get("entity", hit)
            entity["score"] = hit.get("distance", hit.get("score", 0.0))
            docs.append(entity)
        log.debug("milvus_search_temporal", results=len(docs))
        return docs

    # ------------------------------------------------------------------ close

    def close(self) -> None:
        self._client.close()
