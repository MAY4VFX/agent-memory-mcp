"""Eval batch runner — runs golden QA pairs through the pipeline + RAGAS scoring."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import datetime, timezone
from uuid import UUID

import structlog

from agent_memory_mcp.config import settings
from agent_memory_mcp.db import queries_conversations as qc
from agent_memory_mcp.db.engine import async_engine
from agent_memory_mcp.pipeline import eval_datasets
from agent_memory_mcp.pipeline.query_orchestrator import run_query_pipeline
from agent_memory_mcp.storage.embedding_client import EmbeddingClient
from agent_memory_mcp.storage.falkordb_client import FalkorDBStorage
from agent_memory_mcp.storage.milvus_client import MilvusStorage
from agent_memory_mcp.storage.reranker_client import RerankerClient
from agent_memory_mcp.tracing.tracer import get_langfuse

log = structlog.get_logger(__name__)


async def run_eval_batch(
    domain_id: UUID,
    user_id: int,
    run_name: str | None = None,
    progress_callback: Callable | None = None,
) -> dict:
    """Run all golden QA items through the pipeline and score with RAGAS.

    Returns: {run_name, items_count, note}
    """
    items = eval_datasets.list_items(domain_id=str(domain_id))
    if not items:
        # Fallback: try all items (handles legacy domain mismatch)
        items = eval_datasets.list_items(domain_id=None)
    if not items:
        return {"run_name": "", "items_count": 0, "note": "Нет golden пар"}

    if not run_name:
        run_name = f"eval-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M')}"

    # Storage clients (reused across items)
    milvus = MilvusStorage()
    graph = FalkorDBStorage()
    embedder = EmbeddingClient()
    reranker = RerankerClient()

    results: list[dict] = []
    try:
        for idx, item in enumerate(items):
            if progress_callback:
                await progress_callback(idx + 1, len(items))

            # Create temp conversation
            conv = await qc.create_conversation(
                async_engine,
                user_id=user_id,
                domain_id=domain_id,
                title=f"[eval] {item['question'][:40]}",
            )
            conv_id = conv["id"]

            try:
                t0 = time.monotonic()
                answer, payload = await run_query_pipeline(
                    query=item["question"],
                    user_id=user_id,
                    conversation_id=conv_id,
                    domain_ids=[domain_id],
                    engine=async_engine,
                    milvus=milvus,
                    graph=graph,
                    embedder=embedder,
                    reranker=reranker,
                    search_mode="deep",
                )
                elapsed = time.monotonic() - t0

                # Extract contexts from payload chunks
                contexts = [c.get("content", "") for c in payload.chunks if c.get("content")]

                results.append({
                    "item_id": item["id"],
                    "trace_id": answer.langfuse_trace_id,
                    "question": item["question"],
                    "answer": answer.answer,
                    "expected": item["expected"],
                    "contexts": contexts,
                    "payload": payload,
                    "latency": elapsed,
                })
            except Exception:
                log.exception("eval_item_failed", question=item["question"][:50])
            finally:
                # Cleanup temp conversation
                await qc.delete_conversation(async_engine, conv_id)

    finally:
        milvus.close()
        graph.close()
        await embedder.close()
        await reranker.close()

    if not results:
        return {"run_name": run_name, "items_count": 0, "note": "Все items провалились"}

    # Link traces to dataset as a run
    try:
        eval_datasets.create_run(
            run_name,
            [{"item_id": r["item_id"], "trace_id": r["trace_id"]} for r in results if r["trace_id"]],
        )
    except Exception:
        log.exception("eval_create_run_failed")

    # Component-level scores (Level 1)
    try:
        _write_component_scores(results)
    except Exception:
        log.exception("component_scores_failed")

    # RAGAS scoring (non-blocking — runs as separate step)
    try:
        await _run_ragas_scoring(results)
    except Exception:
        log.exception("ragas_scoring_failed")

    return {
        "run_name": run_name,
        "items_count": len(results),
        "note": "Прогон завершён. RAGAS scores записаны в Langfuse.",
    }


def _write_component_scores(results: list[dict]) -> None:
    """Write component-level (Level 1) scores to Langfuse traces."""
    lf = get_langfuse()
    if not lf:
        return
    for r in results:
        trace_id = r.get("trace_id")
        payload = r.get("payload")
        if not trace_id or not payload:
            continue
        chunks = payload.chunks
        total = len(chunks)
        high = sum(1 for c in chunks if c.get("relevance") == "high")
        precision = high / total if total else 0.0

        lf.create_score(trace_id=trace_id, name="retrieval_precision", value=precision)
        lf.create_score(trace_id=trace_id, name="retrieval_count", value=float(total))
        lf.create_score(trace_id=trace_id, name="crag_iterations", value=float(payload.crag_iterations))
        lf.create_score(trace_id=trace_id, name="context_tokens", value=float(payload.token_count))
        lf.create_score(trace_id=trace_id, name="latency_sec", value=r.get("latency", 0.0))
    try:
        lf.flush()
    except Exception:
        log.warning("component_scores_flush_timeout")
    log.info("component_scores_written", items=len(results))


async def _run_ragas_scoring(results: list[dict]) -> None:
    """Compute RAGAS metrics and write scores to Langfuse."""
    try:
        from ragas import evaluate, EvaluationDataset, SingleTurnSample
        from ragas.metrics import (
            Faithfulness, ResponseRelevancy, LLMContextPrecisionWithoutReference,
            LLMContextRecall, ContextEntityRecall, AnswerCorrectness,
            FactualCorrectness, SemanticSimilarity, NoiseSensitivity,
        )
        from ragas.llms import LangchainLLMWrapper
        from ragas.embeddings import LangchainEmbeddingsWrapper
        from langchain_openai import ChatOpenAI, OpenAIEmbeddings
    except ImportError:
        log.warning("ragas_not_installed", hint="pip install ragas langchain-openai")
        return

    lf = get_langfuse()
    if not lf:
        return

    # Build RAGAS samples
    # Truncate answers to avoid LLMDidNotFinishException in Faithfulness
    # Limit contexts to top-K to avoid TimeoutError in ContextPrecision
    _MAX_ANSWER_CHARS = 2000
    _MAX_CONTEXTS = 15
    samples = []
    trace_ids = []
    for r in results:
        if not r.get("trace_id"):
            continue
        answer_text = r["answer"]
        if len(answer_text) > _MAX_ANSWER_CHARS:
            answer_text = answer_text[:_MAX_ANSWER_CHARS] + "..."
        contexts = r["contexts"][:_MAX_CONTEXTS] if r["contexts"] else [""]
        samples.append(
            SingleTurnSample(
                user_input=r["question"],
                response=answer_text,
                retrieved_contexts=contexts,
                reference=r["expected"],
            )
        )
        trace_ids.append(r["trace_id"])

    if not samples:
        return

    dataset = EvaluationDataset(samples=samples)

    # LLM via LiteLLM proxy — high limits for RAGAS evaluation
    llm = ChatOpenAI(
        model="tier2/reasoning",
        api_key=settings.litellm_api_key,
        base_url=f"{settings.litellm_url}/v1",
        temperature=0,
        timeout=300,
        max_tokens=8192,
    )
    embeddings = OpenAIEmbeddings(
        model="bge-m3",
        api_key="dummy",
        base_url=f"{settings.embedding_url}/v1",
    )

    ragas_llm = LangchainLLMWrapper(llm)
    ragas_embeddings = LangchainEmbeddingsWrapper(embeddings)

    # Full RAGAS metric suite
    metric_instances = [
        Faithfulness(llm=ragas_llm),
        ResponseRelevancy(llm=ragas_llm, embeddings=ragas_embeddings),
        LLMContextPrecisionWithoutReference(llm=ragas_llm),
        LLMContextRecall(llm=ragas_llm),
        ContextEntityRecall(llm=ragas_llm),
        AnswerCorrectness(llm=ragas_llm, embeddings=ragas_embeddings),
        FactualCorrectness(llm=ragas_llm),
        SemanticSimilarity(embeddings=ragas_embeddings),
        NoiseSensitivity(llm=ragas_llm),
    ]

    # Run RAGAS evaluate in a thread (it's sync-heavy)
    log.info("ragas_evaluate_start", samples=len(samples), metrics=len(metric_instances))
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        lambda: evaluate(
            dataset=dataset,
            metrics=metric_instances,
            raise_exceptions=False,
        ),
    )
    log.info("ragas_evaluate_done")

    # Write per-item scores to Langfuse
    df = result.to_pandas()
    log.info("ragas_to_pandas_done", shape=str(df.shape), columns=list(df.columns))
    metric_map = {
        "faithfulness": "ragas_faithfulness",
        "answer_relevancy": "ragas_answer_relevancy",
        "llm_context_precision_without_reference": "ragas_context_precision",
        "context_recall": "ragas_context_recall",
        "context_entity_recall": "ragas_context_entity_recall",
        "answer_correctness": "ragas_answer_correctness",
        "factual_correctness": "ragas_factual_correctness",
        "semantic_similarity": "ragas_semantic_similarity",
        "noise_sensitivity_relevant": "ragas_noise_sensitivity",
    }

    for i, trace_id in enumerate(trace_ids):
        if i >= len(df):
            break
        for col, score_name in metric_map.items():
            if col in df.columns:
                value = df.iloc[i][col]
                if value is not None and not (isinstance(value, float) and value != value):
                    try:
                        lf.create_score(trace_id=trace_id, name=score_name, value=float(value))
                    except Exception:
                        log.debug("ragas_score_write_error", trace_id=trace_id, metric=score_name)

    log.info("ragas_scores_queued", items=len(trace_ids))
    try:
        lf.flush()
    except Exception:
        log.warning("ragas_flush_timeout")
    log.info("ragas_scoring_complete", items=len(trace_ids))
