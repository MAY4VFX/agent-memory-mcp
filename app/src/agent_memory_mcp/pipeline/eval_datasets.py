"""Langfuse Datasets wrapper — CRUD for golden QA pairs and eval runs.

Compatible with Langfuse SDK 3.x API.
"""

from __future__ import annotations

from typing import Any

import structlog

from agent_memory_mcp.tracing.tracer import get_langfuse

log = structlog.get_logger(__name__)

DATASET_NAME = "tgkb-eval"


def ensure_dataset() -> str:
    """Create the dataset if it doesn't exist, return its name."""
    lf = get_langfuse()
    if not lf:
        raise RuntimeError("Langfuse not configured")
    try:
        lf.get_dataset(DATASET_NAME)
    except Exception:
        lf.create_dataset(name=DATASET_NAME, description="Golden QA pairs for tgkb eval")
    return DATASET_NAME


def add_item(
    question: str,
    expected_answer: str,
    domain_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict:
    """Add a golden QA pair to the dataset.

    domain_id ties the pair to a specific domain so eval_run only tests
    items matching the active domain.
    """
    lf = get_langfuse()
    if not lf:
        raise RuntimeError("Langfuse not configured")
    ensure_dataset()
    meta = metadata or {}
    if domain_id:
        meta["domain_id"] = str(domain_id)
    item = lf.create_dataset_item(
        dataset_name=DATASET_NAME,
        input={"question": question},
        expected_output={"answer": expected_answer},
        metadata=meta,
    )
    return {"id": item.id, "question": question}


def list_items(domain_id: str | None = None) -> list[dict]:
    """List items in the golden dataset, optionally filtered by domain_id.

    When domain_id is given, only items that either match the domain or have
    no domain_id set (legacy items) are returned.
    """
    lf = get_langfuse()
    if not lf:
        return []
    try:
        dataset = lf.get_dataset(DATASET_NAME)
    except Exception:
        return []
    result = []
    for it in dataset.items:
        if it.status != "ACTIVE":
            continue
        meta = it.metadata or {}
        item_domain = meta.get("domain_id", "")
        # Filter: if domain_id specified, skip items bound to a DIFFERENT domain
        if domain_id and item_domain and item_domain != str(domain_id):
            continue
        result.append({
            "id": it.id,
            "question": it.input.get("question", "") if isinstance(it.input, dict) else str(it.input),
            "expected": (
                it.expected_output.get("answer", "")
                if isinstance(it.expected_output, dict)
                else str(it.expected_output or "")
            ),
            "domain_id": item_domain,
        })
    return result


def create_run(run_name: str, items_with_traces: list[dict]) -> str:
    """Link traces to dataset items as a named run.

    items_with_traces: [{item_id, trace_id}, ...]
    Uses dataset_run_items.create() to link original pipeline traces directly.
    """
    from langfuse.api.resources.dataset_run_items.types import CreateDatasetRunItemRequest

    lf = get_langfuse()
    if not lf:
        raise RuntimeError("Langfuse not configured")

    for entry in items_with_traces:
        try:
            lf.api.dataset_run_items.create(
                request=CreateDatasetRunItemRequest(
                    runName=run_name,
                    datasetItemId=entry["item_id"],
                    traceId=entry["trace_id"],
                ),
            )
        except Exception:
            log.debug("create_run_item_error", item_id=entry["item_id"])

    lf.flush()
    return run_name


def get_run_scores(run_name: str) -> dict[str, float]:
    """Get average scores for a run across all items.

    Fetches scores via Langfuse REST API (trace-level scores).
    """
    lf = get_langfuse()
    if not lf:
        return {}

    try:
        run = lf.get_dataset_run(dataset_name=DATASET_NAME, run_name=run_name)
    except Exception:
        return {}

    if not run or not run.dataset_run_items:
        return {}

    # Collect scores from run items' original pipeline traces
    score_sums: dict[str, list[float]] = {}
    for run_item in run.dataset_run_items:
        trace_id = run_item.trace_id
        if not trace_id:
            continue
        try:
            scores = lf.api.score_v_2.get(trace_id=trace_id, limit=50)
            for sc in scores.data if hasattr(scores, "data") else []:
                name = sc.name
                value = sc.value
                if value is not None:
                    score_sums.setdefault(name, []).append(float(value))
        except Exception:
            log.debug("fetch_trace_scores_error", trace_id=trace_id)

    return {k: sum(v) / len(v) for k, v in score_sums.items() if v}


def list_runs() -> list[dict]:
    """List all runs for the dataset."""
    lf = get_langfuse()
    if not lf:
        return []
    try:
        runs = lf.get_dataset_runs(dataset_name=DATASET_NAME)
    except Exception:
        return []

    return [
        {
            "name": r.name,
            "created_at": str(r.created_at) if hasattr(r, "created_at") else "",
        }
        for r in (runs.data if hasattr(runs, "data") else [])
    ]
