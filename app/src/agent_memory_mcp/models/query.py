"""Pydantic models for the query pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, Field


class SelfRAGDecision(str, Enum):
    needs_retrieval = "needs_retrieval"
    direct_answer = "direct_answer"


class QueryTransformType(str, Enum):
    passthrough = "passthrough"
    expansion = "expansion"
    decomposition = "decomposition"
    hyde = "hyde"


class RouteType(str, Enum):
    vector = "vector"
    graph = "graph"
    keyword = "keyword"
    multi_hop = "multi_hop"
    temporal = "temporal"


class CRAGRelevance(str, Enum):
    high = "high"
    medium = "medium"
    low = "low"


# ---- Pipeline step outputs ----

class SelfRAGResult(BaseModel):
    decision: SelfRAGDecision = SelfRAGDecision.needs_retrieval
    reasoning: str = ""
    direct_answer: str | None = None


class TransformResult(BaseModel):
    transform_type: QueryTransformType = QueryTransformType.passthrough
    queries: list[str] = Field(default_factory=list)
    original_query: str = ""


class RouteResult(BaseModel):
    route: RouteType = RouteType.vector
    reasoning: str = ""
    keywords: list[str] = Field(default_factory=list)


class RetrievedChunk(BaseModel):
    id: str = ""
    content: str = ""
    score: float = 0.0
    channel_id: int = 0
    msg_date: int = 0
    thread_id: str = ""
    language: str = ""
    content_type: str = "text"
    relevance: CRAGRelevance = CRAGRelevance.medium


class GraphContext(BaseModel):
    entities: list[dict] = Field(default_factory=list)
    relations: list[dict] = Field(default_factory=list)
    community_summaries: list[str] = Field(default_factory=list)


class ContextPayloadData(BaseModel):
    query: str = ""
    transform_type: str = "passthrough"
    transformed_queries: list[str] = Field(default_factory=list)
    route: str = "vector"
    chunks: list[dict] = Field(default_factory=list)
    graph_context: dict = Field(default_factory=dict)
    crag_iterations: int = 0
    token_count: int = 0


class SourceReference(BaseModel):
    channel_username: str = ""
    message_id: int = 0
    url: str = ""


class QueryAnswer(BaseModel):
    answer: str = ""
    sources: list[SourceReference] = Field(default_factory=list)
    self_rag_decision: str = "needs_retrieval"
    route: str = "vector"
    chunks_used: int = 0
    crag_iterations: int = 0
    langfuse_trace_id: str = ""


class HistoryMessage(BaseModel):
    role: str
    content: str
    token_count: int = 0


class HistoryContext(BaseModel):
    messages: list[HistoryMessage] = Field(default_factory=list)
    total_tokens: int = 0
    has_summary: bool = False
    summary: str = ""


# ---- Search mode configuration ----

@dataclass
class SearchModeConfig:
    name: str
    always_keyword: bool
    always_vector: bool
    always_graph: bool
    use_router: bool
    vector_top_k: int
    keyword_max: int
    rerank_top_k: int
    crag_max_iterations: int
    context_max_tokens: int
    llm_tier: str
    temperature: float
    max_answer_tokens: int
    sources_limit: int


@dataclass
class AgentBudgetConfig:
    """Budget configuration for the A-RAG agent loop."""

    name: str
    max_steps: int
    llm_model: str
    token_budget: int
    temperature: float
    max_answer_tokens: int
    sources_limit: int


AGENT_BUDGETS: dict[str, AgentBudgetConfig] = {
    "fast": AgentBudgetConfig("Быстрый", 4, "tier2/reasoning", 16000, 0.2, 1024, 5),
    "balanced": AgentBudgetConfig("Точный", 8, "tier3/answer", 32000, 0.3, 4096, 10),
    "deep": AgentBudgetConfig("Глубокий", 15, "tier3/answer", 48000, 0.4, 16384, 20),
}


SEARCH_MODES: dict[str, SearchModeConfig] = {
    "fast": SearchModeConfig(
        name="Быстрый",
        always_keyword=False,
        always_vector=True,
        always_graph=False,
        use_router=False,
        vector_top_k=30,
        keyword_max=0,
        rerank_top_k=10,
        crag_max_iterations=0,
        context_max_tokens=16000,
        llm_tier="tier2/reasoning",
        temperature=0.2,
        max_answer_tokens=1024,
        sources_limit=5,
    ),
    "balanced": SearchModeConfig(
        name="Точный",
        always_keyword=True,
        always_vector=True,
        always_graph=False,
        use_router=True,
        vector_top_k=50,
        keyword_max=200,
        rerank_top_k=15,
        crag_max_iterations=1,
        context_max_tokens=32000,
        llm_tier="tier3/answer",
        temperature=0.3,
        max_answer_tokens=4096,
        sources_limit=10,
    ),
    "deep": SearchModeConfig(
        name="Глубокий",
        always_keyword=True,
        always_vector=True,
        always_graph=True,
        use_router=False,
        vector_top_k=100,
        keyword_max=200,
        rerank_top_k=20,
        crag_max_iterations=3,
        context_max_tokens=48000,
        llm_tier="tier3/answer",
        temperature=0.4,
        max_answer_tokens=16384,
        sources_limit=20,
    ),
}
