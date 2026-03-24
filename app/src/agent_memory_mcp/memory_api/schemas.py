"""Pydantic request/response models for Memory API."""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, Field


# --- Requests ---

class SearchMemoryRequest(BaseModel):
    query: str
    scope: str | None = None  # domain_id or @channel_username
    limit: int = Field(default=10, ge=1, le=100)


class AddSourceRequest(BaseModel):
    handle: str  # @channel or t.me/link
    source_type: str = "channel"  # channel | group | folder
    sync_range: str = "3m"  # 1w | 1m | 3m | 6m | 1y


class SyncSourceRequest(BaseModel):
    source_id: UUID
    sync_range: str | None = None


class GetDigestRequest(BaseModel):
    scope: str  # domain_id or @channel
    period: str = "7d"  # 1d | 3d | 7d | 30d


class GetDecisionsRequest(BaseModel):
    scope: str
    topic: str | None = None


class AgentContextRequest(BaseModel):
    task: str
    scope: str


class DeepAnalysisRequest(BaseModel):
    query: str
    scope: str
    max_posts: int = Field(default=200, ge=10, le=1000)


# --- Responses ---

class SourceItem(BaseModel):
    id: UUID
    channel_username: str | None = None
    display_name: str | None = None
    message_count: int = 0
    sync_status: str = "idle"
    sync_depth: str | None = None
    last_synced: str | None = None


class CreditInfo(BaseModel):
    balance: int
    total_used: int


class AccountResponse(BaseModel):
    telegram_id: int
    api_key_prefix: str
    credits: CreditInfo


class InsufficientCreditsResponse(BaseModel):
    error: str = "insufficient_credits"
    balance: int
    required: int
    topup_url: str


class MemorySearchResult(BaseModel):
    answer: str
    sources: list[dict] = []
    credits_used: int
    balance_after: int


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str = "0.1.0"
