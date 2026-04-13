"""Application settings — loaded from environment variables."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}

    # --- Telegram Bot ---
    telegram_bot_token: str
    admin_telegram_id: int
    allowed_usernames: str = ""  # comma-separated: "user1,user2,user3"

    # --- Telegram Collector (Telethon) ---
    telegram_api_id: int
    telegram_api_hash: str
    telegram_session: str = ""

    # --- PostgreSQL ---
    database_url: str = Field(
        default="postgresql+asyncpg://amm_user:AmmSecure2026Pwd@amm-postgres-rwqeha:5432/agent_memory_mcp",
    )
    database_url_sync: str = Field(
        default="postgresql://amm_user:AmmSecure2026Pwd@amm-postgres-rwqeha:5432/agent_memory_mcp",
    )

    # --- Milvus ---
    milvus_host: str = "amm-milvus-whraed-milvus-1"
    milvus_port: int = 19530

    # --- FalkorDB ---
    falkordb_host: str = "amm-falkordb-pwpx2g-falkordb-1"
    falkordb_port: int = 6379
    falkordb_password: str = "AmmFalkor2026Pwd"
    falkordb_graph: str = "agent_memory_mcp"

    # --- Embedding (TEI) ---
    embedding_url: str = "http://tgkb-embedding-exnntq-embedding-1:8001"
    embedding_dim: int = 1024

    # --- LiteLLM proxy ---
    litellm_url: str = "http://tgkb-litellm-dokpcm-litellm-1:4000"
    litellm_api_key: str = "sk-yBWy0eAlQWaasuPX88tbdXtN-oXEDKoOeLDvKJy_yqs"

    # --- LLM model names (LiteLLM proxy aliases for fallback routing) ---
    llm_tier1_model: str = "tier1/extraction"
    llm_tier2_model: str = "tier2/reasoning"
    llm_tier3_model: str = "tier3/answer"

    # --- Langfuse ---
    langfuse_public_key: str = ""
    langfuse_secret_key: str = ""
    langfuse_host: str = "http://tgkb-langfuse-qo8tul-langfuse-1:3000"

    # --- Proxy (SOCKS5 for Telegram only, NOT for internal services) ---
    telegram_proxy: str = ""  # socks5://192.168.2.140:1080
    http_proxy: str = ""
    https_proxy: str = ""

    # --- Pipeline ---
    extraction_batch_size: int = 5
    extraction_concurrency: int = 20
    schema_discovery_sample_size: int = 75

    # --- Scheduler ---
    scheduler_check_interval: int = 30
    scheduler_max_concurrent: int = 3

    # --- Reranker (TEI) ---
    reranker_url: str = "http://compose-parse-haptic-microchip-b4gwl9-reranker-1:8001"

    # --- GPU lifecycle (on-demand wake/sleep) ---
    gpu_manager_enabled: bool = True
    gpu_idle_timeout: int = 300  # seconds before stopping idle GPU container
    gpu_startup_timeout: int = 120  # max seconds to wait for container healthy
    gpu_docker_host: str = "tcp://192.168.2.140:2375"  # Docker API via TCP
    gpu_coord_redis_url: str = "redis://gpu-coord-redis:6379/0"  # cross-project coordination
    gpu_embedding_container: str = "tgkb-embedding-exnntq-embedding-1"
    gpu_reranker_container: str = "compose-parse-haptic-microchip-b4gwl9-reranker-1"
    gpu_project_id: str = "amm"

    # --- Hybrid Search ---
    hybrid_search_enabled: bool = True

    # --- Query Pipeline ---
    use_agent_pipeline: bool = True
    query_vector_top_k: int = 60
    query_rerank_top_k: int = 25
    query_context_max_tokens: int = 32000
    query_history_max_messages: int = 10
    query_history_max_tokens: int = 4000
    query_crag_max_iterations: int = 3
    query_keyword_max_results: int = 200

    # --- Digest ---
    digest_max_messages: int = 200
    digest_batch_size: int = 15

    # --- API Server ---
    api_port: int = 8002

    # --- MCP ---
    run_mcp: bool = True

    # --- TON ---
    ton_wallet_address: str = ""
    ton_api_url: str = "https://toncenter.com/api/v3"
    ton_api_key: str = ""
    ton_manifest_url: str = ""
    credits_per_ton: int = 330  # 1 TON ≈ $3.30, 1 point ≈ $0.01
    welcome_bonus_credits: int = 100  # ~$1 worth, ~33 searches

    # --- Session encryption ---
    session_encryption_key: str = ""

    # --- Forum bot ---
    forum_chat_id: int = 0


settings = Settings()


def _parse_allowed() -> set[str]:
    raw = settings.allowed_usernames.strip()
    if not raw:
        return set()
    return {u.strip().lower().lstrip("@") for u in raw.split(",") if u.strip()}


_ALLOWED_USERNAMES: set[str] = _parse_allowed()


def is_allowed_user(telegram_id: int, username: str | None = None) -> bool:
    """Check if user is allowed to use the bot."""
    if telegram_id == settings.admin_telegram_id:
        return True
    if username and username.lower() in _ALLOWED_USERNAMES:
        return True
    return False
