# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

**Agent Memory MCP** — shared memory infrastructure for Telegram-native AI agents. Hackathon project for TON AI (Agent Infrastructure track).

## Architecture

- **Backend**: Python 3.12, FastAPI + uvicorn on port 8002
- **Bot**: aiogram 3.x, forum mode (supergroup with topics)
- **Collector**: Telethon (multi-user sessions, encrypted storage)
- **Memory Engine**: PostgreSQL/ParadeDB (BM25) + Milvus 2.5 (hybrid vectors) + FalkorDB (knowledge graph)
- **Embeddings**: BGE-M3 via TEI (shared with tg_kb)
- **LLM**: LiteLLM proxy with 3 tiers (extraction/reasoning/answer)
- **MCP**: FastMCP mounted at /mcp (Streamable HTTP) + separate pip package
- **Payments**: TON credits via TonCenter API polling

## Key Directories

- `app/src/agent_memory_mcp/` — main Python package
  - `memory_api/` — FastAPI routes, auth, credits, MCP tools
  - `pipeline/` — agent tools, query pipeline, orchestrators
  - `decision_pipeline/` — decision/action/question extraction
  - `ton/` — TON payment processing
  - `bot/handlers/` — forum.py (main menu), wallet.py (balance/topup/keys)
  - `collector/` — Telethon ingestion
  - `digest/` — clustering + map-reduce digests
  - `storage/` — Milvus, FalkorDB, embedding, reranker clients
  - `db/` — SQLAlchemy tables, queries, Alembic migrations
- `mcp-package/` — separate pip package (thin MCP client → REST API)
- `infra/` — Docker Compose for Milvus, FalkorDB (reference configs)

## Commands

```bash
# Run locally
cd app && pip install -e . && alembic upgrade head && python -m agent_memory_mcp

# Run migrations
cd app && alembic upgrade head

# Build MCP package
cd mcp-package && pip install build && python -m build
```

## Infrastructure (Dokploy)

- Project: `agent-memory-mcp` on 192.168.2.140
- PostgreSQL: `amm-postgres-rwqeha` (ParadeDB, own instance)
- Milvus: `amm-milvus-whraed` (own instance, S3 bucket: amm-milvus)
- FalkorDB: `amm-falkordb-pwpx2g` (own instance)
- Shared from tg_kb: embedding TEI, reranker TEI, LiteLLM, Langfuse

## Package name

Python package: `agent_memory_mcp` (underscores). pip package: `agent-memory-mcp` (hyphens).
