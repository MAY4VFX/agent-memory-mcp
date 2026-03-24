# Agent Memory MCP

**Shared memory infrastructure for Telegram-native AI agents.**

> Other teams build Telegram agents. We build the memory those agents use.

## Problem

Telegram AI agents can act, but they cannot remember. Every session starts from scratch — no context about past conversations, decisions, or community knowledge.

## Solution

Agent Memory MCP turns Telegram conversations into structured, persistent agent memory with hybrid retrieval, knowledge graphs, and decision tracking.

Any AI agent can connect via **MCP** or **REST API** and instantly access:
- Semantic search across channels and groups
- Structured digests with topic clustering
- Extracted decisions, action items, and open questions
- Full context packages for agent tasks

## Architecture

```
Telegram Sources (channels, groups, folders, topics)
        ↓
   Ingestion Layer (Telethon → noise filter → threading → entity extraction)
        ↓
   Memory Engine (PostgreSQL BM25 + Milvus vectors + FalkorDB knowledge graph)
        ↓
   Agent Interface (REST API + MCP Server)
        ↓
   External AI Agents (Claude, GPT, custom agents)
```

## Quick Start

### 1. MCP (Claude Desktop / Cursor)

```bash
pip install agent-memory-mcp
```

```json
{
  "mcpServers": {
    "agent-memory": {
      "command": "agent-memory-mcp",
      "env": {
        "AGENT_MEMORY_API_KEY": "amk_your_key_here",
        "AGENT_MEMORY_URL": "https://agent.ai-vfx.com"
      }
    }
  }
}
```

### 2. REST API

```bash
curl -X POST https://agent.ai-vfx.com/api/v1/memory/search \
  -H "Authorization: Bearer amk_your_key_here" \
  -H "Content-Type: application/json" \
  -d '{"query": "what decisions were made about wallet integration?"}'
```

### 3. Telegram Bot

Get your API key and manage credits: [@AgentMemoryBot](https://t.me/AgentMemoryBot)

## Memory Primitives

| Tool | Cost | Description |
|------|------|-------------|
| `search_memory` | 3 cr. | Semantic + keyword search with answer generation |
| `get_digest` | 10 cr. | Period digest with topic clustering |
| `get_decisions` | 5 cr. | Extract decisions, action items, open questions |
| `get_agent_context` | 10 cr. | Full context package for agent tasks |
| `add_source` | 5 cr. | Connect a Telegram channel/group |
| `list_sources` | free | List connected sources |

## Pricing

Usage-based credits. 1 TON ≈ 1000 credits.

| Tier | Credits |
|------|---------|
| Welcome bonus | 500 free credits |
| Search | 3 per request |
| Digest | 10 per request |
| Decisions | 5 per request |
| Deep Analysis | 25 per request |

Top up via TON in the Telegram bot.

## TON Integration

- **Credit system**: Pay with TON, use credits for API/MCP calls
- **Wallet-based auth**: Connect TON wallet for seamless top-ups
- **Pay-per-query**: No subscriptions, pay only for what you use

## Tech Stack

Python 3.12, FastAPI, aiogram 3, Telethon, FastMCP, PostgreSQL/ParadeDB, Milvus 2.5, FalkorDB, BGE-M3 embeddings (TEI), LiteLLM, Langfuse

## License

MIT
