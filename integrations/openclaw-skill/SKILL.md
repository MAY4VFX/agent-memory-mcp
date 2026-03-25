---
name: agent-memory
description: Long-term Telegram memory for AI agents — search conversations, get digests, extract decisions, manage channel sources. Connects to any Telegram channel via user's own account.
version: 0.1.0
metadata:
  openclaw:
    requires:
      env:
        - AGENT_MEMORY_API_KEY
      bins:
        - curl
    primaryEnv: AGENT_MEMORY_API_KEY
---

# Agent Memory MCP

You have access to a Telegram conversation memory service. Use it to search messages, generate digests, extract decisions, and manage channel sources.

## Authentication

All requests require `Authorization: Bearer $AGENT_MEMORY_API_KEY` header. The API key is provided in your configuration.

## Available Operations

### Search Memory
Find information across synced Telegram channels.

```
POST https://agent.ai-vfx.com/api/v1/memory/search
Content-Type: application/json
Authorization: Bearer $AGENT_MEMORY_API_KEY

{
  "query": "what was discussed about topic X",
  "scope": "@channel_username",  // optional — omit to search all sources
  "limit": 10
}
```

Response: `{"answer": "...", "sources": [{"msg_id": 123, "url": "https://t.me/...", "channel": "..."}]}`

### List Connected Sources
See which Telegram channels are synced.

```
GET https://agent.ai-vfx.com/api/v1/sources
Authorization: Bearer $AGENT_MEMORY_API_KEY
```

### Add Source
Connect a Telegram channel for syncing.

```
POST https://agent.ai-vfx.com/api/v1/sources/add
Content-Type: application/json
Authorization: Bearer $AGENT_MEMORY_API_KEY

{
  "handle": "@channel_username",
  "sync_range": "1m"  // 1w, 1m, 3m, 6m, 1y
}
```

### Get Digest
Generate a summary of conversations for a time period.

```
POST https://agent.ai-vfx.com/api/v1/digest
Content-Type: application/json
Authorization: Bearer $AGENT_MEMORY_API_KEY

{
  "scope": "@channel_username",
  "period": "7d"  // 1d, 3d, 7d, 30d
}
```

### Extract Decisions
Get decisions, action items, and open questions.

```
POST https://agent.ai-vfx.com/api/v1/decisions
Content-Type: application/json
Authorization: Bearer $AGENT_MEMORY_API_KEY

{
  "scope": "@channel_username",
  "topic": "optional topic filter"
}
```

### Check Sync Status
Monitor sync progress after adding sources.

```
GET https://agent.ai-vfx.com/api/v1/sync-status
Authorization: Bearer $AGENT_MEMORY_API_KEY
```

### Get Account Balance

```
GET https://agent.ai-vfx.com/api/v1/account/balance
Authorization: Bearer $AGENT_MEMORY_API_KEY
```

Response: `{"balance": 467, "total_spent": 33}`

## Usage Guidelines

- Use **search** for specific questions about channel content
- Use **digest** for "what happened this week" type questions
- Use **decisions** to find action items and key decisions
- Use **scope** parameter to narrow to specific channels (e.g., `@durov`)
- Use `scope: "folder:FolderName"` to search within a Telegram folder
- Sources must be connected first via the bot @AgentMemoryBot or add_source API
- Each operation costs points (1 point ≈ $0.01): search=3, digest=25, decisions=12

## Setup

1. Message [@AgentMemoryBot](https://t.me/AgentMemoryBot) on Telegram
2. Connect your Telegram account (📱 Connect Telegram button)
3. Create an API key (🔑 API Keys → ➕ Create Key)
4. Add the key to your OpenClaw config:

```json
// openclaw.json
{
  "skills": {
    "entries": {
      "agent-memory": {
        "env": {
          "AGENT_MEMORY_API_KEY": "amk_your_key_here"
        }
      }
    }
  }
}
```

Or set as environment variable: `export AGENT_MEMORY_API_KEY=amk_your_key_here`
