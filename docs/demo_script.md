# Agent Memory MCP — Demo Script

## 1. Setup (Bot)

1. Open @AgentMemoryBot in Telegram
2. Press `/start` — get welcome message
3. Press **📱 Connect Telegram** — share contact, enter reversed code
4. Press **🔑 Create API Key** — copy `amk_...` key, get 500 bonus credits

## 2. Connect MCP (Claude Code)

```bash
claude mcp add --transport http agent-memory https://agent.ai-vfx.com/mcp/
```

Authenticate with your API key when prompted.

## 3. Add Sources

```
You: "Add @durov channel as a source, sync last week"
→ Agent calls add_source(handle="@durov", sync_range="1w")
→ "✅ @durov added. Sync will start within 30 seconds."
```

Check sync progress:
```
You: "What's the sync status?"
→ Agent calls sync_status()
→ "@durov — completed, 15 messages fetched"
```

## 4. Search Memory

```
You: "What did Durov post about recently?"
→ Agent calls search_memory(query="recent posts", scope="@durov")
→ AI-generated answer with source links
```

## 5. Get Digest

```
You: "Give me a weekly digest of @durov"
→ Agent calls get_digest(scope="@durov", period="7d")
→ Clustered digest with key topics
```

## 6. Extract Decisions

```
You: "What decisions were made in the channel?"
→ Agent calls get_decisions(scope="@durov")
→ List of decisions, action items, open questions
```

## 7. Full Context Package

```
You: "I need context about TON ecosystem updates"
→ Agent calls get_agent_context(task="TON ecosystem updates", scope="@durov")
→ Combined search results + decisions
```

## Key Selling Points

- **One-click setup**: Connect Telegram in 30 seconds
- **Any channel**: Public and private channels/groups via user's own account
- **MCP native**: Works with Claude Code, Claude Desktop, any MCP client
- **Pay-per-use**: TON-based credits, 500 free to start
- **Multi-user**: Each user has isolated data and separate Telethon session
