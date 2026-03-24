"""Bot and dispatcher factory."""

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from agent_memory_mcp.bot.middlewares import UserMiddleware
from agent_memory_mcp.config import settings


def create_bot() -> Bot:
    return Bot(
        token=settings.telegram_bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )


def create_dispatcher() -> Dispatcher:
    dp = Dispatcher()
    dp.message.middleware(UserMiddleware())
    dp.callback_query.middleware(UserMiddleware())

    # Import routers — order matters!
    # 1. Commands (/start, /help)
    from agent_memory_mcp.bot.handlers.start import router as start_router
    # 2. Eval commands (admin-only, must be before conversations to catch /eval_* + FSM)
    from agent_memory_mcp.bot.handlers.eval import router as eval_router
    # 3. Domain management (settings menu, channel links)
    from agent_memory_mcp.bot.handlers.domains import router as domains_router
    # 4. Groups & scope switching (before conversations)
    from agent_memory_mcp.bot.handlers.groups import router as groups_router
    # 5. Digest handlers
    from agent_memory_mcp.bot.handlers.digest import router as digest_router
    # 6. Conversations (menu buttons + text query fallback — must be AFTER domains)
    from agent_memory_mcp.bot.handlers.conversations import router as conversations_router
    # 7. Callback queries (domain actions)
    from agent_memory_mcp.bot.callbacks.domain_actions import router as domain_callbacks_router
    # 8. Callback queries (conversation actions)
    from agent_memory_mcp.bot.callbacks.conversation_actions import router as conv_callbacks_router

    dp.include_router(start_router)
    dp.include_router(eval_router)
    dp.include_router(domains_router)
    dp.include_router(groups_router)
    dp.include_router(digest_router)
    dp.include_router(conversations_router)
    dp.include_router(domain_callbacks_router)
    dp.include_router(conv_callbacks_router)

    return dp
