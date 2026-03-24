"""Bot and dispatcher factory — forum mode."""

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from agent_memory_mcp.bot.middlewares import UserMiddleware
from agent_memory_mcp.config import settings


def create_bot() -> Bot:
    session = None
    if settings.telegram_proxy:
        from aiogram.client.session.aiohttp import AiohttpSession
        session = AiohttpSession(proxy=settings.telegram_proxy)
    return Bot(
        token=settings.telegram_bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        session=session,
    )


def create_dispatcher() -> Dispatcher:
    dp = Dispatcher()
    dp.message.middleware(UserMiddleware())
    dp.callback_query.middleware(UserMiddleware())

    # --- Forum mode handlers (priority) ---
    from agent_memory_mcp.bot.handlers.forum import router as forum_router
    from agent_memory_mcp.bot.handlers.wallet import router as wallet_router
    from agent_memory_mcp.bot.handlers.auth import router as auth_router

    dp.include_router(forum_router)    # /start, main menu, sources, help
    dp.include_router(auth_router)     # Telegram account auth (Telethon multi-session)
    dp.include_router(wallet_router)   # balance, topup, API keys, usage

    # --- Legacy handlers (kept for admin/operator use) ---
    from agent_memory_mcp.bot.handlers.eval import router as eval_router
    from agent_memory_mcp.bot.handlers.domains import router as domains_router
    from agent_memory_mcp.bot.handlers.groups import router as groups_router
    from agent_memory_mcp.bot.handlers.digest import router as digest_router
    from agent_memory_mcp.bot.handlers.conversations import router as conversations_router
    from agent_memory_mcp.bot.callbacks.domain_actions import router as domain_callbacks_router
    from agent_memory_mcp.bot.callbacks.conversation_actions import router as conv_callbacks_router

    dp.include_router(eval_router)
    dp.include_router(domains_router)
    dp.include_router(groups_router)
    dp.include_router(digest_router)
    dp.include_router(conversations_router)  # catch-all text → agent pipeline
    dp.include_router(domain_callbacks_router)
    dp.include_router(conv_callbacks_router)

    return dp
