"""Application entrypoint — runs bot + scheduler."""

import os

# App only connects to internal Docker services (LiteLLM, TEI, Milvus, etc.)
# Proxy env vars break internal requests by routing them through Privoxy.
# LiteLLM handles its own external API proxying independently.
for _var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
    os.environ.pop(_var, None)

import asyncio

import structlog
from aiohttp import web

from agent_memory_mcp.api.server import create_web_app
from agent_memory_mcp.bot.app import create_bot, create_dispatcher
from agent_memory_mcp.collector.client import TelegramCollector
from agent_memory_mcp.scheduler.scheduler import SyncScheduler
from agent_memory_mcp.storage.milvus_client import MilvusStorage

log = structlog.get_logger()


async def main() -> None:
    log.info("starting_agent_memory_mcp")

    # Ensure Milvus collection exists (auto-migrates if schema changed)
    milvus = MilvusStorage()
    milvus.migrate_collection()
    milvus.close()

    # Connect Telethon (needed for channel resolution in bot)
    collector = TelegramCollector()
    await collector.connect()

    # Create bot + dispatcher
    bot = create_bot()
    dp = create_dispatcher()

    # Store collector in dispatcher workflow data for handler injection
    dp["collector"] = collector

    # Warm up folder cache in background
    asyncio.create_task(collector.get_dialog_filters(), name="warmup_folders")

    # Start scheduler with shared collector and bot (for digest)
    scheduler = SyncScheduler(collector=collector, bot=bot)
    scheduler_task = asyncio.create_task(scheduler.start())

    # Start HTTP API server on port 8002
    api_app = create_web_app(bot)
    runner = web.AppRunner(api_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8002)
    await site.start()
    log.info("api_server_started", port=8002)

    try:
        # Drop stale getUpdates session from previous container.
        # After a rolling update the old container's long-poll may still be
        # held by Telegram for up to 30s.  Retry delete_webhook + explicit
        # short getUpdates to drain the old session.
        for attempt in range(3):
            try:
                await bot.delete_webhook(drop_pending_updates=True)
                break
            except Exception:
                log.warning("delete_webhook_retry", attempt=attempt)
                await asyncio.sleep(2)
        # Short getUpdates to drain old long-poll, then wait for it to expire
        try:
            from aiogram.methods import GetUpdates
            await bot(GetUpdates(offset=-1, limit=1, timeout=1))
        except Exception:
            pass
        await asyncio.sleep(5)
        log.info("bot_polling_start")
        # Start bot polling
        await dp.start_polling(bot)
    finally:
        await runner.cleanup()
        scheduler.stop()
        scheduler_task.cancel()
        await collector.disconnect()
        await bot.session.close()
        log.info("agent_memory_mcp_stopped")


if __name__ == "__main__":
    asyncio.run(main())
