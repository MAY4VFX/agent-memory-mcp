"""Application entrypoint — runs FastAPI (REST + MCP) + bot + scheduler."""

import os

# App only connects to internal Docker services (LiteLLM, TEI, Milvus, etc.)
# Proxy env vars break internal requests by routing them through Privoxy.
for _var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
    os.environ.pop(_var, None)

import asyncio

import structlog
import uvicorn

from agent_memory_mcp.bot.app import create_bot, create_dispatcher
from agent_memory_mcp.collector.client import TelegramCollector
from agent_memory_mcp.collector import pool as pool_mod
from agent_memory_mcp.collector.pool import CollectorPool
from agent_memory_mcp.config import settings
from agent_memory_mcp.memory_api.app import create_api_app
from agent_memory_mcp.scheduler.scheduler import SyncScheduler
from agent_memory_mcp.storage.milvus_client import MilvusStorage

log = structlog.get_logger()


async def main() -> None:
    log.info("starting_agent_memory_mcp")

    # Ensure Milvus collection exists (auto-migrates if schema changed)
    milvus = MilvusStorage()
    milvus.migrate_collection()
    milvus.close()

    # Connect Telethon (optional — needed for channel ingestion)
    collector = None
    if settings.telegram_session:
        try:
            collector = TelegramCollector()
            await collector.connect()
            log.info("telethon_connected")
        except Exception:
            log.warning("telethon_connect_failed", exc_info=True)
            collector = None
    else:
        log.info("telethon_skipped", reason="no TELEGRAM_SESSION configured")

    # Initialize collector pool (multi-user Telethon sessions from DB)
    cpool = CollectorPool()
    await cpool.start()
    pool_mod.collector_pool = cpool  # set module-level singleton
    log.info("collector_pool_started")

    # Create bot + dispatcher
    bot = create_bot()
    dp = create_dispatcher()

    # Store collector and pool in dispatcher workflow data for handler injection
    dp["collector"] = collector
    dp["collector_pool"] = cpool

    # Warm up folder cache in background
    if collector:
        asyncio.create_task(collector.get_dialog_filters(), name="warmup_folders")

    # Start scheduler with shared collector and bot (for digest)
    scheduler = SyncScheduler(collector=collector, bot=bot)
    scheduler_task = asyncio.create_task(scheduler.start())

    # Start FastAPI server (REST API + MCP on /mcp)
    api_app = create_api_app()
    config = uvicorn.Config(
        api_app,
        host="0.0.0.0",
        port=settings.api_port,
        log_level="info",
    )
    server = uvicorn.Server(config)
    api_task = asyncio.create_task(server.serve())
    log.info("api_server_started", port=settings.api_port, mcp=settings.run_mcp)

    try:
        # Drop stale getUpdates session from previous container.
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
        server.should_exit = True
        api_task.cancel()
        scheduler.stop()
        scheduler_task.cancel()
        if collector:
            await collector.disconnect()
        await cpool.shutdown()
        await bot.session.close()
        log.info("agent_memory_mcp_stopped")


if __name__ == "__main__":
    asyncio.run(main())
