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


async def _wait_for_milvus(max_retries: int = 30, base_delay: float = 2.0) -> None:
    """Wait for Milvus to become available with exponential backoff."""
    for attempt in range(1, max_retries + 1):
        try:
            milvus = MilvusStorage()
            milvus.migrate_collection()
            milvus.close()
            return
        except Exception:
            delay = min(base_delay * (1.5 ** (attempt - 1)), 30.0)
            log.warning("milvus_not_ready", attempt=attempt, retry_in=delay)
            await asyncio.sleep(delay)
    raise RuntimeError("Milvus unavailable after retries")


async def _wait_for_db(max_retries: int = 30, base_delay: float = 2.0) -> None:
    """Wait for PostgreSQL to become available."""
    from sqlalchemy import text
    from agent_memory_mcp.db.engine import async_engine

    for attempt in range(1, max_retries + 1):
        try:
            async with async_engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
            return
        except Exception:
            delay = min(base_delay * (1.5 ** (attempt - 1)), 30.0)
            log.warning("db_not_ready", attempt=attempt, retry_in=delay)
            await asyncio.sleep(delay)
    raise RuntimeError("PostgreSQL unavailable after retries")


async def main() -> None:
    log.info("starting_agent_memory_mcp")

    # Wait for infrastructure to become available after server reboot
    await _wait_for_db()
    await _wait_for_milvus()

    # Initialize GPU manager (on-demand TEI containers)
    if settings.gpu_manager_enabled:
        try:
            from agent_memory_mcp.gpu.manager import (
                GpuService, GpuServiceManager, set_gpu_manager,
            )
            gpu_mgr = GpuServiceManager(
                docker_host=settings.gpu_docker_host,
                redis_url=settings.gpu_coord_redis_url,
                project_id=settings.gpu_project_id,
            )
            gpu_mgr.register(GpuService(
                name="embedding",
                container_name=settings.gpu_embedding_container,
                health_url=f"{settings.embedding_url}/health",
                health_timeout=settings.gpu_startup_timeout,
            ))
            gpu_mgr.register(GpuService(
                name="reranker",
                container_name=settings.gpu_reranker_container,
                health_url=f"{settings.reranker_url}/health",
                health_timeout=settings.gpu_startup_timeout,
            ))
            set_gpu_manager(gpu_mgr)
            gpu_mgr.start_idle_checker_thread(idle_timeout=settings.gpu_idle_timeout)
            log.info("gpu_manager_started", idle_timeout=settings.gpu_idle_timeout)
        except Exception:
            log.exception("gpu_manager_init_failed")

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
