"""FastAPI application — Memory API + MCP server."""

from __future__ import annotations

from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from agent_memory_mcp.memory_api.routes import router as api_router

log = structlog.get_logger(__name__)


def create_api_app() -> FastAPI:
    """Create the FastAPI application with Memory API routes + MCP endpoint."""
    from agent_memory_mcp.config import settings
    from agent_memory_mcp.memory_api.mcp_tools import mcp

    # Build MCP sub-app — path="/" because app.mount("/mcp") already adds prefix
    mcp_app = mcp.http_app(path="/")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        log.info("api_startup", mcp_enabled=settings.run_mcp)
        async with mcp_app.router.lifespan_context(app):
            yield
        log.info("api_shutdown")

    app = FastAPI(
        title="Agent Memory MCP",
        description="Shared memory infrastructure for Telegram-native AI agents",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # REST API routes
    app.include_router(api_router)

    # Mount MCP Streamable HTTP server
    if settings.run_mcp:
        app.mount("/mcp", mcp_app)
        log.info("mcp_mounted", path="/mcp")

    return app
