"""FastAPI application — Memory API + MCP server."""

from __future__ import annotations

from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from agent_memory_mcp.memory_api.routes import router as api_router

log = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("api_startup")
    yield
    log.info("api_shutdown")


def create_api_app() -> FastAPI:
    """Create the FastAPI application with Memory API routes."""
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

    app.include_router(api_router)

    return app
