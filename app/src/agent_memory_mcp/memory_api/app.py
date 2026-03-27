"""FastAPI application — Memory API + MCP server + OAuth."""

from __future__ import annotations

from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from agent_memory_mcp.memory_api.routes import router as api_router
from agent_memory_mcp.memory_api.oauth import router as oauth_router

log = structlog.get_logger(__name__)


def create_api_app() -> FastAPI:
    """Create the FastAPI application with Memory API routes + MCP endpoint."""
    from agent_memory_mcp.config import settings
    from agent_memory_mcp.memory_api.mcp_tools import mcp

    # Build MCP sub-app (stateless = sessions survive server restarts)
    mcp_app = mcp.http_app(path="/", stateless_http=True)

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

    # Auth middleware — require Bearer token for /mcp/ requests
    @app.middleware("http")
    async def mcp_auth_middleware(request: Request, call_next) -> Response:
        path = request.url.path

        # Only check /mcp/ paths (not /api/, /oauth/, etc.)
        if not path.startswith("/mcp"):
            return await call_next(request)

        # Allow well-known (OAuth discovery)
        if "/.well-known/" in path:
            return await call_next(request)

        # Allow OPTIONS (CORS)
        if request.method == "OPTIONS":
            return await call_next(request)

        # Allow GET on /mcp/ root (SSE connection setup)
        if request.method == "GET":
            # Check auth on GET too
            auth = request.headers.get("authorization", "")
            if not auth.startswith("Bearer "):
                return JSONResponse(
                    status_code=401,
                    headers={"WWW-Authenticate": "Bearer"},
                    content={
                        "error": "unauthorized",
                        "error_description": "Bearer token required. Get your API key at https://t.me/AgentMemoryBot",
                    },
                )
            return await call_next(request)

        # POST /mcp/ — main MCP requests
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer ") or len(auth) < 15:
            return JSONResponse(
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
                content={
                    "error": "unauthorized",
                    "error_description": "Bearer token required. Get your API key at https://t.me/AgentMemoryBot",
                },
            )

        return await call_next(request)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    # REST API routes
    app.include_router(api_router)

    # OAuth routes (for MCP authentication)
    app.include_router(oauth_router)

    # Mount MCP Streamable HTTP server
    if settings.run_mcp:
        app.mount("/mcp", mcp_app)
        log.info("mcp_mounted", path="/mcp", auth="required")

    return app
