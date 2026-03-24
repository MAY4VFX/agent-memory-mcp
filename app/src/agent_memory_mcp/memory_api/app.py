"""FastAPI application — Memory API + MCP server + OAuth."""

from __future__ import annotations

from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from agent_memory_mcp.memory_api.routes import router as api_router
from agent_memory_mcp.memory_api.oauth import router as oauth_router

log = structlog.get_logger(__name__)


class MCPAuthMiddleware(BaseHTTPMiddleware):
    """Require Bearer token on MCP endpoints. Return 401 to trigger OAuth flow."""

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path

        # Allow OAuth discovery and well-known endpoints without auth
        if "/.well-known/" in path or "/oauth/" in path:
            return await call_next(request)

        # Allow OPTIONS (CORS preflight)
        if request.method == "OPTIONS":
            return await call_next(request)

        # Check for Bearer token
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer ") or len(auth) < 15:
            # Return 401 with OAuth resource metadata to trigger auth flow
            base = str(request.base_url).rstrip("/")
            return JSONResponse(
                status_code=401,
                headers={
                    "WWW-Authenticate": 'Bearer',
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Expose-Headers": "WWW-Authenticate",
                },
                content={
                    "error": "unauthorized",
                    "error_description": "API key required. Authenticate via OAuth or pass Bearer token.",
                },
            )

        return await call_next(request)


def create_api_app() -> FastAPI:
    """Create the FastAPI application with Memory API routes + MCP endpoint."""
    from agent_memory_mcp.config import settings
    from agent_memory_mcp.memory_api.mcp_tools import mcp

    # Build MCP sub-app with auth middleware
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

    # OAuth routes (for MCP authentication)
    app.include_router(oauth_router)

    # Mount MCP with auth middleware wrapper
    if settings.run_mcp:
        # Wrap MCP app with auth middleware
        authed_mcp = Starlette(
            routes=mcp_app.routes,
            middleware=[Middleware(MCPAuthMiddleware)],
            lifespan=mcp_app.router.lifespan_context,
        )
        app.mount("/mcp", authed_mcp)
        log.info("mcp_mounted", path="/mcp", auth="required")

    return app
