"""OAuth 2.0 endpoints for MCP authentication.

Flow (as seen by Claude Code / Cursor):
1. MCP client connects → server requires auth
2. Client fetches /.well-known/oauth-authorization-server
3. Client opens browser → /oauth/authorize
4. User enters API key from the bot
5. Redirect back with auth code
6. Client exchanges code for token via /oauth/token
7. Client sends Bearer token on all MCP requests
"""

from __future__ import annotations

import hashlib
import html
import secrets
import time
from urllib.parse import urlparse

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from agent_memory_mcp.config import settings

router = APIRouter()

# Temporary storage for auth codes (code → (api_key, expires, challenge, method))
_auth_codes: dict[str, tuple[str, float, str, str]] = {}
# Registered clients
_clients: dict[str, dict] = {}

AUTH_CODE_TTL = 300  # 5 minutes


def _force_https(url: str) -> str:
    """Force HTTPS — server is behind Cloudflare/Nginx reverse proxy."""
    return url.replace("http://", "https://", 1)


def _redirect_uri_allowed(client_id: str, redirect_uri: str) -> bool:
    """Check redirect_uri against the client's registered URIs (RFC 6749 §3.1.2).

    Exact match required. Per RFC 8252, loopback redirect URIs may use a dynamic
    port, so for 127.0.0.1/localhost/::1 we match on scheme+host+path and ignore
    the port. Everything else must match exactly — this closes the open-redirect.
    """
    client = _clients.get(client_id)
    if not client:
        return False
    registered = client.get("redirect_uris") or []
    if redirect_uri in registered:
        return True
    try:
        want = urlparse(redirect_uri)
    except Exception:
        return False
    if want.hostname not in ("127.0.0.1", "localhost", "::1"):
        return False
    for reg in registered:
        r = urlparse(reg)
        if (r.scheme, r.hostname, r.path) == (want.scheme, want.hostname, want.path):
            return True
    return False


def _build_metadata(base: str) -> dict:
    base = _force_https(base)
    return {
        "issuer": base,
        "authorization_endpoint": f"{base}/oauth/authorize",
        "token_endpoint": f"{base}/oauth/token",
        "registration_endpoint": f"{base}/oauth/register",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code"],
        "token_endpoint_auth_methods_supported": ["client_secret_post", "none"],
        "code_challenge_methods_supported": ["S256", "plain"],
    }


@router.api_route("/.well-known/oauth-authorization-server", methods=["GET", "POST"])
async def oauth_metadata(request: Request):
    """OAuth 2.0 Authorization Server Metadata (RFC 8414)."""
    base = str(request.base_url).rstrip("/")
    return JSONResponse(_build_metadata(base))


@router.api_route("/mcp/.well-known/oauth-authorization-server", methods=["GET", "POST"])
async def oauth_metadata_mcp(request: Request):
    base = str(request.base_url).rstrip("/")
    return JSONResponse(_build_metadata(base))


def _build_protected_resource(base: str) -> dict:
    base = _force_https(base)
    return {
        "resource": f"{base}/mcp/",
        "authorization_servers": [base],
        "bearer_methods_supported": ["header"],
    }


@router.api_route("/.well-known/oauth-protected-resource", methods=["GET", "POST"])
async def oauth_protected_resource(request: Request):
    """OAuth 2.0 Protected Resource Metadata (RFC 9728)."""
    base = str(request.base_url).rstrip("/")
    return JSONResponse(_build_protected_resource(base))


@router.api_route("/mcp/.well-known/oauth-protected-resource", methods=["GET", "POST"])
async def oauth_protected_resource_mcp(request: Request):
    base = str(request.base_url).rstrip("/")
    return JSONResponse(_build_protected_resource(base))


@router.api_route("/oauth/register", methods=["GET", "POST"])
async def oauth_register(request: Request):
    """Dynamic Client Registration (RFC 7591)."""
    if request.method == "GET":
        return JSONResponse({"error": "use_post", "description": "POST with client metadata to register"})
    body = await request.json()
    client_id = f"client_{secrets.token_hex(8)}"
    client_secret = secrets.token_hex(16)
    _clients[client_id] = {
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uris": body.get("redirect_uris", []),
        "client_name": body.get("client_name", "MCP Client"),
    }
    return JSONResponse({
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uris": body.get("redirect_uris", []),
        "client_name": body.get("client_name", "MCP Client"),
    })


@router.get("/oauth/authorize")
async def oauth_authorize_page(
    request: Request,
    client_id: str = "",
    redirect_uri: str = "",
    state: str = "",
    code_challenge: str = "",
    code_challenge_method: str = "",
    response_type: str = "code",
):
    """Authorization page — user enters their API key."""
    # Reject unregistered/mismatched redirect_uri before rendering the form —
    # never echo an attacker-controlled redirect target into a working form.
    if not _redirect_uri_allowed(client_id, redirect_uri):
        return HTMLResponse(
            "<h2>Invalid client or redirect_uri.</h2>"
            "<p>This authorization request could not be verified.</p>",
            status_code=400,
        )

    # Escape every user-controlled value before interpolating into HTML (XSS).
    e_client_id = html.escape(client_id, quote=True)
    e_redirect_uri = html.escape(redirect_uri, quote=True)
    e_state = html.escape(state, quote=True)
    e_code_challenge = html.escape(code_challenge, quote=True)
    e_code_challenge_method = html.escape(code_challenge_method, quote=True)
    e_bot_url = html.escape(settings.bot_url, quote=True)
    e_bot_username = html.escape(settings.bot_username, quote=True)

    page = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Agent Memory MCP — Connect</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: #0a0a0a; color: #e0e0e0;
            display: flex; justify-content: center; align-items: center;
            min-height: 100vh; padding: 20px;
        }}
        .card {{
            background: #1a1a1a; border-radius: 16px; padding: 40px;
            max-width: 440px; width: 100%; border: 1px solid #333;
        }}
        h1 {{ font-size: 24px; margin-bottom: 8px; }}
        .subtitle {{ color: #888; margin-bottom: 24px; font-size: 14px; }}
        label {{ display: block; margin-bottom: 6px; font-size: 14px; color: #aaa; }}
        input {{
            width: 100%; padding: 12px 16px; border-radius: 8px;
            border: 1px solid #444; background: #111; color: #fff;
            font-size: 16px; font-family: monospace; margin-bottom: 20px;
        }}
        input:focus {{ outline: none; border-color: #0088cc; }}
        button {{
            width: 100%; padding: 14px; border-radius: 8px;
            background: #0088cc; color: white; border: none;
            font-size: 16px; cursor: pointer; font-weight: 600;
        }}
        button:hover {{ background: #0077b3; }}
        .hint {{
            margin-top: 16px; font-size: 13px; color: #666;
            text-align: center;
        }}
        .hint a {{ color: #0088cc; text-decoration: none; }}
        .logo {{ font-size: 32px; margin-bottom: 16px; }}
    </style>
</head>
<body>
    <div class="card">
        <div class="logo">🧠</div>
        <h1>Agent Memory MCP</h1>
        <p class="subtitle">Enter your API key to connect memory to your AI agent.</p>

        <form method="POST" action="/oauth/authorize">
            <input type="hidden" name="client_id" value="{e_client_id}">
            <input type="hidden" name="redirect_uri" value="{e_redirect_uri}">
            <input type="hidden" name="state" value="{e_state}">
            <input type="hidden" name="code_challenge" value="{e_code_challenge}">
            <input type="hidden" name="code_challenge_method" value="{e_code_challenge_method}">

            <label for="api_key">API Key</label>
            <input type="text" id="api_key" name="api_key"
                   placeholder="amk_..." autocomplete="off" required>

            <button type="submit">Connect</button>
        </form>

        <p class="hint">
            Don't have a key? Get one from
            <a href="{e_bot_url}" target="_blank">@{e_bot_username}</a>
        </p>
    </div>
</body>
</html>"""
    return HTMLResponse(page)


@router.post("/oauth/authorize")
async def oauth_authorize_submit(request: Request):
    """Handle API key submission — generate auth code and redirect."""
    form = await request.form()
    api_key = form.get("api_key", "").strip()
    client_id = form.get("client_id", "")
    redirect_uri = form.get("redirect_uri", "")
    state = form.get("state", "")
    code_challenge = form.get("code_challenge", "")
    code_challenge_method = form.get("code_challenge_method", "")

    # Re-validate redirect_uri on submit — the form fields are attacker-controllable.
    if not _redirect_uri_allowed(client_id, redirect_uri):
        return HTMLResponse(
            "<h2>Invalid client or redirect_uri.</h2>", status_code=400,
        )

    if not api_key or not api_key.startswith("amk_"):
        return HTMLResponse(
            "<h2>Invalid API key. Must start with amk_</h2>"
            "<a href='javascript:history.back()'>Try again</a>",
            status_code=400,
        )

    # Verify the key exists
    from agent_memory_mcp.memory_api.auth import get_api_key_by_hash
    from agent_memory_mcp.db.engine import async_engine
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    key_record = await get_api_key_by_hash(async_engine, key_hash)
    if not key_record or not key_record["is_active"]:
        return HTMLResponse(
            "<h2>Invalid or deactivated API key.</h2>"
            "<a href='javascript:history.back()'>Try again</a>",
            status_code=401,
        )

    # Generate auth code
    auth_code = secrets.token_urlsafe(32)
    _auth_codes[auth_code] = (api_key, time.time() + AUTH_CODE_TTL, code_challenge, code_challenge_method)

    # Redirect back to client
    sep = "&" if "?" in redirect_uri else "?"
    redirect_url = f"{redirect_uri}{sep}code={auth_code}"
    if state:
        redirect_url += f"&state={state}"

    return RedirectResponse(redirect_url, status_code=302)


@router.post("/oauth/token")
async def oauth_token(request: Request):
    """Exchange auth code for access token."""
    # Support both JSON and form-encoded
    content_type = request.headers.get("content-type", "")
    if "json" in content_type:
        body = await request.json()
    else:
        form = await request.form()
        body = dict(form)

    code = body.get("code", "")
    code_verifier = body.get("code_verifier", "")
    grant_type = body.get("grant_type", "")

    if grant_type != "authorization_code":
        return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)

    if code not in _auth_codes:
        return JSONResponse({"error": "invalid_grant", "error_description": "Invalid or expired code"}, status_code=400)

    api_key, expires, challenge, challenge_method = _auth_codes[code]

    if time.time() > expires:
        del _auth_codes[code]
        return JSONResponse({"error": "invalid_grant", "error_description": "Code expired"}, status_code=400)

    # PKCE: if a challenge was bound to the code at /authorize, the verifier is
    # MANDATORY on exchange (RFC 7636). A stolen code without the verifier fails.
    if challenge:
        if not code_verifier:
            return JSONResponse({"error": "invalid_grant", "error_description": "code_verifier required"}, status_code=400)
        method = challenge_method or "plain"
        if method == "S256":
            import base64
            computed = base64.urlsafe_b64encode(
                hashlib.sha256(code_verifier.encode()).digest()
            ).rstrip(b"=").decode()
            ok = secrets.compare_digest(computed, challenge)
        elif method == "plain":
            ok = secrets.compare_digest(code_verifier, challenge)
        else:
            return JSONResponse({"error": "invalid_request", "error_description": "unsupported code_challenge_method"}, status_code=400)
        if not ok:
            return JSONResponse({"error": "invalid_grant", "error_description": "PKCE verification failed"}, status_code=400)

    # Clean up used code
    del _auth_codes[code]

    # The access token IS the API key — MCP client will send it as Bearer token
    return JSONResponse({
        "access_token": api_key,
        "token_type": "bearer",
        "expires_in": 86400 * 30,  # 30 days
    })
