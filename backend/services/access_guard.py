# ╔══════════════════════════════════════════════════════════════════╗
# ║  VIKAA.AI — PUBLIC SHOWCASE                                      ║
# ║  This file requires environment credentials to run.              ║
# ║  Architecture and API signatures are shown for reference.        ║
# ║  See README.md and docs/ for setup guide.                        ║
# ╚══════════════════════════════════════════════════════════════════╝

import logging
import ipaddress
import time
from typing import Literal
from urllib.parse import urlparse

import httpx
from fastapi import Header, HTTPException, Request

from datastore.supabase_client import SUPABASE_ANON_KEY, SUPABASE_URL, supabase

logger = logging.getLogger(__name__)

AclStatus = Literal["whitelist", "blacklist", "guest", "unknown"]

# ── JWT validation cache (avoids one Supabase HTTP call per request) ──────────
_token_cache: dict[str, tuple[dict, float]] = {}
_TOKEN_CACHE_TTL = 300  # 5 minutes


def _host_from_url(url: str) -> str | None:
    ...


def _is_loopback_host(host: str) -> bool:
    ...


def _is_private_or_loopback_host(host: str) -> bool:
    """RFC1918 / loopback / link-local IP literals from Origin (e.g. http://192.168.1.5:5500)."""
    ...


def _is_cursor_or_vscode_dev_host(host: str) -> bool:
    """Cursor / VS Code webview-style hosts: always treat as dev execution context."""
    ...


def is_trusted_dev_execution_context(request: Request | None) -> bool:
    """
    True when the request should bypass ACL for RUN/execute (local / LAN / Cursor preview).

    Uses the API server's host (same-machine calls) and Origin/Referer from the browser
    (frontend on loopback, RFC1918 LAN, or Cursor-style dev hosts). This matches deployed
    APIs where request.url.hostname is not localhost but the SPA is served from LAN or Cursor.
    """
    ...


async def validate_access_token(authorization: str | None = Header(default=None)) -> dict:
    """Validate Supabase JWT and return decoded user JSON."""
    ...


def get_acl_status(email: str) -> AclStatus:
    """
    Resolve ACL status from access_control_list table.
    whitelist => can execute
    blacklist/guest/unknown => read-only
    """
    ...


async def require_whitelisted_user(
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict:
    """
    Dependency for RUN endpoints.
    Only whitelist users can execute costly actions.
    """
    ...


def _oauth_provider(user: dict) -> str:
    """Supabase /auth/v1/user: app_metadata.provider (e.g. google, github)."""
    ...


def _bearer_raw_token(authorization: str | None) -> str:
    ...


# Supabase OAuth for “Sign in with Google” sets app_metadata.provider to "google" (Gmail uses this same IdP).
GOOGLE_OAUTH_REQUIRED_DETAIL = (
    "Gmail Intelligence is only available if you signed in to Vikaa.AI with Google (Gmail OAuth). "
    "Please sign out, then sign in again using the Google option."
)


async def require_google_oauth_user(
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict:
    """
    Production (e.g. Render): valid JWT and Google OAuth only.

    Trusted dev: may omit Authorization entirely (local Gmail tool + demo / fixed test mailbox).
    With a Bearer token, dev still validates the user like before.
    """
    ...


async def require_gmail_intelligence_user(
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict:
    """
    Production: Google OAuth + whitelist.

    Trusted dev: may omit Bearer token (anonymous local runs) or send any valid token;
    whitelist is always skipped on trusted dev.
    """
    ...

