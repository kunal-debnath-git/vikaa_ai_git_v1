# ╔══════════════════════════════════════════════════════════════════╗
# ║  VIKAA.AI — PUBLIC SHOWCASE                                      ║
# ║  This file requires environment credentials to run.              ║
# ║  Architecture and API signatures are shown for reference.        ║
# ║  See README.md and docs/ for setup guide.                        ║
# ╚══════════════════════════════════════════════════════════════════╝

"""Shared Unity Catalog client construction for FastAPI routes (503 when misconfigured)."""

from __future__ import annotations

from fastapi import HTTPException

from backend.integrations.databricks.read_unity_catalog import (
    UnityCatalogClient,
    _resolve_host,
    _resolve_token,
)


def require_unity_catalog_client(*, pat_detail: str | None = None) -> UnityCatalogClient:
    """
    Build ``UnityCatalogClient`` from env or raise HTTP 503 with a clear message.
    """
    ...
