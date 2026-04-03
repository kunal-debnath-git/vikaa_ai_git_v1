"""
Natural-language search over Unity Catalog table/column comments (metadata only).
"""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from backend.integrations.databricks.read_unity_catalog import (
    build_tree,
    _resolve_host,
    _resolve_token,
)
from backend.services.access_guard import require_whitelisted_user
from backend.services.catalog_search_service import (
    answer_catalog_question,
    build_context_excerpt,
    iter_catalog_blocks,
    resolve_catalog_and_schema_filters,
)
from backend.services.unity_catalog_client import require_unity_catalog_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tools", tags=["Catalog Search"])
router_api_alias = APIRouter(prefix="/api/tools", tags=["Catalog Search"])
router_root = APIRouter(tags=["Catalog Search"])

_ALL_ROUTERS = (router, router_api_alias, router_root)


def _health_payload() -> dict[str, Any]:
    host = _resolve_host()
    token = _resolve_token()
    ak = (os.getenv("ANTHROPIC_API_KEY") or "").strip()
    return {
        "status": "ok",
        "service": "catalog-search",
        "paths": [
            "/tools/catalog-search/health",
            "/api/tools/catalog-search/health",
            "/catalog-search/health",
        ],
        "databricks_host_configured": bool(host),
        "databricks_token_configured": bool(token),
        "anthropic_configured": bool(ak and ak != "your-anthropic-api-key-here"),
    }


class CatalogSearchAskRequest(BaseModel):
    query: str = Field(..., min_length=2, max_length=8000)
    catalog: str | None = None
    schemas: list[str] | None = Field(
        default=None,
        description="Exact Unity Catalog schema names (optional).",
    )
    schema_substrings: list[str] | None = Field(
        default=None,
        description='If set, only schemas whose name contains one of these '
        'substrings, e.g. ["silver","gold"].',
    )
    max_tables_per_schema: int = Field(50, ge=1, le=200)
    max_context_chars: int = Field(100_000, ge=8000, le=250_000)


async def _cs_health() -> dict[str, Any]:
    return _health_payload()


async def _cs_ask(
    request: Request,
    body: CatalogSearchAskRequest,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    try:
        cat, exact_schemas, subs = resolve_catalog_and_schema_filters(
            body.catalog,
            body.schemas,
            body.schema_substrings,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        client = require_unity_catalog_client()
        tree = build_tree(
            client,
            catalog_filter=cat,
            schema_filter=None,
            schema_names=exact_schemas,
            schema_name_contains_any=subs if not exact_schemas else None,
            max_tables_per_schema=body.max_tables_per_schema,
            include_columns=True,
        )
    except HTTPException:
        raise
    except RuntimeError as exc:
        logger.warning("Unity Catalog build_tree failed: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    blocks = iter_catalog_blocks(tree)
    if not blocks:
        raise HTTPException(
            status_code=404,
            detail="No tables matched the filters. Check catalog/schema names and permissions.",
        )

    context, n_included = build_context_excerpt(
        blocks,
        body.query.strip(),
        body.max_context_chars,
    )
    try:
        answer = answer_catalog_question(context, body.query.strip())
    except RuntimeError as exc:
        logger.warning("Catalog search LLM error: %s", exc)
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "answer": answer,
        "catalog": cat,
        "tables_in_context": n_included,
        "tables_matched_filters": len(blocks),
        "schema_filter_mode": (
            "exact_schemas"
            if exact_schemas
            else ("substrings" if subs else "all_schemas_in_catalog")
        ),
    }


for _r in _ALL_ROUTERS:
    _r.add_api_route("/catalog-search/health", _cs_health, methods=["GET"])
    _r.add_api_route("/catalog-search/ask", _cs_ask, methods=["POST"])
