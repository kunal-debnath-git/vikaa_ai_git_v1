"""
Unity Catalog read-only API — lists catalogs, schemas, and tables using server-side
Databricks credentials (DATABRICKS_HOST / DATABRICKS_HOST_STORY + TOKEN vars).
"""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from backend.integrations.databricks.read_unity_catalog import (
    build_tree,
    _resolve_host,
    _resolve_token,
)
from backend.services.access_guard import require_whitelisted_user
from backend.services.unity_catalog_client import require_unity_catalog_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tools", tags=["Unity Catalog"])
router_api_alias = APIRouter(prefix="/api/tools", tags=["Unity Catalog"])
router_root = APIRouter(tags=["Unity Catalog"])

_ALL_ROUTERS = (router, router_api_alias, router_root)


def _health_payload() -> dict[str, Any]:
    host = _resolve_host()
    token = _resolve_token()
    return {
        "status": "ok",
        "service": "unity-catalog",
        "paths": [
            "/tools/unity-catalog/catalogs",
            "/api/tools/unity-catalog/catalogs",
            "/unity-catalog/catalogs",
        ],
        "databricks_host_configured": bool(host),
        "databricks_token_configured": bool(token),
        "host_preview": (host[:40] + "…") if host and len(host) > 40 else (host or None),
    }


async def _uc_health() -> dict[str, Any]:
    return _health_payload()


async def _uc_catalogs(
    request: Request,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    try:
        client = require_unity_catalog_client()
        catalogs = client.list_catalogs()
    except HTTPException:
        raise
    except RuntimeError as exc:
        logger.warning("Unity Catalog list catalogs failed: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unity Catalog unexpected error")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"catalogs": catalogs}


async def _uc_schemas(
    request: Request,
    catalog_name: str = Query(..., min_length=1),
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    try:
        client = require_unity_catalog_client()
        schemas = client.list_schemas(catalog_name=catalog_name.strip())
    except HTTPException:
        raise
    except RuntimeError as exc:
        logger.warning("Unity Catalog list schemas failed: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"catalog_name": catalog_name, "schemas": schemas}


async def _uc_tables(
    request: Request,
    catalog_name: str = Query(..., min_length=1),
    schema_name: str = Query(..., min_length=1),
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    try:
        client = require_unity_catalog_client()
        tables = client.list_tables(
            catalog_name=catalog_name.strip(),
            schema_name=schema_name.strip(),
        )
    except HTTPException:
        raise
    except RuntimeError as exc:
        logger.warning("Unity Catalog list tables failed: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "tables": tables,
    }


async def _uc_tree(
    request: Request,
    catalog: str | None = Query(None),
    schema: str | None = Query(None),
    max_tables: int = Query(30, ge=1, le=500),
    include_columns: bool = Query(False),
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    if schema and not catalog:
        raise HTTPException(status_code=400, detail="schema requires catalog")
    try:
        client = require_unity_catalog_client()
        tree = build_tree(
            client,
            catalog_filter=(catalog.strip() if catalog else None),
            schema_filter=(schema.strip() if schema else None),
            max_tables_per_schema=max_tables,
            include_columns=include_columns,
        )
    except HTTPException:
        raise
    except RuntimeError as exc:
        logger.warning("Unity Catalog tree failed: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return tree


for _r in _ALL_ROUTERS:
    _r.add_api_route("/unity-catalog/health", _uc_health, methods=["GET"])
    _r.add_api_route("/unity-catalog/catalogs", _uc_catalogs, methods=["GET"])
    _r.add_api_route("/unity-catalog/schemas", _uc_schemas, methods=["GET"])
    _r.add_api_route("/unity-catalog/tables", _uc_tables, methods=["GET"])
    _r.add_api_route("/unity-catalog/tree", _uc_tree, methods=["GET"])
