"""
Natural language answers backed by Unity Catalog metadata + Databricks SQL warehouse (live data).

Requires DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_SQL_WAREHOUSE_ID, and an LLM key:
GEMINI_API_KEY or GOOGLE_API_KEY (primary), with ANTHROPIC_API_KEY optional fallback.

Config (optional env):
  GEMINI_API_KEY / GOOGLE_API_KEY — primary LLM for NL→SQL and summaries.
  ANTHROPIC_API_KEY — optional fallback if Gemini fails or is absent.
  DATABRICKS_QUERY_GEMINI_MODEL — Gemini model id (default gemini-2.0-flash).
  DATABRICKS_QUERY_ENFORCE_FQN — default true; require catalog.schema.table in generated SQL.
  DATABRICKS_QUERY_ALLOWED_CATALOGS — comma-separated allowlist; empty = any.
  DATABRICKS_QUERY_PREFLIGHT_EXPLAIN — if true, run EXPLAIN before the main statement (extra warehouse call).
  DATABRICKS_QUERY_STATEMENT_TIMEOUT_S — warehouse wait_timeout (10–300, default 50).
  DATABRICKS_QUERY_MAX_POLL_S — max seconds polling statement (default 120).
  DATABRICKS_QUERY_SCOPE_WARN_TABLES — warn when ``All schemas`` and table count exceeds this (default 40).
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from datetime import datetime, timezone
from collections import OrderedDict
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from backend.integrations.databricks.read_unity_catalog import (
    build_tree,
    _resolve_host,
    _resolve_token,
)
from backend.integrations.databricks.sql_statements import execute_sql_statement
from backend.integrations.databricks.warehouse_errors import classify_warehouse_error
from backend.services.access_guard import require_whitelisted_user
from backend.services.catalog_search_service import (
    build_context_excerpt,
    iter_catalog_blocks,
    resolve_catalog_and_schema_filters,
)
from backend.services.databricks_nl_sql_service import (
    assert_fully_qualified_table_names,
    generate_sql_for_question,
    summarize_result,
)
from backend.services.unity_catalog_client import require_unity_catalog_client

logger = logging.getLogger(__name__)

_MAX_SESSIONS = 250
_SESSIONS: OrderedDict[str, dict[str, Any]] = OrderedDict()


def _session_get(sid: str) -> dict[str, Any] | None:
    return _SESSIONS.get(sid)


def _session_put(sid: str, turns: list[dict[str, str]]) -> None:
    _SESSIONS[sid] = {"turns": turns[-8:], "updated": time.time()}
    _SESSIONS.move_to_end(sid, last=True)
    while len(_SESSIONS) > _MAX_SESSIONS:
        _SESSIONS.popitem(last=False)


def _session_context_lines(sid: str) -> str | None:
    entry = _session_get(sid)
    if not entry:
        return None
    turns = entry.get("turns") or []
    if not turns:
        return None
    lines: list[str] = []
    for t in turns[-3:]:
        q = (t.get("q") or "").strip()
        sql = (t.get("sql") or "").strip()
        summ = (t.get("summary") or "").strip()[:1200]
        if q:
            lines.append(f"Q: {q}")
        if sql:
            lines.append(f"SQL: {sql}")
        if summ:
            lines.append(f"Answer summary: {summ}")
    return "\n".join(lines) if lines else None


def _allowed_catalogs() -> set[str] | None:
    raw = (os.getenv("DATABRICKS_QUERY_ALLOWED_CATALOGS") or "").strip()
    if not raw:
        return None
    return {x.strip() for x in raw.split(",") if x.strip()}


def _enforce_fqn() -> bool:
    return os.getenv("DATABRICKS_QUERY_ENFORCE_FQN", "true").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _preflight_explain() -> bool:
    return os.getenv("DATABRICKS_QUERY_PREFLIGHT_EXPLAIN", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _stmt_timeout_s() -> int:
    try:
        v = int(os.getenv("DATABRICKS_QUERY_STATEMENT_TIMEOUT_S", "50"))
    except ValueError:
        v = 50
    return max(10, min(300, v))


def _max_poll_s() -> float:
    try:
        return float(os.getenv("DATABRICKS_QUERY_MAX_POLL_S", "120"))
    except ValueError:
        return 120.0


def _scope_warn_threshold() -> int:
    try:
        return max(5, int(os.getenv("DATABRICKS_QUERY_SCOPE_WARN_TABLES", "40")))
    except ValueError:
        return 40


def _query_runtime_config() -> dict[str, Any]:
    allowed = _allowed_catalogs()
    return {
        "enforce_fqn": _enforce_fqn(),
        "preflight_explain": _preflight_explain(),
        "statement_timeout_s": _stmt_timeout_s(),
        "max_poll_s": _max_poll_s(),
        "scope_warn_tables": _scope_warn_threshold(),
        "allowed_catalogs": sorted(allowed) if allowed else None,
    }

router = APIRouter(prefix="/tools", tags=["Databricks SQL Query"])
router_api_alias = APIRouter(prefix="/api/tools", tags=["Databricks SQL Query"])
router_root = APIRouter(tags=["Databricks SQL Query"])
_ALL_ROUTERS = (router, router_api_alias, router_root)


def _warehouse_id(request_id: str | None) -> str:
    """Resolve warehouse UUID for Statement Execution API (not the display name)."""
    req = (request_id or "").strip()
    if req:
        return req
    for key in (
        "DATABRICKS_SQL_WAREHOUSE_ID",
        "DATABRICKS_WAREHOUSE_ID",
    ):
        v = (os.getenv(key) or "").strip()
        if v:
            return v
    return ""


def _health_payload() -> dict[str, Any]:
    host = _resolve_host()
    token = _resolve_token()
    wid = _warehouse_id(None)
    gk = (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()
    gemini_ok = bool(gk)
    ak = (os.getenv("ANTHROPIC_API_KEY") or "").strip()
    anthropic_ok = bool(ak and ak != "your-anthropic-api-key-here")
    llm_ok = gemini_ok or anthropic_ok
    payload: dict[str, Any] = {
        "status": "ok",
        "service": "databricks-query",
        "paths": [
            "/tools/databricks-query/health",
            "/api/tools/databricks-query/health",
            "/databricks-query/health",
        ],
        "databricks_host_configured": bool(host),
        "databricks_token_configured": bool(token),
        "sql_warehouse_configured": bool(wid),
        "gemini_configured": gemini_ok,
        "anthropic_configured": anthropic_ok,
        "llm_configured": llm_ok,
    }
    if not wid:
        payload["note"] = (
            "sql_warehouse_configured is false: set DATABRICKS_SQL_WAREHOUSE_ID "
            "(or DATABRICKS_WAREHOUSE_ID) to your SQL warehouse UUID from "
            "Databricks → SQL Warehouses → open warehouse → copy ID. "
            "Display names like 'Serverless Starter Warehouse' will not work; restart uvicorn after .env changes. "
            "Alternatively pass warehouse_id in POST /tools/databricks-query/ask."
        )
    elif not llm_ok:
        payload["note"] = (
            "Set GEMINI_API_KEY or GOOGLE_API_KEY (recommended), "
            "and/or ANTHROPIC_API_KEY as fallback, for NL→SQL and summaries."
        )
    payload["runtime_config"] = _query_runtime_config()
    return payload


class DatabricksQueryAskRequest(BaseModel):
    query: str = Field(..., min_length=2, max_length=8000)
    catalog: str | None = None
    schemas: list[str] | None = None
    schema_substrings: list[str] | None = None
    max_tables_per_schema: int = Field(50, ge=1, le=200)
    max_context_chars: int = Field(100_000, ge=8000, le=250_000)
    warehouse_id: str | None = Field(
        default=None,
        description="Optional override; default DATABRICKS_SQL_WAREHOUSE_ID.",
    )
    max_result_rows: int = Field(500, ge=1, le=2000)
    session_id: str | None = Field(
        default=None,
        description="Opaque id for multi-turn context (server retains last few turns).",
    )


async def _dq_health() -> dict[str, Any]:
    return _health_payload()


async def _dq_ask(
    request: Request,
    body: DatabricksQueryAskRequest,
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

    allowed_cat = _allowed_catalogs()
    if allowed_cat is not None and cat not in allowed_cat:
        raise HTTPException(
            status_code=403,
            detail=f"Catalog '{cat}' is not in DATABRICKS_QUERY_ALLOWED_CATALOGS.",
        )

    wid = _warehouse_id(body.warehouse_id)
    if not wid:
        raise HTTPException(
            status_code=503,
            detail="SQL warehouse not configured. Set DATABRICKS_SQL_WAREHOUSE_ID "
            "(SQL warehouse ID from Databricks) or pass warehouse_id in the request.",
        )

    sid = (body.session_id or "").strip() or str(uuid.uuid4())

    try:
        client = require_unity_catalog_client(pat_detail="Databricks PAT not configured.")
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

    scope_warnings: list[str] = []
    all_schemas_mode = body.schemas is None and body.schema_substrings is None
    if all_schemas_mode and len(blocks) > _scope_warn_threshold():
        scope_warnings.append(
            f"Wide scope: {len(blocks)} tables across all schemas in this catalog are in context. "
            f"Consider choosing a single schema in the UI for faster, cheaper, more accurate answers."
        )

    context, n_included = build_context_excerpt(
        blocks,
        body.query.strip(),
        body.max_context_chars,
    )
    if n_included < 1:
        raise HTTPException(
            status_code=400,
            detail="Could not build catalog context for the model (try widening filters).",
        )

    convo = _session_context_lines(sid)

    try:
        sql, llm_sql_label = generate_sql_for_question(
            context,
            body.query.strip(),
            conversation_context=convo,
        )
        assert_fully_qualified_table_names(sql, enforce=_enforce_fqn())
    except (RuntimeError, ValueError) as exc:
        logger.warning("SQL generation failed: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    host = _resolve_host()
    token = _resolve_token()
    timeout = _stmt_timeout_s()
    poll_max = _max_poll_s()
    user_email = str(_acl.get("email") or "unknown")

    logger.info(
        "audit.databricks_query user=%s catalog=%s tables_in_scope=%s session=%s",
        user_email,
        cat,
        len(blocks),
        sid or "—",
    )
    logger.info("audit.databricks_query sql_preview=%s", sql[:1200].replace("\n", " "))

    if _preflight_explain():
        explain_stmt = f"EXPLAIN {sql}"
        ex = execute_sql_statement(
            host,
            token,
            wid,
            explain_stmt,
            wait_timeout_s=min(timeout, 45),
            max_poll_s=min(poll_max, 90.0),
        )
        if not ex.get("ok"):
            kind = classify_warehouse_error(
                ex.get("error"),
                http_status=ex.get("http_status"),
            )
            raise HTTPException(
                status_code=502,
                detail={
                    "message": f"Preflight EXPLAIN failed: {kind['message']}",
                    "category": kind["category"],
                    "sql": sql,
                    "warehouse_error_detail": (ex.get("error") or "")[:1500],
                },
            )

    exec_result = execute_sql_statement(
        host,
        token,
        wid,
        sql,
        wait_timeout_s=timeout,
        max_poll_s=poll_max,
    )

    if not exec_result.get("ok"):
        kind = classify_warehouse_error(
            exec_result.get("error"),
            http_status=exec_result.get("http_status"),
        )
        raise HTTPException(
            status_code=502,
            detail={
                "message": kind["message"],
                "category": kind["category"],
                "sql": sql,
                "warehouse_error_detail": (exec_result.get("error") or "")[:1500],
                "statement_id": exec_result.get("statement_id"),
                "elapsed_ms": exec_result.get("elapsed_ms"),
            },
        )

    cols = list(exec_result.get("columns") or [])
    rows = list(exec_result.get("rows") or [])
    max_rows = body.max_result_rows
    truncated = len(rows) > max_rows
    if truncated:
        rows = rows[:max_rows]

    llm_summary_label = "—"
    try:
        summary, llm_summary_label = summarize_result(
            body.query.strip(), sql, cols, rows
        )
    except RuntimeError as exc:
        logger.warning("Summarization failed: %s", exc)
        summary = f"(Could not summarize: {exc}) Raw row count returned: {len(rows)}."

    prev = _session_get(sid)
    turns = list(prev.get("turns") or []) if prev else []
    turns.append(
        {
            "q": body.query.strip(),
            "sql": sql,
            "summary": summary[:4000],
        }
    )
    _session_put(sid, turns)

    workspace_host = (_resolve_host() or "").strip().rstrip("/")
    completed_at = datetime.now(timezone.utc)

    return {
        "summary": summary,
        "sql": sql,
        "columns": cols,
        "rows": rows,
        "row_count_returned": len(rows),
        "result_truncated": truncated or bool(exec_result.get("truncated")),
        "catalog": cat,
        "tables_in_model_context": n_included,
        "tables_matched_filters": len(blocks),
        "statement_id": exec_result.get("statement_id"),
        "elapsed_ms": exec_result.get("elapsed_ms"),
        "scope_warnings": scope_warnings,
        "session_id": sid,
        "preflight_explain_ran": _preflight_explain(),
        "runtime_config": _query_runtime_config(),
        "workspace_sql_console_base": workspace_host + "/sql" if workspace_host else None,
        "llm_sql_label": llm_sql_label,
        "llm_summary_label": llm_summary_label,
        "completed_at_utc": completed_at.isoformat().replace("+00:00", "Z"),
    }


for _r in _ALL_ROUTERS:
    _r.add_api_route("/databricks-query/health", _dq_health, methods=["GET"])
    _r.add_api_route("/databricks-query/ask", _dq_ask, methods=["POST"])
