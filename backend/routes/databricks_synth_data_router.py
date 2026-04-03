"""
Synthetic Data Generation (Databricks) API.

v1 scope:
- Validate Databricks-native synthetic generation requests.
- Execute one-time runs directly on Databricks SQL Warehouse (COPY INTO CSV on DBFS).
- Register interval schedules in-memory for controlled recurring runs (app-managed registry).
"""

from __future__ import annotations

import os
import re
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any

import requests
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from backend.integrations.databricks.read_unity_catalog import _resolve_host, _resolve_token
from backend.integrations.databricks.sql_statements import execute_sql_statement
from backend.integrations.databricks.warehouse_errors import classify_warehouse_error
from backend.services.access_guard import require_whitelisted_user
from backend.services.unity_catalog_client import require_unity_catalog_client


router = APIRouter(prefix="/tools", tags=["Databricks Synthetic Data"])
router_api_alias = APIRouter(prefix="/api/tools", tags=["Databricks Synthetic Data"])
router_root = APIRouter(tags=["Databricks Synthetic Data"])
_ALL_ROUTERS = (router, router_api_alias, router_root)

_DEFAULT_OUTPUT_PATH = "dbfs:/FileStore/vikaa/synthetic-data/"
_DEFAULT_WAREHOUSE_ID = "f0ec353cb2780a8c"
_ALLOWED_OUTPUT_FORMATS = {"csv"}
_SAMPLE_PERCENT_MIN = 1
_SAMPLE_PERCENT_MAX = 100
_SAMPLE_PERCENT_PRESETS = (1, 5)
_ALLOWED_SAMPLE_ANCHORS = {"initial", "delta"}
_ALLOWED_FREQUENCY = {"one-time", "interval"}
_MAX_TABLES_PER_RUN = 30
_DELTA_SAMPLE_MULTIPLIER = 5
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
_SCHEDULES: OrderedDict[str, dict[str, Any]] = OrderedDict()
_MAX_SCHEDULES = 200
_AUDIT_RUNS: OrderedDict[str, dict[str, Any]] = OrderedDict()
_MAX_AUDIT_RUNS = 1000


def _env_or_default(name: str, default: str) -> str:
    value = (os.getenv(name) or "").strip()
    return value or default


def _validate_dbfs_path(path: str) -> str:
    p = (path or "").strip()
    if not p:
        raise HTTPException(status_code=400, detail="output_path cannot be empty.")
    if not p.startswith("dbfs:/"):
        raise HTTPException(
            status_code=400,
            detail="output_path must start with dbfs:/ for v1.",
        )
    return p


def _to_sql_path(dbfs_path: str) -> str:
    """
    Databricks SQL COPY INTO expects POSIX-style DBFS/Volumes path (e.g. /Volumes/...),
    not dbfs:/ URI format.
    """
    p = (dbfs_path or "").strip()
    if p.startswith("dbfs:/"):
        p = p.replace("dbfs:/", "/", 1)
    if not p.startswith("/"):
        p = "/" + p
    return p


def _warehouse_id(request_id: str | None) -> str:
    req = (request_id or "").strip()
    if req:
        return req
    for key in (
        "DATABRICKS_SYNTH_DEFAULT_WAREHOUSE_ID",
        "DATABRICKS_SQL_WAREHOUSE_ID",
        "DATABRICKS_WAREHOUSE_ID",
    ):
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    return _DEFAULT_WAREHOUSE_ID


def _normalize_model_name(name: str | None) -> str:
    model = (name or "").strip() or _env_or_default(
        "DATABRICKS_SYNTH_DEFAULT_MODEL",
        "databricks-sonnet-4-5",
    )
    if not model.startswith("databricks-"):
        raise HTTPException(
            status_code=400,
            detail="llm_model_name must be a Databricks-hosted model (prefix: databricks-).",
        )
    return model


def _safe_ident(value: str, label: str) -> str:
    clean = (value or "").strip()
    if not clean or not _SAFE_IDENTIFIER.fullmatch(clean):
        raise HTTPException(
            status_code=400,
            detail=f"{label} has invalid characters. Use letters, numbers, underscore only.",
        )
    return clean


def _quote_ident(value: str) -> str:
    return f"`{value}`"


def _choose_delta_column(table_detail: dict[str, Any]) -> str | None:
    detail = table_detail.get("detail") or {}
    cols = detail.get("columns") or []
    priority = (
        "updated_at",
        "update_ts",
        "modified_at",
        "last_modified",
        "event_ts",
        "event_time",
        "created_at",
        "created_ts",
    )
    names = [str(c.get("name") or "").strip() for c in cols if isinstance(c, dict)]
    low_to_name = {n.lower(): n for n in names if n}
    for p in priority:
        if p in low_to_name:
            return low_to_name[p]
    for c in cols:
        if not isinstance(c, dict):
            continue
        n = str(c.get("name") or "").strip()
        t = str(c.get("type_text") or c.get("type_name") or "").lower()
        if n and ("timestamp" in t or t == "date"):
            return n
    return None


class SynthDataRunRequest(BaseModel):
    catalog_name: str = Field(..., min_length=1, max_length=255)
    schema_name: str = Field(..., min_length=1, max_length=255)
    table_names: list[str] | None = Field(
        default=None,
        description="Optional source tables; if omitted, all eligible tables in schema are considered.",
    )
    output_format: str = Field(default="csv")
    output_path: str = Field(default=_DEFAULT_OUTPUT_PATH)
    sample_percent: int = Field(default=1, ge=_SAMPLE_PERCENT_MIN, le=_SAMPLE_PERCENT_MAX)
    sample_anchor: str = Field(default="initial")
    frequency_mode: str = Field(default="one-time")
    interval_minutes: int | None = Field(default=None, ge=1, le=10080)
    delta_lookback_minutes: int | None = Field(default=None, ge=1, le=525600)
    llm_model_name: str | None = Field(
        default=None,
        description="Databricks workspace model endpoint, e.g. databricks-sonnet-4-5.",
    )
    warehouse_id: str | None = Field(
        default=None,
        description="Databricks SQL Warehouse ID override. Defaults to configured warehouse.",
    )
    seed: int | None = Field(default=None)
    max_rows_cap: int | None = Field(default=None, ge=1)


class SynthDataPreflightRequest(SynthDataRunRequest):
    large_run_threshold: int = Field(default=100_000, ge=1)


async def _synth_health() -> dict[str, Any]:
    host = _resolve_host()
    token = _resolve_token()
    warehouse = _warehouse_id(None)
    return {
        "status": "ok",
        "service": "databricks-synth-data",
        "paths": [
            "/tools/databricks-synth-data/health",
            "/api/tools/databricks-synth-data/health",
            "/databricks-synth-data/health",
        ],
        "defaults": {
            "output_format": "csv",
            "output_path": _env_or_default(
                "DATABRICKS_SYNTH_DEFAULT_OUTPUT_PATH",
                _DEFAULT_OUTPUT_PATH,
            ),
            "warehouse_id": warehouse,
            "llm_model_name": _env_or_default(
                "DATABRICKS_SYNTH_DEFAULT_MODEL",
                "databricks-sonnet-4-5",
            ),
        },
        "allowed_sample_percent_presets": list(_SAMPLE_PERCENT_PRESETS),
        "allowed_sample_percent_range": {
            "min": _SAMPLE_PERCENT_MIN,
            "max": _SAMPLE_PERCENT_MAX,
        },
        "databricks_host_configured": bool(host),
        "databricks_token_configured": bool(token),
        "sql_warehouse_configured": bool(warehouse),
        "schedule_registry_size": len(_SCHEDULES),
        "audit_registry_size": len(_AUDIT_RUNS),
    }


async def _synth_wake_warehouse(
    request: Request,
    warehouse_id: str,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    wid = (warehouse_id or "").strip()
    if not wid:
        raise HTTPException(status_code=400, detail="warehouse_id is required.")
    host = _resolve_host()
    token = _resolve_token()
    if not host or not token:
        raise HTTPException(status_code=503, detail="Databricks host/token are not configured.")
    base = host.rstrip("/")
    headers = {"Authorization": f"Bearer {token.strip()}"}

    # Read current warehouse status (best effort).
    state = "UNKNOWN"
    try:
        gr = requests.get(
            f"{base}/api/2.0/sql/warehouses/{wid}",
            headers=headers,
            timeout=30,
        )
        if gr.ok:
            state = str((gr.json() or {}).get("state") or "UNKNOWN").upper()
    except Exception:
        state = "UNKNOWN"

    # Start warehouse unless already running.
    started = False
    if state != "RUNNING":
        sr = requests.post(
            f"{base}/api/2.0/sql/warehouses/{wid}/start",
            headers=headers,
            timeout=30,
        )
        if not sr.ok:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to wake warehouse: {sr.status_code} {sr.text[:500]}",
            )
        started = True

    return {
        "status": "ok",
        "warehouse_id": wid,
        "previous_state": state,
        "start_requested": started,
        "message": (
            "Warehouse is already RUNNING."
            if not started
            else "Warehouse start requested. It may take a short time to become RUNNING."
        ),
    }


async def _synth_warehouse_status(
    request: Request,
    warehouse_id: str,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    wid = (warehouse_id or "").strip()
    if not wid:
        raise HTTPException(status_code=400, detail="warehouse_id is required.")
    host = _resolve_host()
    token = _resolve_token()
    if not host or not token:
        raise HTTPException(status_code=503, detail="Databricks host/token are not configured.")
    base = host.rstrip("/")
    headers = {"Authorization": f"Bearer {token.strip()}"}
    gr = requests.get(
        f"{base}/api/2.0/sql/warehouses/{wid}",
        headers=headers,
        timeout=30,
    )
    if not gr.ok:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch warehouse status: {gr.status_code} {gr.text[:500]}",
        )
    payload = gr.json() or {}
    return {
        "status": "ok",
        "warehouse_id": wid,
        "state": str(payload.get("state") or "UNKNOWN").upper(),
        "warehouse": payload,
    }


async def _synth_volumes(
    request: Request,
    catalog_name: str,
    schema_name: str | None = None,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    catalog = _safe_ident(catalog_name, "catalog_name")
    schema = _safe_ident(schema_name, "schema_name") if schema_name else None
    host = _resolve_host()
    token = _resolve_token()
    if not host or not token:
        raise HTTPException(status_code=503, detail="Databricks host/token are not configured.")

    base = host.rstrip("/")
    headers = {"Authorization": f"Bearer {token.strip()}"}
    url = f"{base}/api/2.1/unity-catalog/volumes"
    raw: list[dict[str, Any]] = []
    if schema:
        resp = requests.get(
            url,
            headers=headers,
            params={"catalog_name": catalog, "schema_name": schema},
            timeout=30,
        )
        if not resp.ok:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to fetch volumes: {resp.status_code} {resp.text[:500]}",
            )
        payload = resp.json() or {}
        raw = payload.get("volumes") or []
    else:
        # Catalog-wide fallback: enumerate schemas and query volumes per schema.
        client = require_unity_catalog_client(pat_detail="Databricks PAT not configured.")
        schemas = client.list_schemas(catalog_name=catalog)
        for sch in schemas:
            sname = str((sch or {}).get("name") or "").strip()
            if not sname:
                continue
            resp = requests.get(
                url,
                headers=headers,
                params={"catalog_name": catalog, "schema_name": sname},
                timeout=30,
            )
            if not resp.ok:
                # Ignore non-volume schemas; continue best-effort.
                continue
            payload = resp.json() or {}
            raw.extend(payload.get("volumes") or [])
    volumes: list[dict[str, Any]] = []
    for v in raw:
        if not isinstance(v, dict):
            continue
        name = str(v.get("name") or "").strip()
        if not name:
            continue
        sname = str(v.get("schema_name") or schema or "").strip()
        fqn = f"{catalog}.{sname}.{name}" if sname else f"{catalog}.{name}"
        volumes.append(
            {
                "name": name,
                "schema_name": sname or None,
                "full_name": fqn,
                "volume_type": v.get("volume_type"),
                "storage_location": v.get("storage_location"),
                "output_path": (
                    f"dbfs:/Volumes/{catalog}/{sname}/{name}/synthetic-data/"
                    if sname
                    else f"dbfs:/Volumes/{catalog}/{name}/synthetic-data/"
                ),
            }
        )
    return {
        "catalog_name": catalog,
        "schema_name": schema,
        "count": len(volumes),
        "volumes": volumes,
    }


async def _synth_stop_warehouse(
    request: Request,
    warehouse_id: str,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    wid = (warehouse_id or "").strip()
    if not wid:
        raise HTTPException(status_code=400, detail="warehouse_id is required.")
    host = _resolve_host()
    token = _resolve_token()
    if not host or not token:
        raise HTTPException(status_code=503, detail="Databricks host/token are not configured.")
    base = host.rstrip("/")
    headers = {"Authorization": f"Bearer {token.strip()}"}
    sr = requests.post(
        f"{base}/api/2.0/sql/warehouses/{wid}/stop",
        headers=headers,
        timeout=30,
    )
    if not sr.ok:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to stop warehouse: {sr.status_code} {sr.text[:500]}",
        )
    return {
        "status": "ok",
        "warehouse_id": wid,
        "stop_requested": True,
        "message": "Warehouse stop requested.",
    }


def _register_schedule(config: dict[str, Any]) -> dict[str, Any]:
    schedule_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    rec = {
        "schedule_id": schedule_id,
        "created_at_utc": now,
        "updated_at_utc": now,
        "status": "active",
        "config": config,
        "note": "In-memory schedule registry (resets on API restart).",
    }
    _SCHEDULES[schedule_id] = rec
    _SCHEDULES.move_to_end(schedule_id, last=True)
    while len(_SCHEDULES) > _MAX_SCHEDULES:
        _SCHEDULES.popitem(last=False)
    return rec


def _append_audit(record: dict[str, Any]) -> None:
    run_id = str(record.get("run_id") or str(uuid.uuid4()))
    _AUDIT_RUNS[run_id] = record
    _AUDIT_RUNS.move_to_end(run_id, last=True)
    while len(_AUDIT_RUNS) > _MAX_AUDIT_RUNS:
        _AUDIT_RUNS.popitem(last=False)


def _build_copy_sql(
    *,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    output_root: str,
    run_id: str,
    sample_percent: int,
    sample_anchor: str,
    delta_lookback_minutes: int | None,
    max_rows_cap: int | None,
    delta_column: str | None,
) -> tuple[str, str]:
    q_catalog = _quote_ident(catalog_name)
    q_schema = _quote_ident(schema_name)
    q_table = _quote_ident(table_name)
    source_ref = f"{q_catalog}.{q_schema}.{q_table}"

    where_clause = ""
    if sample_anchor == "delta":
        if not delta_column:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"sample_anchor='delta' requires a timestamp/date column in {catalog_name}.{schema_name}.{table_name} "
                    "or a conventional name such as updated_at/created_at."
                ),
            )
        q_delta_col = _quote_ident(delta_column)
        lookback = int(delta_lookback_minutes or 0)
        where_clause = (
            f" WHERE {q_delta_col} >= current_timestamp() - INTERVAL {lookback} MINUTES"
        )

    limit_clause = ""
    if max_rows_cap:
        limit_clause = f" LIMIT {int(max_rows_cap)}"

    source_query = (
        f"SELECT * FROM {source_ref} TABLESAMPLE ({int(sample_percent)} PERCENT)"
        f"{where_clause}{limit_clause}"
    )

    table_folder = (
        f"{output_root.rstrip('/')}/{catalog_name}/{schema_name}/{table_name}/{run_id}/"
    )
    sql_folder = _to_sql_path(table_folder)
    copy_sql = (
        f"COPY INTO '{sql_folder}' "
        f"FROM ({source_query}) "
        "FILEFORMAT = CSV "
        "FORMAT_OPTIONS ('header'='true') "
        "COPY_OPTIONS ('overwrite'='true')"
    )
    return copy_sql, table_folder


async def _synth_run(
    request: Request,
    body: SynthDataRunRequest,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    catalog_name = _safe_ident(body.catalog_name, "catalog_name")
    schema_name = _safe_ident(body.schema_name, "schema_name")

    output_format = (body.output_format or "").strip().lower()
    if output_format not in _ALLOWED_OUTPUT_FORMATS:
        raise HTTPException(
            status_code=400,
            detail=f"output_format must be one of {sorted(_ALLOWED_OUTPUT_FORMATS)} in v1.",
        )

    output_path = _validate_dbfs_path(
        body.output_path
        or _env_or_default("DATABRICKS_SYNTH_DEFAULT_OUTPUT_PATH", _DEFAULT_OUTPUT_PATH)
    )

    if not (_SAMPLE_PERCENT_MIN <= int(body.sample_percent) <= _SAMPLE_PERCENT_MAX):
        raise HTTPException(
            status_code=400,
            detail=f"sample_percent must be between {_SAMPLE_PERCENT_MIN} and {_SAMPLE_PERCENT_MAX}.",
        )

    sample_anchor = (body.sample_anchor or "").strip().lower()
    if sample_anchor not in _ALLOWED_SAMPLE_ANCHORS:
        raise HTTPException(
            status_code=400,
            detail=f"sample_anchor must be one of {sorted(_ALLOWED_SAMPLE_ANCHORS)}.",
        )

    frequency_mode = (body.frequency_mode or "").strip().lower()
    if frequency_mode not in _ALLOWED_FREQUENCY:
        raise HTTPException(
            status_code=400,
            detail=f"frequency_mode must be one of {sorted(_ALLOWED_FREQUENCY)}.",
        )
    if frequency_mode == "interval" and not body.interval_minutes:
        raise HTTPException(
            status_code=400,
            detail="interval_minutes is required when frequency_mode='interval'.",
        )
    if frequency_mode == "one-time" and body.interval_minutes is not None:
        raise HTTPException(
            status_code=400,
            detail="interval_minutes must be null when frequency_mode='one-time'.",
        )

    if sample_anchor == "delta" and body.delta_lookback_minutes is None:
        raise HTTPException(
            status_code=400,
            detail="delta_lookback_minutes is required when sample_anchor='delta'.",
        )
    if sample_anchor == "initial" and body.delta_lookback_minutes is not None:
        raise HTTPException(
            status_code=400,
            detail="delta_lookback_minutes must be null when sample_anchor='initial'.",
        )

    llm_model_name = _normalize_model_name(body.llm_model_name)
    warehouse_id = _warehouse_id(body.warehouse_id)

    client = require_unity_catalog_client(pat_detail="Databricks PAT not configured.")
    available_tables = client.list_tables(catalog_name=catalog_name, schema_name=schema_name)
    available_names = sorted(
        {
            str(t.get("name") or "").strip()
            for t in available_tables
            if isinstance(t, dict) and t.get("name")
        }
    )
    if not available_names:
        raise HTTPException(
            status_code=404,
            detail=f"No tables found in {catalog_name}.{schema_name}.",
        )

    requested_tables = [t.strip() for t in (body.table_names or []) if t and t.strip()]
    if requested_tables:
        safe_tables = [_safe_ident(t, "table_names[]") for t in requested_tables]
    else:
        safe_tables = available_names

    missing = [t for t in safe_tables if t not in available_names]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown table(s) in {catalog_name}.{schema_name}: {missing}",
        )
    if len(safe_tables) > _MAX_TABLES_PER_RUN:
        raise HTTPException(
            status_code=400,
            detail=f"Too many tables requested ({len(safe_tables)}). Max per run is {_MAX_TABLES_PER_RUN}.",
        )

    requested_sample_percent = body.sample_percent
    effective_sample_percent = requested_sample_percent
    if sample_anchor == "delta":
        effective_sample_percent = min(100, requested_sample_percent * _DELTA_SAMPLE_MULTIPLIER)

    run_id = str(uuid.uuid4())
    submitted_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    schedule = (
        {"mode": "one-time"}
        if frequency_mode == "one-time"
        else {"mode": "interval", "interval_minutes": body.interval_minutes}
    )

    resolved_request = {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "table_names": safe_tables,
        "output_format": output_format,
        "output_path": output_path,
        "sample_percent_requested": requested_sample_percent,
        "sample_percent_effective": effective_sample_percent,
        "sample_anchor": sample_anchor,
        "delta_lookback_minutes": body.delta_lookback_minutes,
        "llm_model_name": llm_model_name,
        "warehouse_id": warehouse_id,
        "seed": body.seed,
        "max_rows_cap": body.max_rows_cap,
    }

    initiated_by = str((_acl or {}).get("email") or "unknown")

    if frequency_mode == "interval":
        schedule_rec = _register_schedule(
            {
                "request": resolved_request,
                "frequency_mode": frequency_mode,
                "interval_minutes": body.interval_minutes,
            }
        )
        _append_audit(
            {
                "run_id": run_id,
                "timestamp_utc": submitted_at,
                "initiated_by": initiated_by,
                "mode": "schedule-create",
                "status": "scheduled",
                "source": {
                    "catalog": catalog_name,
                    "schema": schema_name,
                    "tables": safe_tables,
                },
                "sampling": {
                    "anchor": sample_anchor,
                    "sample_percent_requested": requested_sample_percent,
                    "sample_percent_effective": effective_sample_percent,
                    "delta_lookback_minutes": body.delta_lookback_minutes,
                },
                "model": llm_model_name,
                "output_path": output_path,
            }
        )
        return {
            "status": "scheduled",
            "service": "databricks-synth-data",
            "run_id": run_id,
            "submitted_at_utc": submitted_at,
            "request_validated": True,
            "databricks_only_enforced": True,
            "resolved_request": resolved_request,
            "schedule": {
                **schedule,
                "schedule_id": schedule_rec["schedule_id"],
                "status": schedule_rec["status"],
                "registry_note": schedule_rec["note"],
            },
            "execution": {
                "mode": "interval",
                "note": "Use one-time mode or Databricks Workflows integration for immediate execution.",
            },
        }

    host = _resolve_host()
    token = _resolve_token()
    if not host or not token:
        raise HTTPException(
            status_code=503,
            detail="Databricks host/token are not configured.",
        )

    executions: list[dict[str, Any]] = []
    for table_name in safe_tables:
        full_name = f"{catalog_name}.{schema_name}.{table_name}"
        table_detail = client.get_table(full_name)
        delta_column = _choose_delta_column({"detail": table_detail})
        copy_sql, table_folder = _build_copy_sql(
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            output_root=output_path,
            run_id=run_id,
            sample_percent=effective_sample_percent,
            sample_anchor=sample_anchor,
            delta_lookback_minutes=body.delta_lookback_minutes,
            max_rows_cap=body.max_rows_cap,
            delta_column=delta_column,
        )
        exec_result = execute_sql_statement(
            host,
            token,
            warehouse_id,
            copy_sql,
            wait_timeout_s=50,
            max_poll_s=180,
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
                    "table": table_name,
                    "statement_id": exec_result.get("statement_id"),
                    "warehouse_error_detail": (exec_result.get("error") or "")[:1500],
                },
            )
        executions.append(
            {
                "table_name": table_name,
                "output_path": table_folder,
                "statement_id": exec_result.get("statement_id"),
                "elapsed_ms": exec_result.get("elapsed_ms"),
                "state": exec_result.get("state"),
            }
        )

    result = {
        "status": "completed",
        "service": "databricks-synth-data",
        "run_id": run_id,
        "submitted_at_utc": submitted_at,
        "request_validated": True,
        "databricks_only_enforced": True,
        "resolved_request": resolved_request,
        "schedule": schedule,
        "executions": executions,
    }
    _append_audit(
        {
            "run_id": run_id,
            "timestamp_utc": submitted_at,
            "initiated_by": initiated_by,
            "mode": "run",
            "status": "completed",
            "source": {
                "catalog": catalog_name,
                "schema": schema_name,
                "tables": safe_tables,
            },
            "sampling": {
                "anchor": sample_anchor,
                "sample_percent_requested": requested_sample_percent,
                "sample_percent_effective": effective_sample_percent,
                "delta_lookback_minutes": body.delta_lookback_minutes,
            },
            "model": llm_model_name,
            "output_path": output_path,
            "rows_in_source_estimate": None,
            "rows_out_synthetic": None,
            "duration_ms": sum(int(e.get("elapsed_ms") or 0) for e in executions),
            "tables_succeeded": len(executions),
            "tables_failed": 0,
        }
    )
    return result


async def _synth_tables_meta(
    request: Request,
    catalog_name: str,
    schema_name: str,
    warehouse_id: str | None = None,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    catalog = _safe_ident(catalog_name, "catalog_name")
    schema = _safe_ident(schema_name, "schema_name")
    client = require_unity_catalog_client(pat_detail="Databricks PAT not configured.")
    try:
        raw_tables = client.list_tables(catalog_name=catalog, schema_name=schema)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    names = sorted(
        [str(t.get("name") or "").strip() for t in raw_tables if isinstance(t, dict) and t.get("name")]
    )
    if not names:
        return {"catalog_name": catalog, "schema_name": schema, "tables": []}

    host = _resolve_host()
    token = _resolve_token()
    wid = _warehouse_id(warehouse_id)
    tables: list[dict[str, Any]] = []
    for name in names:
        full_name = f"{catalog}.{schema}.{name}"
        column_count = None
        try:
            detail = client.get_table(full_name)
            column_count = len(detail.get("columns") or [])
        except Exception:
            column_count = None

        row_count = None
        last_updated = None
        if host and token and wid:
            desc = execute_sql_statement(
                host,
                token,
                wid,
                f"DESCRIBE DETAIL `{catalog}`.`{schema}`.`{name}`",
                wait_timeout_s=30,
                max_poll_s=60,
            )
            if desc.get("ok"):
                cols = desc.get("columns") or []
                rows = desc.get("rows") or []
                if rows and cols:
                    first = rows[0]
                    idx = {str(c): i for i, c in enumerate(cols)}
                    if "numRows" in idx and idx["numRows"] < len(first):
                        row_count = first[idx["numRows"]]
                    if "lastModified" in idx and idx["lastModified"] < len(first):
                        last_updated = first[idx["lastModified"]]

        upper = name.upper()
        group = "ALL"
        if upper.startswith("FORECAST_"):
            group = "FORECAST"
        elif upper.startswith("HISTORICAL_"):
            group = "HISTORICAL"
        stale = False
        tables.append(
            {
                "name": name,
                "row_count": row_count,
                "last_updated": last_updated,
                "column_count": column_count,
                "group": group,
                "has_read_permission": True,
                "stale": stale,
            }
        )
    return {"catalog_name": catalog, "schema_name": schema, "tables": tables}


def _check_schedule_conflict(interval_minutes: int | None) -> bool:
    if not interval_minutes:
        return False
    for rec in _SCHEDULES.values():
        if rec.get("status") != "active":
            continue
        cfg = rec.get("config") or {}
        iv = cfg.get("interval_minutes")
        try:
            iv_int = int(iv)
        except Exception:
            continue
        if abs(iv_int - int(interval_minutes)) <= 10:
            return True
    return False


async def _synth_preflight(
    request: Request,
    body: SynthDataPreflightRequest,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    catalog_name = _safe_ident(body.catalog_name, "catalog_name")
    schema_name = _safe_ident(body.schema_name, "schema_name")
    output_path = _validate_dbfs_path(body.output_path or _DEFAULT_OUTPUT_PATH)
    host = _resolve_host()
    token = _resolve_token()
    wid = _warehouse_id(body.warehouse_id)

    checks: list[dict[str, Any]] = []
    # Schema compatibility
    try:
        client = require_unity_catalog_client(pat_detail="Databricks PAT not configured.")
        raw_tables = client.list_tables(catalog_name=catalog_name, schema_name=schema_name)
        names = sorted(
            [str(t.get("name") or "").strip() for t in raw_tables if isinstance(t, dict) and t.get("name")]
        )
        req = [t.strip() for t in (body.table_names or []) if t and t.strip()]
        selected = req or names
        missing = [t for t in selected if t not in names]
        if missing:
            checks.append({"id": "schema", "label": "Schema compatibility", "status": "fail", "hint": f"Missing tables: {missing}"})
        else:
            checks.append({"id": "schema", "label": "Schema compatibility", "status": "pass", "hint": f"{len(selected)} table(s) verified."})
    except Exception as exc:
        checks.append({"id": "schema", "label": "Schema compatibility", "status": "fail", "hint": str(exc)})
        selected = []

    # Warehouse running
    try:
        if host and token and wid:
            ws = await _synth_warehouse_status(request, wid, _acl)  # reuse logic
            state = str(ws.get("state") or "UNKNOWN").upper()
            if state == "RUNNING":
                checks.append({"id": "warehouse", "label": "Warehouse running", "status": "pass", "hint": "Warehouse is RUNNING."})
            elif state in {"STARTING", "STOPPING"}:
                checks.append({"id": "warehouse", "label": "Warehouse running", "status": "warn", "hint": f"Warehouse is {state}."})
            else:
                checks.append({"id": "warehouse", "label": "Warehouse running", "status": "fail", "hint": f"Warehouse is {state}. Start it first."})
        else:
            checks.append({"id": "warehouse", "label": "Warehouse running", "status": "fail", "hint": "Databricks host/token/warehouse missing."})
    except Exception as exc:
        checks.append({"id": "warehouse", "label": "Warehouse running", "status": "fail", "hint": str(exc)})

    checks.append({"id": "token", "label": "Token valid", "status": "pass" if bool(token) else "fail", "hint": "Token present." if token else "Token missing."})
    checks.append({"id": "output_path", "label": "Output path writable", "status": "pass" if output_path.startswith("dbfs:/") else "fail", "hint": output_path})
    checks.append({"id": "quota", "label": "Storage quota headroom", "status": "warn", "hint": "Quota API not configured; assume manual monitoring."})
    conflict = _check_schedule_conflict(body.interval_minutes if body.frequency_mode == "interval" else None)
    checks.append({
        "id": "schedule_conflict",
        "label": "No conflicting schedule (±10 min)",
        "status": "warn" if conflict else "pass",
        "hint": "Potential overlapping interval schedule detected." if conflict else "No conflict detected.",
    })

    # simple estimate
    pct = int(body.sample_percent)
    if (body.sample_anchor or "").strip().lower() == "delta":
        pct = min(100, pct * _DELTA_SAMPLE_MULTIPLIER)
    estimated_source_rows = max(0, len(selected) * 10_000)
    estimated_output_rows = int(estimated_source_rows * (pct / 100.0))
    large_run = estimated_output_rows > int(body.large_run_threshold)

    any_fail = any(c["status"] == "fail" for c in checks)
    any_warn = any(c["status"] == "warn" for c in checks)
    gate = "blocked" if any_fail else ("warn" if any_warn else "pass")
    return {
        "status": "ok",
        "gate": gate,
        "checks": checks,
        "estimates": {
            "selected_tables": len(selected),
            "estimated_source_rows": estimated_source_rows,
            "estimated_output_rows": estimated_output_rows,
            "sample_percent_effective": pct,
        },
        "large_run_confirmation_required": large_run,
        "large_run_threshold": body.large_run_threshold,
    }


async def _synth_list_schedules(
    request: Request,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    items = list(reversed(list(_SCHEDULES.values())))
    return {"count": len(items), "schedules": items}


async def _synth_cancel_schedule(
    request: Request,
    schedule_id: str,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    sid = (schedule_id or "").strip()
    rec = _SCHEDULES.get(sid)
    if not rec:
        raise HTTPException(status_code=404, detail="schedule_id not found.")
    rec["status"] = "cancelled"
    rec["updated_at_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    _append_audit(
        {
            "run_id": str(uuid.uuid4()),
            "timestamp_utc": rec["updated_at_utc"],
            "initiated_by": str((_acl or {}).get("email") or "unknown"),
            "mode": "schedule-cancel",
            "status": "cancelled",
            "schedule_id": sid,
        }
    )
    return {"status": "cancelled", "schedule_id": sid}


async def _synth_audit_log(
    request: Request,
    limit: int = 200,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    n = max(1, min(1000, int(limit)))
    items = list(reversed(list(_AUDIT_RUNS.values())))[:n]
    return {"count": len(items), "records": items}


for _r in _ALL_ROUTERS:
    _r.add_api_route("/databricks-synth-data/health", _synth_health, methods=["GET"])
    _r.add_api_route(
        "/databricks-synth-data/warehouse/{warehouse_id}/wake",
        _synth_wake_warehouse,
        methods=["POST"],
    )
    _r.add_api_route(
        "/databricks-synth-data/warehouse/{warehouse_id}/status",
        _synth_warehouse_status,
        methods=["GET"],
    )
    _r.add_api_route(
        "/databricks-synth-data/warehouse/{warehouse_id}/stop",
        _synth_stop_warehouse,
        methods=["POST"],
    )
    _r.add_api_route(
        "/databricks-synth-data/volumes",
        _synth_volumes,
        methods=["GET"],
    )
    _r.add_api_route(
        "/databricks-synth-data/tables-meta",
        _synth_tables_meta,
        methods=["GET"],
    )
    _r.add_api_route(
        "/databricks-synth-data/preflight",
        _synth_preflight,
        methods=["POST"],
    )
    _r.add_api_route("/databricks-synth-data/run", _synth_run, methods=["POST"])
    _r.add_api_route(
        "/databricks-synth-data/schedules",
        _synth_list_schedules,
        methods=["GET"],
    )
    _r.add_api_route(
        "/databricks-synth-data/schedules/{schedule_id}/cancel",
        _synth_cancel_schedule,
        methods=["POST"],
    )
    _r.add_api_route(
        "/databricks-synth-data/audit",
        _synth_audit_log,
        methods=["GET"],
    )

