"""
Synthetic Data Generation (Databricks) API.

v1 scope:
- Validate Databricks-native synthetic generation requests.
- Execute one-time runs directly on Databricks SQL Warehouse (CSV export to DBFS/UC paths).
- Register interval schedules in-memory for controlled recurring runs (app-managed registry).
"""

from __future__ import annotations

import json
import os
import re
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from backend.integrations.databricks.dbfs_io import read_json_from_dbfs_uri, write_json_to_dbfs_uri
from backend.integrations.databricks.read_unity_catalog import _resolve_host, _resolve_token
from backend.integrations.databricks.sql_statements import execute_sql_statement
from backend.integrations.databricks.warehouse_errors import classify_warehouse_error
from backend.services.access_guard import enforce_synth_enterprise_auth, require_whitelisted_user
from backend.services.synth_enterprise import (
    build_manifest,
    llm_export_plan,
    log_synth_event,
    order_tables_for_export,
    repeatable_seed_for_run,
    run_optional_source_rowcount_qa,
    workflow_spec_template,
)
from backend.services.unity_catalog_client import require_unity_catalog_client


router = APIRouter(prefix="/tools", tags=["Databricks Synthetic Data"])
router_api_alias = APIRouter(prefix="/api/tools", tags=["Databricks Synthetic Data"])
router_root = APIRouter(tags=["Databricks Synthetic Data"])
_ALL_ROUTERS = (router, router_api_alias, router_root)

_DEFAULT_OUTPUT_PATH = "dbfs:/FileStore/vikaa/synthetic-data/"
_DEFAULT_WAREHOUSE_ID = "f0ec353cb2780a8c"
_ALLOWED_OUTPUT_FORMATS = frozenset({"csv", "json", "parquet"})
_SAMPLE_PERCENT_MIN = 1
_SAMPLE_PERCENT_MAX = 100
_SAMPLE_PERCENT_PRESETS = (1, 5)
_ALLOWED_SAMPLE_ANCHORS = {"initial", "delta"}
_ALLOWED_FREQUENCY = {"one-time", "interval"}
_ALLOWED_SYNTHETIC_MODES = frozenset({"sample", "generative"})
_MAX_TABLES_PER_RUN = 30
_DELTA_SAMPLE_MULTIPLIER = 5
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
_EXPORT_MONTHS_EN = (
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
)
_SCHEDULES: OrderedDict[str, dict[str, Any]] = OrderedDict()
_MAX_SCHEDULES = 200
_AUDIT_RUNS: OrderedDict[str, dict[str, Any]] = OrderedDict()
_MAX_AUDIT_RUNS = 1000
_SAFE_SUMMARY_FILENAME = re.compile(r"^summary__[A-Za-z0-9_.-]+\.json$")


def _optional_local_summary_dir() -> Path | None:
    """If set, legacy GET /run-summary can read basename-only audit rows from this folder."""
    raw = (os.getenv("DATABRICKS_SYNTH_SUMMARY_DIR") or "").strip()
    if not raw:
        return None
    return Path(raw).expanduser().resolve()


def _summary_filename_for_run(run_id: str, submitted_at_utc: str) -> str:
    """summary__05Apr2026_064949_a035823b.json (UTC, English month)."""
    raw = (submitted_at_utc or "").strip()
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
    except ValueError:
        dt = datetime.now(timezone.utc)
    mo = _EXPORT_MONTHS_EN[dt.month - 1]
    part = f"{dt.day:02d}{mo}{dt.year:04d}_{dt.strftime('%H%M%S')}"
    rid = (run_id or "").replace("-", "")[:8]
    return f"summary__{part}_{rid}.json"


def _persist_run_summary_volume(
    payload: dict[str, Any],
    *,
    output_root_dbfs: str,
    host: str,
    token: str,
) -> tuple[str | None, str | None, str | None]:
    """
    Write summary JSON at the volume root (same output_path as the run).

    Returns (basename, dbfs_uri, error_message). On success error_message is None.
    """
    run_id = str(payload.get("run_id") or "").strip()
    ts = str(payload.get("submitted_at_utc") or "").strip()
    if not run_id or not ts:
        return None, None, "missing run_id or submitted_at_utc"
    name = _summary_filename_for_run(run_id, ts)
    if not _SAFE_SUMMARY_FILENAME.match(name):
        return None, None, "invalid summary filename"
    root = (output_root_dbfs or "").strip().rstrip("/")
    summary_uri = f"{root}/{name}"
    try:
        write_json_to_dbfs_uri(host, token, summary_uri, payload)
    except (OSError, ValueError, RuntimeError) as exc:
        return None, None, str(exc)[:800]
    return name, summary_uri, None


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


_OUTPUT_PATH_PROBE_SCHEMES = frozenset(
    {"https", "http", "abfs", "abfss", "wasb", "wasbs", "s3", "s3a", "s3n", "gs", "azure"}
)


def _validate_output_path_probe(raw: str) -> dict[str, Any]:
    """
    Format check for the UI "validate output path" action.

    - dbfs:/… is accepted and is what v1 Run uses for export paths.
    - Other common destination URIs (cloud storage, public URL) are accepted as format-only;
      execution still requires dbfs:/ until the pipeline supports external writes.
    """
    s = (raw or "").strip()
    if not s:
        return {"ok": False, "detail": "output_path is empty."}
    if "\n" in s or "\r" in s:
        return {"ok": False, "detail": "Path must not contain line breaks."}

    if s.startswith("dbfs:/"):
        try:
            normalized = _validate_dbfs_path(s)
        except HTTPException as exc:
            detail = exc.detail
            if not isinstance(detail, str):
                detail = str(detail)
            return {"ok": False, "detail": detail}
        return {
            "ok": True,
            "detail": "DBFS path format is valid. Writable access is verified at Pre-flight and Run.",
            "normalized_path": normalized,
            "path_kind": "dbfs",
            "runnable_in_v1": True,
        }

    parsed = urlparse(s)
    scheme = (parsed.scheme or "").lower()
    if not scheme:
        return {
            "ok": False,
            "detail": "Unrecognized path. Use dbfs:/… or a URI with a scheme (e.g. https://, abfs://, wasbs://).",
        }
    if scheme not in _OUTPUT_PATH_PROBE_SCHEMES:
        return {
            "ok": False,
            "detail": (
                f"Scheme {scheme!r} is not in the allowed probe list. "
                "Try dbfs:/, https://, abfs://, wasbs://, or s3://."
            ),
        }

    if scheme in {"http", "https"}:
        if not parsed.netloc:
            return {"ok": False, "detail": "URL must include a host (e.g. https://example.com/export/)."}
    elif not parsed.netloc:
        return {
            "ok": False,
            "detail": "Cloud URI must include an authority (e.g. bucket or container@account).",
        }

    return {
        "ok": True,
        "detail": (
            "URI format looks valid (format check only). "
            "v1 Run exports CSV to DBFS — use a dbfs:/ or Unity Catalog volume path to execute here."
        ),
        "normalized_path": s,
        "path_kind": scheme,
        "runnable_in_v1": False,
    }


def _to_sql_path(dbfs_path: str) -> str:
    """
    POSIX-style DBFS/Volumes path (e.g. /Volumes/...) for SQL that expects volume paths,
    not dbfs:/ URI format.
    """
    p = (dbfs_path or "").strip()
    if p.startswith("dbfs:/"):
        p = p.replace("dbfs:/", "/", 1)
    if not p.startswith("/"):
        p = "/" + p
    return p


def _sql_single_quoted_literal(value: str) -> str:
    """SQL string literal with standard single-quote doubling."""
    return "'" + (value or "").replace("'", "''") + "'"


def _export_subfolder_stamp(utc: datetime, run_id: str) -> str:
    """
    Per-run folder under each table path: _DDMMMYYY_HHMMSS (24h UTC) + short run id
    so two runs in the same second do not overwrite the same directory.
    """
    mo = _EXPORT_MONTHS_EN[utc.month - 1]
    base = f"_{utc.day:02d}{mo}{utc.year:04d}_{utc.strftime('%H%M%S')}"
    short = (run_id or "").replace("-", "")[:8]
    return f"{base}_{short}" if short else base


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
    synthetic_mode: str = Field(
        default="sample",
        description="'sample' = user-driven SQL TABLESAMPLE only; 'generative' = LLM planner for table order (execution still deterministic SQL).",
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
            "/tools/databricks-synth-data/workflow-spec",
            "/api/tools/databricks-synth-data/workflow-spec",
            "/databricks-synth-data/workflow-spec",
        ],
        "defaults": {
            "output_format": "csv",
            "allowed_output_formats": sorted(_ALLOWED_OUTPUT_FORMATS),
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
        # Lets you confirm the running process picked up the latest code (see _build_copy_sql).
        "run_export_sql": "insert_overwrite_directory_v2_options",
        "run_summary_storage": "dbfs_volume_root",
        "synthetic_modes": sorted(_ALLOWED_SYNTHETIC_MODES),
        "enterprise_auth_enforced": (os.getenv("VIKAA_SYNTH_ENFORCE_ENTERPRISE_AUTH") or "").strip().lower()
        in {"1", "true", "yes"},
        "synth_qa_source_rowcount": (os.getenv("SYNTH_QA_SOURCE_ROWCOUNT") or "").strip().lower()
        in {"1", "true", "yes"},
        "workflow_spec_path": "/tools/databricks-synth-data/workflow-spec",
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


def _uc_volume_dbfs_root(v: dict[str, Any], param_catalog: str, param_schema: str | None) -> str:
    """
    Build the dbfs: URI for the root of a Unity Catalog volume.

    Must match how Databricks exposes volumes to SQL (dbfs:/Volumes/<catalog>/<schema>/<volume>/).
    Prefer fields returned on each volume object from the 2.1 API, not only the list() query params,
    so catalog-wide enumeration does not attach the wrong catalog/schema to a volume.
    """
    name = str(v.get("name") or "").strip()
    if not name:
        return ""
    cat = str(v.get("catalog_name") or param_catalog or "").strip()
    sch = str(v.get("schema_name") or (param_schema or "") or "").strip()
    full_name = str(v.get("full_name") or "").strip()
    if full_name.count(".") >= 2:
        parts = full_name.split(".", 2)
        cat = cat or parts[0].strip()
        sch = sch or parts[1].strip()
        name = parts[2].strip() or name
    if not cat or not sch:
        return ""
    loc = str(v.get("storage_location") or "").strip()
    if loc.startswith("dbfs:/Volumes/"):
        return loc if loc.endswith("/") else loc + "/"
    if loc.startswith("/Volumes/"):
        return "dbfs:" + (loc if loc.endswith("/") else loc + "/")
    return f"dbfs:/Volumes/{cat}/{sch}/{name}/"


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
        v_cat = str(v.get("catalog_name") or catalog or "").strip()
        v_sch = str(v.get("schema_name") or schema or "").strip()
        full_name = str(v.get("full_name") or "").strip()
        if not full_name and v_cat and v_sch:
            full_name = f"{v_cat}.{v_sch}.{name}"
        out = _uc_volume_dbfs_root(v, catalog, schema)
        if not out:
            continue
        volumes.append(
            {
                "name": name,
                "catalog_name": v_cat or None,
                "schema_name": v_sch or None,
                "full_name": full_name or (f"{v_cat}.{v_sch}.{name}" if v_cat and v_sch else name),
                "volume_type": v.get("volume_type"),
                "storage_location": v.get("storage_location"),
                "output_path": out,
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


def _insert_overwrite_using_clause(output_format: str) -> tuple[str, str]:
    """Returns (USING clause fragment, trailing OPTIONS + space before SELECT).

    Spark writes standard Hadoop layout: ``part-00000-*`` files (no ``.csv`` / ``.parquet``
    suffix in the name). The *contents* match the chosen format (CSV text, NDJSON lines, or
    Parquet binary). Use boolean/string OPTIONS grammar from Spark SQL, not quoted keys.
    """
    fmt = (output_format or "csv").strip().lower()
    if fmt == "csv":
        return "USING CSV", "OPTIONS (header = true)"
    if fmt == "json":
        # Avoid gzip so each part file is plain newline-delimited JSON (easier to inspect in Catalog).
        return "USING JSON", "OPTIONS (compression = 'none')"
    if fmt == "parquet":
        return "USING PARQUET", ""
    raise HTTPException(
        status_code=400,
        detail=f"output_format must be one of {sorted(_ALLOWED_OUTPUT_FORMATS)}.",
    )


def _build_copy_sql(
    *,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    output_root: str,
    export_folder: str,
    output_format: str,
    sample_percent: int,
    sample_anchor: str,
    delta_lookback_minutes: int | None,
    max_rows_cap: int | None,
    delta_column: str | None,
    repeatable_seed: int | None = None,
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

    tsample = f"TABLESAMPLE ({int(sample_percent)} PERCENT)"
    if repeatable_seed is not None:
        tsample += f" REPEATABLE ({int(repeatable_seed) & 0x7FFFFFFF})"
    source_query = f"SELECT * FROM {source_ref} {tsample}{where_clause}{limit_clause}"

    # Land under chosen volume/DBFS root only: root/<table>/<timestamp_stamp>/ (no extra catalog/schema path).
    table_folder = f"{output_root.rstrip('/')}/{table_name}/{export_folder}/"
    sql_dir = _to_sql_path(table_folder)
    if not sql_dir.endswith("/"):
        sql_dir = sql_dir + "/"
    dir_lit = _sql_single_quoted_literal(sql_dir)
    using, opt_tail = _insert_overwrite_using_clause(output_format)
    suffix = (opt_tail.rstrip() + " ") if opt_tail else " "
    export_sql = f"INSERT OVERWRITE DIRECTORY {dir_lit} {using} {suffix}" + source_query.strip()
    return export_sql, table_folder


async def _synth_run(
    request: Request,
    body: SynthDataRunRequest,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    enforce_synth_enterprise_auth(request)
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
    synthetic_mode = (body.synthetic_mode or "sample").strip().lower()
    if synthetic_mode not in _ALLOWED_SYNTHETIC_MODES:
        raise HTTPException(
            status_code=400,
            detail=f"synthetic_mode must be one of {sorted(_ALLOWED_SYNTHETIC_MODES)}.",
        )
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
    run_started_utc = datetime.now(timezone.utc)
    export_folder = _export_subfolder_stamp(run_started_utc, run_id)
    submitted_at = run_started_utc.isoformat().replace("+00:00", "Z")
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
        "synthetic_mode": synthetic_mode,
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
                "synthetic_mode": synthetic_mode,
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
            "synthetic_mode": synthetic_mode,
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

    log_synth_event(
        "synth_run_start",
        run_id=run_id,
        synthetic_mode=synthetic_mode,
        table_count=len(safe_tables),
        export_folder=export_folder,
    )

    llm_plan: dict[str, Any] | None = None
    if synthetic_mode == "generative":
        try:
            llm_plan = llm_export_plan(
                host=host,
                token=token,
                endpoint_name=llm_model_name,
                catalog_name=catalog_name,
                schema_name=schema_name,
                tables=sorted(safe_tables),
                sample_percent=int(effective_sample_percent),
                sample_anchor=sample_anchor,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail=(
                    f"Generative planner (LLM) failed: {exc}. "
                    "Check Databricks model serving endpoint name, availability, and token scopes."
                ),
            ) from exc

    repeatable_seed = repeatable_seed_for_run(run_id, body.seed)
    ordered_tables, order_meta = order_tables_for_export(
        list(safe_tables),
        synthetic_mode=synthetic_mode,
        llm_order=(llm_plan.get("table_execution_order") if llm_plan else None),
    )

    qa_report = run_optional_source_rowcount_qa(
        host=host,
        token=token,
        warehouse_id=warehouse_id,
        catalog_name=catalog_name,
        schema_name=schema_name,
        tables=ordered_tables,
    )
    if qa_report.get("gate") == "fail":
        log_synth_event(
            "synth_run_end",
            run_id=run_id,
            synthetic_mode=synthetic_mode,
            status="qa_blocked",
            qa_gate="fail",
        )
        raise HTTPException(
            status_code=400,
            detail={"message": "Source pre-export QA failed.", "qa_report": qa_report},
        )

    execution_sql_map: dict[str, str] = {}
    executions: list[dict[str, Any]] = []
    for table_name in ordered_tables:
        full_name = f"{catalog_name}.{schema_name}.{table_name}"
        table_detail = client.get_table(full_name)
        delta_column = _choose_delta_column({"detail": table_detail})
        copy_sql, table_folder = _build_copy_sql(
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            output_root=output_path,
            export_folder=export_folder,
            output_format=output_format,
            sample_percent=effective_sample_percent,
            sample_anchor=sample_anchor,
            delta_lookback_minutes=body.delta_lookback_minutes,
            max_rows_cap=body.max_rows_cap,
            delta_column=delta_column,
            repeatable_seed=repeatable_seed,
        )
        execution_sql_map[table_name] = copy_sql
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
                "output_format": output_format,
                "statement_id": exec_result.get("statement_id"),
                "elapsed_ms": exec_result.get("elapsed_ms"),
                "state": exec_result.get("state"),
            }
        )

    manifest = build_manifest(
        run_id=run_id,
        synthetic_mode=synthetic_mode,
        resolved_request=resolved_request,
        export_folder=export_folder,
        table_order_meta=order_meta,
        llm_plan=llm_plan,
        execution_sql_map=execution_sql_map,
        repeatable_seed=repeatable_seed,
        qa_report=qa_report,
    )

    summary_payload: dict[str, Any] = {
        "run_id": run_id,
        "submitted_at_utc": submitted_at,
        "export_folder": export_folder,
        "status": "completed",
        "service": "databricks-synth-data",
        "initiated_by": initiated_by,
        "resolved_request": resolved_request,
        "executions": executions,
        "duration_ms": sum(int(e.get("elapsed_ms") or 0) for e in executions),
        "tables_succeeded": len(executions),
        "tables_failed": 0,
        "synthetic_mode": synthetic_mode,
        "repeatable_seed": repeatable_seed,
        "table_execution_order": ordered_tables,
        "manifest": manifest,
        "qa_report": qa_report,
        "qa_gate": qa_report.get("gate"),
        "llm_plan": llm_plan,
        "export_layout_note": (
            "Spark writes part-00000-* files under each table folder; filenames have no .csv/.parquet suffix. "
            "Content matches output_format (CSV text, NDJSON lines, or Parquet binary)."
        ),
    }
    summary_file, summary_uri, summary_err = _persist_run_summary_volume(
        summary_payload,
        output_root_dbfs=output_path,
        host=host,
        token=token,
    )

    result = {
        "status": "completed",
        "service": "databricks-synth-data",
        "run_id": run_id,
        "submitted_at_utc": submitted_at,
        "export_folder": export_folder,
        "summary_file": summary_file,
        "summary_uri": summary_uri,
        "request_validated": True,
        "databricks_only_enforced": True,
        "resolved_request": resolved_request,
        "schedule": schedule,
        "executions": executions,
        "synthetic_mode": synthetic_mode,
        "repeatable_seed": repeatable_seed,
        "table_execution_order": ordered_tables,
        "manifest": manifest,
        "qa_report": qa_report,
        "qa_gate": qa_report.get("gate"),
        "llm_plan": llm_plan,
        "workflow_spec_hint": "GET /tools/databricks-synth-data/workflow-spec",
    }
    if summary_err:
        result["summary_write_error"] = summary_err
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
                "tables": ordered_tables,
            },
            "sampling": {
                "anchor": sample_anchor,
                "sample_percent_requested": requested_sample_percent,
                "sample_percent_effective": effective_sample_percent,
                "delta_lookback_minutes": body.delta_lookback_minutes,
            },
            "model": llm_model_name,
            "synthetic_mode": synthetic_mode,
            "repeatable_seed": repeatable_seed,
            "qa_gate": qa_report.get("gate"),
            "output_path": output_path,
            "rows_in_source_estimate": None,
            "rows_out_synthetic": None,
            "duration_ms": summary_payload["duration_ms"],
            "tables_succeeded": len(executions),
            "tables_failed": 0,
            "summary_file": summary_file,
            "summary_uri": summary_uri,
            "summary_write_error": summary_err,
        }
    )
    log_synth_event(
        "synth_run_end",
        run_id=run_id,
        synthetic_mode=synthetic_mode,
        status="completed",
        tables=len(executions),
        duration_ms=summary_payload["duration_ms"],
        qa_gate=qa_report.get("gate"),
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
    enforce_synth_enterprise_auth(request)
    synthetic_mode = (body.synthetic_mode or "sample").strip().lower()
    if synthetic_mode not in _ALLOWED_SYNTHETIC_MODES:
        raise HTTPException(
            status_code=400,
            detail=f"synthetic_mode must be one of {sorted(_ALLOWED_SYNTHETIC_MODES)}.",
        )
    catalog_name = _safe_ident(body.catalog_name, "catalog_name")
    schema_name = _safe_ident(body.schema_name, "schema_name")
    output_fmt = (body.output_format or "csv").strip().lower()
    if output_fmt not in _ALLOWED_OUTPUT_FORMATS:
        raise HTTPException(
            status_code=400,
            detail=f"output_format must be one of {sorted(_ALLOWED_OUTPUT_FORMATS)}.",
        )
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
    checks.append({
        "id": "quota",
        "label": "Storage quota headroom",
        "status": "pass",
        "hint": "Not checked in v1 (no quota API wired). Monitor volume usage in Databricks if needed.",
    })
    conflict = _check_schedule_conflict(body.interval_minutes if body.frequency_mode == "interval" else None)
    checks.append({
        "id": "schedule_conflict",
        "label": "No conflicting schedule (±10 min)",
        "status": "warn" if conflict else "pass",
        "hint": "Potential overlapping interval schedule detected." if conflict else "No conflict detected.",
    })
    if synthetic_mode == "generative":
        checks.append({
            "id": "synthetic_mode",
            "label": "Synthetic mode (generative)",
            "status": "pass",
            "hint": "At Run, a Databricks serving endpoint (llm_model_name) plans FK-aware table order; exports remain deterministic SQL.",
        })
    else:
        checks.append({
            "id": "synthetic_mode",
            "label": "Synthetic mode (sample)",
            "status": "pass",
            "hint": "Alphabetical table order; TABLESAMPLE REPEATABLE uses body.seed or a hash of run_id for coherence across tables.",
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


def _load_summary_from_audit_record(rec: dict[str, Any]) -> dict[str, Any]:
    """Load JSON for a completed run from volume (dbfs) or optional local mirror."""
    uri = str(rec.get("summary_uri") or "").strip()
    write_err = str(rec.get("summary_write_error") or "").strip()
    host = _resolve_host()
    token = _resolve_token()
    if uri:
        if not host or not token:
            raise HTTPException(
                status_code=503,
                detail="Databricks host/token are required to read the summary from the volume.",
            )
        try:
            return read_json_from_dbfs_uri(host, token, uri)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=502, detail="Summary file on volume is not valid JSON.") from exc
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Could not read summary from volume: {exc}",
            ) from exc

    name = str(rec.get("summary_file") or "").strip()
    if not name or not _SAFE_SUMMARY_FILENAME.match(name):
        if write_err:
            raise HTTPException(
                status_code=404,
                detail=f"Summary was not written to the volume: {write_err}",
            )
        raise HTTPException(status_code=404, detail="No stored summary for this run.")
    base = _optional_local_summary_dir()
    if not base:
        detail = (
            f"Summary was not written to the volume: {write_err}"
            if write_err
            else "No volume summary URI for this run. Set DATABRICKS_SYNTH_SUMMARY_DIR only for legacy local files."
        )
        raise HTTPException(status_code=404, detail=detail)
    path = (base / name).resolve()
    try:
        path.relative_to(base.resolve())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid summary path.") from exc
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Summary file not found.")
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail="Could not read summary file.") from exc
    if not isinstance(data, dict):
        raise HTTPException(status_code=500, detail="Summary file is invalid.")
    return data


async def _synth_run_summary(
    request: Request,
    run_id: str = "",
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    rid = (run_id or "").strip()
    if not rid:
        raise HTTPException(status_code=400, detail="run_id is required.")
    rec = _AUDIT_RUNS.get(rid)
    if not rec:
        raise HTTPException(status_code=404, detail="run_id not found in audit registry.")
    return _load_summary_from_audit_record(rec)


async def _synth_workflow_spec(
    request: Request,
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    """Return a portable workflow/orchestration template (Jobs, Airflow, etc.)."""
    enforce_synth_enterprise_auth(request)
    return workflow_spec_template()


async def _synth_validate_output_path(
    output_path: str = "",
    _acl: dict = Depends(require_whitelisted_user),
) -> dict[str, Any]:
    """Format check for output destinations: dbfs (runnable in v1) or other URI (probe only)."""
    return _validate_output_path_probe(output_path or "")


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
        "/databricks-synth-data/workflow-spec",
        _synth_workflow_spec,
        methods=["GET"],
    )
    _r.add_api_route(
        "/databricks-synth-data/validate-output-path",
        _synth_validate_output_path,
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
    _r.add_api_route(
        "/databricks-synth-data/run-summary",
        _synth_run_summary,
        methods=["GET"],
    )

