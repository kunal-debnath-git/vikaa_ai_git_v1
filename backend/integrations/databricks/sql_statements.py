"""
Databricks SQL Statement Execution API — run SQL on a warehouse (read-only use intended).

Uses POST /api/2.0/sql/statements and polls GET until terminal state.
"""

from __future__ import annotations

import time
from typing import Any

import requests

from backend.integrations.databricks.read_unity_catalog import _normalize_host


def execute_sql_statement(
    host: str,
    token: str,
    warehouse_id: str,
    statement: str,
    *,
    wait_timeout_s: int = 50,
    poll_interval_s: float = 0.5,
    max_poll_s: float = 120.0,
) -> dict[str, Any]:
    """
    Submit SQL and return a normalized result dict:

    - ``ok`` (bool)
    - ``columns`` list of column names (may be empty on failure)
    - ``rows`` list of row lists (JSON-serializable scalars)
    - ``statement_id``, ``state``
    - ``error`` optional message
    - ``truncated`` if manifest says so
    """
    t0 = time.monotonic()
    base = _normalize_host(host).rstrip("/")
    headers = {
        "Authorization": f"Bearer {token.strip()}",
        "Content-Type": "application/json",
    }
    body = {
        "warehouse_id": warehouse_id.strip(),
        "statement": statement,
        "wait_timeout": f"{max(5, min(wait_timeout_s, 300))}s",
        "on_wait_timeout": "CONTINUE",
    }
    url_submit = f"{base}/api/2.0/sql/statements"
    r = requests.post(url_submit, headers=headers, json=body, timeout=60)
    if not r.ok:
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        return {
            "ok": False,
            "columns": [],
            "rows": [],
            "statement_id": None,
            "state": "FAILED",
            "error": f"{r.status_code}: {r.text[:800]}",
            "http_status": r.status_code,
            "truncated": False,
            "elapsed_ms": elapsed_ms,
        }

    payload = r.json()
    return _finalize_or_poll(
        base, headers, payload, poll_interval_s, max_poll_s, t0=t0
    )


def _finalize_or_poll(
    base: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    poll_interval_s: float,
    max_poll_s: float,
    *,
    t0: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + max_poll_s
    statement_id = payload.get("statement_id")

    def _elapsed() -> int:
        return int((time.monotonic() - t0) * 1000)

    while True:
        status = (payload.get("status") or {}) if isinstance(payload, dict) else {}
        state = str(status.get("state") or "").upper()

        if state in {"SUCCEEDED", "FAILED", "CANCELED"}:
            out = _normalize_success_or_error(payload, state)
            out["elapsed_ms"] = _elapsed()
            return out

        if time.monotonic() > deadline:
            return {
                "ok": False,
                "columns": [],
                "rows": [],
                "statement_id": statement_id,
                "state": state or "TIMEOUT",
                "error": "Statement still running — timeout waiting for SQL warehouse.",
                "truncated": False,
                "elapsed_ms": _elapsed(),
            }

        if not statement_id:
            return {
                "ok": False,
                "columns": [],
                "rows": [],
                "statement_id": None,
                "state": "FAILED",
                "error": "Missing statement_id in warehouse response.",
                "truncated": False,
                "elapsed_ms": _elapsed(),
            }

        time.sleep(poll_interval_s)
        gr = requests.get(
            f"{base}/api/2.0/sql/statements/{statement_id}",
            headers=headers,
            timeout=60,
        )
        if not gr.ok:
            return {
                "ok": False,
                "columns": [],
                "rows": [],
                "statement_id": statement_id,
                "state": "FAILED",
                "error": f"Poll failed {gr.status_code}: {gr.text[:500]}",
                "http_status": gr.status_code,
                "truncated": False,
                "elapsed_ms": _elapsed(),
            }
        payload = gr.json()


def _normalize_success_or_error(
    payload: dict[str, Any],
    state: str,
) -> dict[str, Any]:
    if state != "SUCCEEDED":
        status = payload.get("status") or {}
        err = status.get("error") if isinstance(status, dict) else None
        if isinstance(err, dict):
            msg = str(err.get("message") or err.get("error_code") or err)[:2000]
        else:
            msg = str(err or payload.get("error") or state or "Statement failed")
        return {
            "ok": False,
            "columns": [],
            "rows": [],
            "statement_id": payload.get("statement_id"),
            "state": state,
            "error": msg[:2000],
            "truncated": bool((payload.get("manifest") or {}).get("truncated")),
            "elapsed_ms": 0,
        }

    manifest = payload.get("manifest") or {}
    schema = manifest.get("schema") or {}
    col_objs = schema.get("columns") or []
    columns = []
    for c in col_objs:
        if isinstance(c, dict) and c.get("name"):
            columns.append(str(c["name"]))

    result = payload.get("result") or {}
    data_array = result.get("data_array")
    rows: list[list[Any]] = []
    if isinstance(data_array, list):
        for row in data_array:
            if isinstance(row, list):
                rows.append(row)
            else:
                rows.append([row])

    truncated = bool(manifest.get("truncated"))

    return {
        "ok": True,
        "columns": columns,
        "rows": rows,
        "statement_id": payload.get("statement_id"),
        "state": "SUCCEEDED",
        "error": None,
        "truncated": truncated,
        "elapsed_ms": 0,
    }
