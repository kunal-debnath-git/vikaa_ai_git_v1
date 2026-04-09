"""
Enterprise synthetic-data helpers: LLM export planner, table ordering, manifest/lineage, QA, observability.

Policy / UC column PII enforcement is intentionally out of scope (product decision).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import zlib
from collections import Counter
from typing import Any

from backend.integrations.databricks.model_serving import extract_json_object, invoke_serving_chat
from backend.integrations.databricks.sql_statements import execute_sql_statement

logger = logging.getLogger(__name__)


def sql_fingerprint(sql: str) -> str:
    h = hashlib.sha256((sql or "").encode("utf-8")).hexdigest()
    return f"sha256:{h[:16]}"


def repeatable_seed_for_run(run_id: str, explicit: int | None) -> int:
    if explicit is not None:
        return int(explicit) & 0x7FFFFFFF
    return zlib.crc32(run_id.encode("utf-8")) & 0x7FFFFFFF


def order_tables_for_export(
    safe_tables: list[str],
    *,
    synthetic_mode: str,
    llm_order: list[str] | None,
) -> tuple[list[str], dict[str, Any]]:
    """Return execution order and metadata."""
    meta: dict[str, Any] = {"strategy": "alphabetical", "synthetic_mode": synthetic_mode}
    if synthetic_mode == "generative" and isinstance(llm_order, list) and llm_order:
        seen: set[str] = set()
        out: list[str] = []
        for t in llm_order:
            if t in safe_tables and t not in seen:
                out.append(t)
                seen.add(t)
        for t in sorted(safe_tables):
            if t not in seen:
                out.append(t)
        meta["strategy"] = "llm_plan_with_fallback"
        meta["llm_order_input"] = list(llm_order)
        return out, meta
    return sorted(safe_tables), meta


PLANNER_SYSTEM = """You are an enterprise data export planner. You NEVER invent table names.
Output a single JSON object only (no markdown), with this exact shape:
{
  "table_execution_order": ["<names>"],
  "rationale": "<short why this order helps FK-style coherence — parent-like tables before children when inferable>",
  "coherence": "deterministic_sql_same_seed",
  "risk_notes": ["<optional strings>"]
}
Rules:
- Every name in table_execution_order MUST appear exactly once and MUST be from the provided tables list (same spelling).
- Order parents before children when you can infer from common warehouse naming (e.g. orders before order_items).
- You do not execute SQL; a deterministic engine will run TABLESAMPLE with a shared seed."""


def llm_export_plan(
    *,
    host: str,
    token: str,
    endpoint_name: str,
    catalog_name: str,
    schema_name: str,
    tables: list[str],
    sample_percent: int,
    sample_anchor: str,
) -> dict[str, Any]:
    payload = {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "tables": tables,
        "sample_percent": sample_percent,
        "sample_anchor": sample_anchor,
    }
    raw = invoke_serving_chat(
        host,
        token,
        endpoint_name,
        system_prompt=PLANNER_SYSTEM,
        user_prompt=json.dumps(payload, indent=2),
        max_tokens=2048,
        temperature=0.15,
    )
    plan = extract_json_object(raw)
    order = plan.get("table_execution_order")
    if not isinstance(order, list):
        raise ValueError("LLM plan missing table_execution_order array")
    names = [str(x).strip() for x in order if str(x).strip()]
    if Counter(names) != Counter(tables):
        raise ValueError(
            "LLM plan table_execution_order must list each input table exactly once "
            f"(same multiset as input); plan={names!r} input={tables!r}"
        )
    plan["_raw_model_text_excerpt"] = raw[:2000]
    return plan


def run_optional_source_rowcount_qa(
    *,
    host: str,
    token: str,
    warehouse_id: str,
    catalog_name: str,
    schema_name: str,
    tables: list[str],
) -> dict[str, Any]:
    """Optional heavy check: COUNT(*) per source table (set SYNTH_QA_SOURCE_ROWCOUNT=1)."""
    enabled = (os.getenv("SYNTH_QA_SOURCE_ROWCOUNT") or "").strip().lower() in {"1", "true", "yes"}
    checks: list[dict[str, Any]] = []
    if not enabled:
        checks.append(
            {
                "id": "qa_source_rowcount",
                "status": "pass",
                "hint": "Skipped. Set SYNTH_QA_SOURCE_ROWCOUNT=1 for source COUNT(*) per table (can scan large tables).",
            }
        )
        return {"gate": "pass", "checks": checks}

    qc = _quote_compound(catalog_name, schema_name)
    gate = "pass"
    for t in tables:
        qt = _quote_compound(catalog_name, schema_name, t)
        sql = f"SELECT COUNT(*) AS c FROM {qt}"
        res = execute_sql_statement(
            host,
            token,
            warehouse_id,
            sql,
            wait_timeout_s=50,
            max_poll_s=120,
        )
        if not res.get("ok"):
            checks.append(
                {
                    "id": f"qa_count_{t}",
                    "status": "fail",
                    "hint": (res.get("error") or "unknown")[:500],
                }
            )
            gate = "fail"
            continue
        rows = res.get("rows") or []
        n = int(rows[0][0]) if rows and rows[0] else 0
        st = "warn" if n == 0 else "pass"
        if n == 0:
            gate = "warn" if gate == "pass" else gate
        checks.append({"id": f"qa_count_{t}", "status": st, "hint": f"source_rows={n}"})
    return {"gate": gate, "checks": checks}


def _quote_compound(*parts: str) -> str:
    return ".".join(f"`{p}`" for p in parts)


def build_manifest(
    *,
    run_id: str,
    synthetic_mode: str,
    resolved_request: dict[str, Any],
    export_folder: str,
    table_order_meta: dict[str, Any],
    llm_plan: dict[str, Any] | None,
    execution_sql_map: dict[str, str],
    repeatable_seed: int,
    qa_report: dict[str, Any],
) -> dict[str, Any]:
    fingerprints = {tbl: sql_fingerprint(sql) for tbl, sql in execution_sql_map.items()}
    return {
        "schema_version": "synth_manifest_v1",
        "run_id": run_id,
        "synthetic_mode": synthetic_mode,
        "export_folder": export_folder,
        "repeatable_seed": repeatable_seed,
        "resolved_request_snapshot": resolved_request,
        "table_order": table_order_meta,
        "llm_plan": llm_plan,
        "export_sql_fingerprints": fingerprints,
        "qa_report": qa_report,
        "lineage": {
            "engine": "databricks_sql_warehouse",
            "execution": "INSERT_OVERWRITE_DIRECTORY_TABLESAMPLE",
            "deterministic": True,
            "notes": "Generative mode uses LLM for plan/order only; row materialization is warehouse SQL.",
        },
    }


def workflow_spec_template() -> dict[str, Any]:
    return {
        "schema_version": "synth_workflow_spec_v1",
        "description": "Wire these steps into Databricks Workflows or your orchestrator (Airflow, Azure DF, etc.).",
        "recommended_tasks": [
            {
                "task_key": "warehouse_running",
                "type": "note",
                "detail": "Ensure SQL warehouse RUNNING (use existing wake/status APIs or UI).",
            },
            {
                "task_key": "synth_preflight",
                "type": "http_post",
                "path": "/tools/databricks-synth-data/preflight",
                "headers": {"Content-Type": "application/json", "Authorization": "Bearer <SUPABASE_JWT>"},
                "body_template_ref": "same JSON as Tool UI payloadFromUi() / SynthDataPreflightRequest",
            },
            {
                "task_key": "synth_run",
                "type": "http_post",
                "path": "/tools/databricks-synth-data/run",
                "headers": {"Content-Type": "application/json", "Authorization": "Bearer <SUPABASE_JWT>"},
                "body_template_ref": "same JSON as SynthDataRunRequest including synthetic_mode",
            },
            {
                "task_key": "fetch_summary",
                "type": "http_get",
                "path": "/tools/databricks-synth-data/run-summary?run_id=<uuid_from_run_response>",
                "headers": {"Authorization": "Bearer <SUPABASE_JWT>"},
            },
        ],
        "observability": {
            "server_logs": "Search for synth_run_start / synth_run_end in API logs.",
            "response_fields": ["run_id", "qa_report", "manifest", "executions[].elapsed_ms"],
        },
        "cost_controls": {
            "hint": "Cap tables per run, sample_percent, max_rows_cap; enable SYNTH_QA_SOURCE_ROWCOUNT only when needed.",
        },
    }


def log_synth_event(event: str, **kwargs: Any) -> None:
    try:
        logger.info("%s %s", event, json.dumps(kwargs, default=str)[:4000])
    except Exception:
        logger.info("%s (payload not serializable)", event)
