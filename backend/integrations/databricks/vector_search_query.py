"""
Databricks Vector Search Query API.

POST /api/2.0/vector-search/indexes/{index_name}/query
Supports HYBRID (BM25 + dense) and SIMILARITY (dense-only) query types.
"""

from __future__ import annotations

import logging
from typing import Any

import requests

from backend.integrations.databricks.read_unity_catalog import _normalize_host

logger = logging.getLogger(__name__)

# Default columns that exist in our Delta table DDL.
# Databricks VS requires 'columns' to always be present in the request body.
_DEFAULT_COLUMNS = ["chunk_id", "source", "source_type", "content", "page",
                    "section", "author", "ingested_at", "doc_hash"]


def _headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token.strip()}",
        "Content-Type": "application/json",
    }


def query_index(
    host: str,
    token: str,
    index_full_name: str,
    query_text: str,
    *,
    num_results: int = 10,
    query_type: str = "HYBRID",          # "HYBRID" | "SIMILARITY"
    columns: list[str] | None = None,    # columns to return; None = all
    filters: dict[str, Any] | None = None,
    timeout_s: int = 60,
) -> dict[str, Any]:
    """
    Query a VS index and return a normalised result:
      {
        ok: bool,
        results: [ { chunk_id, source, page, content, score, ... }, ... ],
        count: int,
        raw: <raw API response>
      }
    """
    base = _normalize_host(host).rstrip("/")
    url = f"{base}/api/2.0/vector-search/indexes/{index_full_name}/query"

    body: dict[str, Any] = {
        "query_text": query_text,
        "num_results": num_results,
        "query_type": query_type.upper(),
        "columns": columns if columns else _DEFAULT_COLUMNS,
    }
    if filters:
        body["filters_json"] = filters

    try:
        r = requests.post(url, headers=_headers(token), json=body, timeout=timeout_s)
    except requests.exceptions.Timeout:
        return {
            "ok": False, "results": [], "count": 0, "raw": {},
            "error": (
                f"VS query timed out after {timeout_s}s. "
                "The index may still be PROVISIONING — check Index Status on Tab 1 and retry once it shows ONLINE."
            ),
        }
    except requests.exceptions.ConnectionError as exc:
        return {
            "ok": False, "results": [], "count": 0, "raw": {},
            "error": f"Connection error reaching Databricks: {exc}",
        }
    if not r.ok:
        try:
            detail = r.json().get("message") or r.text[:400]
        except Exception:
            detail = r.text[:400]
        logger.error("VS query HTTP %s: %s", r.status_code, detail)
        return {"ok": False, "results": [], "count": 0, "error": detail, "raw": {}}

    raw = r.json()
    results = _normalise_results(raw)
    return {"ok": True, "results": results, "count": len(results), "raw": raw}


def _normalise_results(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Databricks VS actual response shape:
      {
        "manifest": { "columns": [{"name": "chunk_id"}, ...] },   ← TOP LEVEL
        "result":   { "row_count": N, "data_array": [[...], ...] } ← TOP LEVEL
      }

    Fallback shape (older SDK / direct-access indexes):
      { "results": [ { "fields": {...}, "score": float }, ... ] }
    """
    # Primary shape — manifest at top level, data_array inside result
    manifest   = raw.get("manifest", {})
    data_array = raw.get("result", {}).get("data_array") or []
    if manifest.get("columns") and data_array is not None:
        col_names = [c.get("name", f"col_{i}") for i, c in enumerate(manifest["columns"])]
        out = []
        for row in data_array:
            d = dict(zip(col_names, row))
            # score is included as the last column by Databricks
            d.setdefault("score", d.pop("score", None))
            out.append(d)
        return out

    # Fallback shape
    return [
        {**item.get("fields", {}), "score": item.get("score")}
        for item in raw.get("results", [])
    ]
