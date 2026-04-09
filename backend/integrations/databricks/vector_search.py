"""
Databricks Vector Search REST API client.

Covers:
  - Ensuring a VS endpoint exists (creates one if absent)
  - Creating a Delta-Sync index (embedding model endpoint computes vectors)
  - Triggering a manual sync
  - Polling index status

All calls are synchronous HTTP via requests.
Env vars consumed:  DATABRICKS_HOST  DATABRICKS_TOKEN
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from backend.integrations.databricks.read_unity_catalog import _normalize_host

logger = logging.getLogger(__name__)

_VS_BASE = "/api/2.0/vector-search"


def _headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token.strip()}",
        "Content-Type": "application/json",
    }


# ── Endpoint management ───────────────────────────────────────────────────────

def get_vs_endpoint(host: str, token: str, endpoint_name: str) -> dict[str, Any] | None:
    """Return endpoint dict or None if not found."""
    base = _normalize_host(host).rstrip("/")
    url = f"{base}{_VS_BASE}/endpoints/{endpoint_name}"
    r = requests.get(url, headers=_headers(token), timeout=30)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def create_vs_endpoint(host: str, token: str, endpoint_name: str) -> dict[str, Any]:
    """Create a VS endpoint. Returns endpoint dict."""
    base = _normalize_host(host).rstrip("/")
    url = f"{base}{_VS_BASE}/endpoints"
    body = {"name": endpoint_name, "endpoint_type": "STANDARD"}
    r = requests.post(url, headers=_headers(token), json=body, timeout=60)
    r.raise_for_status()
    return r.json()


def ensure_vs_endpoint(host: str, token: str, endpoint_name: str) -> dict[str, Any]:
    """Get or create the named VS endpoint."""
    existing = get_vs_endpoint(host, token, endpoint_name)
    if existing:
        logger.info("VS endpoint '%s' already exists (state=%s)",
                    endpoint_name, existing.get("endpoint_status", {}).get("state"))
        return existing
    logger.info("Creating VS endpoint '%s'", endpoint_name)
    return create_vs_endpoint(host, token, endpoint_name)


# ── Index management ──────────────────────────────────────────────────────────

def get_index(host: str, token: str, index_full_name: str) -> dict[str, Any] | None:
    """Return index dict (by full name catalog.schema.index) or None."""
    base = _normalize_host(host).rstrip("/")
    url = f"{base}{_VS_BASE}/indexes/{index_full_name}"
    r = requests.get(url, headers=_headers(token), timeout=30)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def create_delta_sync_index(
    host: str,
    token: str,
    *,
    endpoint_name: str,
    index_full_name: str,
    source_table_full_name: str,
    primary_key: str,
    content_column: str,
    embedding_endpoint: str,
    pipeline_type: str = "TRIGGERED",
) -> dict[str, Any]:
    """
    Create a Delta-Sync index that auto-computes embeddings from ``content_column``
    using the Databricks embedding serving endpoint.

    index_full_name:        catalog.schema.index_name
    source_table_full_name: catalog.schema.table_name
    """
    base = _normalize_host(host).rstrip("/")
    url = f"{base}{_VS_BASE}/indexes"
    body = {
        "name": index_full_name,
        "endpoint_name": endpoint_name,
        "primary_key": primary_key,
        "index_type": "DELTA_SYNC",
        "delta_sync_index_spec": {
            "source_table": source_table_full_name,
            "pipeline_type": pipeline_type,
            "embedding_source_columns": [
                {
                    "name": content_column,
                    "embedding_model_endpoint_name": embedding_endpoint,
                }
            ],
        },
    }
    r = requests.post(url, headers=_headers(token), json=body, timeout=60)
    r.raise_for_status()
    return r.json()


def ensure_delta_sync_index(
    host: str,
    token: str,
    *,
    endpoint_name: str,
    index_full_name: str,
    source_table_full_name: str,
    primary_key: str,
    content_column: str,
    embedding_endpoint: str,
    pipeline_type: str = "TRIGGERED",
) -> dict[str, Any]:
    """Get or create the VS index."""
    existing = get_index(host, token, index_full_name)
    if existing:
        logger.info("VS index '%s' already exists", index_full_name)
        return existing
    logger.info("Creating VS index '%s'", index_full_name)
    return create_delta_sync_index(
        host, token,
        endpoint_name=endpoint_name,
        index_full_name=index_full_name,
        source_table_full_name=source_table_full_name,
        primary_key=primary_key,
        content_column=content_column,
        embedding_endpoint=embedding_endpoint,
        pipeline_type=pipeline_type,
    )


def sync_index(host: str, token: str, index_full_name: str) -> dict[str, Any]:
    """
    Trigger a sync on a TRIGGERED pipeline index.

    Raises SyncNotReady (a subclass of RuntimeError) when the index is still
    initializing (HTTP 400) so callers can surface a friendlier message.
    """
    base = _normalize_host(host).rstrip("/")
    url = f"{base}{_VS_BASE}/indexes/{index_full_name}/sync"
    r = requests.post(url, headers=_headers(token), timeout=30)
    if r.status_code == 400:
        # Index is freshly created and still PROVISIONING — sync not yet possible.
        try:
            detail = r.json().get("message") or r.text[:300]
        except Exception:
            detail = r.text[:300]
        raise SyncNotReady(
            f"Index '{index_full_name}' is still initializing and cannot be synced yet. "
            f"It will sync automatically once ONLINE. Detail: {detail}"
        )
    r.raise_for_status()
    return r.json()


class SyncNotReady(RuntimeError):
    """Raised when VS sync is attempted on a not-yet-ready index."""


def get_index_status(host: str, token: str, index_full_name: str) -> dict[str, Any]:
    """
    Return a normalised status dict:
      ready (bool), state, message, last_sync_time, row_count
    """
    info = get_index(host, token, index_full_name)
    if not info:
        return {"ready": False, "state": "NOT_FOUND", "message": "Index does not exist"}

    status = info.get("status", {})
    state = status.get("detailed_state") or status.get("state") or "UNKNOWN"
    return {
        "ready": state in ("ONLINE", "ONLINE_NO_PENDING_UPDATE"),
        "state": state,
        "message": status.get("message") or "",
        "last_sync_time": status.get("indexed_row_count_last_sync"),
        "row_count": status.get("indexed_row_count"),
        "raw": info,
    }


def wait_for_index_ready(
    host: str,
    token: str,
    index_full_name: str,
    *,
    timeout_s: float = 300.0,
    poll_interval_s: float = 8.0,
) -> dict[str, Any]:
    """Poll until the index is ONLINE or timeout. Returns final status dict."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        status = get_index_status(host, token, index_full_name)
        logger.info("VS index '%s' state=%s", index_full_name, status["state"])
        if status["ready"]:
            return status
        if status["state"] in ("FAILED", "OFFLINE"):
            return status
        time.sleep(poll_interval_s)
    return get_index_status(host, token, index_full_name)
