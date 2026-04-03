"""
Map Databricks SQL Statement API errors to operator-friendly messages.
"""

from __future__ import annotations

import re


def classify_warehouse_error(
    raw: str | None,
    *,
    http_status: int | None = None,
) -> dict[str, str]:
    """
    Return ``category`` (machine) and ``message`` (human) for UI and logs.
    """
    text = (raw or "").strip()
    low = text.lower()

    if http_status == 401 or "401" in text or "unauthorized" in low or "invalid access token" in low:
        return {
            "category": "auth",
            "message": "Authentication failed — check that the Databricks PAT is valid and not expired.",
        }
    if http_status == 403 or "forbidden" in low or "permission" in low or "insufficient" in low:
        return {
            "category": "permission",
            "message": "Permission denied — the token or user cannot run this query on the warehouse or tables.",
        }
    if http_status == 404 and "warehouse" in low:
        return {
            "category": "warehouse_not_found",
            "message": "SQL warehouse ID not found — confirm DATABRICKS_SQL_WAREHOUSE_ID (full UUID) matches an existing warehouse.",
        }
    if "warehouse" in low and ("stopped" in low or "not running" in low or "starting" in low):
        return {
            "category": "warehouse_state",
            "message": "The SQL warehouse is stopped or not ready — start it in Databricks SQL Warehouses, then retry.",
        }
    if "invalid" in low and ("warehouse" in low or "statement" in low):
        return {
            "category": "invalid_request",
            "message": "Invalid warehouse or SQL request — check warehouse UUID and SQL syntax.",
        }
    if "canceled" in low or "cancelled" in low:
        return {
            "category": "canceled",
            "message": "Query was canceled (timeout or warehouse policy). Try a smaller date range or add filters.",
        }
    if "timeout" in low or "timed out" in low:
        return {
            "category": "timeout",
            "message": "Query timed out — narrow filters, add LIMIT, or raise DATABRICKS_QUERY_STATEMENT_TIMEOUT_S on the server.",
        }
    if "cannot reach" in low or "connection" in low or "name or service not known" in low:
        return {
            "category": "network",
            "message": "Could not reach Databricks — check DATABRICKS_HOST and network/VPN.",
        }
    if "table or view not found" in low or "table_not_found" in low:
        return {
            "category": "object_not_found",
            "message": "Table or view not found — the name may be wrong or you lack access to that schema.",
        }

    snippet = re.sub(r"\s+", " ", text)[:400]
    return {
        "category": "unknown",
        "message": snippet or "The SQL warehouse returned an error. See server logs for the full response.",
    }
