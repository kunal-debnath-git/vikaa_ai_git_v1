"""
List Unity Catalog metadata (catalogs, schemas, tables) via the Databricks REST API.

MCP note
--------
This repository does not include a working Databricks MCP server. The only MCP entry
in Cursor for this workspace (``user-lf-starter_project``) is unrelated; the product
doc ``docs/tools/Databricks_Intelligence_Agent.md`` expects a future MCP that calls
Databricks APIs. This module is the reusable read-only layer those tools can wrap.

Environment (after loading ``.env`` via python-dotenv if present)
------------------------------------------------------------------
DATABRICKS_HOST
    Workspace URL (no trailing slash). Also supported: ``DATABRICKS_HOST_STORY``.

DATABRICKS_TOKEN
    Workspace personal access token (recommended). Also supported:
    ``DATABRICKS_TOKEN_STORY``, ``DATABRICKS_TOKEN_ANJALI`` (first non-empty wins after
    ``DATABRICKS_TOKEN``).

Required token scope
--------------------
At minimum: ``read`` on Unity Catalog (or a role that can list catalogs / metadata).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Iterator

import requests

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


UC_BASE = "/api/2.1/unity-catalog"


def _strip_env(val: str) -> str:
    return val.strip().strip('"').strip("'")


def _resolve_host() -> str:
    for key in ("DATABRICKS_HOST", "DATABRICKS_HOST_STORY"):
        h = (os.getenv(key) or "").strip()
        if h:
            return _strip_env(h)
    return ""


def _resolve_token() -> str:
    for key in (
        "DATABRICKS_TOKEN",
        "DATABRICKS_TOKEN_STORY",
        "DATABRICKS_TOKEN_ANJALI",
    ):
        v = (os.getenv(key) or "").strip()
        if v:
            return _strip_env(v)
    return ""


def _normalize_host(host: str) -> str:
    h = host.strip().rstrip("/")
    if not h.startswith("http://") and not h.startswith("https://"):
        h = "https://" + h
    return h


@dataclass
class UnityCatalogClient:
    """Thin REST client for Unity Catalog list/describe endpoints."""

    host: str
    token: str
    timeout_s: float = 60.0

    def __post_init__(self) -> None:
        self.host = _normalize_host(self.host)

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def _get_pages(self, path: str, params: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
        """Yield paginated list payloads (each response dict from the API)."""
        page_token: str | None = None
        base_params = dict(params) if params else {}
        while True:
            qp = dict(base_params)
            if page_token:
                qp["page_token"] = page_token
            url = f"{self.host}{path}"
            r = requests.get(
                url,
                headers=self._headers(),
                params=qp,
                timeout=self.timeout_s,
            )
            if not r.ok:
                raise RuntimeError(
                    f"GET {path} failed: {r.status_code} {r.text[:500]}"
                )
            data = r.json()
            yield data
            page_token = data.get("next_page_token")
            if not page_token:
                break

    def list_catalogs(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for page in self._get_pages(f"{UC_BASE}/catalogs"):
            out.extend(page.get("catalogs") or [])
        return out

    def list_schemas(self, catalog_name: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for page in self._get_pages(
            f"{UC_BASE}/schemas",
            params={"catalog_name": catalog_name},
        ):
            out.extend(page.get("schemas") or [])
        return out

    def list_tables(self, catalog_name: str, schema_name: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for page in self._get_pages(
            f"{UC_BASE}/tables",
            params={"catalog_name": catalog_name, "schema_name": schema_name},
        ):
            out.extend(page.get("tables") or [])
        return out

    def get_table(self, full_name: str) -> dict[str, Any]:
        """``full_name`` is ``catalog.schema.table`` (URL-encoded by requests)."""
        url = f"{self.host}{UC_BASE}/tables/{full_name}"
        r = requests.get(url, headers=self._headers(), timeout=self.timeout_s)
        if not r.ok:
            raise RuntimeError(f"GET table {full_name}: {r.status_code} {r.text[:500]}")
        return r.json()


def build_tree(
    client: UnityCatalogClient,
    *,
    catalog_filter: str | None = None,
    schema_filter: str | None = None,
    schema_names: list[str] | None = None,
    schema_name_contains_any: list[str] | None = None,
    max_tables_per_schema: int | None = None,
    include_columns: bool = False,
) -> dict[str, Any]:
    """Structured catalog → schemas → tables (optional column detail per table).

    Schema selection:

    - If ``schema_names`` is set: only those exact schema names (``schema_filter`` ignored).
    - Else if ``schema_filter`` is set: single exact schema name.
    - Else if ``schema_name_contains_any`` is set: schema name must contain one of
      the substrings (case-insensitive), e.g. ``silver`` / ``gold``.
    - Else: all schemas in the catalog(s).
    """
    allowed_exact: set[str] | None = None
    if schema_names:
        allowed_exact = {str(x).strip() for x in schema_names if str(x).strip()}
    needles: list[str] = []
    if schema_name_contains_any:
        needles = [
            str(x).strip().lower()
            for x in schema_name_contains_any
            if str(x).strip()
        ]

    catalogs_raw = client.list_catalogs()
    if catalog_filter:
        catalogs_raw = [
            c for c in catalogs_raw if c.get("name") == catalog_filter
        ]
    tree: dict[str, Any] = {"catalogs": []}
    for cat in catalogs_raw:
        cname = cat.get("name")
        if not cname:
            continue
        entry: dict[str, Any] = {"catalog": cat, "schemas": []}
        for sch in client.list_schemas(cname):
            sname = sch.get("name")
            if not sname:
                continue
            if allowed_exact is not None:
                if sname not in allowed_exact:
                    continue
            elif schema_filter:
                if sname != schema_filter:
                    continue
            elif needles:
                low = sname.lower()
                if not any(n in low for n in needles):
                    continue
            tables = client.list_tables(cname, sname)
            if max_tables_per_schema is not None:
                tables = tables[:max_tables_per_schema]
            schema_entry: dict[str, Any] = {"schema": sch, "tables": []}
            for t in tables:
                t_entry: dict[str, Any] = {"summary": t}
                if include_columns:
                    full = t.get("full_name") or f"{cname}.{sname}.{t.get('name')}"
                    try:
                        t_entry["detail"] = client.get_table(full)
                    except RuntimeError as e:
                        t_entry["detail_error"] = str(e)
                schema_entry["tables"].append(t_entry)
            entry["schemas"].append(schema_entry)
        tree["catalogs"].append(entry)
    return tree


def _print_tree(tree: dict[str, Any]) -> None:
    for centry in tree.get("catalogs") or []:
        cat = centry.get("catalog") or {}
        print(f"Catalog: {cat.get('name')}  ({cat.get('catalog_type', '—')})")
        for sentry in centry.get("schemas") or []:
            sch = sentry.get("schema") or {}
            print(f"  Schema: {sch.get('name')}")
            for t in sentry.get("tables") or []:
                summ = t.get("summary") or {}
                name = summ.get("name") or summ.get("table_type")
                ttype = summ.get("table_type") or "?"
                print(f"    Table: {name}  [{ttype}]")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="List Unity Catalog catalogs, schemas, and tables (read-only)."
    )
    p.add_argument(
        "--host",
        default="",
        help="Workspace URL (default: DATABRICKS_HOST or DATABRICKS_HOST_STORY)",
    )
    p.add_argument(
        "--token",
        default="",
        help="PAT (default: DATABRICKS_TOKEN or legacy vars from .env)",
    )
    p.add_argument("--catalog", default="", help="Only this catalog name")
    p.add_argument("--schema", default="", help="Only this schema (requires --catalog)")
    p.add_argument(
        "--max-tables",
        type=int,
        default=0,
        help="Cap tables per schema (0 = no cap)",
    )
    p.add_argument(
        "--columns",
        action="store_true",
        help="Fetch full table metadata (columns) — more API calls",
    )
    p.add_argument("--json", action="store_true", help="Print JSON to stdout")
    p.add_argument(
        "--list-catalogs-only",
        action="store_true",
        help="Only call list catalogs (fast smoke test; no schemas/tables)",
    )
    args = p.parse_args(argv)

    host = _strip_env(args.host) if args.host else _resolve_host()
    token = _strip_env(args.token) if args.token else _resolve_token()
    if not host:
        print(
            "Missing workspace URL. Set DATABRICKS_HOST / DATABRICKS_HOST_STORY or --host.",
            file=sys.stderr,
        )
        return 1
    if not token:
        print(
            "Missing PAT. Set DATABRICKS_TOKEN / DATABRICKS_TOKEN_STORY (or legacy ANJALI).",
            file=sys.stderr,
        )
        return 2
    if args.schema and not args.catalog:
        print("--schema requires --catalog", file=sys.stderr)
        return 3

    client = UnityCatalogClient(host=host, token=token)
    if args.list_catalogs_only:
        cats = client.list_catalogs()
        if args.json:
            print(json.dumps({"catalogs": cats}, indent=2, default=str))
        else:
            for c in cats:
                print(
                    c.get("name", "?"),
                    f"({c.get('catalog_type', '—')})",
                )
        return 0

    tree = build_tree(
        client,
        catalog_filter=args.catalog or None,
        schema_filter=args.schema or None,
        max_tables_per_schema=args.max_tables or None,
        include_columns=args.columns,
    )
    if args.json:
        print(json.dumps(tree, indent=2, default=str))
    else:
        _print_tree(tree)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
