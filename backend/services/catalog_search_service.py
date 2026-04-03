# ╔══════════════════════════════════════════════════════════════════╗
# ║  VIKAA.AI — PUBLIC SHOWCASE                                      ║
# ║  This file requires environment credentials to run.              ║
# ║  Architecture and API signatures are shown for reference.        ║
# ║  See README.md and docs/ for setup guide.                        ║
# ╚══════════════════════════════════════════════════════════════════╝

"""
Natural-language Q&A over Unity Catalog metadata (table/column comments).

Uses Anthropic Claude with a bounded excerpt of catalog text — metadata only, no warehouse execution.
"""

from __future__ import annotations

import os
import re
from typing import Any

SYSTEM_PROMPT = """You are a data catalog assistant for a Databricks lakehouse.

Rules:
- Base answers ONLY on the Unity Catalog metadata excerpt (table/column names, types, comments). Do not invent columns or tables.
- Cite fully qualified table names as catalog.schema.table when present.
- If the excerpt is insufficient, say what is missing and suggest which schemas or tables to open next.
- You do NOT have query results, row counts, or live data — only documentation-style metadata.
- For mapping business concepts to tables, use comments and names; note uncertainty when comments are thin.
"""

QUERY_TOKEN_RE = re.compile(r"[\W_]+", re.UNICODE)


def env_csv_list(name: str) -> list[str] | None:
    ...


def resolve_catalog_and_schema_filters(
    catalog: str | None,
    schemas: list[str] | None,
    schema_substrings: list[str] | None,
) -> tuple[str, list[str] | None, list[str] | None]:
    """Return catalog name and optional exact schema list or substring filters."""
    ...


def _comment_blob_for_scoring(text: str) -> str:
    """Table + column comment text only (lowercased) — used to boost excerpt ranking."""
    ...


def table_detail_to_text(summary: dict[str, Any], detail: dict[str, Any] | None) -> str:
    ...


def iter_catalog_blocks(tree: dict[str, Any]) -> list[tuple[str, str]]:
    """Pairs of (full_name, excerpt text) for each table in the tree."""
    ...


def _query_tokens(query: str) -> set[str]:
    ...


def build_context_excerpt(
    blocks: list[tuple[str, str]],
    query: str,
    max_chars: int,
) -> tuple[str, int]:
    """Rank blocks by token overlap with query; pack until max_chars. Returns (text, num_tables_included)."""
    ...


def answer_catalog_question(context: str, query: str) -> str:
    """Single-turn Claude completion (no tool_use)."""
    ...
