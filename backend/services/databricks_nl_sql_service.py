# ╔══════════════════════════════════════════════════════════════════╗
# ║  VIKAA.AI — PUBLIC SHOWCASE                                      ║
# ║  This file requires environment credentials to run.              ║
# ║  Architecture and API signatures are shown for reference.        ║
# ║  See README.md and docs/ for setup guide.                        ║
# ╚══════════════════════════════════════════════════════════════════╝

"""
Natural language → read-only Spark SQL → Databricks SQL warehouse execution.

LLM: Gemini first (GEMINI_API_KEY or GOOGLE_API_KEY), then Anthropic if Gemini
is missing or errors. Override model with DATABRICKS_QUERY_GEMINI_MODEL (default
gemini-2.0-flash).
"""

from __future__ import annotations

import json
import logging
import os
import re
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

SQL_FENCE_RE = re.compile(r"```(?:sql)?\s*([\s\S]*?)```", re.IGNORECASE)

DESTRUCTIVE_RE = re.compile(
    r"\b(insert|update|delete|merge|drop|create|alter|truncate|grant|revoke|replace)\b",
    re.IGNORECASE,
)

SQL_GEN_SYSTEM = """You write a single Databricks / Spark SQL statement for the user's question.

Hard rules:
- Use ONLY tables and columns that appear in the catalog excerpt below. Use fully qualified names: catalog.schema.table.
- The excerpt includes Unity Catalog table comments and per-column comments after "Table comment:" and on column lines (after " — "). When choosing between similar columns, prefer the one whose comment best matches the user's intent; if comments disagree with names, treat comments as the stronger hint.
- Output must be ONE read-only SELECT (WITH ... CTEs allowed). No DDL, no DML, no multi-statement batches.
- Include a reasonable LIMIT (e.g. 500) when returning many detail rows. Omit LIMIT on the outer query only when the answer is a small aggregate (counts, sums) over a filtered period.
- For time ranges (e.g. last 10 years), filter on the best matching date/timestamp column from the excerpt; if unclear, prefer a column with obvious names (order_date, sale_date, created_at, etc.) and mention assumptions in SQL comments only as /* ... */.
- Respond with nothing but one markdown code block: ```sql ... ```"""


SUMMARY_SYSTEM = """You summarize SQL query results for a business user.
- Be concise; use the exact numbers from the sample rows.
- If the sample is truncated, say so.
- Do not fabricate columns or values not shown."""


def _gemini_key() -> str:
    ...


def _anthropic_key_configured() -> bool:
    ...


def _gemini_text(
    system: str,
    user: str,
    *,
    max_tokens: int,
    temperature: float,
) -> str:
    """Call Gemini generateContent via REST. Raises on missing key, HTTP error, or empty text."""
    ...


def _anthropic_text(system: str, user: str, max_tokens: int = 2048) -> str:
    ...


def _llm_text(
    system: str,
    user: str,
    max_tokens: int,
    *,
    temperature: float = 0.25,
) -> tuple[str, str]:
    """Gemini first; Anthropic if Gemini unavailable or fails. Returns (text, display_label)."""
    ...


def strip_sql_comments(sql: str) -> str:
    ...


def assert_read_only_sql(sql: str) -> str:
    ...


def extract_sql_from_response(text: str) -> str:
    ...


FQN_PATTERN = re.compile(
    r"(?<![\w.])(?:`?[\w]+`?\.){2}`?[\w]+`?(?![\w.])",
    re.IGNORECASE,
)


def assert_fully_qualified_table_names(sql: str, *, enforce: bool) -> None:
    """Require at least one ``catalog.schema.table``-style reference when enforce is True."""
    ...


def generate_sql_for_question(
    catalog_excerpt: str,
    question: str,
    *,
    conversation_context: str | None = None,
) -> tuple[str, str]:
    ...


def summarize_result(question: str, sql: str, columns: list[str], rows: list[list[Any]]) -> tuple[str, str]:
    ...
