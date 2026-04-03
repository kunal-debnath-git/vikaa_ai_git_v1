"""Unit tests for Databricks NL SQL guardrails."""

from __future__ import annotations

import pytest

from backend.services.databricks_nl_sql_service import (
    assert_fully_qualified_table_names,
    assert_read_only_sql,
)


def test_read_only_accepts_select() -> None:
    s = assert_read_only_sql("SELECT 1 AS x FROM workspace.gold.t AS t")
    assert "SELECT" in s


def test_read_only_rejects_insert() -> None:
    with pytest.raises(ValueError, match="SELECT"):
        assert_read_only_sql("INSERT INTO workspace.gold.t SELECT 1")


def test_fqn_enforced_when_enabled() -> None:
    with pytest.raises(ValueError, match="three-part"):
        assert_fully_qualified_table_names(
            "SELECT * FROM t",
            enforce=True,
        )


def test_fqn_passes_three_part() -> None:
    assert_fully_qualified_table_names(
        "SELECT * FROM workspace.brazil_gold.fact_order",
        enforce=True,
    ) is None


def test_fqn_skipped_when_disabled() -> None:
    assert_fully_qualified_table_names("SELECT * FROM t", enforce=False) is None
