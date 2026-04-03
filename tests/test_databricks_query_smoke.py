"""Smoke: databricks-query router is mounted (no warehouse call)."""

from __future__ import annotations

from fastapi.testclient import TestClient


def test_databricks_query_health_route_exists() -> None:
    import main

    c = TestClient(main.app)
    r = c.get("/tools/databricks-query/health")
    assert r.status_code == 200
    body = r.json()
    assert body.get("service") == "databricks-query"
    assert "runtime_config" in body
