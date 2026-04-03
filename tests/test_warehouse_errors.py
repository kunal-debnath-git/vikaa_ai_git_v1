from backend.integrations.databricks.warehouse_errors import classify_warehouse_error


def test_maps_permission() -> None:
    o = classify_warehouse_error("PERMISSION_DENIED: cannot select", http_status=None)
    assert o["category"] == "permission"


def test_maps_warehouse_stopped() -> None:
    o = classify_warehouse_error("warehouse is not running", http_status=None)
    assert o["category"] == "warehouse_state"
