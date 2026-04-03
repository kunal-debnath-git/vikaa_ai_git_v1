"""Register the standard /tools + /api/tools + root path triple for a tool."""

from __future__ import annotations

from typing import Iterable

from fastapi import APIRouter, FastAPI


def include_router_triplet(
    app: FastAPI,
    routers: Iterable[APIRouter],
) -> None:
    """Include each router in order (typically main, api alias, root)."""
    for r in routers:
        app.include_router(r)
