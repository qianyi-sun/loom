"""Hosted Nebius catalog, with worker backends only in explicit local development."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from loom.service_execution_backend import NEBIUS_BACKEND, local_execution_enabled
from loom_service.dependencies import SessionAndCtx
from loom_service.worker_backends import get_active_backends, get_service_execution_backend_pools

router = APIRouter()


@router.get("/backends")
async def list_backends(sc: SessionAndCtx) -> dict[str, Any]:
    session, _ctx = sc
    seen = await get_active_backends(session) if local_execution_enabled() else set()
    pools = await get_service_execution_backend_pools(session)
    names = sorted({NEBIUS_BACKEND} | seen)
    return {"items": [
        {
            "name": name,
            "description": (
                "Nebius Kubernetes execution; scales from zero."
                if name == NEBIUS_BACKEND else f"Local development backend {name!r}."
            ),
            "available": name in seen,
            "cold_start_available": name == NEBIUS_BACKEND and bool(pools),
            "cold_start_pools": sorted({pool.pool_name for pool in pools})
            if name == NEBIUS_BACKEND else [],
        }
        for name in names
    ]}
