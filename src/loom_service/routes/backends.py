"""Backend catalog — GET /api/v1/backends (Plan 28 PR-3).

Returns the union of known, currently-active, and cold-startable backends.
Drives the Backend dropdown on the SPA's submit form.

``available`` remains true only when at least one fresh active worker advertises
the backend. Autoscaler planning headroom is exposed separately through
``cold_start_available`` and ``cold_start_pools`` so clients cannot mistake a
configured ceiling for immediately executable capacity.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from loom_service.dependencies import SessionAndCtx
from loom_service.worker_backends import get_active_backends, get_cold_start_pools

router = APIRouter()


_DESCRIPTIONS: dict[str, str] = {
    "docker": "Local docker on the worker host.",
    "modal": "Cloud sandboxes via the Modal API.",
    "fake": "In-memory driver. Tests + smoke only — no real env.",
}

# Backends Loom ships drivers for. Even when no live worker advertises
# a given backend, we surface it as `available=false` so the dropdown
# shows what's *possible* — better than a confusing empty list.
_KNOWN_BACKENDS: tuple[str, ...] = ("docker", "modal", "fake")


@router.get("/backends")
async def list_backends(sc: SessionAndCtx) -> dict[str, Any]:
    s, _ctx = sc
    seen = await get_active_backends(s)
    cold_start_pools = await get_cold_start_pools(s)
    cold_start_pools_by_backend: dict[str, list[str]] = {}
    for pool in cold_start_pools:
        cold_start_pools_by_backend.setdefault(pool.backend, []).append(pool.pool_name)

    # Union of known + worker-advertised. Each entry marks `available`
    # so the SPA can render greyed-out options for backends that have
    # drivers but no live workers.
    all_names = sorted(set(_KNOWN_BACKENDS) | seen | set(cold_start_pools_by_backend))
    items = [
        {
            "name": name,
            "description": _DESCRIPTIONS.get(
                name, f"Worker-reported backend {name!r}.",
            ),
            "available": name in seen,
            # Planning headroom is intentionally separate from fresh worker
            # evidence. A caller can submit compatible queued demand when this
            # is true, but must not treat it as immediately executable capacity.
            "cold_start_available": bool(cold_start_pools_by_backend.get(name)),
            "cold_start_pools": sorted(cold_start_pools_by_backend.get(name, [])),
        }
        for name in all_names
    ]
    return {"items": items}
