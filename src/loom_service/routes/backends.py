"""Backend catalog — GET /api/v1/backends (Plan 28 PR-3).

Returns the union of backends every currently-active worker
reports as supported. Drives the Backend dropdown on the SPA's
submit form.

A backend appears in the list iff at least one active worker
(`workers.status = 'active'`) advertises it in its capabilities
JSONB. If the last worker for a backend drains mid-flight, the
catalog stops listing it; in-flight batches on that backend
naturally stall (claim returns 204).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from sqlalchemy import select

from loom.db.schema import Worker
from loom_service.dependencies import SessionAndCtx

router = APIRouter()


_DESCRIPTIONS: dict[str, str] = {
    "docker": "Local docker on the worker host.",
    "daytona": "Cloud sandboxes via the Daytona API.",
    "modal": "Cloud sandboxes via the Modal API.",
    "fake": "In-memory driver. Tests + smoke only — no real env.",
}

# Backends Loom ships drivers for. Even when no live worker advertises
# a given backend, we surface it as `available=false` so the dropdown
# shows what's *possible* — better than a confusing empty list.
_KNOWN_BACKENDS: tuple[str, ...] = ("docker", "daytona", "modal", "fake")


@router.get("/backends")
async def list_backends(sc: SessionAndCtx) -> dict[str, Any]:
    s, _ctx = sc
    rows = (await s.execute(
        select(Worker.capabilities).where(Worker.status == "active"),
    )).scalars().all()

    seen: set[str] = set()
    for caps_list in rows:
        # Worker.capabilities is a JSONB list of one-or-more capability
        # dicts. Each cap dict may carry a `backend` key (workers
        # registered after Plan 28 PR-3) — older workers omit it,
        # so we default to "docker" on those rows since the only
        # backend the worker pool shipped before this PR was docker.
        if not isinstance(caps_list, list):
            continue
        for cap in caps_list:
            if not isinstance(cap, dict):
                continue
            backend_name = cap.get("backend", "docker")
            if isinstance(backend_name, str):
                seen.add(backend_name)

    # Union of known + worker-advertised. Each entry marks `available`
    # so the SPA can render greyed-out options for backends that have
    # drivers but no live workers.
    all_names = sorted(set(_KNOWN_BACKENDS) | seen)
    items = [
        {
            "name": name,
            "description": _DESCRIPTIONS.get(
                name, f"Worker-reported backend {name!r}.",
            ),
            "available": name in seen,
        }
        for name in all_names
    ]
    return {"items": items}
