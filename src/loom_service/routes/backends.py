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

from typing import Annotated, Any

from fastapi import APIRouter, Header, Request
from sqlalchemy import select

from loom.auth import verify_bearer_token
from loom.db.schema import Worker
from loom_service.auth_guards import require_human_or_admin

router = APIRouter()


_DESCRIPTIONS: dict[str, str] = {
    "docker": "Local docker on the worker host.",
    "daytona": "Cloud sandboxes via the Daytona API.",
    "modal": "Cloud sandboxes via the Modal API.",
    "fake": "In-memory driver. Tests + smoke only — no real env.",
}


@router.get("/backends")
async def list_backends(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        require_human_or_admin(ctx)
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

    items = [
        {
            "name": name,
            "description": _DESCRIPTIONS.get(
                name, f"Worker-reported backend {name!r}.",
            ),
        }
        for name in sorted(seen)
    ]
    return {"items": items}
