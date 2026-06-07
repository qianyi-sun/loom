"""Task bundle endpoint — returns full TaskConfig + checksum + source.

Closes the v1 limitation flagged in Plan 6's `_fetch_task_config`: workers
need the task body to construct a TrialContext, but the claim payload
only carries trial-level fields. The bundle endpoint is the second
round-trip — workers fetch by `task_id` after claim succeeds.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import select

from loom.auth import verify_bearer_token
from loom.db.schema import Task as TaskRow

router = APIRouter()


@router.get("/tasks/{task_id}/bundle")
async def get_task_bundle(
    task_id: str,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")

    async with request.app.state.session_factory() as session:
        row = (await session.execute(
            select(TaskRow).where(TaskRow.id == task_id),
        )).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="task not found")

    return {
        "id": row.id,
        "checksum": row.checksum,
        "config": row.config,
        "source": row.source,
    }
