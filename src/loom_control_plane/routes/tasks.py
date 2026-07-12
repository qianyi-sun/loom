"""Task bundle endpoint — returns full TaskConfig + checksum + source.

Closes the v1 limitation flagged in Plan 6's `_fetch_task_config`: workers
need the task body to construct a TrialContext, but the claim payload
only carries trial-level fields. The bundle endpoint is the second
round-trip — workers fetch by `task_id` after claim succeeds.

**Scope policy.** Any authenticated bearer token may fetch any task
bundle — there is no per-team scoping. Task config + checksum + source
are not secrets in v1 (tasks are openly shared across teams as
benchmark fixtures). Teams that need to gate task visibility should
publish under a separate namespace. If a future deployment needs team-
scoped tasks, add a `team_id` column to `tasks` and check
`ctx.team_id`; nothing about this route's shape blocks that.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import select

from loom.auth import verify_bearer_token
from loom.db.schema import Task as TaskRow

router = APIRouter()


@router.get("/tasks/{task_id:path}/bundle")
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
        row = (
            await session.execute(
                select(TaskRow).where(TaskRow.id == task_id),
            )
        ).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="task not found")

    return {
        "id": row.id,
        "checksum": row.checksum,
        "config": row.config,
        "source": row.source,
        "source_provenance": row.source_provenance,
    }
