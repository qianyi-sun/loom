"""Tasks browse (spec §5.4).

Any human-or-admin token may browse the tasks catalog. The Task PK is
a string (`humaneval/HumanEval/0` etc., not UUID), so the detail route
takes a path-encoded id. Cursor pagination uses `id` since rows are
ordered by it.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Query, Request
from sqlalchemy import select

from loom.auth import verify_bearer_token
from loom.db.schema import Task
from loom_service.auth_guards import require_human_or_admin

router = APIRouter()


def _task_row(t: Task) -> dict[str, Any]:
    return {
        "id": t.id,
        "checksum": t.checksum,
        "source": t.source,
        "license": t.license,
        "benchmark_id": t.benchmark_id,
        "registered_at": t.registered_at.isoformat(),
    }


@router.get("/tasks")
async def list_tasks(
    request: Request,
    benchmark_id: str | None = Query(default=None),
    license: str | None = Query(default=None),  # noqa: A002
    cursor: str | None = Query(default=None),
    limit: int = Query(default=50, gt=0, le=200),
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        require_human_or_admin(ctx)
        stmt = select(Task).order_by(Task.id)
        if license:
            stmt = stmt.where(Task.license == license)
        if benchmark_id is not None:
            stmt = stmt.where(Task.benchmark_id == benchmark_id)
        if cursor:
            stmt = stmt.where(Task.id > cursor)
        stmt = stmt.limit(limit + 1)
        rows = list((await s.execute(stmt)).scalars().all())

    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor: str | None = rows[-1].id
    else:
        next_cursor = None
    return {
        "items": [_task_row(r) for r in rows],
        "next_cursor": next_cursor,
    }


@router.get("/tasks/{task_id:path}")
async def get_task(
    request: Request,
    task_id: str,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        require_human_or_admin(ctx)
        t = (await s.execute(
            select(Task).where(Task.id == task_id),
        )).scalar_one_or_none()
        if t is None:
            raise HTTPException(status_code=404, detail="task not found")
        d = _task_row(t)
        d["config"] = t.config
        return d
