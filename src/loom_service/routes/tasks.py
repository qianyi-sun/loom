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
    """List/detail shape. Surfaces the fields the SPA browse view
    needs (name, description, agent/verifier/step counts) without
    requiring a per-row config fetch — the JSON path lookups are
    cheap and we already loaded the row."""
    cfg = t.config or {}
    task_meta = cfg.get("task") or {}
    agent = cfg.get("agent") or {}
    verifier = cfg.get("verifier") or {}
    steps = cfg.get("steps") or []
    return {
        "id": t.id,
        "name": task_meta.get("name"),
        "description": task_meta.get("description"),
        "agent_name": agent.get("name"),
        "verifier_name": verifier.get("name"),
        "step_count": len(steps) if isinstance(steps, list) else 0,
        "checksum": t.checksum,
        "source": t.source,
        "license": t.license,
        "benchmark_id": t.benchmark_id,
        "registered_at": t.registered_at.isoformat(),
    }


@router.get("/tasks")
async def list_tasks(
    request: Request,
    benchmark_id: Annotated[str | None, Query()] = None,
    # Plan 24: drop the license-spdx filter from the SPA browse path
    # (operators almost never search by SPDX tag) and add an
    # id-substring search so users can locate a task by typing part
    # of its id. Bounded length so the LIKE pattern can't be abused.
    q: Annotated[
        str | None,
        Query(
            description="substring match against task id",
            max_length=200,
        ),
    ] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        require_human_or_admin(ctx)
        stmt = select(Task).order_by(Task.id)
        if benchmark_id is not None:
            stmt = stmt.where(Task.benchmark_id == benchmark_id)
        if q:
            # `ilike` with explicit '%' wrapping. Postgres LIKE
            # patterns treat `%` and `_` as metacharacters, but a
            # query that happens to contain them just matches more
            # broadly — no injection risk through the SQL boundary.
            stmt = stmt.where(Task.id.ilike(f"%{q}%"))
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
