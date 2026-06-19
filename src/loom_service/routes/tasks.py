"""Tasks browse (spec §5.4).

Any human-or-admin token may browse the tasks catalog. The Task PK is
a string (`humaneval/HumanEval/0` etc., not UUID), so the detail route
takes a path-encoded id. Cursor pagination uses `id` since rows are
ordered by it.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select

from loom.db.schema import Task
from loom_service.dependencies import SessionAndCtx
from loom_service.task_config_validation import split_valid_task_configs
from loom_service.task_filter import resolve_task_filter

router = APIRouter()


class _CountReq(BaseModel):
    """Body shape for `POST /tasks/count` — matches the `task_filter`
    field of `POST /batches` exactly so the SPA can use the same payload
    shape for both endpoints."""

    model_config = ConfigDict(extra="forbid")

    task_filter: dict[str, Any] = Field(default_factory=dict)


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
    sc: SessionAndCtx,
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
) -> dict[str, Any]:
    s, _ctx = sc
    # Shared where clauses so the page query and the count query
    # touch the same predicate (count gets a clean COUNT(*) plan
    # rather than COUNT over an ordered subquery).
    conds: list[Any] = []
    if benchmark_id is not None:
        conds.append(Task.benchmark_id == benchmark_id)
    if q:
        # `ilike` with explicit '%' wrapping. Postgres LIKE
        # patterns treat `%` and `_` as metacharacters, but a
        # query that happens to contain them just matches more
        # broadly — no injection risk through the SQL boundary.
        conds.append(Task.id.ilike(f"%{q}%"))
    total = (
        await s.execute(
            select(func.count()).select_from(Task).where(*conds),
        )
    ).scalar_one()
    stmt = select(Task).where(*conds).order_by(Task.id)
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
        "total": int(total),
    }


@router.post("/tasks/count")
async def count_tasks(payload: _CountReq, sc: SessionAndCtx) -> dict[str, int]:
    """Return the exact count of tasks matching `task_filter`.

    Backs the SPA's NewBatch real-count check (issue #28). Uses the
    same materialization path the batch creator runs (`resolve_task_filter`)
    so the count is identical to what `POST /batches` would produce —
    no upper-bound estimation drift when tag_filters are active.

    Returns 0 (not 400) for filters that match no rows; the SPA needs
    to distinguish "no match" from "invalid filter". The downstream
    `POST /batches` keeps its empty-rejection so a creator can't
    accidentally produce a stuck batch.

    Note: this route reads the full materialized id list and returns
    `len()` rather than `SELECT COUNT(*)`. That matches the batch
    creator's semantics exactly (including the subset_kind /
    first_n / last_n / random_n trimming). For very large filters
    the cost is dominated by the candidate query that COUNT(*) would
    do anyway; the in-Python trim is O(N) on top.
    """
    s, _ctx = sc
    task_ids = await resolve_task_filter(s, payload.task_filter)
    valid_task_ids, _invalid_tasks = await split_valid_task_configs(s, task_ids)
    return {"count": len(valid_task_ids)}


@router.get("/tasks/{task_id:path}")
async def get_task(task_id: str, sc: SessionAndCtx) -> dict[str, Any]:
    s, _ctx = sc
    t = (
        await s.execute(
            select(Task).where(Task.id == task_id),
        )
    ).scalar_one_or_none()
    if t is None:
        raise HTTPException(status_code=404, detail="task not found")
    d = _task_row(t)
    d["config"] = t.config
    return d
