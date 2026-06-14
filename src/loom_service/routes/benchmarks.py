"""Benchmarks browse (spec §5.4).

Schema-real shape: `Benchmark.id` is a text PK (e.g. `humaneval`),
fields are `display_name`, `upstream_kind`, `upstream_locator`,
`upstream_revision`, `license_spdx`, `license_url`, `splits`,
`imported_at`, `imported_by`.

Cursor pagination uses `display_name` since rows are sorted by it.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Query, Request
from sqlalchemy import func, select, text

from loom.auth import verify_bearer_token
from loom.db.schema import Benchmark, Task
from loom_service.auth_guards import require_human_or_admin

router = APIRouter()


def _bench_row(b: Benchmark, task_count: int = 0) -> dict[str, Any]:
    return {
        "id": b.id,
        "display_name": b.display_name,
        "upstream_kind": b.upstream_kind,
        "upstream_locator": b.upstream_locator,
        "upstream_revision": b.upstream_revision,
        "license_spdx": b.license_spdx,
        "license_url": b.license_url,
        "splits": list(b.splits),
        "imported_at": b.imported_at.isoformat(),
        "imported_by": b.imported_by,
        # Plan 28: surface the imported-task count so the SPA can
        # distinguish "ready to submit" benchmarks from metadata-only
        # rows. Empty benchmarks are excluded from the default listing
        # (see `include_empty` below).
        "task_count": task_count,
        # PR-2 series/tags: surface series so the SPA can group
        # related benchmarks in the dropdown. NULL = standalone.
        "series": b.series,
    }


@router.get("/benchmarks")
async def list_benchmarks(
    request: Request,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
    # By default the SPA-facing listing hides benchmarks with zero
    # imported tasks — "registered" on its own (metadata-only seeding)
    # would otherwise show benchmarks in the dropdown that produce a
    # confusing "0 tasks match" when picked. Operators can pass
    # `?include_empty=true` to see every registered benchmark (e.g.
    # admin tools that drive imports).
    include_empty: Annotated[bool, Query()] = False,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        require_human_or_admin(ctx)
        # LEFT JOIN tasks for the count so empty benchmarks still show
        # up when include_empty=True. GROUP BY on the PK is safe.
        stmt = (
            select(Benchmark, func.count(Task.id).label("task_count"))
            .join(Task, Task.benchmark_id == Benchmark.id, isouter=True)
            .group_by(Benchmark.id)
            .order_by(Benchmark.display_name)
        )
        if cursor:
            stmt = stmt.where(Benchmark.display_name > cursor)
        if not include_empty:
            stmt = stmt.having(func.count(Task.id) > 0)
        stmt = stmt.limit(limit + 1)
        rows = list((await s.execute(stmt)).all())
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor: str | None = rows[-1][0].display_name
    else:
        next_cursor = None
    return {
        "items": [_bench_row(b, int(c)) for b, c in rows],
        "next_cursor": next_cursor,
    }


@router.get("/benchmarks/{benchmark_id}")
async def get_benchmark(
    request: Request,
    benchmark_id: str,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        require_human_or_admin(ctx)
        row = (await s.execute(
            select(Benchmark, func.count(Task.id).label("task_count"))
            .join(Task, Task.benchmark_id == Benchmark.id, isouter=True)
            .where(Benchmark.id == benchmark_id)
            .group_by(Benchmark.id),
        )).one_or_none()
        if row is None:
            raise HTTPException(
                status_code=404, detail="benchmark not found",
            )
        b, count = row
        return _bench_row(b, int(count))


@router.get("/benchmarks/{benchmark_id}/tags")
async def list_benchmark_tags(
    request: Request,
    benchmark_id: str,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    """Distinct tag keys → distinct values for a benchmark's tasks.

    Drives the SPA's tag-filter UI (PR-3): one dropdown per key, each
    populated with the values actually present in that benchmark. The
    SPA can then build a `task_filter.tag_filters` payload from user
    selections without guessing what's in the data.

    Implementation: `jsonb_each_text(tags)` cross-joined to each task
    row in the benchmark, grouped by key, distinct values aggregated.
    Result shape:

        {
          "items": [
            {"key": "year", "values": ["2022", "2023", "2024"]},
            {"key": "exam", "values": ["I", "II"]},
            {"key": "problem", "values": ["1", "2", ..., "15"]}
          ]
        }
    """
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        require_human_or_admin(ctx)
        # 404 if the benchmark itself doesn't exist so the SPA can
        # distinguish "no tags here" (empty list, 200) from "wrong id"
        # (404).
        exists = (await s.execute(
            select(Benchmark.id).where(Benchmark.id == benchmark_id),
        )).scalar_one_or_none()
        if exists is None:
            raise HTTPException(
                status_code=404, detail="benchmark not found",
            )
        rows = (await s.execute(
            text(
                "SELECT kv.key, ARRAY_AGG(DISTINCT kv.value ORDER BY kv.value) "
                "FROM tasks t, jsonb_each_text(t.tags) AS kv(key, value) "
                "WHERE t.benchmark_id = :bid "
                "GROUP BY kv.key "
                "ORDER BY kv.key",
            ),
            {"bid": benchmark_id},
        )).all()
    return {
        "items": [
            {"key": key, "values": list(values)}
            for key, values in rows
        ],
    }
