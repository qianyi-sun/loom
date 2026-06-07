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
from sqlalchemy import select

from loom.auth import verify_bearer_token
from loom.db.schema import Benchmark
from loom_service.auth_guards import require_human_or_admin

router = APIRouter()


def _bench_row(b: Benchmark) -> dict[str, Any]:
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
    }


@router.get("/benchmarks")
async def list_benchmarks(
    request: Request,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        require_human_or_admin(ctx)
        stmt = select(Benchmark).order_by(Benchmark.display_name)
        if cursor:
            stmt = stmt.where(Benchmark.display_name > cursor)
        stmt = stmt.limit(limit + 1)
        rows = list((await s.execute(stmt)).scalars().all())
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor: str | None = rows[-1].display_name
    else:
        next_cursor = None
    return {
        "items": [_bench_row(r) for r in rows],
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
        b = (await s.execute(
            select(Benchmark).where(Benchmark.id == benchmark_id),
        )).scalar_one_or_none()
        if b is None:
            raise HTTPException(
                status_code=404, detail="benchmark not found",
            )
        return _bench_row(b)
