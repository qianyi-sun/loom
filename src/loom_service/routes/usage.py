"""Usage rollup (spec §5.7).

GET /api/v1/usage?team_id=...&start=YYYY-MM-DD&end=YYYY-MM-DD&group_by=day|week|month

Aggregates `llm_calls JOIN trials` by `date_trunc(group_by,
captured_at)`. Returns per-bucket counts (trial count, succeeded,
failed) + token totals + cost. The `llm_calls` table is part of the
canonical Plan 9 schema, so the `degraded` flag is defensive
defense-in-depth — if some future maintenance dropped the table the
route still 200s with empty buckets instead of 500'ing the SPA.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import verify_bearer_token
from loom_service.auth_guards import (
    is_admin,
    require_human_or_admin,
    require_team_or_admin,
)

router = APIRouter()

_TRUNC_UNITS: frozenset[str] = frozenset({"day", "week", "month"})


async def _llm_calls_exists(session: AsyncSession) -> bool:
    res = await session.execute(
        text("SELECT to_regclass('llm_calls') IS NOT NULL"),
    )
    return bool(res.scalar())


@router.get("/usage")
async def get_usage(
    request: Request,
    start: Annotated[date, Query()],
    end: Annotated[date, Query()],
    team_id: Annotated[UUID | None, Query()] = None,
    group_by: Annotated[str, Query()] = "day",
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    if group_by not in _TRUNC_UNITS:
        raise HTTPException(
            status_code=400,
            detail=f"group_by must be one of {sorted(_TRUNC_UNITS)}",
        )
    if end < start:
        raise HTTPException(
            status_code=400, detail="end must be >= start",
        )

    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)

        target_team = team_id
        if target_team is not None:
            require_team_or_admin(ctx, target_team)
        elif not is_admin(ctx):
            target_team = ctx.team_id

        if not await _llm_calls_exists(s):
            return {"buckets": [], "degraded": True}

        start_ts = datetime.combine(start, time.min, tzinfo=UTC)
        # `end` is inclusive — use the start of the day AFTER `end` as
        # the upper bound and BETWEEN ... < end_ts. To keep the query
        # simple, use `time.max` (microsecond precision) so the
        # comparison is `<=`.
        end_ts = datetime.combine(end, time.max, tzinfo=UTC)

        params: dict[str, Any] = {"start": start_ts, "end": end_ts}
        team_clause = ""
        if target_team is not None:
            team_clause = "AND t.team_id = :team_id"
            params["team_id"] = str(target_team)

        # `date_trunc` argument is a literal interpolated into the SQL
        # text — only the 3 strings in `_TRUNC_UNITS` can reach it, so
        # the substitution is safe even though it isn't parameterized.
        sql = f"""
            SELECT
                date_trunc('{group_by}', l.captured_at) AS bucket_start,
                COUNT(DISTINCT t.id) AS trial_count,
                COUNT(DISTINCT t.id)
                    FILTER (WHERE t.state = 'succeeded')
                    AS succeeded_count,
                COUNT(DISTINCT t.id)
                    FILTER (WHERE t.state = 'failed')
                    AS failed_count,
                COALESCE(SUM(l.cost_usd), 0) AS total_cost_usd,
                COALESCE(SUM(l.input_tokens), 0) AS llm_input_tokens,
                COALESCE(SUM(l.output_tokens), 0) AS llm_output_tokens
              FROM llm_calls l
              JOIN trials t ON t.id = l.trial_id
             WHERE l.captured_at BETWEEN :start AND :end
               {team_clause}
          GROUP BY bucket_start
          ORDER BY bucket_start
        """
        rows = (await s.execute(text(sql), params)).all()

    buckets: list[dict[str, Any]] = []
    for r in rows:
        bs = r.bucket_start
        buckets.append({
            "start_at": bs.isoformat(),
            # The bucket close is `start_at + 1 {group_by}` — computed
            # client-side because Postgres `date_trunc` only returns
            # the start. Keeping it here would require interval
            # arithmetic; the SPA already knows the group_by.
            "end_at": None,
            "trial_count": int(r.trial_count),
            "succeeded_count": int(r.succeeded_count),
            "failed_count": int(r.failed_count),
            "total_cost_usd": float(r.total_cost_usd),
            "llm_input_tokens": int(r.llm_input_tokens),
            "llm_output_tokens": int(r.llm_output_tokens),
        })
    return {"buckets": buckets, "degraded": False}
