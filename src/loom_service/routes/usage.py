"""Usage rollup (spec §5.7).

GET /api/v1/usage?team_id=...&start=YYYY-MM-DD&end=YYYY-MM-DD&group_by=day|week|month

Aggregates `llm_calls JOIN trials` by `date_trunc(group_by,
captured_at)`. Returns per-bucket counts (trial count + each
trial's CURRENT state — succeeded/failed/etc — at query time, NOT
at the time of the llm_call) + token totals + cost.

Note: the per-state counts are point-in-time (they reflect
`Trial.state` as of the SQL execution). A trial that had llm_calls
inside the bucket but was later retried and re-finalized will be
counted under its current state. Field names reflect this:
`trials_currently_succeeded`, etc. — not "trials succeeded during
the bucket".

The `llm_calls` table is part of the canonical Plan 9 schema and
v0.7 has no retention/delete path on it, so the INNER JOIN below
doesn't undercount in practice. The `degraded` flag is
defense-in-depth — if some future maintenance dropped the table
the route still 200s with empty buckets instead of 500'ing the SPA.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_service.auth_guards import (
    is_admin,
    require_team_or_admin,
)
from loom_service.dependencies import SessionAndCtx

router = APIRouter()

_TRUNC_UNITS: frozenset[str] = frozenset({"day", "week", "month"})


async def _llm_calls_exists(session: AsyncSession) -> bool:
    res = await session.execute(
        text("SELECT to_regclass('llm_calls') IS NOT NULL"),
    )
    return bool(res.scalar())


async def _cloud_compute_records_exists(session: AsyncSession) -> bool:
    res = await session.execute(
        text("SELECT to_regclass('cloud_compute_records') IS NOT NULL"),
    )
    return bool(res.scalar())


@router.get("/usage")
async def get_usage(
    request: Request,
    sc: SessionAndCtx,
    start: Annotated[date, Query()],
    end: Annotated[date, Query()],
    team_id: Annotated[UUID | None, Query()] = None,
    group_by: Annotated[str, Query()] = "day",
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

    s, ctx = sc

    target_team = team_id
    if target_team is not None:
        require_team_or_admin(ctx, target_team)
    elif not is_admin(ctx):
        target_team = ctx.team_id

    if not await _llm_calls_exists(s):
        return {"buckets": [], "degraded": True}

    has_cloud = await _cloud_compute_records_exists(s)

    start_ts = datetime.combine(start, time.min, tzinfo=UTC)
    end_ts = datetime.combine(end, time.max, tzinfo=UTC)

    params: dict[str, Any] = {"start": start_ts, "end": end_ts}
    team_clause = ""
    cloud_team_clause = ""
    if target_team is not None:
        team_clause = "AND t.team_id = :team_id"
        cloud_team_clause = "AND d.team_id = :team_id"
        params["team_id"] = str(target_team)

    # `date_trunc` argument is a literal interpolated into the SQL
    # text — only the 3 strings in `_TRUNC_UNITS` can reach it, so
    # the substitution is safe even though it isn't parameterized.
    # When the cloud_compute_records table exists (post-Plan-26
    # migration 0008), we LEFT JOIN it for the per-provider compute
    # fields; otherwise we run the original LLM-only query.
    # The cloud CTE uses FILTER aggregates so each provider gets its
    # own column without needing a separate CTE per provider.
    if has_cloud:
        sql = f"""
            WITH llm_buckets AS (
                SELECT
                    date_trunc('{group_by}', l.captured_at) AS bucket_start,
                    COUNT(DISTINCT t.id) AS trial_count,
                    COUNT(DISTINCT t.id)
                        FILTER (WHERE t.state = 'succeeded')
                        AS trials_currently_succeeded,
                    COUNT(DISTINCT t.id)
                        FILTER (WHERE t.state = 'failed')
                        AS trials_currently_failed,
                    COALESCE(SUM(l.cost_usd), 0) AS total_cost_usd,
                    COALESCE(SUM(l.input_tokens), 0) AS llm_input_tokens,
                    COALESCE(SUM(l.output_tokens), 0) AS llm_output_tokens
                  FROM llm_calls l
                  JOIN trials t ON t.id = l.trial_id
                 WHERE l.captured_at BETWEEN :start AND :end
                   {team_clause}
              GROUP BY bucket_start
            ),
            cloud_buckets AS (
                SELECT
                    date_trunc('{group_by}', d.stopped_at) AS bucket_start,
                    COALESCE(
                        SUM(d.compute_seconds)
                            FILTER (WHERE d.cloud_provider = 'daytona'),
                        0
                    ) AS daytona_compute_seconds,
                    COALESCE(
                        SUM(d.cost_usd)
                            FILTER (WHERE d.cloud_provider = 'daytona'),
                        0
                    ) AS daytona_cost_usd,
                    COALESCE(
                        SUM(d.compute_seconds)
                            FILTER (WHERE d.cloud_provider = 'modal'),
                        0
                    ) AS modal_compute_seconds,
                    COALESCE(
                        SUM(d.cost_usd)
                            FILTER (WHERE d.cloud_provider = 'modal'),
                        0
                    ) AS modal_cost_usd,
                    COALESCE(SUM(d.compute_seconds), 0)
                        AS cloud_compute_seconds,
                    COALESCE(SUM(d.cost_usd), 0) AS cloud_cost_usd
                  FROM cloud_compute_records d
                 WHERE d.stopped_at BETWEEN :start AND :end
                   {cloud_team_clause}
              GROUP BY bucket_start
            )
            SELECT
                COALESCE(l.bucket_start, d.bucket_start) AS bucket_start,
                COALESCE(l.trial_count, 0) AS trial_count,
                COALESCE(l.trials_currently_succeeded, 0)
                    AS trials_currently_succeeded,
                COALESCE(l.trials_currently_failed, 0)
                    AS trials_currently_failed,
                COALESCE(l.total_cost_usd, 0) AS total_cost_usd,
                COALESCE(l.llm_input_tokens, 0) AS llm_input_tokens,
                COALESCE(l.llm_output_tokens, 0) AS llm_output_tokens,
                COALESCE(d.daytona_compute_seconds, 0)
                    AS daytona_compute_seconds,
                COALESCE(d.daytona_cost_usd, 0) AS daytona_cost_usd,
                COALESCE(d.modal_compute_seconds, 0)
                    AS modal_compute_seconds,
                COALESCE(d.modal_cost_usd, 0) AS modal_cost_usd,
                COALESCE(d.cloud_compute_seconds, 0)
                    AS cloud_compute_seconds,
                COALESCE(d.cloud_cost_usd, 0) AS cloud_cost_usd
              FROM llm_buckets l
              FULL OUTER JOIN cloud_buckets d USING (bucket_start)
          ORDER BY bucket_start
        """
    else:
        sql = f"""
            SELECT
                date_trunc('{group_by}', l.captured_at) AS bucket_start,
                COUNT(DISTINCT t.id) AS trial_count,
                COUNT(DISTINCT t.id)
                    FILTER (WHERE t.state = 'succeeded')
                    AS trials_currently_succeeded,
                COUNT(DISTINCT t.id)
                    FILTER (WHERE t.state = 'failed')
                    AS trials_currently_failed,
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
        bucket = {
            "start_at": bs.isoformat(),
            "end_at": None,
            "trial_count": int(r.trial_count),
            "trials_currently_succeeded": int(r.trials_currently_succeeded),
            "trials_currently_failed": int(r.trials_currently_failed),
            "succeeded_count": int(r.trials_currently_succeeded),
            "failed_count": int(r.trials_currently_failed),
            "total_cost_usd": float(r.total_cost_usd),
            "llm_input_tokens": int(r.llm_input_tokens),
            "llm_output_tokens": int(r.llm_output_tokens),
            "daytona_compute_seconds": (
                float(r.daytona_compute_seconds) if has_cloud else 0.0
            ),
            "daytona_cost_usd": (
                float(r.daytona_cost_usd) if has_cloud else 0.0
            ),
            "modal_compute_seconds": (
                float(r.modal_compute_seconds) if has_cloud else 0.0
            ),
            "modal_cost_usd": (
                float(r.modal_cost_usd) if has_cloud else 0.0
            ),
            "cloud_compute_seconds": (
                float(r.cloud_compute_seconds) if has_cloud else 0.0
            ),
            "cloud_cost_usd": (
                float(r.cloud_cost_usd) if has_cloud else 0.0
            ),
        }
        buckets.append(bucket)
    return {"buckets": buckets, "degraded": False}
