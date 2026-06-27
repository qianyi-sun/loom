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
from loom_service.usage_accounting import summarize_usage_counts

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
    include_batches: Annotated[bool, Query()] = False,
) -> dict[str, Any]:
    if group_by not in _TRUNC_UNITS:
        raise HTTPException(
            status_code=400,
            detail=f"group_by must be one of {sorted(_TRUNC_UNITS)}",
        )
    if end < start:
        raise HTTPException(
            status_code=400,
            detail="end must be >= start",
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
                    COALESCE(SUM(l.output_tokens), 0) AS llm_output_tokens,
                    COUNT(l.id)
                        FILTER (
                            WHERE l.rate_card_hash NOT LIKE 'facade:tokens-only%%'
                              AND l.rate_card_hash NOT LIKE 'facade:rate-card:missing%%'
                        ) AS priced_llm_calls_count,
                    COUNT(l.id)
                        FILTER (
                            WHERE l.rate_card_hash LIKE 'facade:tokens-only%%'
                        ) AS token_only_llm_calls_count,
                    COUNT(l.id)
                        FILTER (
                            WHERE l.rate_card_hash LIKE 'facade:rate-card:missing%%'
                        ) AS price_unknown_llm_calls_count
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
                COALESCE(l.priced_llm_calls_count, 0)
                    AS priced_llm_calls_count,
                COALESCE(l.token_only_llm_calls_count, 0)
                    AS token_only_llm_calls_count,
                COALESCE(l.price_unknown_llm_calls_count, 0)
                    AS price_unknown_llm_calls_count,
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
                COALESCE(SUM(l.output_tokens), 0) AS llm_output_tokens,
                COUNT(l.id)
                    FILTER (
                        WHERE l.rate_card_hash NOT LIKE 'facade:tokens-only%%'
                          AND l.rate_card_hash NOT LIKE 'facade:rate-card:missing%%'
                    ) AS priced_llm_calls_count,
                COUNT(l.id)
                    FILTER (
                        WHERE l.rate_card_hash LIKE 'facade:tokens-only%%'
                    ) AS token_only_llm_calls_count,
                COUNT(l.id)
                    FILTER (
                        WHERE l.rate_card_hash LIKE 'facade:rate-card:missing%%'
                    ) AS price_unknown_llm_calls_count
              FROM llm_calls l
              JOIN trials t ON t.id = l.trial_id
             WHERE l.captured_at BETWEEN :start AND :end
               {team_clause}
          GROUP BY bucket_start
          ORDER BY bucket_start
        """
    rows = (await s.execute(text(sql), params)).all()
    batches_by_bucket = (
        await _batch_usage_rollups(
            s,
            group_by=group_by,
            params=params,
            team_clause=team_clause,
        )
        if include_batches
        else {}
    )

    buckets: list[dict[str, Any]] = []
    for r in rows:
        bs = r.bucket_start
        usage = summarize_usage_counts(
            llm_calls_count=int(
                (r.priced_llm_calls_count or 0)
                + (r.token_only_llm_calls_count or 0)
                + (r.price_unknown_llm_calls_count or 0)
            ),
            total_prompt_tokens=int(r.llm_input_tokens or 0),
            total_completion_tokens=int(r.llm_output_tokens or 0),
            total_cost_usd=r.total_cost_usd,
            priced_llm_calls_count=int(r.priced_llm_calls_count or 0),
            token_only_llm_calls_count=int(r.token_only_llm_calls_count or 0),
            price_unknown_llm_calls_count=int(
                r.price_unknown_llm_calls_count or 0,
            ),
        )
        bucket = {
            "start_at": bs.isoformat(),
            "end_at": None,
            "trial_count": int(r.trial_count),
            "trials_currently_succeeded": int(r.trials_currently_succeeded),
            "trials_currently_failed": int(r.trials_currently_failed),
            "succeeded_count": int(r.trials_currently_succeeded),
            "failed_count": int(r.trials_currently_failed),
            "total_cost_usd": usage["total_cost_usd"],
            "estimated_cost_usd": usage["estimated_cost_usd"],
            "cost_currency": usage["cost_currency"],
            "cost_status": usage["cost_status"],
            "pricing_modes": usage["pricing_modes"],
            "priced_llm_calls_count": usage["priced_llm_calls_count"],
            "token_only_llm_calls_count": usage["token_only_llm_calls_count"],
            "price_unknown_llm_calls_count": usage["price_unknown_llm_calls_count"],
            "llm_input_tokens": int(r.llm_input_tokens),
            "llm_output_tokens": int(r.llm_output_tokens),
            "daytona_compute_seconds": (float(r.daytona_compute_seconds) if has_cloud else 0.0),
            "daytona_cost_usd": (float(r.daytona_cost_usd) if has_cloud else 0.0),
            "modal_compute_seconds": (float(r.modal_compute_seconds) if has_cloud else 0.0),
            "modal_cost_usd": (float(r.modal_cost_usd) if has_cloud else 0.0),
            "cloud_compute_seconds": (float(r.cloud_compute_seconds) if has_cloud else 0.0),
            "cloud_cost_usd": (float(r.cloud_cost_usd) if has_cloud else 0.0),
        }
        if include_batches:
            bucket["batches"] = batches_by_bucket.get(bs, [])
        buckets.append(bucket)
    return {"buckets": buckets, "degraded": False}


async def _batch_usage_rollups(
    session: AsyncSession,
    *,
    group_by: str,
    params: dict[str, Any],
    team_clause: str,
) -> dict[datetime, list[dict[str, Any]]]:
    sql = f"""
        SELECT
            date_trunc('{group_by}', l.captured_at) AS bucket_start,
            b.id AS batch_id,
            b.name AS batch_name,
            t.team_id AS team_id,
            tm.name AS team_name,
            COUNT(DISTINCT t.id) AS trial_count,
            COALESCE(SUM(l.cost_usd), 0) AS total_cost_usd,
            COALESCE(SUM(l.input_tokens), 0) AS llm_input_tokens,
            COALESCE(SUM(l.output_tokens), 0) AS llm_output_tokens,
            COUNT(l.id)
                FILTER (
                    WHERE l.rate_card_hash NOT LIKE 'facade:tokens-only%%'
                      AND l.rate_card_hash NOT LIKE 'facade:rate-card:missing%%'
                ) AS priced_llm_calls_count,
            COUNT(l.id)
                FILTER (
                    WHERE l.rate_card_hash LIKE 'facade:tokens-only%%'
                ) AS token_only_llm_calls_count,
            COUNT(l.id)
                FILTER (
                    WHERE l.rate_card_hash LIKE 'facade:rate-card:missing%%'
                ) AS price_unknown_llm_calls_count
          FROM llm_calls l
          JOIN trials t ON t.id = l.trial_id
          JOIN batches b ON b.id = t.batch_id
          LEFT JOIN teams tm ON tm.id = t.team_id
         WHERE l.captured_at BETWEEN :start AND :end
           {team_clause}
      GROUP BY bucket_start, b.id, b.name, t.team_id, tm.name
      ORDER BY bucket_start, b.name, b.id
    """
    out: dict[datetime, list[dict[str, Any]]] = {}
    rows = (await session.execute(text(sql), params)).all()
    for row in rows:
        usage = summarize_usage_counts(
            llm_calls_count=int(
                (row.priced_llm_calls_count or 0)
                + (row.token_only_llm_calls_count or 0)
                + (row.price_unknown_llm_calls_count or 0)
            ),
            total_prompt_tokens=int(row.llm_input_tokens or 0),
            total_completion_tokens=int(row.llm_output_tokens or 0),
            total_cost_usd=row.total_cost_usd,
            priced_llm_calls_count=int(row.priced_llm_calls_count or 0),
            token_only_llm_calls_count=int(row.token_only_llm_calls_count or 0),
            price_unknown_llm_calls_count=int(
                row.price_unknown_llm_calls_count or 0,
            ),
        )
        item = {
            "batch_id": str(row.batch_id),
            "batch_name": row.batch_name,
            "team_id": str(row.team_id),
            "team_name": row.team_name,
            "trial_count": int(row.trial_count or 0),
            "llm_input_tokens": usage["total_prompt_tokens"],
            "llm_output_tokens": usage["total_completion_tokens"],
            **{
                key: value
                for key, value in usage.items()
                if key
                not in {
                    "total_prompt_tokens",
                    "total_completion_tokens",
                }
            },
        }
        out.setdefault(row.bucket_start, []).append(item)
    return out
