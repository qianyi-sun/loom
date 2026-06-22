"""Worker-facing trial-cache build coordination (#317 Phase 1).

Workers building a layered agent-install image race for the right to
build. This module exposes 4 short-transaction operations on the
`active_trial_cache_builds` table, so workers can:

- claim a builder slot (atomic INSERT-ON-CONFLICT with TTL stealing)
- check if a slot is held (cheap SELECT used by the waiter poll loop)
- refresh the slot TTL (heartbeat during long builds)
- release the slot (best-effort on completion)

All ops are one short DB statement → won't tie up CP's connection
pool during the 30-90s build. Compare to `pg_advisory_lock`, which
holds the connection for the entire build duration.

Routes live under `/api/v1/internal/trial-cache/*` and require a
worker bearer token (`worker:claim` scope, same as `POST /trials/claim`).
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import verify_bearer_token

router = APIRouter(prefix="/api/v1/internal/trial-cache")


# ─── Service helpers ───────────────────────────────────────────────


async def claim_builder_slot(
    session: AsyncSession,
    *,
    cache_key: str,
    worker_id: UUID,
    ttl_sec: float,
) -> bool:
    """Atomic claim with TTL-based expired-slot stealing.

    Returns True if this worker is now the builder. False if another
    worker holds a non-expired slot. One short transaction."""
    expires = datetime.now(UTC) + timedelta(seconds=ttl_sec)
    result = await session.execute(text("""
        INSERT INTO active_trial_cache_builds
            (cache_key, builder_worker_id, expires_at)
        VALUES (:k, :w, :e)
        ON CONFLICT (cache_key) DO UPDATE
            SET builder_worker_id = EXCLUDED.builder_worker_id,
                started_at = now(),
                expires_at = EXCLUDED.expires_at
            WHERE active_trial_cache_builds.expires_at < now()
        RETURNING builder_worker_id = :w AS i_am_builder
    """), {"k": cache_key, "w": worker_id, "e": expires})
    await session.commit()
    row = result.scalar_one_or_none()
    return bool(row) if row is not None else False


async def release_builder_slot(
    session: AsyncSession,
    *,
    cache_key: str,
    worker_id: UUID,
) -> None:
    """Delete the slot row (only if we still own it — TTL may have
    stolen it, in which case some other worker is the active builder)."""
    await session.execute(text("""
        DELETE FROM active_trial_cache_builds
        WHERE cache_key = :k AND builder_worker_id = :w
    """), {"k": cache_key, "w": worker_id})
    await session.commit()


async def builder_slot_exists(
    session: AsyncSession, *, cache_key: str,
) -> bool:
    """Cheap probe for the waiter loop. Returns True iff a non-expired
    slot exists. No mutation — waiters poll without firing
    INSERT-ON-CONFLICT every tick."""
    result = await session.execute(text("""
        SELECT 1 FROM active_trial_cache_builds
        WHERE cache_key = :k AND expires_at > now()
    """), {"k": cache_key})
    return result.scalar_one_or_none() is not None


async def refresh_builder_slot(
    session: AsyncSession,
    *,
    cache_key: str,
    worker_id: UUID,
    ttl_sec: float,
) -> bool:
    """Heartbeat: extend our slot's TTL. Returns False if we no longer
    own the slot (another worker stole it after our TTL expired). In
    that case the calling builder should abort its in-progress work."""
    expires = datetime.now(UTC) + timedelta(seconds=ttl_sec)
    result = await session.execute(text("""
        UPDATE active_trial_cache_builds
        SET expires_at = :e
        WHERE cache_key = :k AND builder_worker_id = :w
        RETURNING 1
    """), {"k": cache_key, "w": worker_id, "e": expires})
    await session.commit()
    return result.scalar_one_or_none() is not None


# ─── Route layer ───────────────────────────────────────────────────


async def _verify_worker_request(
    request: Request, authorization: str | None,
) -> None:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:claim" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="worker token required")


def _parse_uuid(payload: dict[str, Any], key: str) -> UUID:
    try:
        return UUID(payload[key])
    except (KeyError, ValueError, TypeError) as exc:
        raise HTTPException(
            status_code=400, detail=f"{key} must be a UUID string",
        ) from exc


def _parse_float(payload: dict[str, Any], key: str, *, min_val: float = 0.0) -> float:
    try:
        value = float(payload[key])
    except (KeyError, ValueError, TypeError) as exc:
        raise HTTPException(
            status_code=400, detail=f"{key} must be a float",
        ) from exc
    if value < min_val:
        raise HTTPException(
            status_code=400, detail=f"{key} must be >= {min_val}",
        )
    return value


@router.post("/claim")
async def claim_route(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, bool]:
    """Atomic claim of a builder slot. Body: `cache_key`, `worker_id`,
    `ttl_sec`. Response: `{i_am_builder: bool}`."""
    await _verify_worker_request(request, authorization)
    cache_key = str(payload.get("cache_key") or "")
    if not cache_key:
        raise HTTPException(status_code=400, detail="cache_key required")
    worker_id = _parse_uuid(payload, "worker_id")
    ttl_sec = _parse_float(payload, "ttl_sec", min_val=1.0)

    async with request.app.state.session_factory() as session:
        i_am = await claim_builder_slot(
            session,
            cache_key=cache_key, worker_id=worker_id, ttl_sec=ttl_sec,
        )
    return {"i_am_builder": i_am}


@router.get("/{cache_key}")
async def exists_route(
    request: Request,
    cache_key: str,
    authorization: str | None = Header(default=None),
) -> dict[str, bool]:
    """Cheap slot-existence probe for waiter polls."""
    await _verify_worker_request(request, authorization)
    if not cache_key:
        raise HTTPException(status_code=400, detail="cache_key required")
    async with request.app.state.session_factory() as session:
        exists = await builder_slot_exists(session, cache_key=cache_key)
    return {"exists": exists}


@router.delete("/{cache_key}", status_code=204)
async def release_route(
    request: Request,
    cache_key: str,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> None:
    """Delete the slot if we still own it."""
    await _verify_worker_request(request, authorization)
    if not cache_key:
        raise HTTPException(status_code=400, detail="cache_key required")
    worker_id = _parse_uuid(payload, "worker_id")
    async with request.app.state.session_factory() as session:
        await release_builder_slot(
            session, cache_key=cache_key, worker_id=worker_id,
        )


@router.post("/{cache_key}/refresh")
async def refresh_route(
    request: Request,
    cache_key: str,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, bool]:
    """Heartbeat: extend our slot's TTL. Returns `{refreshed: False}`
    if we no longer own the slot."""
    await _verify_worker_request(request, authorization)
    if not cache_key:
        raise HTTPException(status_code=400, detail="cache_key required")
    worker_id = _parse_uuid(payload, "worker_id")
    ttl_sec = _parse_float(payload, "ttl_sec", min_val=1.0)
    async with request.app.state.session_factory() as session:
        refreshed = await refresh_builder_slot(
            session,
            cache_key=cache_key, worker_id=worker_id, ttl_sec=ttl_sec,
        )
    return {"refreshed": refreshed}
