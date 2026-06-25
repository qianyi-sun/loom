"""Trajectory paginated read + authenticated download (spec §5.2) +
seq-cursor event replay + SSE live stream (#5 Slice 1 / Slice 3c).

The legacy events.jsonl object lives in the same `trajectories`
bucket the worker's TrajectoryWriter writes to, at the key
`<team_id>/<trial_id>/events.jsonl`. Slice 1's first cut of
`/trials/{id}/events?after_seq=N` + `/trials/{id}/stream` read
events from there.

#5 Slice 3c flips both reads to the Postgres `trial_events` table
(populated by Slice 3a's CP endpoint via Slice 3b's worker
dual-write). MinIO remains the audit-log copy and the trajectory
download endpoint; the cursor-based read now hits an indexed table
instead of fetching + splitting a multi-MB object.

MinIO fallback: a trial whose worker shipped before Slice 3b will
have 0 rows in `trial_events` but a populated MinIO trajectory. To
preserve UX for those trials, the route falls back to MinIO when
Postgres returns no rows AND the trial state suggests events
should exist. The fallback is removed in a future cleanup pass
once 3b has been deployed long enough to cover all observable
trials.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Annotated, Any, cast
from uuid import UUID

from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select

from loom.db.schema import Trial, TrialEvent
from loom_service.auth_guards import (
    require_scope,
    require_team_or_admin,
)
from loom_service.dependencies import SessionAndCtx
from loom_service.routes.object_downloads import stream_object_response

logger = logging.getLogger(__name__)

router = APIRouter()

_TERMINAL_TRIAL_STATES = frozenset({"succeeded", "failed", "cancelled"})
# Default SSE polling cadence — the backend reads the MinIO object on
# this interval to detect new events. Phase 2 (Postgres event table +
# LISTEN/NOTIFY) makes this push-based; until then this trades a few
# seconds of latency for zero new infra. Kept small enough that the
# user-visible "live" feel is preserved.
_DEFAULT_SSE_POLL_INTERVAL_SEC = 1.5
# Cap any single SSE response at this long to bound resource cost
# from forgotten browser tabs and to give the client a deterministic
# reconnect point. The client should reconnect with the last seen
# seq as `after_seq` — standard SSE Last-Event-ID semantics.
_DEFAULT_SSE_MAX_CONNECTION_SEC = 600.0


def _key(team_id: UUID, trial_id: UUID) -> str:
    return f"{team_id}/{trial_id}/events.jsonl"


async def _load_trial(session: Any, trial_id: UUID, ctx: Any) -> Trial:
    trial = (await session.execute(
        select(Trial).where(Trial.id == trial_id),
    )).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    require_team_or_admin(ctx, trial.team_id)
    return cast(Trial, trial)


def _read_events_after_seq(
    client: Any,
    *,
    bucket: str,
    key: str,
    after_seq: int,
    limit: int,
) -> list[dict[str, Any]]:
    """Fetch the MinIO object and return events whose `seq` is strictly
    greater than `after_seq`, ordered by seq, capped at `limit`.

    Mirrors the line-cursor read in `list_events` but pivots on the
    event payload's own `seq` field. This is what the upcoming
    Postgres event table indexes on, so consumers built against this
    endpoint don't change when the storage moves.

    Events without a numeric `seq` are skipped — they can't be ordered
    or resumed against. The worker's TrajectoryWriter always emits
    `seq`, so this only filters legacy/test data.
    """
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404"):
            return []
        raise
    out: list[dict[str, Any]] = []
    body = obj["Body"]
    try:
        for raw in body.iter_lines():
            text = raw.decode("utf-8", errors="replace").strip()
            if not text:
                continue
            try:
                ev = json.loads(text)
            except json.JSONDecodeError:
                continue
            seq = ev.get("seq")
            if not isinstance(seq, int):
                continue
            if seq <= after_seq:
                continue
            out.append(ev)
            if len(out) >= limit:
                break
    finally:
        body.close()
    out.sort(key=lambda e: e["seq"])
    return out


async def _read_events_from_postgres(
    session: Any, *, trial_id: UUID, after_seq: int, limit: int,
) -> list[dict[str, Any]]:
    """Slice 3c primary read path. Pulls payloads from the
    `trial_events` table indexed on (trial_id, seq).

    The payload column already carries the full typed event body
    (kind, seq, emitted_at, plus per-type fields), so the response
    shape matches what `_read_events_after_seq` returns from MinIO —
    callers can swap reads transparently."""
    rows = (
        await session.execute(
            select(TrialEvent.payload)
            .where(
                TrialEvent.trial_id == trial_id,
                TrialEvent.seq > after_seq,
            )
            .order_by(TrialEvent.seq.asc())
            .limit(limit),
        )
    ).all()
    return [row[0] for row in rows]


@router.get("/trials/{trial_id}/trajectory")
async def list_events(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
    cursor: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(gt=0, le=1000)] = 200,
) -> dict[str, Any]:
    settings = request.app.state.settings
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = await _load_trial(s, trial_id, ctx)

    client = request.app.state.minio_client
    try:
        obj = client.get_object(
            Bucket=settings.trajectories_bucket,
            Key=_key(trial.team_id, trial.id),
        )
    except ClientError as exc:
        # A missing object means the trial hasn't written a first event
        # yet (queued/just-claimed) OR the worker crashed pre-first-event.
        # Either way we return an empty page rather than 404 — the trial
        # row exists (we already validated), so the UI's polling loop
        # should show "no events yet" not a scary 404. Other S3 errors
        # (perms, bucket missing) keep propagating.
        code = exc.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404"):
            return {"events": [], "next_cursor": None}
        raise
    # Stream-decode the JSONL line by line instead of materializing
    # the whole object into memory — a 100k-event trial would otherwise
    # cost ~200 MB raw + ~400 MB after split. `iter_lines()` lets us
    # skip lines up to `cursor`, decode `limit` events, and exit
    # early; remaining bytes stay on the wire (the response.close()
    # triggers a connection-close).
    body = obj["Body"]
    events: list[dict[str, Any]] = []
    next_cursor: int | None = None
    line_index = 0  # 1-based count of non-blank lines seen
    try:
        for raw in body.iter_lines():
            text = raw.decode("utf-8", errors="replace").strip()
            if not text:
                continue
            line_index += 1
            if line_index <= cursor:
                continue
            if len(events) >= limit:
                next_cursor = cursor + limit
                break
            try:
                events.append(json.loads(text))
            except json.JSONDecodeError:
                # Tolerate truncation tails — finalize crashes can
                # leave a partial last line.
                continue
    finally:
        body.close()
    return {"events": events, "next_cursor": next_cursor}


@router.get("/trials/{trial_id}/trajectory/download")
async def download_trajectory(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
) -> StreamingResponse:
    settings = request.app.state.settings
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = await _load_trial(s, trial_id, ctx)

    return stream_object_response(
        client=request.app.state.minio_client,
        bucket=settings.trajectories_bucket,
        key=_key(trial.team_id, trial.id),
        filename=f"{trial.id}-events.jsonl",
        artifact_kind="trajectory",
        media_type="application/x-ndjson",
    )


async def _read_events_with_minio_fallback(
    session: Any,
    minio_client: Any,
    *,
    trial: Trial,
    bucket: str,
    after_seq: int,
    limit: int,
) -> list[dict[str, Any]]:
    """Primary: read from `trial_events` (#5 Slice 3c).

    Fallback: if the table is empty for this trial AND the trial has
    reached a state where the worker should have shipped events
    (running/succeeded/failed/cancelled), read from MinIO. This
    preserves UX for trials whose worker shipped before Slice 3b
    (those have a populated MinIO trajectory but no Postgres rows).
    The fallback path will be removed in a future cleanup pass.

    Trials still in `queued` or `claimed` correctly return [] from
    both paths — no events have been emitted yet, so 0 is the right
    answer regardless of source.
    """
    events = await _read_events_from_postgres(
        session, trial_id=trial.id, after_seq=after_seq, limit=limit,
    )
    if events:
        return events
    if trial.state in _TERMINAL_TRIAL_STATES or trial.state == "running":
        # Empty Postgres + a state that should have produced events =
        # legacy trial. Fall through to MinIO to preserve UX. Log so
        # the fallback rate is observable and a future cleanup can
        # remove the path once it drops to ~0.
        legacy = await asyncio.to_thread(
            _read_events_after_seq,
            minio_client,
            bucket=bucket,
            key=_key(trial.team_id, trial.id),
            after_seq=after_seq, limit=limit,
        )
        if legacy:
            logger.info(
                "events_minio_fallback trial=%s state=%s n=%d "
                "after_seq=%d — pre-Slice-3b trial, served from MinIO",
                trial.id, trial.state, len(legacy), after_seq,
            )
        return legacy
    return events


@router.get("/trials/{trial_id}/events")
async def list_events_by_seq(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
    after_seq: Annotated[int, Query(ge=-1)] = -1,
    limit: Annotated[int, Query(gt=0, le=1000)] = 200,
) -> dict[str, Any]:
    """Return events with `seq > after_seq`, capped at `limit`.

    Slice 3c reads from the Postgres `trial_events` table. MinIO is
    the fallback for legacy trials (worker shipped before Slice 3b).

    Use `after_seq=-1` (the default) to start from the beginning;
    `after_seq=N` to resume after event seq `N`. `next_after_seq` in
    the response is the seq of the last event returned, or `null`
    when no events were returned (caller should re-poll with the
    same cursor)."""
    settings = request.app.state.settings
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = await _load_trial(s, trial_id, ctx)

    events = await _read_events_with_minio_fallback(
        s,
        request.app.state.minio_client,
        trial=trial,
        bucket=settings.trajectories_bucket,
        after_seq=after_seq,
        limit=limit,
    )
    next_after_seq = events[-1]["seq"] if events else None
    return {"events": events, "next_after_seq": next_after_seq}


def _sse_format(event_kind: str | None, data: dict[str, Any], event_id: str | None = None) -> bytes:
    """Format one SSE message. `event_kind` is optional (defaults to
    `message`); `event_id` populates `id:` so clients reconnect with
    Last-Event-ID."""
    lines: list[str] = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    if event_kind is not None:
        lines.append(f"event: {event_kind}")
    payload = json.dumps(data, separators=(",", ":"))
    lines.append(f"data: {payload}")
    return ("\n".join(lines) + "\n\n").encode("utf-8")


@router.get("/trials/{trial_id}/stream")
async def stream_events(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
    after_seq: Annotated[int, Query(ge=-1)] = -1,
) -> StreamingResponse:
    """SSE live event stream for `trial_id`, starting at `after_seq + 1`.

    The connection emits all available events on first read, then
    polls MinIO every `_DEFAULT_SSE_POLL_INTERVAL_SEC` for new events,
    and terminates when the trial reaches a terminal state OR the
    client disconnects OR the connection has been open for longer
    than `_DEFAULT_SSE_MAX_CONNECTION_SEC` (clients reconnect with
    the last seen seq as `after_seq`).

    Phase 2 (Postgres event table + LISTEN/NOTIFY) will replace the
    inner poll with push semantics; the on-wire contract here stays
    stable so callers don't change.

    The frontend should still implement an `useAdaptivePolling`
    fallback for environments without working `EventSource` (some
    corp proxies strip `text/event-stream`).
    """
    settings = request.app.state.settings
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = await _load_trial(s, trial_id, ctx)

    session_factory = request.app.state.session_factory
    minio_client = request.app.state.minio_client
    bucket = settings.trajectories_bucket
    poll_interval = _DEFAULT_SSE_POLL_INTERVAL_SEC
    max_connection_sec = _DEFAULT_SSE_MAX_CONNECTION_SEC

    async def _read_loop_chunk(
        seq_cursor: int,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """One poll iteration: read events from Postgres (with MinIO
        fallback) AND check trial state. Returning both from one
        helper keeps the per-iteration session lifetime clean."""
        async with session_factory() as fresh:
            fresh_trial = (await fresh.execute(
                select(Trial).where(Trial.id == trial.id),
            )).scalar_one_or_none()
            if fresh_trial is None:
                return [], None
            evts = await _read_events_with_minio_fallback(
                fresh,
                minio_client,
                trial=fresh_trial,
                bucket=bucket,
                after_seq=seq_cursor,
                limit=200,
            )
            return evts, fresh_trial.state

    async def event_source() -> AsyncIterator[bytes]:
        current_seq = after_seq
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        # Emit a comment line up front so any proxy that buffers SSE
        # gets a flush before the first real event lands.
        yield b": stream open\n\n"
        while True:
            if await request.is_disconnected():
                return
            events, current_state = await _read_loop_chunk(current_seq)
            for ev in events:
                yield _sse_format(
                    event_kind=None,
                    data=ev,
                    event_id=str(ev["seq"]),
                )
                current_seq = int(ev["seq"])

            # Terminal-state detection — once terminal AND we've
            # emitted everything currently available, close cleanly.
            if current_state in _TERMINAL_TRIAL_STATES:
                # One more read to flush any events that landed
                # between the previous read and the state check.
                tail, _ = await _read_loop_chunk(current_seq)
                for ev in tail:
                    yield _sse_format(
                        event_kind=None,
                        data=ev,
                        event_id=str(ev["seq"]),
                    )
                    current_seq = int(ev["seq"])
                yield _sse_format(
                    event_kind="complete",
                    data={"final_state": current_state, "last_seq": current_seq},
                )
                return

            # Connection-budget exhaustion: client reconnects with
            # `after_seq=current_seq` to resume — standard SSE
            # Last-Event-ID semantics.
            if loop.time() - started_at >= max_connection_sec:
                yield _sse_format(
                    event_kind="reconnect",
                    data={"reason": "max_connection_sec", "last_seq": current_seq},
                )
                return

            await asyncio.sleep(poll_interval)

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={
            # Block proxy/CDN buffering — SSE needs every chunk
            # flushed immediately.
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
