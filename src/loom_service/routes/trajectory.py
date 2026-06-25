"""Trajectory paginated read + authenticated download (spec §5.2) +
seq-cursor event replay + SSE live stream (#5 Slice 1).

The events.jsonl object lives in the same `trajectories` bucket the
worker's TrajectoryWriter writes to, at the key
`<team_id>/<trial_id>/events.jsonl`. We fetch the whole object, split
on newlines, slice by integer cursor (line index), and return the
requested page. This is fine for v1 (trajectory files are bounded by
the trial wall budget and event size); future revisions could move
to byte-range reads + sidecar index.

#5 Slice 1 adds two new endpoints alongside the legacy /trajectory:
- `/trials/{id}/events?after_seq=N` — seq-cursor replay (forward-
  compatible naming for the upcoming Postgres event table in Phase 2).
- `/trials/{id}/stream` — SSE wrapper that emits an initial replay
  then polls MinIO for new events until the trial reaches a terminal
  state or the client disconnects. Backend remains poll-based until
  Phase 2's event table enables LISTEN/NOTIFY-style push.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Annotated, Any, cast
from uuid import UUID

from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select

from loom.db.schema import Trial
from loom_service.auth_guards import (
    require_scope,
    require_team_or_admin,
)
from loom_service.dependencies import SessionAndCtx
from loom_service.routes.object_downloads import stream_object_response

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


@router.get("/trials/{trial_id}/events")
async def list_events_by_seq(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
    after_seq: Annotated[int, Query(ge=-1)] = -1,
    limit: Annotated[int, Query(gt=0, le=1000)] = 200,
) -> dict[str, Any]:
    """Return events with `seq > after_seq`, capped at `limit`.

    Forward-compatible naming for the upcoming Postgres event table
    (#5 Phase 2): clients consume seq-cursor semantics today and the
    storage swap stays transparent. Pairs with `/trials/{id}/stream`
    which uses the same seq cursor.

    Use `after_seq=-1` (the default) to start from the beginning;
    `after_seq=N` to resume after event seq `N`. `next_after_seq` in
    the response is the seq of the last event returned, or `null`
    when no events were returned (caller should re-poll with the
    same cursor)."""
    settings = request.app.state.settings
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = await _load_trial(s, trial_id, ctx)

    events = _read_events_after_seq(
        request.app.state.minio_client,
        bucket=settings.trajectories_bucket,
        key=_key(trial.team_id, trial.id),
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
    key = _key(trial.team_id, trial.id)
    poll_interval = _DEFAULT_SSE_POLL_INTERVAL_SEC
    max_connection_sec = _DEFAULT_SSE_MAX_CONNECTION_SEC

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
            events = await asyncio.to_thread(
                _read_events_after_seq,
                minio_client,
                bucket=bucket,
                key=key,
                after_seq=current_seq,
                limit=200,
            )
            for ev in events:
                yield _sse_format(
                    event_kind=None,
                    data=ev,
                    event_id=str(ev["seq"]),
                )
                current_seq = int(ev["seq"])

            # Terminal-state detection — re-read trial state from DB.
            # Once terminal AND we've emitted everything currently
            # in the JSONL, close the stream cleanly.
            async with session_factory() as fresh_session:
                state_row = (await fresh_session.execute(
                    select(Trial.state).where(Trial.id == trial.id),
                )).scalar_one_or_none()
            if state_row in _TERMINAL_TRIAL_STATES:
                # One more read to flush any events that landed
                # between the previous poll and the state check.
                tail = await asyncio.to_thread(
                    _read_events_after_seq,
                    minio_client,
                    bucket=bucket, key=key,
                    after_seq=current_seq, limit=200,
                )
                for ev in tail:
                    yield _sse_format(
                        event_kind=None,
                        data=ev,
                        event_id=str(ev["seq"]),
                    )
                    current_seq = int(ev["seq"])
                yield _sse_format(
                    event_kind="complete",
                    data={"final_state": state_row, "last_seq": current_seq},
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
