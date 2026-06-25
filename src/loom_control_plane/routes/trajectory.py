"""Trajectory index PATCH + read endpoints + event ingest (#5 Slice 3a)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import bindparam, select
from sqlalchemy import text as sql_text
from sqlalchemy.dialects.postgresql import JSONB

from loom.auth import verify_bearer_token
from loom.db.schema import Trial as TrialRow

router = APIRouter()


# #5 Slice 3a: batched event ingest. Workers POST batches of typed
# trajectory events here; CP appends them to `trial_events`. The
# UNIQUE (trial_id, seq) index doubles as the idempotency key — a
# worker that retries after a partial ack gets `inserted=N` reflecting
# only the rows that actually landed, with no error on dupes.
#
# Worker fence: the row is gated on `worker_id = :worker_id` matching
# the trial's current owner — same pattern as `_INDEX_PATCH` above.
# A reclaim that nulled the trial's worker_id 409s the batch; the
# worker should give up and let the reclaim-sweep / runner reassign.
_INSERT_EVENT_SQL = sql_text("""
INSERT INTO trial_events (
    trial_id, seq, kind, source, schema_version, payload
)
VALUES (
    (:trial_id)::uuid,
    (:seq)::bigint,
    (:kind)::text,
    (:source)::text,
    (:schema_version)::int,
    :payload
)
ON CONFLICT (trial_id, seq) DO NOTHING
RETURNING seq;
""").bindparams(bindparam("payload", type_=JSONB))


_MAX_BATCH = 500
_MAX_PAYLOAD_BYTES = 256 * 1024  # 256 KiB per event payload


_INDEX_PATCH = sql_text("""
UPDATE trials
   SET trajectory_index = :index_payload,
       result = CASE WHEN (:has_result)::boolean
                     THEN :result_payload ELSE result END
 WHERE id = (:trial_id)::uuid AND worker_id = (:worker_id)::uuid
 RETURNING id;
""").bindparams(
    bindparam("index_payload", type_=JSONB),
    bindparam("result_payload", type_=JSONB),
)


@router.patch("/trials/{trial_id}/trajectory_index")
async def patch_trajectory_index(
    trial_id: UUID,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:index" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized")

    try:
        worker_id = UUID(payload["worker_id"])
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=400, detail=f"worker_id required: {exc}",
        ) from exc
    result_payload = payload.get("result")
    index_payload = {
        k: v for k, v in payload.items()
        if k not in {"worker_id", "result"}
    }

    async with request.app.state.session_factory() as session:
        row = (await session.execute(_INDEX_PATCH, {
            "trial_id": trial_id, "worker_id": worker_id,
            "index_payload": index_payload,
            "result_payload": result_payload,
            "has_result": result_payload is not None,
        })).mappings().one_or_none()
        await session.commit()
    if row is None:
        raise HTTPException(status_code=409, detail="worker lost claim")
    return {"trial_id": str(row["id"])}


@router.get("/trials/{trial_id}/trajectory")
async def get_trajectory_url(
    trial_id: UUID,
    request: Request,
    authorization: str | None = Header(default=None),
) -> RedirectResponse:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")
    async with request.app.state.session_factory() as session:
        row = (await session.execute(
            select(TrialRow).where(TrialRow.id == trial_id),
        )).scalar_one_or_none()
    if row is None or not row.trajectory_index:
        raise HTTPException(status_code=404, detail="no trajectory recorded")
    if ctx.team_id is not None and row.team_id != ctx.team_id:
        raise HTTPException(
            status_code=403, detail="trajectory belongs to another team",
        )

    settings = request.app.state.settings
    url = request.app.state.minio_client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": "trajectories",
            "Key": f"{row.team_id}/{trial_id}/events.jsonl",
        },
        ExpiresIn=settings.signed_url_expiry_sec,
    )
    return RedirectResponse(url=url, status_code=302)


@router.post("/trials/{trial_id}/events")
async def append_events(
    trial_id: UUID,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    """Append a batch of typed trajectory events to `trial_events`.

    Body shape:
        {
            "worker_id": "<uuid>",
            "events": [
                {
                    "seq": 0,
                    "kind": "trial_start",
                    "source": "worker",
                    "schema_version": 1,
                    "payload": {<TrajectoryEvent body>},
                },
                ...
            ],
        }

    Per-event behavior:
    - INSERT ... ON CONFLICT (trial_id, seq) DO NOTHING
    - Duplicates from worker retries return inserted=N reflecting only
      newly-landed rows; no error.
    - Worker fence: the trial's current `worker_id` must match the
      `worker_id` in the body. Mismatch = 409 (worker lost claim);
      writers should give up and let reclaim re-route.

    Limits:
    - At most `_MAX_BATCH` events per request (500).
    - Each event's payload at most `_MAX_PAYLOAD_BYTES` (256 KiB).
    - Both limits are 413 / 400 respectively — bigger payloads or
      bigger batches indicate an upstream bug, not a normal flow.
    """
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:index" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized")

    try:
        worker_id = UUID(payload["worker_id"])
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=400, detail=f"worker_id required: {exc}",
        ) from exc

    events = payload.get("events")
    if not isinstance(events, list) or not events:
        raise HTTPException(
            status_code=400, detail="events must be a non-empty list",
        )
    if len(events) > _MAX_BATCH:
        raise HTTPException(
            status_code=413,
            detail=f"batch too large: {len(events)} > {_MAX_BATCH}",
        )

    # Pre-validate every event up front so we either accept or reject
    # the whole batch — partial inserts followed by a 400 would force
    # workers into per-event recovery logic.
    rows: list[dict[str, Any]] = []
    for i, ev in enumerate(events):
        if not isinstance(ev, dict):
            raise HTTPException(
                status_code=400,
                detail=f"events[{i}] must be an object",
            )
        try:
            seq = int(ev["seq"])
            kind = str(ev["kind"])
            source = str(ev["source"])
            schema_version = int(ev.get("schema_version", 1))
            evt_payload = ev["payload"]
        except (KeyError, TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"events[{i}] missing/invalid field: {exc}. Required "
                    "keys: seq (int>=0), kind (str), source (str), "
                    "payload (object); optional schema_version (int>=1)."
                ),
            ) from exc
        if seq < 0:
            raise HTTPException(
                status_code=400,
                detail=f"events[{i}].seq must be >= 0",
            )
        if schema_version < 1:
            raise HTTPException(
                status_code=400,
                detail=f"events[{i}].schema_version must be >= 1",
            )
        if not isinstance(evt_payload, dict):
            raise HTTPException(
                status_code=400,
                detail=f"events[{i}].payload must be an object",
            )
        # Cheap bytes-bound on payload — gate against an oversized
        # event slipping past the multipart layer. Approximate via
        # repr length; a tighter check would re-serialize but the
        # repr is close enough for the safety floor.
        if len(repr(evt_payload)) > _MAX_PAYLOAD_BYTES:
            raise HTTPException(
                status_code=413,
                detail=(
                    f"events[{i}].payload exceeds "
                    f"{_MAX_PAYLOAD_BYTES} bytes"
                ),
            )
        rows.append({
            "trial_id": trial_id,
            "seq": seq,
            "kind": kind,
            "source": source,
            "schema_version": schema_version,
            "payload": evt_payload,
        })

    # Fence check: refuse the whole batch if the trial's current
    # worker_id doesn't match. Worker reclaim nulls worker_id, so a
    # reclaim mid-batch surfaces here as a 409.
    async with request.app.state.session_factory() as session:
        owner_row = (await session.execute(
            select(TrialRow.worker_id).where(TrialRow.id == trial_id),
        )).one_or_none()
        if owner_row is None:
            raise HTTPException(status_code=404, detail="trial not found")
        if owner_row[0] != worker_id:
            raise HTTPException(
                status_code=409, detail="worker lost claim",
            )

        inserted = 0
        for row_params in rows:
            result = await session.execute(_INSERT_EVENT_SQL, row_params)
            if result.first() is not None:
                inserted += 1
        await session.commit()

    return {
        "trial_id": str(trial_id),
        "submitted": len(rows),
        "inserted": inserted,
        "deduped": len(rows) - inserted,
    }
