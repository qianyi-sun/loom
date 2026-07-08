"""Worker-facing endpoints: claim, register, heartbeat."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy import insert, text, update

from loom.auth import verify_bearer_token
from loom.db.schema import Worker
from loom.models.capabilities import Capabilities
from loom.models.result import FailureReason
from loom_control_plane.metrics import CLAIM_LATENCY_SEC
from loom_control_plane.scheduler.claim import claim_one

router = APIRouter()

_WORKER_HEARTBEAT_STATUSES = {"active", "idle-exit", "shutting-down"}

_REQUEUE_TRIAL_RETRY_SQL = text("""
UPDATE trials t
   SET state = 'queued',
       worker_id = NULL,
       failure_reason = (:failure_reason)::text,
       failure_message = (:failure_message)::text,
       next_attempt_at = NOW() + (:retry_after_sec)::double precision
                         * INTERVAL '1 second'
  FROM team_quotas q
 WHERE t.id = (:trial_id)::uuid
   AND t.worker_id = (:worker_id)::uuid
   AND t.state = 'claimed'
   AND t.started_at IS NULL
   AND q.team_id = t.team_id
   AND t.attempt_count < q.max_attempts_ceiling
 RETURNING t.id;
""")

_PRE_START_HEARTBEAT_SQL = text("""
UPDATE trials
   SET pre_start_heartbeat_at = NOW()
 WHERE id = (:trial_id)::uuid
   AND worker_id = (:worker_id)::uuid
   AND state = 'claimed'
   AND started_at IS NULL
 RETURNING id, pre_start_heartbeat_at;
""")


@router.post("/trials/claim")
async def claim_trial(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> Response:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:claim" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized to claim")

    try:
        worker_id = UUID(payload["worker_id"])
        caps = payload["caps"]
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=400, detail=f"worker_id + caps required: {exc}",
        ) from exc

    worker_os = sorted({c["os"] for c in caps})
    worker_cpu_arches = sorted({c.get("cpu_arch", "x86_64") for c in caps})
    worker_gpu = sorted({c["gpu_vendor"] for c in caps})
    worker_network = sorted({
        p for c in caps for p in c["network_policies"]
    })

    import time as _time
    t0 = _time.perf_counter()
    async with request.app.state.session_factory() as session:
        row = await claim_one(
            session,
            worker_id=worker_id,
            worker_os=worker_os, worker_cpu_arches=worker_cpu_arches,
            worker_gpu_vendors=worker_gpu,
            worker_network_policies=worker_network,
        )
        await session.commit()
    elapsed = _time.perf_counter() - t0

    if row is None:
        CLAIM_LATENCY_SEC.labels(result="miss").observe(elapsed)
        return Response(status_code=204)
    CLAIM_LATENCY_SEC.labels(result="hit").observe(elapsed)

    pc_id = row["provider_connection_id"]
    # #672 family-runs: propagate the family gate so the worker can
    # bind-mount the resolved state_uri before starting the sandbox.
    family_run_spec = row.get("family_run_spec") if hasattr(row, "get") else row["family_run_spec"]
    family_state_uri = row.get("family_state_uri") if hasattr(row, "get") else row["family_state_uri"]
    family_key = row.get("family_key") if hasattr(row, "get") else row["family_key"]
    return JSONResponse({
        "trial_id": str(row["id"]),
        "team_id": str(row["team_id"]),
        "task_id": row["task_id"],
        "config": row["config"],
        "requires_caps": row["requires_caps"],
        "attempt_count": row["attempt_count"],
        "provider_connection_id": (
            str(pc_id) if pc_id is not None else None
        ),
        "family_key": family_key,
        "family_state_uri": family_state_uri,
        "family_run_spec": family_run_spec,
        "state": "claimed",
    })


@router.post("/trials/{trial_id}/retry")
async def requeue_trial_retry(
    trial_id: UUID,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:report" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized to report")

    try:
        worker_id = UUID(payload["worker_id"])
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=400, detail=f"worker_id required: {exc}",
        ) from exc

    failure_reason_str = payload.get("failure_reason")
    if not isinstance(failure_reason_str, str):
        raise HTTPException(
            status_code=400,
            detail="failure_reason must be a string",
        )
    try:
        failure_reason = FailureReason(failure_reason_str)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"invalid failure_reason {failure_reason_str!r}",
        ) from exc

    failure_message = payload.get("failure_message")
    if failure_message is not None and not isinstance(failure_message, str):
        raise HTTPException(
            status_code=400,
            detail="failure_message must be a string",
        )

    try:
        retry_after_sec = float(payload["retry_after_sec"])
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"retry_after_sec required: {exc}",
        ) from exc
    if retry_after_sec < 0:
        raise HTTPException(
            status_code=400,
            detail="retry_after_sec must be >= 0",
        )

    async with request.app.state.session_factory() as session:
        row = (
            await session.execute(
                _REQUEUE_TRIAL_RETRY_SQL,
                {
                    "trial_id": trial_id,
                    "worker_id": worker_id,
                    "failure_reason": failure_reason.value,
                    "failure_message": failure_message,
                    "retry_after_sec": retry_after_sec,
                },
            )
        ).mappings().one_or_none()
        await session.commit()

    if row is None:
        raise HTTPException(
            status_code=409,
            detail=(
                "worker lost claim, trial has started, or retry transition "
                "is not allowed"
            ),
        )
    return {"trial_id": str(row["id"]), "state": "queued"}


@router.post("/trials/{trial_id}/pre-start-heartbeat")
async def pre_start_heartbeat(
    trial_id: UUID,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:report" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized to report")

    try:
        worker_id = UUID(payload["worker_id"])
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=400, detail=f"worker_id required: {exc}",
        ) from exc

    async with request.app.state.session_factory() as session:
        row = (
            await session.execute(
                _PRE_START_HEARTBEAT_SQL,
                {"trial_id": trial_id, "worker_id": worker_id},
            )
        ).mappings().one_or_none()
        await session.commit()

    if row is None:
        raise HTTPException(
            status_code=409,
            detail="worker lost claim, trial has started, or trial is not claimed",
        )
    return {
        "trial_id": str(row["id"]),
        "pre_start_heartbeat_at": row["pre_start_heartbeat_at"].isoformat(),
    }


@router.post("/workers/register")
async def register_worker(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:report" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized to register")

    # Bug 5 fix: validate each capabilities entry against the Capabilities
    # Pydantic model so garbage (typo'd OS, unknown gpu_vendor, etc.) is
    # rejected at the boundary rather than silently mis-matching DRF claim
    # queries later.
    raw_caps = payload.get("capabilities")
    if not isinstance(raw_caps, list) or not raw_caps:
        raise HTTPException(
            status_code=400,
            detail="capabilities must be a non-empty list",
        )
    try:
        validated_caps = [
            Capabilities.model_validate(c).model_dump(mode="json")
            for c in raw_caps
        ]
    except ValidationError as exc:
        raise HTTPException(
            status_code=400, detail=f"invalid capabilities: {exc.errors()}",
        ) from exc

    raw_max_concurrent = payload.get("max_concurrent", 1)
    try:
        max_concurrent = int(raw_max_concurrent)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail="max_concurrent must be a positive integer",
        ) from exc
    if max_concurrent < 1:
        raise HTTPException(
            status_code=400,
            detail="max_concurrent must be a positive integer",
        )

    raw_pool_name = payload.get("pool_name", "default")
    pool_name = str(raw_pool_name).strip()
    if not pool_name:
        raise HTTPException(
            status_code=400,
            detail="pool_name must be a non-empty string",
        )

    worker_id = uuid4()
    async with request.app.state.session_factory() as session:
        await session.execute(insert(Worker).values(
            id=worker_id,
            hostname=payload.get("hostname", "unknown"),
            version=payload.get("version", "unknown"),
            capabilities=validated_caps,
            max_concurrent=max_concurrent,
            pool_name=pool_name,
            registered_at=datetime.now(UTC),
            last_seen_at=datetime.now(UTC),
            status="active",
        ))
        await session.commit()

    return {
        "worker_id": str(worker_id),
        "heartbeat_interval_sec": 5,
        "claim_poll_interval_sec": 1.0,
        "drain_timeout_sec": 600,
    }


@router.post("/workers/{worker_id}/heartbeat")
async def heartbeat(
    worker_id: UUID,
    request: Request,
    payload: dict[str, Any] | None = None,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:report" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized")

    status = None
    if payload is not None:
        raw_status = payload.get("status")
        if raw_status is not None:
            if raw_status not in _WORKER_HEARTBEAT_STATUSES:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "invalid worker heartbeat status: "
                        f"{raw_status!r}"
                    ),
                )
            status = raw_status

    values: dict[str, Any] = {"last_seen_at": datetime.now(UTC)}
    if status is not None:
        values["status"] = status
    async with request.app.state.session_factory() as session:
        await session.execute(update(Worker).where(Worker.id == worker_id).values(
            **values,
        ))
        await session.commit()
    return {"status": "ok"}
