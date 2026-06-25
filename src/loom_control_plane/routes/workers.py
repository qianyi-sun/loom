"""Worker-facing endpoints: claim, register, heartbeat."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy import insert, update

from loom.auth import verify_bearer_token
from loom.db.schema import Worker
from loom.models.capabilities import Capabilities
from loom_control_plane.metrics import CLAIM_LATENCY_SEC
from loom_control_plane.scheduler.claim import claim_one

router = APIRouter()

_WORKER_HEARTBEAT_STATUSES = {"active", "idle-exit", "shutting-down"}


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
        "state": "claimed",
    })


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

    worker_id = uuid4()
    async with request.app.state.session_factory() as session:
        await session.execute(insert(Worker).values(
            id=worker_id,
            hostname=payload.get("hostname", "unknown"),
            version=payload.get("version", "unknown"),
            capabilities=validated_caps,
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
