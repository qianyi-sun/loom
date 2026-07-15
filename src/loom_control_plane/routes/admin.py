"""Admin token endpoints."""

from __future__ import annotations

import hashlib
import re
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import insert, text

from loom.auth import verify_bearer_token
from loom.db.schema import Token
from loom_control_plane.gb10_worker_lifecycle import (
    GB10NodeReport,
    UnsafeDesiredEnvError,
    desired_state_to_dict,
    fetch_lifecycle_status,
    get_desired_state,
    node_status_to_dict,
    record_node_report,
    upsert_desired_state,
)
from loom_control_plane.prod_pressure_control import (
    ProdPressureSignal,
    apply_prod_pressure_signal,
    fetch_prod_pressure_signal,
)
from loom_control_plane.slurm_worker_jobs import (
    SlurmWorkerJobObservation,
    fetch_slurm_worker_job_status,
    reconcile_slurm_worker_jobs,
    record_slurm_worker_job,
    slurm_worker_job_to_dict,
)
from loom_control_plane.worker_pool_autoscaler import (
    autoscaler_policy_to_dict,
    fetch_autoscaler_status,
    get_autoscaler_policy,
    upsert_autoscaler_policy,
)

router = APIRouter(prefix="/admin")

# Bug 3 fix: revoke prefix must be hex (the same charset `encode(.., 'hex')`
# emits) and at least 4 chars long, otherwise `%` or `` would revoke every
# token in the table.
_HEX_PREFIX_RE = re.compile(r"^[0-9a-f]{4,64}$")


class _SlurmWorkerJobCreate(BaseModel):
    environment: str
    pool_name: str
    nodelist: str
    requested_cpus: int | None = Field(default=None, ge=1)
    requested_memory_mib: int | None = Field(default=None, ge=1)
    requested_concurrency: int = Field(gt=0)
    job_id: str | None = None
    slurm_state: str | None = None
    pending_reason: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    submitted_at: datetime | None = None
    submission_error: str | None = None


class _SlurmWorkerObservation(BaseModel):
    job_id: str
    slurm_state: str
    nodelist: str | None = None
    pending_reason: str | None = None
    worker_id: UUID | None = None
    observed_at: datetime | None = None


class _SlurmWorkerReconcileRequest(BaseModel):
    observations: list[_SlurmWorkerObservation] = Field(default_factory=list)
    stale_after_seconds: int = Field(default=300, ge=0)


class _GB10DesiredStatePayload(BaseModel):
    image_tag: str
    max_concurrent: int = Field(gt=0)
    env_config_version: str
    source_git_commit: str | None = None
    target_slots: int | None = Field(default=None, ge=0)
    host_intents: dict[str, str] = Field(default_factory=dict)
    rollout_policy: dict[str, Any] = Field(default_factory=dict)
    env: dict[str, str] = Field(default_factory=dict)
    force: bool = False


class _GB10NodeReportPayload(BaseModel):
    current_image_tag: str | None = None
    current_max_concurrent: int | None = Field(default=None, gt=0)
    current_env_config_version: str | None = None
    current_intent: str | None = None
    apply_state: str = "unknown"
    last_apply_result: str | None = None
    error_message: str | None = None
    agent_version: str | None = None
    compose_project_dir: str | None = None
    source_git_commit: str | None = None
    source_git_dirty: bool | None = None
    worker_id: UUID | None = None
    last_apply_at: datetime | None = None


class _ProdPressurePayload(BaseModel):
    prod_pending_count: int = Field(ge=0)
    prod_active_count: int = Field(ge=0)
    prod_capacity_shortfall: int = Field(ge=0)
    source: str = "control-plane prod queue summary"
    preemptible: bool = True
    grace_period_seconds: int = Field(default=600, ge=0)


class _AutoscalerPolicyPayload(BaseModel):
    actuator: str
    enabled: bool = False
    min_slots: int = Field(default=0, ge=0)
    max_slots: int = Field(ge=0)
    scale_up_threshold_slots: int = Field(default=1, ge=0)
    scale_down_idle_seconds: int = Field(default=600, ge=0)
    scale_up_cooldown_seconds: int = Field(default=60, ge=0)
    scale_down_cooldown_seconds: int = Field(default=300, ge=0)
    drain_timeout_seconds: int = Field(default=600, gt=0)
    force: bool = False
    disabled_reason: str | None = None
    actuator_config: dict[str, Any] = Field(default_factory=dict)


async def _require_admin_scope(
    request: Request,
    authorization: str | None,
    scope: str,
) -> None:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(
            session,
            authorization,
            admin_verifier=getattr(
                request.app.state, "admin_secret_verifier", None,
            ),
        )
    if ctx is None or scope not in ctx.scopes:
        raise HTTPException(status_code=403, detail=f"missing scope {scope}")


@router.post("/worker-tokens", status_code=201)
async def issue_worker_token(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    await _require_admin_scope(request, authorization, "admin:tokens")

    raw_bytes = secrets.token_bytes(32)
    raw = "loom_w_" + raw_bytes.hex()
    token_hash = hashlib.sha256(raw.encode()).digest()
    expires_at: datetime | None = None
    days = payload.get("expires_in_days")
    if days is not None:
        expires_at = datetime.now(UTC) + timedelta(days=int(days))

    async with request.app.state.session_factory() as session:
        await session.execute(insert(Token).values(
            token_hash=token_hash,
            type="worker",
            scopes=["worker:claim", "worker:report", "worker:index"],
            team_id=None,
            issued_at=datetime.now(UTC),
            expires_at=expires_at,
        ))
        await session.commit()

    return {
        "token": raw,
        "token_hash_prefix": token_hash.hex()[:8],
    }


@router.post("/batch-runner-tokens", status_code=201)
async def issue_batch_runner_token(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    await _require_admin_scope(request, authorization, "admin:tokens")

    raw = "loom_br_" + secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(raw.encode()).digest()
    expires_at: datetime | None = None
    days = payload.get("expires_in_days")
    if days is not None:
        expires_at = datetime.now(UTC) + timedelta(days=int(days))

    async with request.app.state.session_factory() as session:
        await session.execute(insert(Token).values(
            token_hash=token_hash,
            type="worker",
            scopes=["submit:batch"],
            team_id=None,
            issued_at=datetime.now(UTC),
            expires_at=expires_at,
        ))
        await session.commit()

    return {
        "token": raw,
        "token_hash_prefix": token_hash.hex()[:8],
    }


@router.delete("/worker-tokens/{prefix}")
async def revoke_token(
    prefix: str,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    await _require_admin_scope(request, authorization, "admin:tokens")
    if not _HEX_PREFIX_RE.fullmatch(prefix):
        raise HTTPException(
            status_code=400,
            detail="prefix must be 4-64 hex characters",
        )
    async with request.app.state.session_factory() as session:
        await session.execute(
            text("""
                UPDATE tokens SET revoked_at = NOW()
                 WHERE encode(token_hash, 'hex') LIKE :prefix
            """),
            {"prefix": prefix + "%"},
        )
        await session.commit()
    return {"status": "revoked"}


@router.post("/slurm-worker-jobs", status_code=201, response_model=None)
async def create_slurm_worker_job(
    request: Request,
    payload: _SlurmWorkerJobCreate,
    authorization: str | None = Header(default=None),
) -> Any:
    await _require_admin_scope(request, authorization, "admin:slurm_workers")
    async with request.app.state.session_factory() as session:
        job, duplicate = await record_slurm_worker_job(
            session,
            environment=payload.environment,
            pool_name=payload.pool_name,
            nodelist=payload.nodelist,
            requested_cpus=payload.requested_cpus,
            requested_memory_mib=payload.requested_memory_mib,
            requested_concurrency=payload.requested_concurrency,
            job_id=payload.job_id,
            slurm_state=payload.slurm_state,
            pending_reason=payload.pending_reason,
            env=payload.env,
            submitted_at=payload.submitted_at,
            submission_error=payload.submission_error,
        )
        if duplicate:
            existing_id = str(job.id)
            existing_job_id = job.job_id
            await session.rollback()
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "active Slurm worker job already exists",
                    "existing_id": existing_id,
                    "existing_job_id": existing_job_id,
                },
            )
        await session.commit()
        return slurm_worker_job_to_dict(job)


@router.post("/slurm-worker-jobs/reconcile")
async def reconcile_slurm_worker_job_records(
    request: Request,
    payload: _SlurmWorkerReconcileRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, int]:
    await _require_admin_scope(request, authorization, "admin:slurm_workers")
    observations = [
        SlurmWorkerJobObservation(
            job_id=obs.job_id,
            slurm_state=obs.slurm_state,
            nodelist=obs.nodelist,
            pending_reason=obs.pending_reason,
            worker_id=obs.worker_id,
            observed_at=obs.observed_at,
        )
        for obs in payload.observations
    ]
    async with request.app.state.session_factory() as session:
        result = await reconcile_slurm_worker_jobs(
            session,
            observations,
            stale_after_seconds=payload.stale_after_seconds,
        )
        await session.commit()
    return result.as_dict()


@router.get("/slurm-worker-jobs/status")
async def get_slurm_worker_job_status(
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    await _require_admin_scope(request, authorization, "admin:slurm_workers")
    async with request.app.state.session_factory() as session:
        summary, jobs = await fetch_slurm_worker_job_status(session)
    return {
        "summary": summary.as_list(),
        "jobs": jobs,
    }


@router.put("/worker-pool-autoscaler-policies/{environment}/{pool_name}")
async def put_worker_pool_autoscaler_policy(
    environment: str,
    pool_name: str,
    request: Request,
    payload: _AutoscalerPolicyPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    await _require_admin_scope(request, authorization, "admin:worker_pools")
    try:
        async with request.app.state.session_factory() as session:
            row = await upsert_autoscaler_policy(
                session,
                environment=environment,
                pool_name=pool_name,
                actuator=payload.actuator,
                enabled=payload.enabled,
                min_slots=payload.min_slots,
                max_slots=payload.max_slots,
                scale_up_threshold_slots=payload.scale_up_threshold_slots,
                scale_down_idle_seconds=payload.scale_down_idle_seconds,
                scale_up_cooldown_seconds=payload.scale_up_cooldown_seconds,
                scale_down_cooldown_seconds=payload.scale_down_cooldown_seconds,
                drain_timeout_seconds=payload.drain_timeout_seconds,
                force=payload.force,
                disabled_reason=payload.disabled_reason,
                actuator_config=payload.actuator_config,
            )
            await session.commit()
            return autoscaler_policy_to_dict(row)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/worker-pool-autoscaler-policies/{environment}/{pool_name}")
async def get_worker_pool_autoscaler_policy(
    environment: str,
    pool_name: str,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    await _require_admin_scope(request, authorization, "admin:worker_pools")
    async with request.app.state.session_factory() as session:
        row = await get_autoscaler_policy(
            session,
            environment=environment,
            pool_name=pool_name,
        )
    if row is None:
        raise HTTPException(
            status_code=404,
            detail="worker pool autoscaler policy not found",
        )
    return autoscaler_policy_to_dict(row)


@router.get("/worker-pool-autoscalers/status")
async def get_worker_pool_autoscaler_status(
    request: Request,
    environment: str | None = Query(default=None),
    pool_name: str | None = Query(default=None),
    authorization: str | None = Header(default=None),
) -> dict[str, list[dict[str, object]]]:
    await _require_admin_scope(request, authorization, "admin:worker_pools")
    async with request.app.state.session_factory() as session:
        return await fetch_autoscaler_status(
            session,
            environment=environment,
            pool_name=pool_name,
        )


@router.get("/worker-pools/{pool_name}/prod-pressure")
async def get_worker_pool_prod_pressure(
    pool_name: str,
    request: Request,
    freshness_sec: int = Query(default=120, gt=0),
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    """Return the secret-free pressure signal from this (prod) CP."""
    await _require_admin_scope(request, authorization, "admin:worker_pools")
    try:
        async with request.app.state.session_factory() as session:
            signal = await fetch_prod_pressure_signal(
                session,
                pool_name=pool_name,
                freshness_sec=freshness_sec,
            )
        return signal.public_dict()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/gb10-worker-pools/{environment}/{pool_name}/desired-state")
async def put_gb10_worker_pool_desired_state(
    environment: str,
    pool_name: str,
    request: Request,
    payload: _GB10DesiredStatePayload,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    await _require_admin_scope(request, authorization, "admin:gb10_workers")
    try:
        async with request.app.state.session_factory() as session:
            row = await upsert_desired_state(
                session,
                environment=environment,
                pool_name=pool_name,
                image_tag=payload.image_tag,
                max_concurrent=payload.max_concurrent,
                env_config_version=payload.env_config_version,
                source_git_commit=payload.source_git_commit,
                target_slots=payload.target_slots,
                host_intents=payload.host_intents,
                rollout_policy=payload.rollout_policy,
                env=payload.env,
                force=payload.force,
            )
            await session.commit()
            return desired_state_to_dict(row)
    except UnsafeDesiredEnvError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/gb10-worker-pools/{environment}/{pool_name}/prod-pressure")
async def put_gb10_worker_pool_prod_pressure(
    environment: str,
    pool_name: str,
    request: Request,
    payload: _ProdPressurePayload,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    """Consume a prod pressure signal into desired state and claim fencing."""
    await _require_admin_scope(request, authorization, "admin:gb10_workers")
    try:
        async with request.app.state.session_factory() as session:
            result = await apply_prod_pressure_signal(
                session,
                environment=environment,
                pool_name=pool_name,
                signal=ProdPressureSignal(
                    prod_pending_count=payload.prod_pending_count,
                    prod_active_count=payload.prod_active_count,
                    prod_capacity_shortfall=payload.prod_capacity_shortfall,
                    source=payload.source,
                ),
                preemptible=payload.preemptible,
                grace_period_seconds=payload.grace_period_seconds,
            )
            await session.commit()
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/gb10-worker-pools/{environment}/{pool_name}/desired-state")
async def get_gb10_worker_pool_desired_state(
    environment: str,
    pool_name: str,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    await _require_admin_scope(request, authorization, "admin:gb10_workers")
    async with request.app.state.session_factory() as session:
        row = await get_desired_state(
            session,
            environment=environment,
            pool_name=pool_name,
        )
    if row is None:
        raise HTTPException(status_code=404, detail="GB10 desired state not found")
    return desired_state_to_dict(row)


@router.post("/gb10-worker-pools/{environment}/{pool_name}/nodes/{hostname}/report")
async def report_gb10_worker_node_status(
    environment: str,
    pool_name: str,
    hostname: str,
    request: Request,
    payload: _GB10NodeReportPayload,
    authorization: str | None = Header(default=None),
) -> dict[str, object]:
    await _require_admin_scope(request, authorization, "admin:gb10_workers")
    async with request.app.state.session_factory() as session:
        row = await record_node_report(
            session,
            environment=environment,
            pool_name=pool_name,
            hostname=hostname,
            report=GB10NodeReport(
                current_image_tag=payload.current_image_tag,
                current_max_concurrent=payload.current_max_concurrent,
                current_env_config_version=payload.current_env_config_version,
                current_intent=payload.current_intent,
                apply_state=payload.apply_state,
                last_apply_result=payload.last_apply_result,
                error_message=payload.error_message,
                agent_version=payload.agent_version,
                compose_project_dir=payload.compose_project_dir,
                source_git_commit=payload.source_git_commit,
                source_git_dirty=payload.source_git_dirty,
                worker_id=payload.worker_id,
                last_apply_at=payload.last_apply_at,
            ),
        )
        await session.commit()
        return node_status_to_dict(row)


@router.get("/gb10-worker-pools/status")
async def get_gb10_worker_pool_status(
    request: Request,
    environment: str | None = Query(default=None),
    pool_name: str | None = Query(default=None),
    authorization: str | None = Header(default=None),
) -> dict[str, list[dict[str, object]]]:
    await _require_admin_scope(request, authorization, "admin:gb10_workers")
    async with request.app.state.session_factory() as session:
        return await fetch_lifecycle_status(
            session,
            environment=environment,
            pool_name=pool_name,
        )
