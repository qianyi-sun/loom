"""Admin token endpoints."""

from __future__ import annotations

import hashlib
import re
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import insert, text

from loom.auth import verify_bearer_token
from loom.db.schema import Token
from loom_control_plane.slurm_worker_jobs import (
    SlurmWorkerJobObservation,
    fetch_slurm_worker_job_status,
    reconcile_slurm_worker_jobs,
    record_slurm_worker_job,
    slurm_worker_job_to_dict,
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
