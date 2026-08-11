"""Owner-scoped API for immutable personal-development source candidates."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Annotated, Literal, Protocol, cast
from uuid import UUID

from fastapi import APIRouter, File, Form, HTTPException, Query, Request, Response, UploadFile
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.personal_dev_candidate import (
    CandidateRegistration,
    CandidateRegistry,
    PersonalDevCandidateLimits,
    PersonalDevCandidateQuotaError,
)
from loom.personal_dev_candidate_store import SqlAlchemyPersonalDevCandidateStore
from loom_service.auth_guards import is_admin, require_scope, require_submitting_user
from loom_service.dependencies import SessionAndCtx
from loom_service.personal_dev_candidate_intake import intake_personal_dev_candidate

router = APIRouter()


class VisibleCandidateStore(CandidateRegistry, Protocol):
    async def get(self, candidate_id: UUID) -> CandidateRegistration | None: ...

    async def list_visible(
        self,
        *,
        owner_user_id: UUID | None,
        limit: int = 100,
    ) -> list[CandidateRegistration]: ...


StoreFactory = Callable[[AsyncSession], VisibleCandidateStore]


class PersonalDevBuildAttemptResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    attempt_sequence: int
    state: Literal["queued", "claimed", "running", "succeeded", "failed"]
    lease_epoch: int
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    failure_reason: str | None


class PersonalDevCandidateResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    attestation_scope: Literal["personal-dev-only"] = "personal-dev-only"
    promotable: Literal[False] = False
    created: bool
    candidate_sha: str
    source_sha256: str
    archive_sha256: str
    build_contract_sha256: str
    source_commit: str
    dirty: bool
    archive_size_bytes: int
    status: Literal["uploaded", "queued", "building", "ready", "failed"]
    artifact_state: Literal["retained", "collecting", "collected"]
    artifact_gc_blocked_reason: str | None
    image_manifest_digest: str | None
    publication_sha256: str | None
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime
    ready_at: datetime | None
    artifact_collected_at: datetime | None
    build_attempt: PersonalDevBuildAttemptResponse | None


class PersonalDevCandidateListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[PersonalDevCandidateResponse] = Field(default_factory=list)


def _store(request: Request, session: AsyncSession) -> VisibleCandidateStore:
    factory = getattr(request.app.state, "personal_dev_candidate_store_factory", None)
    if factory is None:
        defaults = PersonalDevCandidateLimits()
        settings = getattr(request.app.state, "settings", None)
        return SqlAlchemyPersonalDevCandidateStore(
            session,
            limits=PersonalDevCandidateLimits(
                per_owner_retained_candidates=getattr(
                    settings,
                    "personal_dev_candidate_retained_count_limit",
                    defaults.per_owner_retained_candidates,
                ),
                per_owner_retained_archive_bytes=getattr(
                    settings,
                    "personal_dev_candidate_retained_bytes_limit",
                    defaults.per_owner_retained_archive_bytes,
                ),
                global_active_builds=getattr(
                    settings,
                    "personal_dev_builder_global_concurrency",
                    defaults.global_active_builds,
                ),
                per_owner_active_builds=getattr(
                    settings,
                    "personal_dev_builder_per_owner_concurrency",
                    defaults.per_owner_active_builds,
                ),
            ),
        )
    if not callable(factory):
        raise HTTPException(status_code=503, detail="personal-dev candidate registry unavailable")
    return cast(StoreFactory, factory)(session)


def _response(registration: CandidateRegistration) -> PersonalDevCandidateResponse:
    candidate = registration.candidate
    attempt = registration.build_attempt
    return PersonalDevCandidateResponse(
        id=candidate.id,
        created=registration.created,
        candidate_sha=candidate.candidate_sha,
        source_sha256=candidate.source_sha256,
        archive_sha256=candidate.archive_sha256,
        build_contract_sha256=candidate.build_contract_sha256,
        source_commit=candidate.source_commit,
        dirty=candidate.dirty,
        archive_size_bytes=candidate.archive_size_bytes,
        status=candidate.status,
        artifact_state=candidate.artifact_state,
        artifact_gc_blocked_reason=candidate.artifact_gc_blocked_reason,
        image_manifest_digest=candidate.image_manifest_digest,
        publication_sha256=candidate.publication_sha256,
        failure_reason=candidate.failure_reason,
        created_at=candidate.created_at,
        updated_at=candidate.updated_at,
        ready_at=candidate.ready_at,
        artifact_collected_at=candidate.artifact_collected_at,
        build_attempt=(
            PersonalDevBuildAttemptResponse(
                id=attempt.id,
                attempt_sequence=attempt.attempt_sequence,
                state=attempt.state,
                lease_epoch=attempt.lease_epoch,
                created_at=attempt.created_at,
                updated_at=attempt.updated_at,
                started_at=attempt.started_at,
                finished_at=attempt.finished_at,
                failure_reason=attempt.failure_reason,
            )
            if attempt is not None
            else None
        ),
    )


def _visible(
    registration: CandidateRegistration | None,
    ctx: AuthContext,
) -> CandidateRegistration:
    if registration is None or (
        not is_admin(ctx) and registration.candidate.owner_user_id != ctx.user_id
    ):
        raise HTTPException(status_code=404, detail="personal-dev candidate not found")
    return registration


def _owner(ctx: AuthContext) -> tuple[UUID, UUID]:
    require_submitting_user(ctx)
    if ctx.user_id is None or ctx.team_id is None:
        raise HTTPException(status_code=403, detail="a current team is required")
    return ctx.user_id, ctx.team_id


def _require_enabled(request: Request) -> None:
    settings = getattr(request.app.state, "settings", None)
    if settings is None or not getattr(settings, "dev_instances_enabled", False):
        raise HTTPException(
            status_code=503,
            detail="personal-dev candidate intake is not enabled in this environment",
        )


@router.post(
    "/personal-dev-candidates",
    status_code=201,
    response_model=PersonalDevCandidateResponse,
)
async def create_personal_dev_candidate(
    request: Request,
    response: Response,
    sc: SessionAndCtx,
    source: Annotated[UploadFile, File()],
    source_sha256: Annotated[str, Form(min_length=64, max_length=64)],
    archive_sha256: Annotated[str, Form(min_length=64, max_length=64)],
) -> PersonalDevCandidateResponse:
    session, ctx = sc
    require_scope(ctx, "submit")
    _require_enabled(request)
    owner_user_id, owner_team_id = _owner(ctx)
    settings = request.app.state.settings
    try:
        registration = await intake_personal_dev_candidate(
            registry=_store(request, session),
            object_store=request.app.state.minio_client,
            bucket=settings.artifacts_bucket,
            owner_user_id=owner_user_id,
            owner_team_id=owner_team_id,
            source_upload=source,
            expected_source_sha256=source_sha256,
            expected_archive_sha256=archive_sha256,
            max_archive_bytes=settings.personal_dev_source_max_archive_bytes,
        )
    except PersonalDevCandidateQuotaError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    response.status_code = 201 if registration.created else 200
    return _response(registration)


@router.get(
    "/personal-dev-candidates",
    response_model=PersonalDevCandidateListResponse,
)
async def list_personal_dev_candidates(
    request: Request,
    sc: SessionAndCtx,
    mine: bool = Query(default=False),
    limit: int = Query(default=100, ge=1, le=500),
) -> PersonalDevCandidateListResponse:
    session, ctx = sc
    require_scope(ctx, "read:own")
    _require_enabled(request)
    owner_user_id = ctx.user_id
    if is_admin(ctx) and not mine:
        owner_user_id = None
    elif owner_user_id is None:
        raise HTTPException(status_code=403, detail="a user identity is required")
    rows = await _store(request, session).list_visible(
        owner_user_id=owner_user_id,
        limit=limit,
    )
    return PersonalDevCandidateListResponse(items=[_response(row) for row in rows])


@router.get(
    "/personal-dev-candidates/{candidate_id}",
    response_model=PersonalDevCandidateResponse,
)
async def get_personal_dev_candidate(
    candidate_id: UUID,
    request: Request,
    sc: SessionAndCtx,
) -> PersonalDevCandidateResponse:
    session, ctx = sc
    require_scope(ctx, "read:own")
    _require_enabled(request)
    registration = _visible(await _store(request, session).get(candidate_id), ctx)
    return _response(registration)


__all__ = ["StoreFactory", "router"]
