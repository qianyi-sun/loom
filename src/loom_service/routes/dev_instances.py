"""Guarded self-service lifecycle API for shared-fleet dev environments."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Literal, Protocol, cast
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.dev_instance import PER_INSTANCE_CAP, DevInstanceIdentity, derive_identity
from loom.dev_instance_provisioner import (
    DevInstanceConflictError,
    DevInstanceOperationFencedError,
    DevInstanceProvisioner,
    DevInstanceRecord,
    DevInstanceRejectedError,
    InstanceStore,
    OwnerAccessSnapshot,
)
from loom.dev_instance_store import SqlAlchemyDevInstanceStore
from loom.personal_dev_environment import (
    PersonalDevApplyReservation,
    PersonalDevEnvironmentApplyRequest,
    PersonalDevEnvironmentRecord,
    PersonalDevLifecycleLimits,
    PersonalDevLifecycleOperationRecord,
)
from loom.personal_dev_environment_store import (
    PersonalDevEnvironmentConflictError,
    PersonalDevEnvironmentEpochFencedError,
    PersonalDevEnvironmentNotFoundError,
    PersonalDevEnvironmentOperationFencedError,
    SqlAlchemyPersonalDevEnvironmentAuthority,
)
from loom_service.auth_guards import is_admin, require_scope, require_submitting_user
from loom_service.dependencies import SessionAndCtx
from loom_service.dev_instance_access import (
    DevInstanceAccessError,
    load_owner_access_snapshot,
)

logger = logging.getLogger(__name__)
router = APIRouter()

ProvisionerFactory = Callable[[InstanceStore], DevInstanceProvisioner]


class VisibleInstanceStore(InstanceStore, Protocol):
    async def list_visible(
        self,
        *,
        owner_user_id: UUID | None,
        include_deleted: bool = False,
    ) -> list[DevInstanceRecord]: ...


StoreFactory = Callable[[AsyncSession], VisibleInstanceStore]
PersonalAuthorityFactory = Callable[
    [AsyncSession],
    SqlAlchemyPersonalDevEnvironmentAuthority,
]
AccessSnapshotFactory = Callable[
    [AsyncSession, AuthContext],
    Awaitable[OwnerAccessSnapshot],
]


class LifecycleRunner(Protocol):
    def submit_create(
        self,
        record: DevInstanceRecord,
        access: OwnerAccessSnapshot,
    ) -> bool: ...

    def submit_destroy(self, record: DevInstanceRecord) -> bool: ...


class DevInstanceCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=20)
    min_slots: int = Field(default=0, ge=0, le=PER_INSTANCE_CAP)
    max_slots: int = Field(default=2, ge=0, le=PER_INSTANCE_CAP)


class PersonalDevEnvironmentApplyPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_id: UUID
    candidate_sha: str = Field(pattern=r"^[0-9a-f]{64}$")
    min_slots: int = Field(default=0, ge=0, le=PER_INSTANCE_CAP)
    max_slots: int = Field(default=2, ge=0, le=PER_INSTANCE_CAP)
    expected_operation_epoch: int = Field(ge=0)
    idempotency_key: UUID


class DevInstanceIdentityResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment: str
    namespace: str
    database: str
    task_bucket: str
    trajectories_bucket: str
    artifacts_bucket: str
    route_host: str
    worker_control_plane_host: str
    worker_gateway_host: str
    route_path: str
    worker_pool: str


class DevInstanceResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    owner_user_id: UUID
    owner_team_id: UUID
    status: Literal[
        "provisioning",
        "ready",
        "updating",
        "activating",
        "deleting",
        "draining",
        "failed",
        "deleted",
    ]
    min_slots: int
    max_slots: int
    deployment_generation: int
    candidate_sha: str
    candidate_id: UUID | None = None
    subject_id: UUID | None = None
    subject_incarnation: UUID | None = None
    operation_epoch: int
    operation_step: str
    keep_data: bool
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime
    ready_at: datetime | None
    deleted_at: datetime | None
    identity: DevInstanceIdentityResponse


class DevInstanceListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[DevInstanceResponse] = Field(default_factory=list)


class PersonalDevEnvironmentResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    subject_id: UUID
    subject_incarnation: UUID
    owner_user_id: UUID
    owner_team_id: UUID
    status: Literal[
        "provisioning",
        "ready",
        "updating",
        "activating",
        "deleting",
        "draining",
        "failed",
        "deleted",
    ]
    min_slots: int
    max_slots: int
    deployment_generation: int
    candidate_id: UUID | None
    candidate_sha: str
    operation_epoch: int
    operation_id: UUID
    operation_step: str
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime
    ready_at: datetime | None
    deleted_at: datetime | None
    identity: DevInstanceIdentityResponse


class PersonalDevLifecycleOperationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    idempotency_key: UUID
    environment_name: str
    subject_id: UUID
    subject_incarnation: UUID
    operation_epoch: int
    expected_operation_epoch: int
    kind: Literal["create", "update", "capacity", "noop"]
    state: Literal[
        "requested",
        "running",
        "activating",
        "succeeded",
        "failed",
        "cancelling",
        "cancelled",
    ]
    attempt_id: UUID
    attempt_sequence: int
    candidate_id: UUID
    candidate_sha: str
    min_slots: int
    max_slots: int
    deployment_generation: int
    checkpoint: str
    failure_reason: str | None
    readiness_evidence_sha256: str | None
    activation_acknowledgement_sha256: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    attestation_scope: Literal["personal-dev-only"] = "personal-dev-only"
    promotable: Literal[False] = False


class PersonalDevEnvironmentApplyResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment: PersonalDevEnvironmentResponse
    operation: PersonalDevLifecycleOperationResponse


def _identity_response(identity: DevInstanceIdentity) -> DevInstanceIdentityResponse:
    return DevInstanceIdentityResponse(
        environment=identity.runtime_environment,
        namespace=identity.namespace,
        database=identity.database,
        task_bucket=identity.task_bucket,
        trajectories_bucket=identity.trajectories_bucket,
        artifacts_bucket=identity.artifacts_bucket,
        route_host=identity.route_host,
        worker_control_plane_host=identity.worker_control_plane_host,
        worker_gateway_host=identity.worker_gateway_host,
        route_path=identity.route_path,
        worker_pool=identity.worker_pool,
    )


def _response(record: DevInstanceRecord) -> DevInstanceResponse:
    return DevInstanceResponse(
        name=record.name,
        owner_user_id=record.owner_user_id,
        owner_team_id=record.owner_team_id,
        status=record.status,
        min_slots=record.min_slots,
        max_slots=record.max_slots,
        deployment_generation=record.deployment_generation,
        candidate_sha=record.candidate_sha,
        candidate_id=record.candidate_id,
        subject_id=record.subject_id,
        subject_incarnation=record.subject_incarnation,
        operation_epoch=record.operation_epoch,
        operation_step=record.operation_step,
        keep_data=record.keep_data,
        failure_reason=record.failure_reason,
        created_at=record.created_at,
        updated_at=record.updated_at,
        ready_at=record.ready_at,
        deleted_at=record.deleted_at,
        identity=_identity_response(derive_identity(record.name)),
    )


def _personal_environment_response(
    record: PersonalDevEnvironmentRecord,
) -> PersonalDevEnvironmentResponse:
    return PersonalDevEnvironmentResponse(
        name=record.name,
        subject_id=record.subject_id,
        subject_incarnation=record.subject_incarnation,
        owner_user_id=record.owner_user_id,
        owner_team_id=record.owner_team_id,
        status=record.status,
        min_slots=record.min_slots,
        max_slots=record.max_slots,
        deployment_generation=record.deployment_generation,
        candidate_id=record.candidate_id,
        candidate_sha=record.candidate_sha,
        operation_epoch=record.operation_epoch,
        operation_id=record.operation_id,
        operation_step=record.operation_step,
        failure_reason=record.failure_reason,
        created_at=record.created_at,
        updated_at=record.updated_at,
        ready_at=record.ready_at,
        deleted_at=record.deleted_at,
        identity=_identity_response(derive_identity(record.name)),
    )


def _personal_operation_response(
    record: PersonalDevLifecycleOperationRecord,
) -> PersonalDevLifecycleOperationResponse:
    return PersonalDevLifecycleOperationResponse(
        id=record.id,
        idempotency_key=record.idempotency_key,
        environment_name=record.environment_name,
        subject_id=record.subject_id,
        subject_incarnation=record.subject_incarnation,
        operation_epoch=record.operation_epoch,
        expected_operation_epoch=record.expected_operation_epoch,
        kind=record.kind,
        state=record.state,
        attempt_id=record.attempt_id,
        attempt_sequence=record.attempt_sequence,
        candidate_id=record.candidate_id,
        candidate_sha=record.candidate_sha,
        min_slots=record.min_slots,
        max_slots=record.max_slots,
        deployment_generation=record.deployment_generation,
        checkpoint=record.checkpoint,
        failure_reason=record.failure_reason,
        readiness_evidence_sha256=record.readiness_evidence_sha256,
        activation_acknowledgement_sha256=(
            record.activation_acknowledgement_sha256
        ),
        created_at=record.created_at,
        updated_at=record.updated_at,
        started_at=record.started_at,
        finished_at=record.finished_at,
    )


def _personal_apply_response(
    reservation: PersonalDevApplyReservation,
) -> PersonalDevEnvironmentApplyResponse:
    return PersonalDevEnvironmentApplyResponse(
        environment=_personal_environment_response(reservation.environment),
        operation=_personal_operation_response(reservation.operation),
    )


def _require_user_identity(ctx: AuthContext) -> tuple[UUID, UUID]:
    require_submitting_user(ctx)
    if ctx.user_id is None or ctx.team_id is None:
        raise HTTPException(status_code=403, detail="a current team is required")
    return ctx.user_id, ctx.team_id


def _factory(request: Request) -> ProvisionerFactory:
    factory = getattr(request.app.state, "dev_instance_provisioner_factory", None)
    if factory is None or not callable(factory):
        raise HTTPException(
            status_code=503,
            detail="shared-fleet dev instance provisioning is not configured",
        )
    return cast(ProvisionerFactory, factory)


def _store(request: Request, session: AsyncSession) -> VisibleInstanceStore:
    """Construct the registry adapter, with a narrow test/runtime override."""
    factory = getattr(request.app.state, "dev_instance_store_factory", None)
    if factory is None:
        return SqlAlchemyDevInstanceStore(session)
    if not callable(factory):
        raise HTTPException(status_code=503, detail="dev instance registry is unavailable")
    return cast(StoreFactory, factory)(session)


def _runner(request: Request) -> LifecycleRunner | None:
    value = getattr(request.app.state, "dev_instance_lifecycle_runner", None)
    return cast(LifecycleRunner, value) if value is not None else None


def _personal_authority(
    request: Request,
    session: AsyncSession,
) -> SqlAlchemyPersonalDevEnvironmentAuthority:
    settings = getattr(request.app.state, "settings", None)
    if settings is None or not getattr(settings, "dev_instances_enabled", False):
        raise HTTPException(
            status_code=503,
            detail="personal-dev environment lifecycle is not enabled",
        )
    factory = getattr(
        request.app.state,
        "personal_dev_environment_authority_factory",
        None,
    )
    if factory is None:
        return SqlAlchemyPersonalDevEnvironmentAuthority(
            session,
            limits=PersonalDevLifecycleLimits(
                global_live_instances=(
                    settings.personal_dev_global_live_instance_limit
                ),
                per_owner_live_instances=(
                    settings.personal_dev_per_owner_live_instance_limit
                ),
                per_owner_aggregate_min_slots=(
                    settings.personal_dev_per_owner_aggregate_min_slots
                ),
                per_owner_aggregate_max_slots=(
                    settings.personal_dev_per_owner_aggregate_max_slots
                ),
            ),
        )
    if not callable(factory):
        raise HTTPException(status_code=503, detail="personal-dev lifecycle authority is unavailable")
    return cast(PersonalAuthorityFactory, factory)(session)


def _visible(record: DevInstanceRecord | None, ctx: AuthContext) -> DevInstanceRecord:
    if record is None or (not is_admin(ctx) and record.owner_user_id != ctx.user_id):
        raise HTTPException(status_code=404, detail="dev instance not found")
    return record


async def _access_snapshot(
    request: Request,
    session: AsyncSession,
    ctx: AuthContext,
) -> OwnerAccessSnapshot:
    factory = getattr(request.app.state, "dev_instance_access_snapshot_factory", None)
    if factory is None:
        return await load_owner_access_snapshot(session, ctx)
    if not callable(factory):
        raise HTTPException(status_code=503, detail="dev instance access bootstrap is unavailable")
    return await cast(AccessSnapshotFactory, factory)(session, ctx)


@router.put(
    "/dev-instances/{name}",
    status_code=202,
    response_model=PersonalDevEnvironmentApplyResponse,
)
async def apply_personal_dev_environment(
    name: str,
    payload: PersonalDevEnvironmentApplyPayload,
    request: Request,
    sc: SessionAndCtx,
    response: Response,
) -> PersonalDevEnvironmentApplyResponse:
    """Atomically bind candidate, capacity, owner, and expected lifecycle epoch."""
    session, ctx = sc
    require_scope(ctx, "submit")
    owner_user_id, owner_team_id = _require_user_identity(ctx)
    if payload.min_slots > payload.max_slots:
        raise HTTPException(status_code=400, detail="min_slots must not exceed max_slots")
    try:
        reservation = await _personal_authority(request, session).apply(
            PersonalDevEnvironmentApplyRequest(
                name=name,
                owner_user_id=owner_user_id,
                owner_team_id=owner_team_id,
                candidate_id=payload.candidate_id,
                candidate_sha=payload.candidate_sha,
                min_slots=payload.min_slots,
                max_slots=payload.max_slots,
                expected_operation_epoch=payload.expected_operation_epoch,
                idempotency_key=payload.idempotency_key,
            ),
        )
    except PersonalDevEnvironmentNotFoundError as exc:
        raise HTTPException(status_code=404, detail="personal-dev resource not found") from exc
    except (
        PersonalDevEnvironmentConflictError,
        PersonalDevEnvironmentEpochFencedError,
        PersonalDevEnvironmentOperationFencedError,
    ) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception:
        logger.exception(
            "personal_dev_environment_apply_failed",
            extra={"dev_instance_name": name},
        )
        raise HTTPException(
            status_code=503,
            detail="personal-dev environment apply failed before external activation",
        ) from None
    response.status_code = 200 if reservation.operation.state == "succeeded" else 202
    return _personal_apply_response(reservation)


@router.post("/dev-instances", status_code=202, response_model=DevInstanceResponse)
async def create_dev_instance(
    payload: DevInstanceCreateRequest,
    request: Request,
    sc: SessionAndCtx,
) -> DevInstanceResponse:
    session, ctx = sc
    require_scope(ctx, "submit")
    owner_user_id, owner_team_id = _require_user_identity(ctx)
    settings = getattr(request.app.state, "settings", None)
    if settings is not None and getattr(settings, "dev_instances_enabled", False):
        raise HTTPException(
            status_code=410,
            detail=(
                "candidate-less dev creation is retired; use PUT /dev-instances/{name} "
                "with an immutable personal-dev candidate and expected operation epoch"
            ),
        )
    if payload.min_slots > payload.max_slots:
        raise HTTPException(status_code=400, detail="min_slots must not exceed max_slots")
    store = _store(request, session)
    provisioner = _factory(request)(store)
    try:
        access = await _access_snapshot(request, session, ctx)
        runner = _runner(request)
        if runner is None:
            record = await provisioner.create(
                payload.name,
                owner_user_id=owner_user_id,
                owner_team_id=owner_team_id,
                min_slots=payload.min_slots,
                max_slots=payload.max_slots,
                access=access,
            )
        else:
            reservation = await provisioner.claim_create(
                payload.name,
                owner_user_id=owner_user_id,
                owner_team_id=owner_team_id,
                min_slots=payload.min_slots,
                max_slots=payload.max_slots,
            )
            record = reservation.record
            if record.status == "provisioning" and not runner.submit_create(record, access):
                # False also means this exact operation is already running,
                # which is a successful idempotent submission.
                logger.info(
                    "dev_instance_create_already_running",
                    extra={"dev_instance_name": payload.name},
                )
    except DevInstanceRejectedError as exc:
        raise HTTPException(status_code=400, detail={"reasons": exc.reasons}) from exc
    except DevInstanceConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except DevInstanceOperationFencedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except DevInstanceAccessError as exc:
        raise HTTPException(
            status_code=409,
            detail="authenticated owner cannot be bootstrapped into the dev instance",
        ) from exc
    except Exception:
        logger.exception(
            "dev_instance_create_failed",
            extra={"dev_instance_name": payload.name},
        )
        raise HTTPException(
            status_code=503,
            detail="dev instance provisioning failed; status is available for retry",
        ) from None
    return _response(record)


@router.get("/dev-instances", response_model=DevInstanceListResponse)
async def list_dev_instances(
    request: Request,
    sc: SessionAndCtx,
    mine: bool = Query(default=False),
    include_deleted: bool = Query(default=False),
) -> DevInstanceListResponse:
    session, ctx = sc
    require_scope(ctx, "read:own")
    owner_filter = ctx.user_id
    if is_admin(ctx) and not mine:
        owner_filter = None
    elif owner_filter is None:
        raise HTTPException(status_code=403, detail="a user identity is required")
    rows = await _store(request, session).list_visible(
        owner_user_id=owner_filter,
        include_deleted=include_deleted,
    )
    return DevInstanceListResponse(items=[_response(row) for row in rows])


@router.get("/dev-instances/{name}", response_model=DevInstanceResponse)
async def get_dev_instance(
    name: str,
    request: Request,
    sc: SessionAndCtx,
) -> DevInstanceResponse:
    session, ctx = sc
    require_scope(ctx, "read:own")
    record = _visible(await _store(request, session).get(name), ctx)
    return _response(record)


@router.get(
    "/dev-instances/{name}/operations/{operation_id}",
    response_model=PersonalDevLifecycleOperationResponse,
)
async def get_personal_dev_lifecycle_operation(
    name: str,
    operation_id: UUID,
    request: Request,
    sc: SessionAndCtx,
) -> PersonalDevLifecycleOperationResponse:
    session, ctx = sc
    require_scope(ctx, "read:own")
    operation = await _personal_authority(request, session).get_operation(operation_id)
    if (
        operation is None
        or operation.environment_name != name
        or (not is_admin(ctx) and operation.owner_user_id != ctx.user_id)
    ):
        raise HTTPException(status_code=404, detail="personal-dev lifecycle operation not found")
    return _personal_operation_response(operation)


@router.delete("/dev-instances/{name}", status_code=202, response_model=DevInstanceResponse)
async def delete_dev_instance(
    name: str,
    request: Request,
    sc: SessionAndCtx,
    response: Response,
    keep_data: bool = Query(default=False),
) -> DevInstanceResponse:
    session, ctx = sc
    if not is_admin(ctx):
        require_scope(ctx, "submit")
        _require_user_identity(ctx)
    store = _store(request, session)
    visible = _visible(await store.get(name), ctx)
    if visible.candidate_id is not None:
        raise HTTPException(
            status_code=409,
            detail="guarded personal-dev destroy is not available until drain authority is ready",
        )
    provisioner = _factory(request)(store)
    try:
        runner = _runner(request)
        if runner is None:
            record = await provisioner.destroy(name, keep_data=keep_data)
        else:
            reservation = await provisioner.claim_destroy(name, keep_data=keep_data)
            record = reservation.record if reservation is not None else None
            if record is not None and record.status == "deleting":
                runner.submit_destroy(record)
    except DevInstanceConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except DevInstanceOperationFencedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception:
        logger.exception(
            "dev_instance_delete_failed",
            extra={"dev_instance_name": name},
        )
        raise HTTPException(
            status_code=503,
            detail="dev instance deletion failed; status is available for retry",
        ) from None
    if record is None:
        raise HTTPException(status_code=404, detail="dev instance not found")
    response.status_code = 202
    return _response(record)


__all__ = ["AccessSnapshotFactory", "ProvisionerFactory", "StoreFactory", "router"]
