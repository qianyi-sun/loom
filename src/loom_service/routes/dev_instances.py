"""Guarded self-service lifecycle API for shared-fleet dev environments."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Annotated, Literal, Protocol, cast
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request, Response
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
from loom.personal_dev_activation import (
    PersonalDevActivationAcknowledgement,
    PersonalDevActivationIntent,
    PersonalDevActivationIntentRequest,
    PersonalDevActivationVerifier,
)
from loom.personal_dev_capacity import PersonalDevCapacityAvailability
from loom.personal_dev_environment import (
    PersonalDevApplyReservation,
    PersonalDevEnvironmentApplyRequest,
    PersonalDevEnvironmentDestroyRequest,
    PersonalDevEnvironmentRecord,
    PersonalDevLifecycleLimits,
    PersonalDevLifecycleOperationRecord,
)
from loom.personal_dev_environment_store import (
    PersonalDevEnvironmentConflictError,
    PersonalDevEnvironmentEpochFencedError,
    PersonalDevEnvironmentNotFoundError,
    PersonalDevEnvironmentOperationFencedError,
    SqlAlchemyPersonalDevActivationIntentReader,
    SqlAlchemyPersonalDevEnvironmentAuthority,
)
from loom.personal_dev_expected_denial import (
    EXPECTED_HIDDEN_DENIAL_PHASE_HEADER,
    expected_hidden_denial_phase,
)
from loom.personal_dev_runtime import (
    PersonalDevAcceptanceInterlockError,
    PersonalDevOperationalInterlockError,
)
from loom_service.auth_guards import is_admin, require_scope, require_submitting_user
from loom_service.dependencies import SessionAndCtx
from loom_service.dev_instance_access import (
    DevInstanceAccessError,
    access_binding_from_context,
    load_owner_access_snapshot,
)

logger = logging.getLogger(__name__)
router = APIRouter()
internal_router = APIRouter()

ProvisionerFactory = Callable[[InstanceStore], DevInstanceProvisioner]


async def _assert_personal_dev_acceptance(request: Request) -> None:
    mode = getattr(request.app.state, "personal_dev_runtime_mode", None)
    enablement_required = getattr(
        request.app.state,
        "personal_dev_enablement_required",
        None,
    )
    if enablement_required is None:
        # Preserve the owner-local request harness contract while old callers
        # migrate to the explicit runtime mode.
        enablement_required = getattr(
            request.app.state,
            "personal_dev_acceptance_required",
            False,
        )
        if enablement_required and mode is None:
            mode = "acceptance"
    if enablement_required is not True:
        return
    if mode == "operational":
        interlock = getattr(
            request.app.state,
            "personal_dev_operational_interlock",
            None,
        )
        assert_ready = getattr(interlock, "assert_ready", None)
        if not callable(assert_ready):
            raise HTTPException(
                status_code=503,
                detail="personal-dev operational operational-interlock-unavailable",
            )
        try:
            await assert_ready(now=datetime.now(UTC))
        except PersonalDevOperationalInterlockError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"personal-dev operational {exc.code}",
            ) from None
        except Exception:
            raise HTTPException(
                status_code=503,
                detail="personal-dev operational operational-interlock-unavailable",
            ) from None
        return
    interlock = getattr(request.app.state, "personal_dev_acceptance_interlock", None)
    assert_ready = getattr(interlock, "assert_ready", None)
    if not callable(assert_ready):
        raise HTTPException(
            status_code=503,
            detail="personal-dev acceptance acceptance-interlock-unavailable",
        )
    try:
        await assert_ready(now=datetime.now(UTC))
    except PersonalDevAcceptanceInterlockError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"personal-dev acceptance {exc.code}",
        ) from None
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="personal-dev acceptance acceptance-interlock-unavailable",
        ) from None


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


class PersonalDevActivationAcknowledgementPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment_name: str = Field(min_length=1, max_length=20)
    subject_id: UUID
    subject_incarnation: UUID
    operation_id: UUID
    operation_epoch: int = Field(gt=0)
    attempt_id: UUID
    candidate_id: UUID
    candidate_sha: str = Field(pattern=r"^[0-9a-f]{64}$")
    deployment_generation: int = Field(gt=0)
    readiness_evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    local_activation_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    agent_key_id: str = Field(pattern=r"^[a-z][a-z0-9._-]{0,63}$")
    observed_at: datetime


class PersonalDevActivationIntentRequestPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_key_id: str = Field(pattern=r"^[a-z][a-z0-9._-]{0,63}$")
    request_nonce: UUID
    requested_at: datetime
    operation_id: UUID | None = None
    exclude_operation_ids: list[UUID] = Field(default_factory=list, max_length=16)


class PersonalDevActivationIntentPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment_name: str
    subject_id: UUID
    subject_incarnation: UUID
    operation_id: UUID
    operation_epoch: int
    attempt_id: UUID
    attempt_sequence: int
    candidate_id: UUID
    candidate_sha: str
    candidate_publication_sha256: str
    deployment_generation: int
    readiness_evidence_sha256: str
    min_slots: int
    max_slots: int
    images: dict[str, str]
    intent_created_at: datetime
    intent_sha256: str


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
    application_status: Literal[
        "provisioning",
        "ready",
        "updating",
        "activating",
        "deleting",
        "draining",
        "failed",
        "deleted",
    ]
    capacity_status: Literal["shadow", "prepared", "waiting", "available"]
    capacity_prepared: bool
    worker_available: bool
    min_slots: int
    max_slots: int
    deployment_generation: int
    candidate_sha: str
    candidate_id: UUID | None = None
    subject_id: UUID | None = None
    subject_incarnation: UUID | None = None
    operation_epoch: int
    operation_id: UUID
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
    application_status: Literal[
        "provisioning",
        "ready",
        "updating",
        "activating",
        "deleting",
        "draining",
        "failed",
        "deleted",
    ]
    capacity_status: Literal["shadow", "prepared", "waiting", "available"]
    capacity_prepared: bool
    worker_available: bool
    min_slots: int
    max_slots: int
    deployment_generation: int
    candidate_id: UUID | None
    candidate_sha: str
    operation_epoch: int
    operation_id: UUID
    operation_step: str
    keep_data: bool
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
    kind: Literal["create", "update", "capacity", "destroy", "noop"]
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
    keep_data: bool
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


def _response(
    record: DevInstanceRecord | PersonalDevEnvironmentRecord,
    availability: PersonalDevCapacityAvailability | None = None,
) -> DevInstanceResponse:
    personal = (
        record.subject_id is not None
        and record.subject_incarnation is not None
        and record.candidate_id is not None
    )
    status = availability or PersonalDevCapacityAvailability(
        "waiting" if personal else "shadow", personal, False
    )
    return DevInstanceResponse(
        name=record.name,
        owner_user_id=record.owner_user_id,
        owner_team_id=record.owner_team_id,
        status=record.status,
        application_status=record.status,
        capacity_status=status.capacity_status,
        capacity_prepared=status.capacity_prepared,
        worker_available=status.worker_available,
        min_slots=record.min_slots,
        max_slots=record.max_slots,
        deployment_generation=record.deployment_generation,
        candidate_sha=record.candidate_sha,
        candidate_id=record.candidate_id,
        subject_id=record.subject_id,
        subject_incarnation=record.subject_incarnation,
        operation_epoch=record.operation_epoch,
        operation_id=record.operation_id,
        operation_step=record.operation_step,
        keep_data=record.keep_data,
        failure_reason=record.failure_reason,
        created_at=record.created_at,
        updated_at=record.updated_at,
        ready_at=record.ready_at,
        deleted_at=record.deleted_at,
        identity=_identity_response(derive_identity(record.name)),
    )


async def _enriched_response(request: Request, record: DevInstanceRecord) -> DevInstanceResponse:
    """Keep GET/list truthful when the optional observer cannot prove a worker."""

    if (
        record.subject_id is None
        or record.subject_incarnation is None
        or record.candidate_id is None
        or record.capacity_namespace is None
        or record.capacity_database is None
    ):
        return _response(record)
    reader = getattr(request.app.state, "personal_dev_capacity_status_reader", None)
    if reader is None:
        return _response(record, PersonalDevCapacityAvailability("waiting", True, False))
    try:
        availability = await reader.read(
            namespace=record.capacity_namespace,
            database=record.capacity_database,
            subject_id=record.subject_id,
            subject_incarnation=record.subject_incarnation,
            deployment_generation=record.deployment_generation,
        )
    except Exception:
        availability = PersonalDevCapacityAvailability("waiting", True, False)
    return _response(record, availability)


def _personal_environment_response(
    record: PersonalDevEnvironmentRecord,
) -> PersonalDevEnvironmentResponse:
    capacity_prepared = record.capacity_configuration_epoch is not None
    return PersonalDevEnvironmentResponse(
        name=record.name,
        subject_id=record.subject_id,
        subject_incarnation=record.subject_incarnation,
        owner_user_id=record.owner_user_id,
        owner_team_id=record.owner_team_id,
        status=record.status,
        application_status=record.status,
        capacity_status="prepared" if capacity_prepared else "shadow",
        capacity_prepared=capacity_prepared,
        # No protected worker-availability evidence is stored on this record.
        # Application/pod readiness must never imply worker availability.
        worker_available=False,
        min_slots=record.min_slots,
        max_slots=record.max_slots,
        deployment_generation=record.deployment_generation,
        candidate_id=record.candidate_id,
        candidate_sha=record.candidate_sha,
        operation_epoch=record.operation_epoch,
        operation_id=record.operation_id,
        operation_step=record.operation_step,
        keep_data=record.keep_data,
        failure_reason=record.failure_reason,
        created_at=record.created_at,
        updated_at=record.updated_at,
        ready_at=record.ready_at,
        deleted_at=record.deleted_at,
        identity=_identity_response(derive_identity(record.name)),
    )


def _personal_activation_intent_response(
    intent: PersonalDevActivationIntent,
) -> PersonalDevActivationIntentPayload:
    return PersonalDevActivationIntentPayload(
        environment_name=intent.environment_name,
        subject_id=intent.subject_id,
        subject_incarnation=intent.subject_incarnation,
        operation_id=intent.operation_id,
        operation_epoch=intent.operation_epoch,
        attempt_id=intent.attempt_id,
        attempt_sequence=intent.attempt_sequence,
        candidate_id=intent.candidate_id,
        candidate_sha=intent.candidate_sha,
        candidate_publication_sha256=intent.candidate_publication_sha256,
        deployment_generation=intent.deployment_generation,
        readiness_evidence_sha256=intent.readiness_evidence_sha256,
        min_slots=intent.min_slots,
        max_slots=intent.max_slots,
        images=dict(intent.images),
        intent_created_at=intent.intent_created_at,
        intent_sha256=intent.intent_sha256,
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
        keep_data=record.keep_data,
        failure_reason=record.failure_reason,
        readiness_evidence_sha256=record.readiness_evidence_sha256,
        activation_acknowledgement_sha256=(record.activation_acknowledgement_sha256),
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
                global_live_instances=(settings.personal_dev_global_live_instance_limit),
                per_owner_live_instances=(settings.personal_dev_per_owner_live_instance_limit),
                per_owner_aggregate_min_slots=(settings.personal_dev_per_owner_aggregate_min_slots),
                per_owner_aggregate_max_slots=(settings.personal_dev_per_owner_aggregate_max_slots),
            ),
        )
    if not callable(factory):
        raise HTTPException(
            status_code=503, detail="personal-dev lifecycle authority is unavailable"
        )
    return cast(PersonalAuthorityFactory, factory)(session)


def _visible(
    record: DevInstanceRecord | None,
    ctx: AuthContext,
    *,
    operation: Literal["read", "destroy"],
) -> DevInstanceRecord:
    if record is None:
        raise HTTPException(status_code=404, detail="dev instance not found")
    if not is_admin(ctx) and record.owner_user_id != ctx.user_id:
        raise HTTPException(
            status_code=404,
            detail="dev instance not found",
            headers={
                EXPECTED_HIDDEN_DENIAL_PHASE_HEADER: expected_hidden_denial_phase(
                    operation,
                ),
            },
        )
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
    if getattr(request.app.state, "personal_dev_builder_available", None) is False:
        raise HTTPException(
            status_code=503,
            detail="personal-dev candidate builder is not activated in this environment",
        )
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
            access_binding=access_binding_from_context(ctx),
        )
    except PersonalDevEnvironmentNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail="personal-dev resource not found",
            headers={
                EXPECTED_HIDDEN_DENIAL_PHASE_HEADER: expected_hidden_denial_phase(
                    "update",
                ),
            },
        ) from exc
    except (
        PersonalDevEnvironmentConflictError,
        PersonalDevEnvironmentEpochFencedError,
        PersonalDevEnvironmentOperationFencedError,
    ) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except DevInstanceAccessError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
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


@internal_router.post(
    "/personal-dev/activation-intents/next",
    response_model=PersonalDevActivationIntentPayload,
    responses={204: {"description": "No current activation intent"}},
)
async def next_personal_dev_activation_intent(
    payload: PersonalDevActivationIntentRequestPayload,
    request: Request,
    signature: Annotated[str, Header(alias="X-Loom-Activation-Signature")],
) -> PersonalDevActivationIntentPayload | Response:
    """Return one current intent only to an agent proving signing-key possession."""
    await _assert_personal_dev_acceptance(request)
    verifier = getattr(request.app.state, "personal_dev_activation_verifier", None)
    session_factory = getattr(request.app.state, "session_factory", None)
    if not isinstance(verifier, PersonalDevActivationVerifier) or session_factory is None:
        raise HTTPException(status_code=503, detail="personal-dev activation authority unavailable")
    try:
        intent_request = PersonalDevActivationIntentRequest(
            agent_key_id=payload.agent_key_id,
            request_nonce=payload.request_nonce,
            requested_at=payload.requested_at,
            operation_id=payload.operation_id,
            exclude_operation_ids=tuple(payload.exclude_operation_ids),
        )
        verifier.verify_intent_request(
            intent_request,
            signature=signature,
            now=datetime.now(payload.requested_at.tzinfo),
        )
    except ValueError:
        raise HTTPException(
            status_code=403,
            detail="personal-dev activation intent request is invalid",
        ) from None
    try:
        async with session_factory() as session:
            intent = await SqlAlchemyPersonalDevActivationIntentReader(session).next_intent(
                operation_id=intent_request.operation_id,
                exclude_operation_ids=intent_request.exclude_operation_ids,
            )
    except PersonalDevEnvironmentOperationFencedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception:
        logger.exception("personal_dev_activation_intent_read_failed")
        raise HTTPException(
            status_code=503,
            detail="personal-dev activation intent could not be read",
        ) from None
    if intent is None:
        return Response(status_code=204)
    return _personal_activation_intent_response(intent)


@internal_router.post(
    "/personal-dev/activation-acknowledgements",
    response_model=PersonalDevEnvironmentApplyResponse,
)
async def acknowledge_personal_dev_activation(
    payload: PersonalDevActivationAcknowledgementPayload,
    request: Request,
    signature: Annotated[str, Header(alias="X-Loom-Activation-Signature")],
) -> PersonalDevEnvironmentApplyResponse:
    """Accept only fresh evidence signed by the trusted environment agent."""
    await _assert_personal_dev_acceptance(request)
    verifier = getattr(request.app.state, "personal_dev_activation_verifier", None)
    session_factory = getattr(request.app.state, "session_factory", None)
    if not isinstance(verifier, PersonalDevActivationVerifier) or session_factory is None:
        raise HTTPException(status_code=503, detail="personal-dev activation authority unavailable")
    try:
        acknowledgement = PersonalDevActivationAcknowledgement(**payload.model_dump())
        verified = verifier.verify(
            acknowledgement,
            signature=signature,
            now=datetime.now(acknowledgement.observed_at.tzinfo),
        )
    except ValueError:
        raise HTTPException(
            status_code=403,
            detail="personal-dev activation acknowledgement is invalid",
        ) from None
    try:
        async with session_factory() as session:
            reservation = await _personal_authority(request, session).acknowledge_activation(
                verified=verified,
            )
    except (
        PersonalDevEnvironmentConflictError,
        PersonalDevEnvironmentOperationFencedError,
    ) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception:
        logger.exception(
            "personal_dev_activation_acknowledgement_failed",
            extra={"dev_instance_name": payload.environment_name},
        )
        raise HTTPException(
            status_code=503,
            detail="personal-dev activation acknowledgement could not be committed",
        ) from None
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
    return DevInstanceListResponse(items=[await _enriched_response(request, row) for row in rows])


@router.get("/dev-instances/{name}", response_model=DevInstanceResponse)
async def get_dev_instance(
    name: str,
    request: Request,
    sc: SessionAndCtx,
) -> DevInstanceResponse:
    session, ctx = sc
    require_scope(ctx, "read:own")
    record = _visible(
        await _store(request, session).get(name),
        ctx,
        operation="read",
    )
    return await _enriched_response(request, record)


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
    expected_operation_epoch: int | None = Query(default=None, ge=1),
    idempotency_key: Annotated[UUID | None, Query()] = None,
) -> DevInstanceResponse:
    session, ctx = sc
    if not is_admin(ctx):
        require_scope(ctx, "submit")
        _require_user_identity(ctx)
    store = _store(request, session)
    visible = _visible(await store.get(name), ctx, operation="destroy")
    if visible.candidate_id is not None:
        if expected_operation_epoch is None or idempotency_key is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "personal-dev destroy requires expected_operation_epoch and idempotency_key"
                ),
            )
        try:
            personal_reservation = await _personal_authority(request, session).destroy(
                PersonalDevEnvironmentDestroyRequest(
                    name=name,
                    owner_user_id=visible.owner_user_id,
                    owner_team_id=visible.owner_team_id,
                    expected_operation_epoch=expected_operation_epoch,
                    idempotency_key=idempotency_key,
                    keep_data=keep_data,
                ),
                access_binding=access_binding_from_context(ctx),
            )
        except PersonalDevEnvironmentNotFoundError as exc:
            raise HTTPException(status_code=404, detail="personal-dev resource not found") from exc
        except (
            PersonalDevEnvironmentConflictError,
            PersonalDevEnvironmentEpochFencedError,
            PersonalDevEnvironmentOperationFencedError,
        ) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except (DevInstanceAccessError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception:
            logger.exception(
                "personal_dev_environment_destroy_failed",
                extra={"dev_instance_name": name},
            )
            raise HTTPException(
                status_code=503,
                detail="personal-dev destroy failed before manager retirement",
            ) from None
        response.status_code = 200 if personal_reservation.operation.state == "succeeded" else 202
        return _response(personal_reservation.environment)
    provisioner = _factory(request)(store)
    try:
        runner = _runner(request)
        if runner is None:
            record = await provisioner.destroy(name, keep_data=keep_data)
        else:
            legacy_reservation = await provisioner.claim_destroy(name, keep_data=keep_data)
            record = legacy_reservation.record if legacy_reservation is not None else None
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
