"""Serializable, fenced persistence for capacity-manager shadow state."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, TypeVar, cast
from uuid import UUID

from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.contracts import (
    AccountPolicyV1,
    AllocationInputV1,
    ConfigurationActivationV1,
    ConfigurationGenerationRefV1,
    ConfigurationSnapshotV1,
    DemandSnapshotV1,
    DynamicDevelopmentSubjectProjectionV1,
    FairnessCursorV1,
    FleetManifestV1,
    InputFreshnessV1,
    ObservedCommitmentV1,
    PoolAllocationInputV1,
    PoolObservationV1,
    ProfileReferenceV1,
    ResourceVectorV1,
    ShadowEpochV1,
    StrictV1Model,
    SubjectAllocationInputV1,
    SubjectConfigurationV1,
    canonical_digest,
    canonical_digest_excluding,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorRegistrationV2,
    ExecutionActivationV2,
    ExecutionAuthorityV2,
    ExecutionContextV2,
    ExecutionPreparationPolicyV2,
    ExecutionPreparationV2,
    LegacyWriterFenceV2,
    canonical_executable_digest,
)
from loom_capacity_manager.fleet_state import (
    FleetStateError,
    validate_fleet_manifest_digests,
    validate_profile_narrowing,
)
from loom_capacity_manager.models import (
    CapacityAccountPolicy,
    CapacityAllocation,
    CapacityAllocationEpoch,
    CapacityAuditEvent,
    CapacityAuthorityState,
    CapacityCandidate,
    CapacityConfigGeneration,
    CapacityConfigurationEpoch,
    CapacityDemandReporter,
    CapacityDemandSnapshot,
    CapacityDeploymentGeneration,
    CapacityDevelopmentProjection,
    CapacityExecutionEpoch,
    CapacityExecutionExecutor,
    CapacityFairnessState,
    CapacityObservedCommitment,
    CapacityPool,
    CapacityPoolObservation,
    CapacityPoolReporter,
    CapacityReservationShape,
    CapacityReservationTranche,
    CapacitySubject,
    CapacityTier,
    CapacityWorkerProfile,
)


class CapacityStoreError(RuntimeError):
    """Base class for bounded store failures."""


class ConfigurationConflictError(CapacityStoreError):
    pass


class IdempotencyConflictError(CapacityStoreError):
    pass


class AuthorityRecoveryError(CapacityStoreError):
    pass


class StaleWriterError(CapacityStoreError):
    pass


class UnknownReporterError(CapacityStoreError):
    pass


class StaleReportError(CapacityStoreError):
    pass


class ReportEquivocationError(CapacityStoreError):
    pass


class StaleAllocationInputError(CapacityStoreError):
    pass


class ExecutionConflictError(CapacityStoreError):
    pass


class ExecutionPreparationDisabledError(ExecutionConflictError):
    pass


@dataclass(frozen=True, slots=True)
class ProposedConfiguration:
    configuration_id: UUID
    scope: Literal["fleet", "subject"]
    generation: int
    digest: str
    subject_id: UUID | None
    subject_incarnation: UUID | None


@dataclass(frozen=True, slots=True)
class ActivatedConfiguration:
    configuration_epoch: int
    digest: str
    snapshot: ConfigurationSnapshotV1


@dataclass(frozen=True, slots=True)
class ProjectedDevelopmentSubject:
    configuration_epoch: int
    configuration_digest: str
    subject: SubjectConfigurationV1
    account: AccountPolicyV1
    replayed: bool


@dataclass(frozen=True, slots=True)
class WriterFence:
    authority_incarnation: UUID
    writer_epoch: int


@dataclass(frozen=True, slots=True)
class IngestResult:
    snapshot_id: UUID
    digest: str
    sequence: int
    replayed: bool


@dataclass(frozen=True, slots=True)
class CommittedShadowEpoch:
    allocation_epoch: int
    input_digest: str


@dataclass(frozen=True, slots=True)
class RecordedShadowFailure:
    allocation_epoch: int | None
    input_digest: str


@dataclass(frozen=True, slots=True)
class CapacityStatusPageV1:
    authority_incarnation: UUID
    writer_epoch: int
    configuration_epoch: int
    configuration_digest: str | None
    latest_allocation_epoch: int | None
    execution_epoch: int
    execution_state: Literal["shadow", "prepared", "active", "drain-only"]
    execution_manifest_sha256: str | None
    executable_new_capacity_ceiling: int
    increase_freeze: bool
    items: tuple[dict[str, Any], ...]
    next_cursor: int | None


_ContractT = TypeVar("_ContractT", bound=StrictV1Model)


def _parse_contract(model: type[_ContractT], payload: dict[str, Any]) -> _ContractT:
    return model.model_validate_json(json.dumps(payload, sort_keys=True, separators=(",", ":")))


def _deduplicate_observed_commitments(
    values: list[ObservedCommitmentV1],
) -> tuple[ObservedCommitmentV1, ...]:
    """Merge exact multi-source observations; retain conflicts as extra charge."""

    grouped: dict[tuple[str, str], list[ObservedCommitmentV1]] = {}
    for value in values:
        grouped.setdefault((value.kind, value.commitment_id), []).append(value)
    result: list[ObservedCommitmentV1] = []
    state_rank = {
        "proposed": 0,
        "accepted": 1,
        "pending": 2,
        "live": 3,
        "observed": 4,
        "draining": 5,
        "cancel-pending": 6,
        "submitting-unknown": 7,
        "unknown": 8,
        "quarantined": 9,
    }
    ignored = {"state", "ownership_state", "reservation_identity"}
    for key in sorted(grouped):
        group = grouped[key]
        payloads = []
        for item in group:
            payload = item.model_dump(mode="json", exclude_none=False)
            for field in ignored:
                payload.pop(field, None)
            payloads.append(payload)
        authenticated = [item for item in group if item.ownership_state == "authenticated"]
        authenticated_reservations = {item.reservation_identity for item in authenticated}
        if (
            all(payload == payloads[0] for payload in payloads[1:])
            and len(authenticated_reservations) <= 1
        ):
            chosen = authenticated[0] if authenticated else group[0]
            conservative = max(group, key=lambda item: state_rank[item.state]).state
            result.append(chosen.model_copy(update={"state": conservative}))
            continue
        for item in group:
            digest = canonical_digest_excluding(item, "state")[:24]
            result.append(
                item.model_copy(
                    update={
                        "commitment_id": f"conflict-{digest}",
                        "state": "quarantined",
                        "ownership_state": "unverified",
                        "reservation_identity": None,
                    }
                )
            )
    return tuple(sorted(result, key=lambda item: (item.kind, item.commitment_id)))


@asynccontextmanager
async def _write_transaction(session: AsyncSession) -> AsyncIterator[None]:
    transaction = session.begin_nested() if session.in_transaction() else session.begin()
    try:
        async with transaction:
            connection = await session.connection()
            isolation_level = await connection.get_isolation_level()
            if isolation_level.upper() != "SERIALIZABLE":
                raise CapacityStoreError(
                    "capacity mutations require a SERIALIZABLE database session"
                )
            yield
    except DBAPIError as exc:
        sqlstate = getattr(exc.orig, "sqlstate", None)
        if sqlstate in {"40001", "40P01"}:
            raise CapacityStoreError("serializable capacity transaction must be retried") from exc
        raise


async def _db_now(session: AsyncSession) -> datetime:
    value = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    value = cast(datetime, value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


async def _lock_authority(session: AsyncSession) -> CapacityAuthorityState:
    authority = await _lock_any_authority(session)
    if authority.executable_new_capacity_ceiling != 0:
        raise AuthorityRecoveryError("capacity authority is not shadow-only")
    return authority


async def _lock_any_authority(session: AsyncSession) -> CapacityAuthorityState:
    authority = (
        await session.execute(
            select(CapacityAuthorityState)
            .where(CapacityAuthorityState.singleton_id == 1)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if authority is None:
        raise AuthorityRecoveryError("capacity authority row is missing")
    return authority


def _bounded_detail(detail: dict[str, Any]) -> dict[str, Any]:
    encoded = json.dumps(detail, sort_keys=True, separators=(",", ":"), default=str)
    if len(encoded.encode("utf-8")) > 16 * 1024:
        raise CapacityStoreError("audit detail exceeds 16 KiB")
    return detail


def _canonical_json_digest(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
        default=str,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _legacy_writer_key(
    value: LegacyWriterFenceV2,
) -> tuple[str, str, str, str]:
    return (value.scope_kind, value.scope_id, value.writer_kind, value.writer_id)


def _audit(
    *,
    actor_kind: str,
    actor_id: str,
    event_kind: str,
    object_binding: dict[str, Any],
    detail: dict[str, Any],
) -> CapacityAuditEvent:
    return CapacityAuditEvent(
        actor_kind=actor_kind,
        actor_id=actor_id,
        event_kind=event_kind,
        object_binding=_bounded_detail(object_binding),
        detail=_bounded_detail(detail),
    )


def _owner_account_id(owner_id: UUID) -> str:
    return f"dev-owner-{owner_id.hex}"


def _derive_owner_account(
    fleet: FleetManifestV1,
    owner_id: UUID,
) -> AccountPolicyV1:
    template = fleet.development_subject_template
    if template is None:
        raise ConfigurationConflictError(
            "active fleet does not permit dynamic development subjects"
        )
    source = next(
        (
            account
            for account in fleet.account_policies
            if account.account_id == template.owner_account_template_id
            and account.kind == "owner_template"
        ),
        None,
    )
    if source is None:  # FleetManifestV1 already proves this binding.
        raise ConfigurationConflictError("development owner template is unavailable")
    return source.model_copy(
        update={
            "account_id": _owner_account_id(owner_id),
            "kind": "owner",
            "owner_id": owner_id,
        }
    )


class CapacityManagementStore:
    """Database-time, serializable store with separately fenced execution authority."""

    def __init__(
        self,
        *,
        freshness_seconds: int = 120,
        execution_policy: ExecutionPreparationPolicyV2 | None = None,
    ) -> None:
        if type(freshness_seconds) is not int or freshness_seconds <= 0:
            raise ValueError("freshness_seconds must be a positive integer")
        self._freshness = timedelta(seconds=freshness_seconds)
        self._execution_policy = execution_policy

    async def propose_fleet_configuration(
        self,
        session: AsyncSession,
        manifest: FleetManifestV1,
        *,
        actor: str,
        idempotency_key: UUID,
    ) -> ProposedConfiguration:
        return await self._propose_configuration(
            session,
            scope="fleet",
            value=manifest,
            generation=manifest.fleet_generation,
            subject_id=None,
            subject_incarnation=None,
            actor=actor,
            idempotency_key=idempotency_key,
        )

    async def propose_subject_configuration(
        self,
        session: AsyncSession,
        subject: SubjectConfigurationV1,
        *,
        actor: str,
        idempotency_key: UUID,
    ) -> ProposedConfiguration:
        return await self._propose_configuration(
            session,
            scope="subject",
            value=subject,
            generation=subject.configuration_generation,
            subject_id=subject.subject_id,
            subject_incarnation=subject.subject_incarnation,
            actor=actor,
            idempotency_key=idempotency_key,
        )

    async def _propose_configuration(
        self,
        session: AsyncSession,
        *,
        scope: Literal["fleet", "subject"],
        value: StrictV1Model,
        generation: int,
        subject_id: UUID | None,
        subject_incarnation: UUID | None,
        actor: str,
        idempotency_key: UUID,
    ) -> ProposedConfiguration:
        digest = canonical_digest(value)
        payload = value.model_dump(mode="json", exclude_none=False)
        async with _write_transaction(session):
            await _lock_authority(session)
            replay = (
                await session.execute(
                    select(CapacityConfigGeneration).where(
                        CapacityConfigGeneration.idempotency_key == idempotency_key
                    )
                )
            ).scalar_one_or_none()
            if replay is not None:
                if (
                    replay.scope != scope
                    or replay.digest != digest
                    or replay.payload != payload
                    or replay.actor != actor
                ):
                    raise IdempotencyConflictError(
                        "configuration idempotency key was reused with different input"
                    )
                return self._proposal_result(replay)

            if isinstance(value, FleetManifestV1):
                try:
                    validate_fleet_manifest_digests(value)
                except FleetStateError as exc:
                    raise ConfigurationConflictError(str(exc)) from exc

            generation_row = (
                await session.execute(
                    select(CapacityConfigGeneration).where(
                        CapacityConfigGeneration.scope == scope,
                        CapacityConfigGeneration.subject_id.is_(subject_id)
                        if subject_id is None
                        else CapacityConfigGeneration.subject_id == subject_id,
                        CapacityConfigGeneration.subject_incarnation.is_(subject_incarnation)
                        if subject_incarnation is None
                        else CapacityConfigGeneration.subject_incarnation == subject_incarnation,
                        CapacityConfigGeneration.scope_generation == generation,
                    )
                )
            ).scalar_one_or_none()
            if generation_row is not None:
                raise ConfigurationConflictError(
                    "immutable configuration generation already exists"
                )

            row = CapacityConfigGeneration(
                scope=scope,
                subject_id=subject_id,
                subject_incarnation=subject_incarnation,
                scope_generation=generation,
                digest=digest,
                payload=payload,
                state="proposed",
                actor=actor,
                idempotency_key=idempotency_key,
            )
            session.add(row)
            await session.flush()
            session.add(
                _audit(
                    actor_kind="operator",
                    actor_id=actor,
                    event_kind="capacity_configuration_proposed",
                    object_binding={
                        "scope": scope,
                        "configuration_id": str(row.id),
                        "generation": generation,
                    },
                    detail={"digest": digest},
                )
            )
            return self._proposal_result(row)

    @staticmethod
    def _proposal_result(row: CapacityConfigGeneration) -> ProposedConfiguration:
        return ProposedConfiguration(
            configuration_id=row.id,
            scope=row.scope,  # type: ignore[arg-type]
            generation=row.scope_generation,
            digest=row.digest,
            subject_id=row.subject_id,
            subject_incarnation=row.subject_incarnation,
        )

    async def activate_configuration(
        self,
        session: AsyncSession,
        proposal: ConfigurationActivationV1,
        *,
        actor: str,
        idempotency_key: UUID,
    ) -> ActivatedConfiguration:
        request_digest = canonical_digest(proposal)
        async with _write_transaction(session):
            authority = await _lock_authority(session)
            replay = (
                await session.execute(
                    select(CapacityConfigurationEpoch).where(
                        CapacityConfigurationEpoch.activation_idempotency_key == idempotency_key
                    )
                )
            ).scalar_one_or_none()
            if replay is not None:
                if (
                    replay.activation_actor != actor
                    or replay.activation_request_digest != request_digest
                ):
                    raise IdempotencyConflictError(
                        "configuration activation idempotency key was reused with different input"
                    )
                snapshot = ConfigurationSnapshotV1(
                    configuration_epoch=replay.configuration_epoch,
                    fleet=ConfigurationGenerationRefV1(
                        scope="fleet",
                        generation=replay.fleet_generation,
                        digest=replay.fleet_digest,
                    ),
                    subjects=tuple(
                        _parse_contract(ConfigurationGenerationRefV1, item)
                        for item in replay.subject_generation_manifest
                    ),
                )
                return ActivatedConfiguration(
                    configuration_epoch=replay.configuration_epoch,
                    digest=replay.canonical_digest,
                    snapshot=snapshot,
                )
            latest = (
                (
                    await session.execute(
                        select(CapacityConfigurationEpoch).order_by(
                            CapacityConfigurationEpoch.configuration_epoch.desc()
                        )
                    )
                )
                .scalars()
                .first()
            )
            current_epoch = 0 if latest is None else latest.configuration_epoch
            if current_epoch != proposal.expected_configuration_epoch:
                raise ConfigurationConflictError("expected active configuration epoch is stale")

            fleet_row = await self._load_proposal_row(session, proposal.fleet)
            fleet = _parse_contract(FleetManifestV1, fleet_row.payload)
            subject_rows = [
                await self._load_proposal_row(session, reference) for reference in proposal.subjects
            ]
            subjects = tuple(
                _parse_contract(SubjectConfigurationV1, row.payload) for row in subject_rows
            )
            derived_accounts: tuple[AccountPolicyV1, ...] = ()
            if latest is not None:
                previous_accounts = (
                    (
                        await session.execute(
                            select(CapacityAccountPolicy).where(
                                CapacityAccountPolicy.configuration_epoch
                                == latest.configuration_epoch,
                                CapacityAccountPolicy.kind == "owner",
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                derived_accounts = tuple(
                    _derive_owner_account(fleet, account.owner_id)
                    for account in previous_accounts
                    if account.owner_id is not None
                )
            self._validate_activation(fleet, subjects, derived_accounts)
            authority.global_pending_slot_ceiling = fleet.global_max_pending_slots
            authority.global_pending_job_ceiling = fleet.global_max_pending_jobs
            authority.global_submission_rate_ceiling = fleet.global_submission_rate_per_minute
            authority.updated_at = await _db_now(session)

            if latest is not None:
                previous_ids = {
                    UUID(item["subject_id"]) for item in latest.subject_generation_manifest
                }
                new_ids = {subject.subject_id for subject in subjects}
                if new_ids != previous_ids:
                    raise ConfigurationConflictError(
                        "active subject manifest must be complete; deletion is unavailable"
                    )

            configuration_epoch = current_epoch + 1
            snapshot = ConfigurationSnapshotV1(
                configuration_epoch=configuration_epoch,
                fleet=ConfigurationGenerationRefV1(
                    scope="fleet",
                    generation=fleet_row.scope_generation,
                    digest=fleet_row.digest,
                ),
                subjects=tuple(
                    ConfigurationGenerationRefV1(
                        scope="subject",
                        generation=row.scope_generation,
                        digest=row.digest,
                        subject_id=row.subject_id,
                        subject_incarnation=row.subject_incarnation,
                    )
                    for row in subject_rows
                ),
            )
            snapshot_digest = canonical_digest(snapshot)
            session.add(
                CapacityConfigurationEpoch(
                    configuration_epoch=configuration_epoch,
                    fleet_generation=fleet.fleet_generation,
                    fleet_digest=fleet_row.digest,
                    subject_generation_manifest=[
                        reference.model_dump(mode="json", exclude_none=False)
                        for reference in snapshot.subjects
                    ],
                    canonical_digest=snapshot_digest,
                    activation_idempotency_key=idempotency_key,
                    activation_actor=actor,
                    activation_request_digest=request_digest,
                )
            )
            await session.flush()
            await self._persist_active_configuration(
                session,
                configuration_epoch=configuration_epoch,
                fleet=fleet,
                subjects=subjects,
                derived_accounts=derived_accounts,
            )
            await session.execute(
                update(CapacityConfigGeneration)
                .where(CapacityConfigGeneration.state == "active")
                .values(state="retired")
            )
            await session.execute(
                update(CapacityConfigGeneration)
                .where(
                    CapacityConfigGeneration.id.in_(
                        [fleet_row.id, *(row.id for row in subject_rows)]
                    )
                )
                .values(state="active")
            )
            session.add(
                _audit(
                    actor_kind="operator",
                    actor_id=actor,
                    event_kind="capacity_configuration_activated",
                    object_binding={"configuration_epoch": configuration_epoch},
                    detail={
                        "digest": snapshot_digest,
                        "idempotency_key": str(idempotency_key),
                        "subject_count": len(subjects),
                    },
                )
            )
            return ActivatedConfiguration(
                configuration_epoch=configuration_epoch,
                digest=snapshot_digest,
                snapshot=snapshot,
            )

    async def project_development_subject(
        self,
        session: AsyncSession,
        request: DynamicDevelopmentSubjectProjectionV1,
        *,
        actor: str,
        idempotency_key: UUID,
    ) -> ProjectedDevelopmentSubject:
        """Atomically derive one personal subject from active operator policy."""

        request_digest = canonical_digest(request)
        request_payload = request.model_dump(mode="json", exclude_none=False)
        async with _write_transaction(session):
            await _lock_authority(session)
            replays = (
                (
                    await session.execute(
                        select(CapacityDevelopmentProjection)
                        .where(
                            or_(
                                CapacityDevelopmentProjection.operation_id == request.operation_id,
                                CapacityDevelopmentProjection.idempotency_key == idempotency_key,
                            )
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .all()
            )
            if len(replays) > 1:
                raise IdempotencyConflictError(
                    "development projection identities belong to different requests"
                )
            replay = replays[0] if replays else None
            if replay is not None:
                if (
                    replay.operation_id != request.operation_id
                    or replay.idempotency_key != idempotency_key
                    or replay.request_digest != request_digest
                    or replay.request_payload != request_payload
                    or replay.actor != actor
                ):
                    raise IdempotencyConflictError(
                        "development projection identity was reused with different input"
                    )
                return ProjectedDevelopmentSubject(
                    configuration_epoch=replay.configuration_epoch,
                    configuration_digest=replay.result_digest,
                    subject=_parse_contract(
                        SubjectConfigurationV1,
                        replay.result_payload["subject"],
                    ),
                    account=_parse_contract(
                        AccountPolicyV1,
                        replay.result_payload["account"],
                    ),
                    replayed=True,
                )

            latest = (
                (
                    await session.execute(
                        select(CapacityConfigurationEpoch)
                        .order_by(CapacityConfigurationEpoch.configuration_epoch.desc())
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if latest is None:
                raise ConfigurationConflictError(
                    "dynamic development projection requires an active fleet"
                )
            if latest.configuration_epoch != request.expected_configuration_epoch:
                raise ConfigurationConflictError("expected active configuration epoch is stale")

            fleet_row = (
                await session.execute(
                    select(CapacityConfigGeneration).where(
                        CapacityConfigGeneration.scope == "fleet",
                        CapacityConfigGeneration.state == "active",
                        CapacityConfigGeneration.digest == latest.fleet_digest,
                    )
                )
            ).scalar_one()
            fleet = _parse_contract(FleetManifestV1, fleet_row.payload)
            template = fleet.development_subject_template
            if template is None:
                raise ConfigurationConflictError(
                    "active fleet does not permit dynamic development subjects"
                )
            if (
                request.operation_kind != "destroy"
                and request.max_slots > template.max_slots_per_subject
            ):
                raise ConfigurationConflictError(
                    "development subject max_slots exceeds the operator template"
                )

            active_rows = (
                (
                    await session.execute(
                        select(CapacityConfigGeneration).where(
                            CapacityConfigGeneration.scope == "subject",
                            CapacityConfigGeneration.state == "active",
                        )
                    )
                )
                .scalars()
                .all()
            )
            active_subjects = {
                subject.subject_id: (row, subject)
                for row in active_rows
                for subject in (_parse_contract(SubjectConfigurationV1, row.payload),)
            }
            existing_pair = active_subjects.get(request.subject_id)
            existing = None if existing_pair is None else existing_pair[1]
            if request.operation_kind == "destroy" and existing is None:
                raise ConfigurationConflictError("dynamic development subject is not active")
            display_name = f"dev-{request.environment_name}"
            if any(
                subject.display_name == display_name and subject.subject_id != request.subject_id
                for _, subject in active_subjects.values()
            ):
                raise ConfigurationConflictError(
                    "dynamic development environment name is already active"
                )
            if existing is not None and (
                existing.subject_incarnation != request.subject_incarnation
                or existing.display_name != display_name
            ):
                raise ConfigurationConflictError(
                    "dynamic development subject identity conflicts with active state"
                )
            if existing is not None and request.operation_kind == "create":
                raise ConfigurationConflictError("dynamic development subject is already active")

            account = _derive_owner_account(fleet, request.owner_id)
            if existing is not None and existing.account_id != account.account_id:
                raise ConfigurationConflictError(
                    "dynamic development subject owner cannot be changed"
                )
            if existing is not None and (
                request.configuration_generation <= existing.configuration_generation
            ):
                raise ConfigurationConflictError(
                    "dynamic development configuration generation is not monotonic"
                )
            if (
                request.operation_kind == "update"
                and existing is not None
                and request.deployment_generation <= existing.deployment_generation
            ):
                raise ConfigurationConflictError(
                    "dynamic development deployment generation is not monotonic"
                )
            if (
                request.operation_kind in {"capacity", "destroy"}
                and existing is not None
                and (
                    request.deployment_generation != existing.deployment_generation
                    or request.candidate_generation != existing.candidate_generation
                    or request.demand_reporter_incarnation != existing.demand_reporter_incarnation
                )
            ):
                raise ConfigurationConflictError(
                    "non-deployment projection must retain its deployment and reporter binding"
                )
            if (
                request.operation_kind == "update"
                and existing is not None
                and (existing.demand_reporter_incarnation == request.demand_reporter_incarnation)
            ):
                raise ConfigurationConflictError(
                    "a new deployment must rotate the demand reporter incarnation"
                )
            current_candidate: CapacityCandidate | None = None
            current_deployment: CapacityDeploymentGeneration | None = None
            current_reporter: CapacityDemandReporter | None = None
            if existing is not None:
                current_candidate = (
                    await session.execute(
                        select(CapacityCandidate).where(
                            CapacityCandidate.subject_id == request.subject_id,
                            CapacityCandidate.subject_incarnation == request.subject_incarnation,
                            CapacityCandidate.candidate_generation == existing.candidate_generation,
                        )
                    )
                ).scalar_one_or_none()
                current_deployment = (
                    await session.execute(
                        select(CapacityDeploymentGeneration).where(
                            CapacityDeploymentGeneration.subject_id == request.subject_id,
                            CapacityDeploymentGeneration.subject_incarnation
                            == request.subject_incarnation,
                            CapacityDeploymentGeneration.deployment_generation
                            == existing.deployment_generation,
                        )
                    )
                ).scalar_one_or_none()
                current_reporter = (
                    await session.execute(
                        select(CapacityDemandReporter).where(
                            CapacityDemandReporter.subject_id == request.subject_id,
                            CapacityDemandReporter.subject_incarnation
                            == request.subject_incarnation,
                            CapacityDemandReporter.reporter_incarnation
                            == existing.demand_reporter_incarnation,
                            CapacityDemandReporter.state == "current",
                        )
                    )
                ).scalar_one_or_none()
            if request.operation_kind in {"capacity", "destroy"} and existing is not None:
                expected_architecture = {
                    "supported_architectures": list(request.supported_architectures),
                    "supported_pool_ids": list(request.supported_pool_ids),
                }
                expected_cutover = {
                    "local_activation_sha256": request.local_activation_sha256,
                    "candidate_publication_sha256": request.candidate_publication_sha256,
                    "protected_admission_sha256": request.protected_admission_sha256,
                    "capacity_agent_installation_sha256": (
                        request.capacity_agent_installation_sha256
                    ),
                }
                if (
                    current_candidate is None
                    or current_deployment is None
                    or current_reporter is None
                    or current_candidate.candidate_digest != request.candidate_sha256
                    or current_candidate.source_payload
                    != {"publication_sha256": request.candidate_publication_sha256}
                    or current_candidate.architecture_payload != expected_architecture
                    or current_candidate.protocol_payload != request.protocol_versions
                    or current_deployment.candidate_digest != request.candidate_sha256
                    or current_deployment.cutover_payload != expected_cutover
                    or current_reporter.token_sha256 != request.demand_reporter_token_sha256
                ):
                    raise ConfigurationConflictError(
                        "non-deployment projection cannot change candidate or reporter evidence"
                    )
            else:
                reporter_identity_conflict = (
                    await session.execute(
                        select(CapacityDemandReporter)
                        .where(
                            or_(
                                CapacityDemandReporter.token_sha256
                                == request.demand_reporter_token_sha256,
                                CapacityDemandReporter.reporter_incarnation
                                == request.demand_reporter_incarnation,
                            )
                        )
                        .limit(1)
                    )
                ).scalar_one_or_none()
                if reporter_identity_conflict is not None:
                    raise ConfigurationConflictError(
                        "dynamic development demand reporter identity was already used"
                    )

            owner_subjects = [
                subject
                for _, subject in active_subjects.values()
                if subject.account_id == account.account_id
                and subject.subject_id != request.subject_id
                and subject.lifecycle_state != "disabled"
            ]
            retiring = request.operation_kind == "destroy"
            if not retiring and len(owner_subjects) + 1 > account.max_live_subjects:
                raise ConfigurationConflictError("development owner exceeds max_live_subjects")
            if not retiring and sum(
                subject.min_slots for subject in owner_subjects
            ) + request.min_slots > (account.min_reservation_slots):
                raise ConfigurationConflictError(
                    "development owner minimum aggregate exceeds its reservation"
                )

            subject = SubjectConfigurationV1(
                subject_id=request.subject_id,
                subject_incarnation=request.subject_incarnation,
                display_name=display_name,
                account_id=account.account_id,
                tier_id="development",
                min_slots=0 if retiring else request.min_slots,
                max_slots=0 if retiring else request.max_slots,
                rollout_surge_slots=template.rollout_surge_slots,
                max_pending_slots=template.max_pending_slots_per_subject,
                max_pending_jobs=template.max_pending_jobs_per_subject,
                lifecycle_state="disabled" if retiring else "active",
                candidate_generation=request.candidate_generation,
                deployment_generation=request.deployment_generation,
                configuration_generation=request.configuration_generation,
                demand_reporter_incarnation=request.demand_reporter_incarnation,
                profiles=template.profiles,
            )
            next_subject_values = [
                value
                for _, value in active_subjects.values()
                if value.subject_id != subject.subject_id
            ]
            if not retiring:
                next_subject_values.append(subject)
            next_subjects = tuple(
                sorted(
                    next_subject_values,
                    key=lambda value: value.subject_id.hex,
                )
            )
            existing_owners = (
                (
                    await session.execute(
                        select(CapacityAccountPolicy).where(
                            CapacityAccountPolicy.configuration_epoch == latest.configuration_epoch,
                            CapacityAccountPolicy.kind == "owner",
                        )
                    )
                )
                .scalars()
                .all()
            )
            owner_accounts = {
                value.account_id: value
                for value in (
                    _derive_owner_account(fleet, row.owner_id)
                    for row in existing_owners
                    if row.owner_id is not None
                )
            }
            owner_accounts[account.account_id] = account
            active_owner_account_ids = {value.account_id for value in next_subjects}
            derived_accounts = tuple(
                value
                for account_id, value in owner_accounts.items()
                if account_id in active_owner_account_ids
            )
            self._validate_activation(fleet, next_subjects, derived_accounts)

            subject_digest = canonical_digest(subject)
            subject_row = CapacityConfigGeneration(
                scope="subject",
                subject_id=subject.subject_id,
                subject_incarnation=subject.subject_incarnation,
                scope_generation=subject.configuration_generation,
                digest=subject_digest,
                payload=subject.model_dump(mode="json", exclude_none=False),
                state="retired" if retiring else "proposed",
                actor=actor,
                idempotency_key=request.operation_id,
            )
            session.add(subject_row)
            if request.operation_kind in {"create", "update"} or (
                request.operation_kind == "capacity" and existing is None
            ):
                session.add(
                    CapacityCandidate(
                        subject_id=request.subject_id,
                        subject_incarnation=request.subject_incarnation,
                        candidate_generation=request.candidate_generation,
                        candidate_digest=request.candidate_sha256,
                        source_payload={"publication_sha256": request.candidate_publication_sha256},
                        artifact_payload={"candidate_sha256": request.candidate_sha256},
                        architecture_payload={
                            "supported_architectures": list(request.supported_architectures),
                            "supported_pool_ids": list(request.supported_pool_ids),
                        },
                        launcher_payload={
                            "local_activation_sha256": request.local_activation_sha256
                        },
                        attestation_payload={
                            "operation_id": str(request.operation_id),
                            "operation_epoch": request.operation_epoch,
                            "protected_admission_sha256": request.protected_admission_sha256,
                            "capacity_agent_installation_sha256": (
                                request.capacity_agent_installation_sha256
                            ),
                        },
                        protocol_payload=request.protocol_versions,
                    )
                )
                session.add(
                    CapacityDeploymentGeneration(
                        subject_id=request.subject_id,
                        subject_incarnation=request.subject_incarnation,
                        deployment_generation=request.deployment_generation,
                        candidate_digest=request.candidate_sha256,
                        required_profiles=[
                            profile.model_dump(mode="json", exclude_none=False)
                            for profile in subject.profiles
                        ],
                        readiness_state="ready",
                        lifecycle_state="active",
                        cutover_payload={
                            "local_activation_sha256": request.local_activation_sha256,
                            "candidate_publication_sha256": (request.candidate_publication_sha256),
                            "protected_admission_sha256": request.protected_admission_sha256,
                            "capacity_agent_installation_sha256": (
                                request.capacity_agent_installation_sha256
                            ),
                        },
                    )
                )
            await session.flush()

            configuration_epoch = latest.configuration_epoch + 1
            reference_pairs = [
                pair
                for subject_id, pair in active_subjects.items()
                if subject_id != subject.subject_id
            ]
            if not retiring:
                reference_pairs.append((subject_row, subject))
            references = tuple(
                ConfigurationGenerationRefV1(
                    scope="subject",
                    generation=(
                        subject.configuration_generation
                        if value.subject_id == subject.subject_id
                        else row.scope_generation
                    ),
                    digest=(
                        subject_digest if value.subject_id == subject.subject_id else row.digest
                    ),
                    subject_id=value.subject_id,
                    subject_incarnation=value.subject_incarnation,
                )
                for row, value in sorted(
                    reference_pairs,
                    key=lambda pair: pair[1].subject_id.hex,
                )
            )
            snapshot = ConfigurationSnapshotV1(
                configuration_epoch=configuration_epoch,
                fleet=ConfigurationGenerationRefV1(
                    scope="fleet",
                    generation=fleet_row.scope_generation,
                    digest=fleet_row.digest,
                ),
                subjects=references,
            )
            snapshot_digest = canonical_digest(snapshot)
            session.add(
                CapacityConfigurationEpoch(
                    configuration_epoch=configuration_epoch,
                    fleet_generation=fleet.fleet_generation,
                    fleet_digest=fleet_row.digest,
                    subject_generation_manifest=[
                        reference.model_dump(mode="json", exclude_none=False)
                        for reference in references
                    ],
                    canonical_digest=snapshot_digest,
                    activation_idempotency_key=idempotency_key,
                    activation_actor=actor,
                    activation_request_digest=request_digest,
                )
            )
            await session.flush()
            await self._persist_active_configuration(
                session,
                configuration_epoch=configuration_epoch,
                fleet=fleet,
                subjects=next_subjects,
                derived_accounts=derived_accounts,
                reporter_tokens=(
                    {}
                    if retiring
                    else {
                        request.demand_reporter_incarnation: (request.demand_reporter_token_sha256)
                    }
                ),
            )
            if retiring:
                await session.execute(
                    update(CapacityDemandReporter)
                    .where(
                        CapacityDemandReporter.subject_id == request.subject_id,
                        CapacityDemandReporter.subject_incarnation == request.subject_incarnation,
                        CapacityDemandReporter.reporter_incarnation
                        == request.demand_reporter_incarnation,
                        CapacityDemandReporter.state == "current",
                    )
                    .values(state="fenced")
                )
            await session.execute(
                update(CapacityConfigGeneration)
                .where(CapacityConfigGeneration.state == "active")
                .values(state="retired")
            )
            active_ids = [fleet_row.id]
            if not retiring:
                active_ids.append(subject_row.id)
            active_ids.extend(
                row.id
                for subject_id, (row, _) in active_subjects.items()
                if subject_id != subject.subject_id
            )
            await session.execute(
                update(CapacityConfigGeneration)
                .where(CapacityConfigGeneration.id.in_(active_ids))
                .values(state="active")
            )
            result_payload = {
                "subject": subject.model_dump(mode="json", exclude_none=False),
                "account": account.model_dump(mode="json", exclude_none=False),
            }
            session.add(
                CapacityDevelopmentProjection(
                    operation_id=request.operation_id,
                    idempotency_key=idempotency_key,
                    request_digest=request_digest,
                    request_payload=request_payload,
                    subject_id=request.subject_id,
                    subject_incarnation=request.subject_incarnation,
                    configuration_generation=request.configuration_generation,
                    configuration_epoch=configuration_epoch,
                    result_digest=snapshot_digest,
                    result_payload=result_payload,
                    actor=actor,
                )
            )
            session.add(
                _audit(
                    actor_kind="environment-lifecycle",
                    actor_id=actor,
                    event_kind=(
                        "capacity_development_subject_retired"
                        if retiring
                        else "capacity_development_subject_projected"
                    ),
                    object_binding={
                        "configuration_epoch": configuration_epoch,
                        "subject_id": str(subject.subject_id),
                        "subject_incarnation": str(subject.subject_incarnation),
                    },
                    detail={
                        "account_id": account.account_id,
                        "configuration_digest": snapshot_digest,
                        "executable_new_capacity_ceiling": 0,
                    },
                )
            )
            return ProjectedDevelopmentSubject(
                configuration_epoch=configuration_epoch,
                configuration_digest=snapshot_digest,
                subject=subject,
                account=account,
                replayed=False,
            )

    async def _load_proposal_row(
        self,
        session: AsyncSession,
        reference: ConfigurationGenerationRefV1,
    ) -> CapacityConfigGeneration:
        conditions = [
            CapacityConfigGeneration.scope == reference.scope,
            CapacityConfigGeneration.scope_generation == reference.generation,
            CapacityConfigGeneration.digest == reference.digest,
        ]
        if reference.scope == "subject":
            conditions.extend(
                [
                    CapacityConfigGeneration.subject_id == reference.subject_id,
                    CapacityConfigGeneration.subject_incarnation == reference.subject_incarnation,
                ]
            )
        row = (
            await session.execute(
                select(CapacityConfigGeneration).where(*conditions).with_for_update()
            )
        ).scalar_one_or_none()
        if row is None or row.state not in {"proposed", "active"}:
            raise ConfigurationConflictError(
                f"referenced {reference.scope} configuration is unavailable"
            )
        return row

    @staticmethod
    def _validate_activation(
        fleet: FleetManifestV1,
        subjects: tuple[SubjectConfigurationV1, ...],
        derived_accounts: tuple[AccountPolicyV1, ...] = (),
    ) -> None:
        accounts = (*fleet.account_policies, *derived_accounts)
        if len({account.account_id for account in accounts}) != len(accounts):
            raise ConfigurationConflictError("capacity account identity collision")
        account_ids = {account.account_id for account in accounts}
        tier_ids = {tier.tier_id for tier in fleet.tiers}
        minimum_by_account: dict[str, int] = {}
        for subject in subjects:
            if subject.account_id not in account_ids:
                raise ConfigurationConflictError("subject references an unknown account")
            if subject.tier_id not in tier_ids:
                raise ConfigurationConflictError("subject references an unknown tier")
            for profile in subject.profiles:
                try:
                    validate_profile_narrowing(fleet, profile)
                except FleetStateError as exc:
                    raise ConfigurationConflictError(str(exc)) from exc
            minimum_by_account[subject.account_id] = (
                minimum_by_account.get(subject.account_id, 0) + subject.min_slots
            )
        for account in accounts:
            if minimum_by_account.get(account.account_id, 0) > account.min_reservation_slots:
                raise ConfigurationConflictError(
                    "subject minimum aggregate exceeds account reservation"
                )

    async def _persist_active_configuration(
        self,
        session: AsyncSession,
        *,
        configuration_epoch: int,
        fleet: FleetManifestV1,
        subjects: tuple[SubjectConfigurationV1, ...],
        derived_accounts: tuple[AccountPolicyV1, ...] = (),
        reporter_tokens: dict[UUID, str] | None = None,
    ) -> None:
        for tier in fleet.tiers:
            session.add(
                CapacityTier(
                    configuration_epoch=configuration_epoch,
                    tier_id=tier.tier_id,
                    priority=tier.priority,
                    max_slots=tier.max_slots,
                    resource_ceilings={},
                    max_pending_slots=tier.max_pending_slots,
                    max_pending_jobs=tier.max_pending_jobs,
                )
            )
        for account in (*fleet.account_policies, *derived_accounts):
            session.add(
                CapacityAccountPolicy(
                    configuration_epoch=configuration_epoch,
                    account_id=account.account_id,
                    kind=account.kind,
                    owner_id=account.owner_id,
                    min_reservation_slots=account.min_reservation_slots,
                    max_slots=account.max_slots,
                    max_surge_slots=account.max_surge_slots,
                    max_pending_slots=account.max_pending_slots,
                    max_pending_jobs=account.max_pending_jobs,
                    submission_rate_per_minute=account.submission_rate_per_minute,
                    max_live_subjects=account.max_live_subjects,
                    max_builds=0,
                    max_artifact_bytes=0,
                    payload=account.model_dump(mode="json", exclude_none=False),
                )
            )
        for pool in fleet.pools:
            session.add(
                CapacityPool(
                    configuration_epoch=configuration_epoch,
                    pool_id=pool.pool_id,
                    pool_generation=pool.pool_generation,
                    pool_digest=pool.pool_digest,
                    controller=pool.controller,
                    partition=pool.partition,
                    association=pool.association,
                    protocol_generation=pool.protocol_generation,
                    protocol_digest=pool.protocol_digest,
                    topology={
                        "resource_domains": [
                            domain.model_dump(mode="json", exclude_none=False)
                            for domain in pool.resource_domains
                        ]
                    },
                    envelope={"max_slots": pool.max_slots},
                    health=pool.health,
                    max_slots=pool.max_slots,
                    max_pending_slots=pool.max_pending_slots,
                    max_pending_jobs=pool.max_pending_jobs,
                    submission_rate_per_minute=pool.submission_rate_per_minute,
                )
            )
            await self._register_pool_reporter(session, pool)
        for subject in subjects:
            session.add(
                CapacitySubject(
                    configuration_epoch=configuration_epoch,
                    subject_id=subject.subject_id,
                    subject_incarnation=subject.subject_incarnation,
                    display_name=subject.display_name,
                    account_id=subject.account_id,
                    tier_id=subject.tier_id,
                    min_slots=subject.min_slots,
                    max_slots=subject.max_slots,
                    rollout_surge_slots=subject.rollout_surge_slots,
                    max_pending_slots=subject.max_pending_slots,
                    max_pending_jobs=subject.max_pending_jobs,
                    submission_rate_per_minute=subject.submission_rate_per_minute,
                    lifecycle_state=subject.lifecycle_state,
                    candidate_generation=subject.candidate_generation,
                    deployment_generation=subject.deployment_generation,
                    configuration_generation=subject.configuration_generation,
                    demand_reporter_incarnation=subject.demand_reporter_incarnation,
                    payload=subject.model_dump(mode="json", exclude_none=False),
                )
            )
            for profile in subject.profiles:
                await self._persist_worker_profile(session, subject, profile)
            await self._register_demand_reporter(
                session,
                subject,
                token_sha256=(reporter_tokens or {}).get(subject.demand_reporter_incarnation),
            )
        await session.flush()

    async def _persist_worker_profile(
        self,
        session: AsyncSession,
        subject: SubjectConfigurationV1,
        profile: ProfileReferenceV1,
    ) -> None:
        existing = (
            await session.execute(
                select(CapacityWorkerProfile).where(
                    CapacityWorkerProfile.subject_id == subject.subject_id,
                    CapacityWorkerProfile.subject_incarnation == subject.subject_incarnation,
                    CapacityWorkerProfile.deployment_generation == subject.deployment_generation,
                    CapacityWorkerProfile.pool_id == profile.pool_id,
                    CapacityWorkerProfile.profile_generation == profile.profile_generation,
                )
            )
        ).scalar_one_or_none()
        shape_catalog = [
            shape.model_dump(mode="json", exclude_none=False) for shape in profile.worker_shapes
        ]
        narrowing = {"eligible_resource_domains": list(profile.eligible_resource_domains)}
        if existing is None:
            session.add(
                CapacityWorkerProfile(
                    subject_id=subject.subject_id,
                    subject_incarnation=subject.subject_incarnation,
                    deployment_generation=subject.deployment_generation,
                    pool_id=profile.pool_id,
                    pool_generation=profile.pool_generation,
                    profile_generation=profile.profile_generation,
                    profile_digest=profile.profile_digest,
                    shape_catalog=shape_catalog,
                    narrowing_constraints=narrowing,
                )
            )
            return
        if (
            existing.pool_generation != profile.pool_generation
            or existing.profile_digest != profile.profile_digest
            or existing.shape_catalog != shape_catalog
            or existing.narrowing_constraints != narrowing
        ):
            raise ConfigurationConflictError(
                "immutable worker profile generation conflicts with active configuration"
            )

    async def _register_demand_reporter(
        self,
        session: AsyncSession,
        subject: SubjectConfigurationV1,
        *,
        token_sha256: str | None = None,
    ) -> None:
        existing = (
            await session.execute(
                select(CapacityDemandReporter).where(
                    CapacityDemandReporter.subject_id == subject.subject_id,
                    CapacityDemandReporter.subject_incarnation == subject.subject_incarnation,
                    CapacityDemandReporter.reporter_incarnation
                    == subject.demand_reporter_incarnation,
                )
            )
        ).scalar_one_or_none()
        await session.execute(
            update(CapacityDemandReporter)
            .where(
                CapacityDemandReporter.subject_id == subject.subject_id,
                CapacityDemandReporter.state == "current",
                CapacityDemandReporter.reporter_incarnation != subject.demand_reporter_incarnation,
            )
            .values(state="fenced")
        )
        if existing is None:
            session.add(
                CapacityDemandReporter(
                    subject_id=subject.subject_id,
                    subject_incarnation=subject.subject_incarnation,
                    reporter_incarnation=subject.demand_reporter_incarnation,
                    configuration_generation=subject.configuration_generation,
                    deployment_generation=subject.deployment_generation,
                    high_water=0,
                    state="current",
                    token_sha256=token_sha256,
                )
            )
        elif existing.state != "current":
            raise ConfigurationConflictError("retired demand reporter cannot be reactivated")
        elif token_sha256 is not None and existing.token_sha256 != token_sha256:
            raise ConfigurationConflictError("demand reporter token binding conflicts")
        elif existing.deployment_generation != subject.deployment_generation:
            raise ConfigurationConflictError("demand reporter deployment binding conflicts")
        elif existing.configuration_generation > subject.configuration_generation:
            raise ConfigurationConflictError("demand reporter configuration binding regressed")
        else:
            # Capacity-only policy changes retain the protected reporter and
            # high-water, but advance the generation accepted by the manager.
            existing.configuration_generation = subject.configuration_generation

    async def _register_pool_reporter(self, session: AsyncSession, pool: Any) -> None:
        existing = (
            await session.execute(
                select(CapacityPoolReporter).where(
                    CapacityPoolReporter.pool_id == pool.pool_id,
                    CapacityPoolReporter.reporter_incarnation == pool.pool_reporter_incarnation,
                )
            )
        ).scalar_one_or_none()
        await session.execute(
            update(CapacityPoolReporter)
            .where(
                CapacityPoolReporter.pool_id == pool.pool_id,
                CapacityPoolReporter.state == "current",
                CapacityPoolReporter.reporter_incarnation != pool.pool_reporter_incarnation,
            )
            .values(state="fenced")
        )
        if existing is None:
            session.add(
                CapacityPoolReporter(
                    pool_id=pool.pool_id,
                    reporter_incarnation=pool.pool_reporter_incarnation,
                    pool_generation=pool.pool_generation,
                    high_water=0,
                    state="current",
                )
            )
        elif existing.state != "current":
            raise ConfigurationConflictError("retired pool reporter cannot be reactivated")

    async def register_writer(
        self,
        session: AsyncSession,
        authority_incarnation: UUID,
        *,
        expected_epoch: int,
    ) -> WriterFence:
        async with _write_transaction(session):
            authority = await _lock_any_authority(session)
            if authority.authority_incarnation != authority_incarnation:
                raise AuthorityRecoveryError("authority incarnation mismatch")
            if authority.writer_epoch != expected_epoch:
                raise StaleWriterError("writer epoch compare-and-set failed")
            now = await _db_now(session)
            successor_epoch = authority.writer_epoch + 1
            if authority.execution_state == "prepared":
                if authority.execution_epoch <= 0:
                    raise AuthorityRecoveryError("prepared execution epoch binding is incomplete")
                prepared = (
                    await session.execute(
                        select(CapacityExecutionEpoch)
                        .where(CapacityExecutionEpoch.execution_epoch == authority.execution_epoch)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if (
                    prepared is None
                    or prepared.state != "prepared"
                    or prepared.execution_manifest_sha256 != authority.execution_manifest_sha256
                    or prepared.effective_ceiling != 0
                ):
                    raise AuthorityRecoveryError(
                        "prepared execution epoch does not match authority"
                    )
                prepared.state = "retired"
                prepared.retired_at = now
                authority.execution_epoch = 0
                authority.execution_state = "shadow"
                authority.execution_manifest_sha256 = None
                authority.executable_new_capacity_ceiling = 0
                session.add(
                    _audit(
                        actor_kind="manager",
                        actor_id=str(authority_incarnation),
                        event_kind="capacity_execution_epoch_retired_writer_change",
                        object_binding={"execution_epoch": prepared.execution_epoch},
                        detail={"prepared_writer_epoch": prepared.prepared_writer_epoch},
                    )
                )
            elif authority.execution_state == "active":
                active = (
                    await session.execute(
                        select(CapacityExecutionEpoch)
                        .where(CapacityExecutionEpoch.execution_epoch == authority.execution_epoch)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if (
                    active is None
                    or active.state != "active"
                    or active.execution_manifest_sha256 != authority.execution_manifest_sha256
                    or active.current_writer_epoch != authority.writer_epoch
                    or active.effective_ceiling <= 0
                ):
                    raise AuthorityRecoveryError("active execution epoch does not match authority")
                active.state = "drain-only"
                active.effective_ceiling = 0
                active.effective_rate_per_minute = 0
                active.current_writer_epoch = successor_epoch
                active.drain_only_at = now
                authority.execution_state = "drain-only"
                authority.executable_new_capacity_ceiling = 0
                session.add(
                    _audit(
                        actor_kind="manager",
                        actor_id=str(authority_incarnation),
                        event_kind="capacity_execution_epoch_drained_writer_change",
                        object_binding={"execution_epoch": active.execution_epoch},
                        detail={"previous_writer_epoch": authority.writer_epoch},
                    )
                )
            elif authority.execution_state == "drain-only":
                draining = (
                    await session.execute(
                        select(CapacityExecutionEpoch)
                        .where(CapacityExecutionEpoch.execution_epoch == authority.execution_epoch)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if (
                    draining is None
                    or draining.state != "drain-only"
                    or draining.execution_manifest_sha256 != authority.execution_manifest_sha256
                    or draining.current_writer_epoch != authority.writer_epoch
                    or draining.effective_ceiling != 0
                    or draining.effective_rate_per_minute != 0
                ):
                    raise AuthorityRecoveryError(
                        "drain-only execution epoch does not match authority"
                    )
                draining.current_writer_epoch = successor_epoch
                session.add(
                    _audit(
                        actor_kind="manager",
                        actor_id=str(authority_incarnation),
                        event_kind="capacity_execution_epoch_refenced_writer_change",
                        object_binding={"execution_epoch": draining.execution_epoch},
                        detail={"previous_writer_epoch": authority.writer_epoch},
                    )
                )
            authority.writer_epoch = successor_epoch
            authority.increase_freeze = True
            authority.increase_freeze_reason = "writer_epoch_changed"
            authority.updated_at = now
            session.add(
                _audit(
                    actor_kind="manager",
                    actor_id=str(authority_incarnation),
                    event_kind="capacity_writer_registered",
                    object_binding={"writer_epoch": authority.writer_epoch},
                    detail={},
                )
            )
            return WriterFence(
                authority_incarnation=authority.authority_incarnation,
                writer_epoch=authority.writer_epoch,
            )

    async def prepare_execution_epoch(
        self,
        session: AsyncSession,
        request: ExecutionPreparationV2,
        *,
        actor: str,
        idempotency_key: UUID,
    ) -> ExecutionContextV2:
        """Persist one exact prepared epoch while retaining a zero ceiling."""

        self._validate_execution_actor(actor)
        request_digest = canonical_executable_digest(request)
        request_payload = request.model_dump(mode="json", exclude_none=False)
        async with _write_transaction(session):
            authority = await _lock_any_authority(session)
            replay = (
                await session.execute(
                    select(CapacityExecutionEpoch).where(
                        CapacityExecutionEpoch.idempotency_key == idempotency_key
                    )
                )
            ).scalar_one_or_none()
            if replay is not None:
                if (
                    replay.actor != actor
                    or replay.request_digest != request_digest
                    or replay.manifest_payload != request_payload
                ):
                    raise IdempotencyConflictError(
                        "execution preparation idempotency key was reused"
                    )
                if replay.state == "retired":
                    raise ExecutionConflictError("retired execution preparation cannot be replayed")
                return self._execution_context(authority, replay)

            if authority.execution_state != "shadow":
                raise ExecutionConflictError("another execution epoch already owns authority")
            await self._validate_execution_preparation(session, authority, request)
            latest_epoch = (
                await session.execute(select(func.max(CapacityExecutionEpoch.execution_epoch)))
            ).scalar_one()
            execution_epoch = 1 if latest_epoch is None else latest_epoch + 1
            executors = {item.pool_id: item for item in request.executors}
            row = CapacityExecutionEpoch(
                execution_epoch=execution_epoch,
                authority_incarnation=request.authority_incarnation,
                prepared_writer_epoch=request.expected_writer_epoch,
                current_writer_epoch=request.expected_writer_epoch,
                configuration_epoch=request.configuration_epoch,
                fleet_generation=request.fleet_generation,
                fleet_digest=request.fleet_digest,
                execution_manifest_sha256=request_digest,
                manifest_payload=request_payload,
                trusted_fleet_release_sha256=request.trusted_fleet_release_sha256,
                oldlab_executor_id=executors["oldlab"].executor_id,
                oldlab_executor_incarnation=executors["oldlab"].executor_incarnation,
                oldlab_pool_id="oldlab",
                oldlab_pool_generation=executors["oldlab"].pool_generation,
                gb10_executor_id=executors["gb10"].executor_id,
                gb10_executor_incarnation=executors["gb10"].executor_incarnation,
                gb10_pool_id="gb10",
                gb10_pool_generation=executors["gb10"].pool_generation,
                environment_acknowledgements_sha256=_canonical_json_digest(
                    [
                        item.model_dump(mode="json", exclude_none=False)
                        for item in request.subject_acknowledgements
                    ]
                ),
                legacy_writer_manifest_sha256=_canonical_json_digest(
                    [
                        item.model_dump(mode="json", exclude_none=False)
                        for item in request.legacy_writer_fences
                    ]
                ),
                rollback_evidence_sha256=request.rollback_evidence_sha256,
                requested_ceiling=request.requested_ceiling,
                effective_ceiling=0,
                requested_rate_per_minute=request.requested_rate_per_minute,
                effective_rate_per_minute=0,
                state="prepared",
                actor=actor,
                idempotency_key=idempotency_key,
                request_digest=request_digest,
                activation_actor=None,
                activation_idempotency_key=None,
                activation_request_digest=None,
            )
            session.add(row)
            await session.flush()
            authority.execution_epoch = execution_epoch
            authority.execution_state = "prepared"
            authority.execution_manifest_sha256 = request_digest
            authority.executable_new_capacity_ceiling = 0
            authority.increase_freeze = True
            authority.increase_freeze_reason = "execution_epoch_prepared"
            authority.updated_at = await _db_now(session)
            session.add(
                _audit(
                    actor_kind="operator",
                    actor_id=actor,
                    event_kind="capacity_execution_epoch_prepared",
                    object_binding={
                        "execution_epoch": execution_epoch,
                        "execution_manifest_sha256": request_digest,
                    },
                    detail={
                        "configuration_epoch": request.configuration_epoch,
                        "requested_ceiling": request.requested_ceiling,
                        "subject_count": len(request.subject_acknowledgements),
                        "legacy_writer_count": len(request.legacy_writer_fences),
                    },
                )
            )
            return self._execution_context(authority, row)

    async def register_execution_executor(
        self,
        session: AsyncSession,
        request: ExecutableExecutorRegistrationV2,
        *,
        actor: str,
        idempotency_key: UUID,
    ) -> ExecutionContextV2:
        """Persist explicit v2 executor provenance for one prepared epoch."""

        self._validate_execution_actor(actor)
        request_digest = canonical_executable_digest(request)
        request_payload = request.model_dump(mode="json", exclude_none=False)
        async with _write_transaction(session):
            authority = await _lock_any_authority(session)
            replay = (
                await session.execute(
                    select(CapacityExecutionExecutor).where(
                        CapacityExecutionExecutor.idempotency_key == idempotency_key
                    )
                )
            ).scalar_one_or_none()
            if replay is not None:
                if (
                    replay.actor != actor
                    or replay.registration_digest != request_digest
                    or replay.registration_payload != request_payload
                ):
                    raise IdempotencyConflictError(
                        "execution executor registration idempotency key was reused"
                    )
                replay_epoch = (
                    await session.execute(
                        select(CapacityExecutionEpoch).where(
                            CapacityExecutionEpoch.execution_epoch == replay.execution_epoch
                        )
                    )
                ).scalar_one()
                return self._execution_context(authority, replay_epoch)

            if authority.execution_state != "prepared":
                raise ExecutionConflictError(
                    "executable executor registration requires prepared authority"
                )
            current_epoch = (
                await session.execute(
                    select(CapacityExecutionEpoch)
                    .where(
                        CapacityExecutionEpoch.execution_epoch == request.execution.execution_epoch
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if current_epoch is None or current_epoch.state != "prepared":
                raise ExecutionConflictError("prepared execution epoch is unavailable")
            context = self._execution_context(authority, current_epoch)
            if request.execution != context:
                raise ExecutionConflictError("execution executor fence changed")
            policy = self._execution_policy
            if policy is None:
                raise ExecutionPreparationDisabledError(
                    "execution registration requires owner-configured policy"
                )
            if request.pool_id not in {"gb10", "oldlab"}:
                raise ExecutionConflictError("executable executor pool is invalid")
            pool_id = cast(Literal["gb10", "oldlab"], request.pool_id)
            expected = {item.pool_id: item for item in policy.executors}.get(pool_id)
            if expected is None or (
                request.executor_id != expected.executor_id
                or request.executor_incarnation != expected.executor_incarnation
                or request.pool_generation != expected.pool_generation
                or request.signing_key_sha256 != expected.signing_key_sha256
                or request.local_authority_sha256 != expected.local_authority_sha256
                or request.controller_authority_sha256 != expected.controller_authority_sha256
            ):
                raise ExecutionConflictError("executable executor differs from owner policy")
            session.add(
                CapacityExecutionExecutor(
                    execution_epoch=current_epoch.execution_epoch,
                    execution_manifest_sha256=current_epoch.execution_manifest_sha256,
                    executor_id=request.executor_id,
                    executor_incarnation=request.executor_incarnation,
                    pool_id=request.pool_id,
                    pool_generation=request.pool_generation,
                    signing_key_id=request.signing_key_id,
                    signing_key_sha256=request.signing_key_sha256,
                    local_authority_sha256=request.local_authority_sha256,
                    controller_authority_sha256=request.controller_authority_sha256,
                    actor=actor,
                    idempotency_key=idempotency_key,
                    registration_digest=request_digest,
                    registration_payload=request_payload,
                )
            )
            await session.flush()
            return context

    async def activate_execution_epoch(
        self,
        session: AsyncSession,
        request: ExecutionActivationV2,
        *,
        actor: str,
        idempotency_key: UUID,
    ) -> ExecutionAuthorityV2:
        """Atomically activate only the exact still-current prepared epoch."""

        self._validate_execution_actor(actor)
        request_digest = canonical_executable_digest(request)
        async with _write_transaction(session):
            authority = await _lock_any_authority(session)
            replay = (
                await session.execute(
                    select(CapacityExecutionEpoch).where(
                        CapacityExecutionEpoch.activation_idempotency_key == idempotency_key
                    )
                )
            ).scalar_one_or_none()
            if replay is not None:
                if (
                    replay.activation_actor != actor
                    or replay.activation_request_digest != request_digest
                ):
                    raise IdempotencyConflictError(
                        "execution activation idempotency key was reused"
                    )
                context = self._execution_context(authority, replay)
                if not isinstance(context, ExecutionAuthorityV2):
                    raise ExecutionConflictError("execution activation replay is not authoritative")
                return context

            row = (
                await session.execute(
                    select(CapacityExecutionEpoch)
                    .where(CapacityExecutionEpoch.execution_epoch == request.execution_epoch)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None or row.state != "prepared":
                raise ExecutionConflictError("exact prepared execution epoch is unavailable")
            if (
                authority.authority_incarnation != request.authority_incarnation
                or authority.writer_epoch != request.expected_writer_epoch
                or authority.execution_state != "prepared"
                or authority.execution_epoch != request.execution_epoch
                or authority.execution_manifest_sha256 != request.execution_manifest_sha256
                or row.execution_manifest_sha256 != request.execution_manifest_sha256
                or row.requested_ceiling != request.executable_new_capacity_ceiling
                or row.requested_rate_per_minute != request.executable_new_capacity_rate_per_minute
            ):
                raise ExecutionConflictError("prepared execution fence changed")
            if not authority.increase_freeze:
                raise ExecutionConflictError("execution activation requires increase freeze")

            preparation = ExecutionPreparationV2.model_validate_json(
                json.dumps(row.manifest_payload, sort_keys=True, separators=(",", ":"))
            )
            if (
                canonical_executable_digest(preparation) != row.execution_manifest_sha256
                or _canonical_json_digest(
                    [
                        item.model_dump(mode="json", exclude_none=False)
                        for item in preparation.subject_acknowledgements
                    ]
                )
                != row.environment_acknowledgements_sha256
                or _canonical_json_digest(
                    [
                        item.model_dump(mode="json", exclude_none=False)
                        for item in preparation.legacy_writer_fences
                    ]
                )
                != row.legacy_writer_manifest_sha256
                or preparation.rollback_evidence_sha256 != row.rollback_evidence_sha256
            ):
                raise ExecutionConflictError("prepared execution manifest digest changed")
            await self._validate_execution_preparation(session, authority, preparation)
            executable_rows = (
                (
                    await session.execute(
                        select(CapacityExecutionExecutor).where(
                            CapacityExecutionExecutor.execution_epoch == row.execution_epoch,
                            CapacityExecutionExecutor.execution_manifest_sha256
                            == row.execution_manifest_sha256,
                        )
                    )
                )
                .scalars()
                .all()
            )
            expected_executors = {item.pool_id: item for item in preparation.executors}
            if {item.pool_id for item in executable_rows} != {"gb10", "oldlab"}:
                raise ExecutionConflictError("both executable executor registrations are required")
            for executable_row in executable_rows:
                pool_id = cast(Literal["gb10", "oldlab"], executable_row.pool_id)
                expected = expected_executors[pool_id]
                if (
                    executable_row.executor_id != expected.executor_id
                    or executable_row.executor_incarnation != expected.executor_incarnation
                    or executable_row.pool_generation != expected.pool_generation
                    or executable_row.signing_key_sha256 != expected.signing_key_sha256
                    or executable_row.local_authority_sha256 != expected.local_authority_sha256
                    or executable_row.controller_authority_sha256
                    != expected.controller_authority_sha256
                ):
                    raise ExecutionConflictError("executable executor registration changed")
            now = await _db_now(session)
            row.state = "active"
            row.effective_ceiling = request.executable_new_capacity_ceiling
            row.effective_rate_per_minute = request.executable_new_capacity_rate_per_minute
            row.activation_actor = actor
            row.activation_idempotency_key = idempotency_key
            row.activation_request_digest = request_digest
            row.activated_at = now
            authority.execution_state = "active"
            authority.executable_new_capacity_ceiling = request.executable_new_capacity_ceiling
            authority.increase_freeze = False
            authority.increase_freeze_reason = None
            authority.updated_at = now
            await session.flush()
            session.add(
                _audit(
                    actor_kind="operator",
                    actor_id=actor,
                    event_kind="capacity_execution_epoch_activated",
                    object_binding={
                        "execution_epoch": row.execution_epoch,
                        "execution_manifest_sha256": row.execution_manifest_sha256,
                    },
                    detail={
                        "executable_new_capacity_ceiling": row.effective_ceiling,
                        "idempotency_key": str(idempotency_key),
                    },
                )
            )
            context = self._execution_context(authority, row)
            if not isinstance(context, ExecutionAuthorityV2):
                raise ExecutionConflictError("activated execution authority is invalid")
            return context

    async def execution_authority(
        self,
        session: AsyncSession,
    ) -> ExecutionContextV2 | None:
        """Return the exact prepared/active checkpoint, or none in shadow mode."""

        authority = (
            await session.execute(
                select(CapacityAuthorityState).where(CapacityAuthorityState.singleton_id == 1)
            )
        ).scalar_one_or_none()
        if authority is None:
            raise AuthorityRecoveryError("capacity authority row is missing")
        if authority.execution_state == "shadow":
            return None
        if authority.execution_epoch <= 0:
            raise AuthorityRecoveryError("execution authority binding is incomplete")
        row = (
            await session.execute(
                select(CapacityExecutionEpoch).where(
                    CapacityExecutionEpoch.execution_epoch == authority.execution_epoch
                )
            )
        ).scalar_one_or_none()
        if row is None:
            raise AuthorityRecoveryError("execution epoch row is missing")
        if row.state in {"active", "drain-only"}:
            if self._execution_policy is None:
                raise AuthorityRecoveryError("active execution authority requires owner policy")
            try:
                preparation = ExecutionPreparationV2.model_validate_json(
                    json.dumps(row.manifest_payload, sort_keys=True, separators=(",", ":"))
                )
                await self._validate_execution_preparation(
                    session,
                    authority,
                    preparation,
                    require_writer_binding=False,
                )
            except (ExecutionConflictError, ExecutionPreparationDisabledError) as exc:
                raise AuthorityRecoveryError(
                    "active execution authority owner policy changed"
                ) from exc
            executable_pools = set(
                (
                    await session.execute(
                        select(CapacityExecutionExecutor.pool_id).where(
                            CapacityExecutionExecutor.execution_epoch == row.execution_epoch,
                            CapacityExecutionExecutor.execution_manifest_sha256
                            == row.execution_manifest_sha256,
                        )
                    )
                ).scalars()
            )
            if executable_pools != {"gb10", "oldlab"}:
                raise AuthorityRecoveryError("active execution authority executor binding changed")
        return self._execution_context(authority, row)

    async def _validate_execution_preparation(
        self,
        session: AsyncSession,
        authority: CapacityAuthorityState,
        request: ExecutionPreparationV2,
        *,
        require_writer_binding: bool = True,
    ) -> None:
        policy = self._execution_policy
        if policy is None:
            raise ExecutionPreparationDisabledError(
                "execution preparation requires owner-configured policy"
            )
        if authority.authority_incarnation != request.authority_incarnation or (
            require_writer_binding and authority.writer_epoch != request.expected_writer_epoch
        ):
            raise ExecutionConflictError("execution writer authority changed")
        configuration = (
            (
                await session.execute(
                    select(CapacityConfigurationEpoch).order_by(
                        CapacityConfigurationEpoch.configuration_epoch.desc()
                    )
                )
            )
            .scalars()
            .first()
        )
        if (
            configuration is None
            or configuration.configuration_epoch != request.configuration_epoch
            or configuration.fleet_generation != request.fleet_generation
            or configuration.fleet_digest != request.fleet_digest
        ):
            raise ExecutionConflictError("execution configuration or fleet changed")
        if request.trusted_fleet_release_sha256 != policy.trusted_fleet_release_sha256:
            raise ExecutionConflictError("trusted fleet release is not configured exactly")
        if (
            request.requested_ceiling != policy.executable_new_capacity_ceiling
            or request.requested_rate_per_minute != policy.executable_new_capacity_rate_per_minute
            or request.executors != policy.executors
            or request.subject_acknowledgements != policy.subject_acknowledgements
            or request.rollback_evidence_sha256 != policy.rollback_evidence_sha256
            or request.legacy_writer_fences != policy.legacy_writer_fences
        ):
            raise ExecutionConflictError("execution preparation differs from owner policy")

        controller_authorities = {
            item.pool_id: item.controller_authority_sha256 for item in policy.controller_authorities
        }
        if any(
            item.controller_authority_sha256 != controller_authorities[item.pool_id]
            for item in request.executors
        ):
            raise ExecutionConflictError("executor controller authority changed")

        subject_rows = (
            (
                await session.execute(
                    select(CapacitySubject).where(
                        CapacitySubject.configuration_epoch == request.configuration_epoch
                    )
                )
            )
            .scalars()
            .all()
        )
        acknowledgements = {item.subject_id: item for item in request.subject_acknowledgements}
        if {row.subject_id for row in subject_rows} != set(acknowledgements):
            raise ExecutionConflictError("subject execution acknowledgements are incomplete")
        for subject_row in subject_rows:
            acknowledgement = acknowledgements[subject_row.subject_id]
            if (
                acknowledgement.subject_incarnation != subject_row.subject_incarnation
                or acknowledgement.configuration_generation != subject_row.configuration_generation
                or acknowledgement.deployment_generation != subject_row.deployment_generation
                or acknowledgement.reporter_incarnation != subject_row.demand_reporter_incarnation
            ):
                raise ExecutionConflictError("subject execution acknowledgement changed")
            candidate = (
                await session.execute(
                    select(CapacityCandidate).where(
                        CapacityCandidate.subject_id == subject_row.subject_id,
                        CapacityCandidate.subject_incarnation == subject_row.subject_incarnation,
                        CapacityCandidate.candidate_generation == subject_row.candidate_generation,
                    )
                )
            ).scalar_one_or_none()
            if (
                candidate is None
                or candidate.candidate_digest != acknowledgement.candidate.identity
                or candidate.source_payload.get("publication_sha256")
                != acknowledgement.candidate.publication_sha256
            ):
                raise ExecutionConflictError("subject execution candidate provenance changed")

        expected_legacy = {_legacy_writer_key(item) for item in policy.legacy_writer_fences}
        supplied_legacy = {_legacy_writer_key(item) for item in request.legacy_writer_fences}
        if supplied_legacy != expected_legacy:
            raise ExecutionConflictError("legacy writer fence manifest is incomplete")

    @staticmethod
    def _validate_execution_actor(actor: str) -> None:
        if not actor or len(actor.encode("utf-8")) > 256:
            raise ValueError("execution actor is invalid")

    @staticmethod
    def _execution_context(
        authority: CapacityAuthorityState,
        row: CapacityExecutionEpoch,
    ) -> ExecutionContextV2:
        if (
            authority.execution_epoch != row.execution_epoch
            or authority.execution_state != row.state
            or authority.execution_manifest_sha256 != row.execution_manifest_sha256
            or authority.executable_new_capacity_ceiling != row.effective_ceiling
            or authority.authority_incarnation != row.authority_incarnation
            or authority.writer_epoch != row.current_writer_epoch
        ):
            raise AuthorityRecoveryError("execution authority does not match its epoch")
        values = {
            "authority_incarnation": authority.authority_incarnation,
            "writer_epoch": authority.writer_epoch,
            "configuration_epoch": row.configuration_epoch,
            "execution_epoch": row.execution_epoch,
            "execution_manifest_sha256": row.execution_manifest_sha256,
            "execution_state": row.state,
            "executable_new_capacity_ceiling": row.effective_ceiling,
            "executable_new_capacity_rate_per_minute": row.effective_rate_per_minute,
            "trusted_fleet_release_sha256": row.trusted_fleet_release_sha256,
        }
        if row.state in {"active", "drain-only"}:
            return ExecutionAuthorityV2.model_validate(values)
        if row.state == "prepared":
            return ExecutionContextV2.model_validate(values)
        raise AuthorityRecoveryError("execution epoch is not current authority")

    async def ingest_demand_snapshot(
        self,
        session: AsyncSession,
        report: DemandSnapshotV1,
        *,
        actor: str,
    ) -> IngestResult:
        digest = canonical_digest(report)
        equivocation = False
        result: IngestResult | None = None
        async with _write_transaction(session):
            await _lock_authority(session)
            reporter = (
                await session.execute(
                    select(CapacityDemandReporter)
                    .where(
                        CapacityDemandReporter.subject_id == report.subject_id,
                        CapacityDemandReporter.subject_incarnation == report.subject_incarnation,
                        CapacityDemandReporter.reporter_incarnation == report.reporter_incarnation,
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if reporter is None or reporter.state != "current":
                raise UnknownReporterError("demand reporter is not current")
            if (
                reporter.configuration_generation != report.configuration_generation
                or reporter.deployment_generation != report.deployment_generation
            ):
                raise UnknownReporterError("demand reporter binding is stale")
            if report.sequence < reporter.high_water:
                raise StaleReportError("demand report sequence is below high-water")
            if report.sequence == reporter.high_water:
                prior = (
                    await session.execute(
                        select(CapacityDemandSnapshot).where(
                            CapacityDemandSnapshot.reporter_incarnation
                            == report.reporter_incarnation,
                            CapacityDemandSnapshot.sequence == report.sequence,
                        )
                    )
                ).scalar_one_or_none()
                if prior is not None and prior.digest == digest:
                    result = IngestResult(prior.id, digest, report.sequence, True)
                else:
                    reporter.state = "equivocal"
                    session.add(
                        _audit(
                            actor_kind="reporter",
                            actor_id=actor,
                            event_kind="capacity_demand_report_equivocation",
                            object_binding={
                                "subject_id": str(report.subject_id),
                                "sequence": report.sequence,
                            },
                            detail={},
                        )
                    )
                    equivocation = True
            else:
                now = await _db_now(session)
                row = CapacityDemandSnapshot(
                    subject_id=report.subject_id,
                    subject_incarnation=report.subject_incarnation,
                    reporter_incarnation=report.reporter_incarnation,
                    sequence=report.sequence,
                    digest=digest,
                    payload=report.model_dump(mode="json", exclude_none=False),
                    database_received_at=now,
                    validity="valid",
                    acknowledged=True,
                )
                session.add(row)
                reporter.high_water = report.sequence
                reporter.last_receipt_time = now
                reporter.last_digest = digest
                await self._retain_demand_commitments(session, report, now)
                await session.flush()
                result = IngestResult(row.id, digest, report.sequence, False)
        if equivocation:
            raise ReportEquivocationError("demand reporter equivocated at high-water")
        assert result is not None
        return result

    async def authenticate_dynamic_demand_reporter(
        self,
        session: AsyncSession,
        report: DemandSnapshotV1,
        *,
        token_sha256: str,
    ) -> str:
        """Resolve a lifecycle-projected reporter without exposing its token hash."""

        reporter = (
            await session.execute(
                select(CapacityDemandReporter).where(
                    CapacityDemandReporter.token_sha256 == token_sha256,
                    CapacityDemandReporter.state == "current",
                )
            )
        ).scalar_one_or_none()
        if reporter is None or (
            reporter.subject_id != report.subject_id
            or reporter.subject_incarnation != report.subject_incarnation
            or reporter.reporter_incarnation != report.reporter_incarnation
            or reporter.configuration_generation != report.configuration_generation
            or reporter.deployment_generation != report.deployment_generation
        ):
            raise UnknownReporterError("unknown demand reporter")
        return f"dynamic-demand-{report.subject_id}"

    async def _retain_demand_commitments(
        self,
        session: AsyncSession,
        report: DemandSnapshotV1,
        now: datetime,
    ) -> None:
        for claim in report.fixed_claims:
            observed_state: Literal["observed", "unknown", "quarantined"]
            if claim.state == "unknown":
                observed_state = "unknown"
            elif claim.state == "quarantined":
                observed_state = "quarantined"
            else:
                observed_state = "observed"
            observed = ObservedCommitmentV1(
                kind="claim",
                commitment_id=claim.claim_id,
                physical_identity=claim.worker_identity,
                attempt_id=claim.attempt_id,
                concurrency_slots=claim.concurrency_slots,
                subject_id=report.subject_id,
                subject_incarnation=report.subject_incarnation,
                deployment_generation=claim.deployment_generation,
                pool_id=claim.pool_id,
                pool_generation=claim.pool_generation,
                profile_id=claim.profile_id,
                profile_generation=claim.profile_generation,
                profile_digest=claim.profile_digest,
                shape_id=claim.shape_id,
                resources=claim.resources,
                state=observed_state,
            )
            await self._upsert_commitment(
                session,
                kind="claim",
                source_incarnation=report.reporter_incarnation,
                sequence=report.sequence,
                observed=observed,
                now=now,
            )

    async def ingest_pool_observation(
        self,
        session: AsyncSession,
        report: PoolObservationV1,
        *,
        actor: str,
    ) -> IngestResult:
        digest = canonical_digest(report)
        equivocation = False
        result: IngestResult | None = None
        async with _write_transaction(session):
            await _lock_authority(session)
            reporter = (
                await session.execute(
                    select(CapacityPoolReporter)
                    .where(
                        CapacityPoolReporter.pool_id == report.pool_id,
                        CapacityPoolReporter.reporter_incarnation == report.reporter_incarnation,
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if reporter is None or reporter.state != "current":
                raise UnknownReporterError("pool reporter is not current")
            if reporter.pool_generation != report.pool_generation:
                raise UnknownReporterError("pool reporter binding is stale")
            if report.sequence < reporter.high_water:
                raise StaleReportError("pool report sequence is below high-water")
            if report.sequence == reporter.high_water:
                prior = (
                    await session.execute(
                        select(CapacityPoolObservation).where(
                            CapacityPoolObservation.reporter_incarnation
                            == report.reporter_incarnation,
                            CapacityPoolObservation.sequence == report.sequence,
                        )
                    )
                ).scalar_one_or_none()
                if prior is not None and prior.digest == digest:
                    result = IngestResult(prior.id, digest, report.sequence, True)
                else:
                    reporter.state = "equivocal"
                    equivocation = True
                    session.add(
                        _audit(
                            actor_kind="reporter",
                            actor_id=actor,
                            event_kind="capacity_pool_report_equivocation",
                            object_binding={
                                "pool_id": report.pool_id,
                                "sequence": report.sequence,
                            },
                            detail={},
                        )
                    )
            else:
                now = await _db_now(session)
                row = CapacityPoolObservation(
                    pool_id=report.pool_id,
                    reporter_incarnation=report.reporter_incarnation,
                    sequence=report.sequence,
                    digest=digest,
                    payload=report.model_dump(mode="json", exclude_none=False),
                    database_received_at=now,
                    validity="valid",
                )
                session.add(row)
                reporter.high_water = report.sequence
                reporter.last_receipt_time = now
                reporter.last_digest = digest
                for observed in report.commitments:
                    if observed.ownership_state != "unverified":
                        raise ConfigurationConflictError(
                            "pool reporter cannot authenticate executor ownership"
                        )
                    await self._upsert_commitment(
                        session,
                        kind="physical",
                        source_incarnation=report.reporter_incarnation,
                        sequence=report.sequence,
                        observed=observed,
                        now=now,
                    )
                await session.flush()
                result = IngestResult(row.id, digest, report.sequence, False)
        if equivocation:
            raise ReportEquivocationError("pool reporter equivocated at high-water")
        assert result is not None
        return result

    async def _upsert_commitment(
        self,
        session: AsyncSession,
        *,
        kind: Literal["claim", "physical"],
        source_incarnation: UUID,
        sequence: int,
        observed: ObservedCommitmentV1,
        now: datetime,
    ) -> None:
        if observed.kind != kind:
            raise ConfigurationConflictError(
                "commitment contract kind does not match reporter authority"
            )
        existing = (
            await session.execute(
                select(CapacityObservedCommitment)
                .where(
                    CapacityObservedCommitment.kind == kind,
                    CapacityObservedCommitment.commitment_identity == observed.commitment_id,
                    CapacityObservedCommitment.source_incarnation == source_incarnation,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        payload = observed.model_dump(mode="json", exclude_none=False)
        if existing is None:
            session.add(
                CapacityObservedCommitment(
                    kind=kind,
                    commitment_identity=observed.commitment_id,
                    source_incarnation=source_incarnation,
                    subject_id=observed.subject_id,
                    subject_incarnation=observed.subject_incarnation,
                    pool_id=observed.pool_id,
                    pool_generation=observed.pool_generation,
                    deployment_generation=observed.deployment_generation,
                    profile_id=observed.profile_id,
                    profile_generation=observed.profile_generation,
                    profile_digest=observed.profile_digest,
                    shape_id=observed.shape_id,
                    attempt_id=observed.attempt_id,
                    concurrency_slots=observed.concurrency_slots,
                    binding_payload={"observed_contract": payload},
                    resource_vector=observed.resources.model_dump(mode="json", exclude_none=False),
                    state=observed.state,
                    first_reporter_high_water=sequence,
                    last_reporter_high_water=sequence,
                    first_receipt_time=now,
                    last_receipt_time=now,
                )
            )
            return
        prior = existing.binding_payload.get("observed_contract")
        prior_binding = dict(prior) if isinstance(prior, dict) else None
        observed_binding = dict(payload)
        if prior_binding is not None:
            prior_binding.pop("state", None)
        observed_binding.pop("state", None)
        binding_conflict = prior_binding != observed_binding
        already_conflicted = "conflicting_contract" in existing.binding_payload
        if not binding_conflict and not already_conflicted:
            existing.state = observed.state
            existing.binding_payload = {"observed_contract": payload}
        elif binding_conflict:
            existing.state = "quarantined"
            existing.binding_payload = {
                "observed_contract": prior,
                "conflicting_contract": payload,
            }
            conflict_identity = (
                f"{observed.commitment_id}-conflict-"
                f"{canonical_digest_excluding(observed, 'state')[:12]}"
            )
            conflict = (
                await session.execute(
                    select(CapacityObservedCommitment).where(
                        CapacityObservedCommitment.kind == kind,
                        CapacityObservedCommitment.commitment_identity == conflict_identity,
                        CapacityObservedCommitment.source_incarnation == source_incarnation,
                    )
                )
            ).scalar_one_or_none()
            if conflict is None:
                conflict_contract = observed.model_copy(
                    update={
                        "commitment_id": conflict_identity,
                        "state": "quarantined",
                    }
                )
                session.add(
                    CapacityObservedCommitment(
                        kind=kind,
                        commitment_identity=conflict_identity,
                        source_incarnation=source_incarnation,
                        subject_id=observed.subject_id,
                        subject_incarnation=observed.subject_incarnation,
                        pool_id=observed.pool_id,
                        pool_generation=observed.pool_generation,
                        deployment_generation=observed.deployment_generation,
                        profile_id=observed.profile_id,
                        profile_generation=observed.profile_generation,
                        profile_digest=observed.profile_digest,
                        shape_id=observed.shape_id,
                        attempt_id=observed.attempt_id,
                        concurrency_slots=observed.concurrency_slots,
                        binding_payload={
                            "observed_contract": conflict_contract.model_dump(
                                mode="json", exclude_none=False
                            ),
                            "conflicts_with": observed.commitment_id,
                        },
                        resource_vector=observed.resources.model_dump(
                            mode="json", exclude_none=False
                        ),
                        state="quarantined",
                        first_reporter_high_water=sequence,
                        last_reporter_high_water=sequence,
                        first_receipt_time=now,
                        last_receipt_time=now,
                    )
                )
        existing.last_reporter_high_water = max(existing.last_reporter_high_water, sequence)
        existing.last_receipt_time = now

    async def load_allocation_input(
        self,
        session: AsyncSession,
        writer: WriterFence,
    ) -> AllocationInputV1:
        authority = (
            await session.execute(
                select(CapacityAuthorityState).where(CapacityAuthorityState.singleton_id == 1)
            )
        ).scalar_one()
        if (
            authority.authority_incarnation != writer.authority_incarnation
            or authority.writer_epoch != writer.writer_epoch
        ):
            raise StaleWriterError("writer is no longer current")
        active = (
            (
                await session.execute(
                    select(CapacityConfigurationEpoch).order_by(
                        CapacityConfigurationEpoch.configuration_epoch.desc()
                    )
                )
            )
            .scalars()
            .first()
        )
        if active is None:
            raise ConfigurationConflictError("no active configuration")
        fleet_row = (
            await session.execute(
                select(CapacityConfigGeneration).where(
                    CapacityConfigGeneration.scope == "fleet",
                    CapacityConfigGeneration.state == "active",
                    CapacityConfigGeneration.digest == active.fleet_digest,
                )
            )
        ).scalar_one()
        fleet = _parse_contract(FleetManifestV1, fleet_row.payload)
        subject_rows = (
            (
                await session.execute(
                    select(CapacityConfigGeneration).where(
                        CapacityConfigGeneration.scope == "subject",
                        CapacityConfigGeneration.state == "active",
                    )
                )
            )
            .scalars()
            .all()
        )
        subjects = tuple(
            _parse_contract(SubjectConfigurationV1, row.payload) for row in subject_rows
        )
        now = await _db_now(session)
        subject_inputs = tuple(
            [await self._subject_input(session, subject, now) for subject in subjects]
        )
        pool_inputs = tuple([await self._pool_input(session, pool, now) for pool in fleet.pools])
        observed_rows = (
            (
                await session.execute(
                    select(CapacityObservedCommitment).order_by(
                        CapacityObservedCommitment.commitment_identity
                    )
                )
            )
            .scalars()
            .all()
        )
        observed: list[ObservedCommitmentV1] = []
        for row in observed_rows:
            payload = row.binding_payload.get("observed_contract")
            if not isinstance(payload, dict):
                continue
            contract = _parse_contract(ObservedCommitmentV1, payload)
            if row.kind != contract.kind:
                raise ConfigurationConflictError(
                    "stored commitment kind does not match its contract"
                )
            if row.state != contract.state:
                contract = contract.model_copy(update={"state": row.state})
            observed.append(contract)
        reservation_rows = (
            await session.execute(
                select(CapacityReservationTranche, CapacityReservationShape)
                .join(
                    CapacityReservationShape,
                    CapacityReservationShape.tranche_id == CapacityReservationTranche.id,
                )
                .where(
                    or_(
                        CapacityReservationTranche.state == "accepted",
                        and_(
                            CapacityReservationTranche.state == "proposed",
                            CapacityReservationTranche.expires_at > now,
                        ),
                    ),
                    CapacityReservationShape.state != "released",
                )
                .order_by(
                    CapacityReservationTranche.id,
                    CapacityReservationShape.shape_instance_id,
                )
            )
        ).all()
        state_by_shape = {
            "proposed": "proposed",
            "accepted": "accepted",
            "releasing": "draining",
        }
        for tranche, shape in reservation_rows:
            identity_digest = hashlib.sha256(
                f"{tranche.id}:{shape.shape_instance_id}".encode("ascii")
            ).hexdigest()[:24]
            observed.append(
                ObservedCommitmentV1(
                    kind="reserve",
                    commitment_id=f"reservation-{identity_digest}",
                    physical_identity=f"reservation-{identity_digest}",
                    reservation_identity=str(shape.intent_id),
                    subject_id=tranche.subject_id,
                    subject_incarnation=tranche.subject_incarnation,
                    deployment_generation=tranche.deployment_generation,
                    pool_id=tranche.pool_id,
                    pool_generation=tranche.pool_generation,
                    profile_id=shape.profile_id,
                    profile_generation=shape.profile_generation,
                    profile_digest=shape.profile_digest,
                    shape_id=shape.shape_id,
                    resources=ResourceVectorV1.model_validate(shape.resource_vector),
                    state=cast(Any, state_by_shape[shape.state]),
                    node_ids=tuple(shape.node_ids),
                )
            )
        fairness_rows = (
            (
                await session.execute(
                    select(CapacityFairnessState).order_by(
                        CapacityFairnessState.tier_id,
                        CapacityFairnessState.phase,
                        CapacityFairnessState.account_id,
                    )
                )
            )
            .scalars()
            .all()
        )
        fairness = tuple(
            FairnessCursorV1(
                tier_id=row.tier_id,  # type: ignore[arg-type]
                phase=row.phase,  # type: ignore[arg-type]
                account_id=row.cursor_id if row.scope == "tier_account" else row.account_id,
                subject_id=UUID(row.cursor_id)
                if row.scope == "account_subject" and row.cursor_id
                else None,
            )
            for row in fairness_rows
        )
        account_rows = (
            (
                await session.execute(
                    select(CapacityAccountPolicy)
                    .where(CapacityAccountPolicy.configuration_epoch == active.configuration_epoch)
                    .order_by(CapacityAccountPolicy.account_id)
                )
            )
            .scalars()
            .all()
        )
        effective_accounts = tuple(
            _parse_contract(AccountPolicyV1, row.payload) for row in account_rows
        )
        snapshot = ConfigurationSnapshotV1(
            configuration_epoch=active.configuration_epoch,
            fleet=ConfigurationGenerationRefV1(
                scope="fleet",
                generation=fleet_row.scope_generation,
                digest=fleet_row.digest,
            ),
            subjects=tuple(
                ConfigurationGenerationRefV1(
                    scope="subject",
                    generation=row.scope_generation,
                    digest=row.digest,
                    subject_id=row.subject_id,
                    subject_incarnation=row.subject_incarnation,
                )
                for row in subject_rows
            ),
        )
        return AllocationInputV1(
            configuration=snapshot,
            fleet=fleet,
            effective_account_policies=effective_accounts,
            subjects=subject_inputs,
            pools=pool_inputs,
            observed_commitments=_deduplicate_observed_commitments(observed),
            fairness_cursors=fairness,
            existing_pending_slots=0,
            existing_pending_jobs=0,
        )

    async def _subject_input(
        self,
        session: AsyncSession,
        subject: SubjectConfigurationV1,
        now: datetime,
    ) -> SubjectAllocationInputV1:
        reporter = (
            await session.execute(
                select(CapacityDemandReporter).where(
                    CapacityDemandReporter.subject_id == subject.subject_id,
                    CapacityDemandReporter.reporter_incarnation
                    == subject.demand_reporter_incarnation,
                )
            )
        ).scalar_one_or_none()
        last: DemandSnapshotV1 | None = None
        if reporter is not None and reporter.high_water > 0:
            row = (
                await session.execute(
                    select(CapacityDemandSnapshot).where(
                        CapacityDemandSnapshot.reporter_incarnation
                        == reporter.reporter_incarnation,
                        CapacityDemandSnapshot.sequence == reporter.high_water,
                    )
                )
            ).scalar_one_or_none()
            if row is not None:
                last = _parse_contract(DemandSnapshotV1, row.payload)
        freshness = self._freshness_state(
            state=None if reporter is None else reporter.state,
            digest=None if reporter is None else reporter.last_digest,
            received=None if reporter is None else reporter.last_receipt_time,
            now=now,
        )
        return SubjectAllocationInputV1(
            configuration=subject,
            freshness=freshness,
            last_demand=last,
        )

    async def _pool_input(
        self,
        session: AsyncSession,
        pool: Any,
        now: datetime,
    ) -> PoolAllocationInputV1:
        reporter = (
            await session.execute(
                select(CapacityPoolReporter).where(
                    CapacityPoolReporter.pool_id == pool.pool_id,
                    CapacityPoolReporter.reporter_incarnation == pool.pool_reporter_incarnation,
                )
            )
        ).scalar_one_or_none()
        last: PoolObservationV1 | None = None
        if reporter is not None and reporter.high_water > 0:
            row = (
                await session.execute(
                    select(CapacityPoolObservation).where(
                        CapacityPoolObservation.reporter_incarnation
                        == reporter.reporter_incarnation,
                        CapacityPoolObservation.sequence == reporter.high_water,
                    )
                )
            ).scalar_one_or_none()
            if row is not None:
                last = _parse_contract(PoolObservationV1, row.payload)
        freshness = self._freshness_state(
            state=None if reporter is None else reporter.state,
            digest=None if reporter is None else reporter.last_digest,
            received=None if reporter is None else reporter.last_receipt_time,
            now=now,
        )
        return PoolAllocationInputV1(
            configuration=pool,
            freshness=freshness,
            last_observation=last,
        )

    def _freshness_state(
        self,
        *,
        state: str | None,
        digest: str | None,
        received: datetime | None,
        now: datetime,
    ) -> InputFreshnessV1:
        if state == "equivocal":
            status: Literal["valid", "stale", "missing", "invalid", "equivocal"] = "equivocal"
        elif state != "current" or received is None:
            status = "missing"
        elif now - received > self._freshness:
            status = "stale"
        else:
            status = "valid"
        return InputFreshnessV1(
            state=status,
            last_payload_digest=digest,
            database_received_at=received,
        )

    async def commit_shadow_epoch(
        self,
        session: AsyncSession,
        writer: WriterFence,
        epoch: ShadowEpochV1,
    ) -> CommittedShadowEpoch:
        async with _write_transaction(session):
            authority = await _lock_authority(session)
            if (
                authority.authority_incarnation != writer.authority_incarnation
                or authority.writer_epoch != writer.writer_epoch
            ):
                raise StaleWriterError("writer is no longer current")
            current_input = await self.load_allocation_input(session, writer)
            current_digest = canonical_digest(current_input)
            if current_digest != epoch.input_digest:
                raise StaleAllocationInputError("allocation input changed before commit")
            if current_input.configuration != epoch.configuration:
                raise StaleAllocationInputError("configuration changed before commit")
            if epoch.executable or epoch.executable_new_capacity_ceiling != 0:
                raise CapacityStoreError("only shadow epochs may be committed")
            now = await _db_now(session)
            row = CapacityAllocationEpoch(
                writer_epoch=writer.writer_epoch,
                configuration_epoch=epoch.configuration.configuration_epoch,
                input_digest=epoch.input_digest,
                status="shadow",
                failure_reason=None,
                complete_payload=epoch.model_dump(mode="json", exclude_none=False),
                executable=False,
                committed_at=now,
            )
            session.add(row)
            await session.flush()
            for allocation in epoch.allocations:
                session.add(
                    CapacityAllocation(
                        allocation_epoch=row.allocation_epoch,
                        subject_id=allocation.subject_id,
                        subject_incarnation=allocation.subject_incarnation,
                        deployment_generation=allocation.deployment_generation,
                        pool_id=allocation.pool_id,
                        desired_shapes=[
                            item.model_dump(mode="json", exclude_none=False)
                            for item in allocation.desired_shapes
                        ],
                        desired_resources={},
                        commitments=[
                            match.model_dump(mode="json", exclude_none=False)
                            for match in allocation.claim_slot_matches
                        ],
                        drains=[
                            {"shape_id": shape_id} for shape_id in allocation.draining_shape_ids
                        ],
                        allowances=[
                            allowance.model_dump(mode="json", exclude_none=False)
                            for allowance in allocation.placement_allowances
                        ],
                        witness={}
                        if allocation.matching_witness is None
                        else allocation.matching_witness.model_dump(
                            mode="json", exclude_none=False
                        ),
                        mode="shadow",
                        executable=False,
                    )
                )
            await session.execute(delete(CapacityFairnessState))
            for cursor in epoch.next_fairness_cursors:
                session.add(
                    CapacityFairnessState(
                        configuration_epoch=epoch.configuration.configuration_epoch,
                        mode="shadow",
                        scope="tier_account" if cursor.subject_id is None else "account_subject",
                        phase=cursor.phase,
                        tier_id=cursor.tier_id,
                        account_id=cursor.account_id,
                        subject_id=cursor.subject_id,
                        cursor_id=str(
                            cursor.subject_id
                            if cursor.subject_id is not None
                            else cursor.account_id
                        )
                        if (cursor.subject_id is not None or cursor.account_id is not None)
                        else None,
                        last_shadow_epoch=row.allocation_epoch,
                    )
                )
            authority.increase_freeze = False
            authority.increase_freeze_reason = None
            authority.updated_at = now
            session.add(
                _audit(
                    actor_kind="manager",
                    actor_id=str(writer.authority_incarnation),
                    event_kind="capacity_shadow_epoch_committed",
                    object_binding={"allocation_epoch": row.allocation_epoch},
                    detail={
                        "input_digest": epoch.input_digest,
                        "allocation_count": len(epoch.allocations),
                    },
                )
            )
            return CommittedShadowEpoch(row.allocation_epoch, epoch.input_digest)

    async def record_shadow_failure(
        self,
        session: AsyncSession,
        writer: WriterFence,
        *,
        event_kind: Literal[
            "shadow_allocation_timeout",
            "shadow_allocation_invalid",
            "shadow_allocation_failure",
            "shadow_allocation_input_contention",
        ],
        reason: str,
        expected_input_digest: str | None,
        persist_failed_epoch: bool = True,
    ) -> RecordedShadowFailure:
        """Persist one fenced fail-closed diagnostic without allocation rows."""

        if not reason or len(reason.encode("utf-8")) > 1024:
            raise ValueError("shadow failure reason must be between 1 and 1024 bytes")
        if expected_input_digest is not None and (
            len(expected_input_digest) != 64
            or any(character not in "0123456789abcdef" for character in expected_input_digest)
        ):
            raise ValueError("expected input digest must be lowercase SHA-256")
        async with _write_transaction(session):
            authority = await _lock_authority(session)
            if (
                authority.authority_incarnation != writer.authority_incarnation
                or authority.writer_epoch != writer.writer_epoch
            ):
                raise StaleWriterError("writer is no longer current")
            current_input: AllocationInputV1 | None = None
            if expected_input_digest is not None:
                current_input = await self.load_allocation_input(session, writer)
                if canonical_digest(current_input) != expected_input_digest:
                    raise StaleAllocationInputError(
                        "allocation input changed before failure record"
                    )
            configuration_epoch = (
                current_input.configuration.configuration_epoch
                if current_input is not None
                else (
                    await session.execute(
                        select(func.max(CapacityConfigurationEpoch.configuration_epoch))
                    )
                ).scalar_one()
            )
            input_digest = expected_input_digest or ("0" * 64)
            allocation_epoch: int | None = None
            if persist_failed_epoch and configuration_epoch is not None:
                row = CapacityAllocationEpoch(
                    writer_epoch=writer.writer_epoch,
                    configuration_epoch=configuration_epoch,
                    input_digest=input_digest,
                    status="failed",
                    failure_reason=reason,
                    complete_payload={
                        "schema_version": 1,
                        "input_digest": input_digest,
                        "reason": reason,
                        "executable": False,
                    },
                    executable=False,
                    committed_at=await _db_now(session),
                )
                session.add(row)
                await session.flush()
                allocation_epoch = row.allocation_epoch
            now = await _db_now(session)
            authority.increase_freeze = True
            authority.increase_freeze_reason = event_kind
            authority.updated_at = now
            session.add(
                _audit(
                    actor_kind="manager",
                    actor_id=str(writer.authority_incarnation),
                    event_kind=event_kind,
                    object_binding={
                        "allocation_epoch": allocation_epoch,
                        "writer_epoch": writer.writer_epoch,
                    },
                    detail={"input_digest": input_digest, "reason": reason},
                )
            )
            return RecordedShadowFailure(allocation_epoch, input_digest)

    async def status(
        self,
        session: AsyncSession,
        *,
        cursor: int | None,
        limit: int,
    ) -> CapacityStatusPageV1:
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        if cursor is not None and (type(cursor) is not int or cursor < 0):
            raise ValueError("cursor must be a nonnegative integer")
        authority = (
            await session.execute(
                select(CapacityAuthorityState).where(CapacityAuthorityState.singleton_id == 1)
            )
        ).scalar_one()
        configuration = (
            (
                await session.execute(
                    select(CapacityConfigurationEpoch).order_by(
                        CapacityConfigurationEpoch.configuration_epoch.desc()
                    )
                )
            )
            .scalars()
            .first()
        )
        latest_allocation = (
            await session.execute(select(func.max(CapacityAllocationEpoch.allocation_epoch)))
        ).scalar_one()
        query = select(CapacityAuditEvent).order_by(CapacityAuditEvent.id)
        if cursor is not None:
            query = query.where(CapacityAuditEvent.id > cursor)
        rows = (await session.execute(query.limit(limit + 1))).scalars().all()
        page = rows[:limit]
        next_cursor = page[-1].id if len(rows) > limit and page else None
        return CapacityStatusPageV1(
            authority_incarnation=authority.authority_incarnation,
            writer_epoch=authority.writer_epoch,
            configuration_epoch=0 if configuration is None else configuration.configuration_epoch,
            configuration_digest=None if configuration is None else configuration.canonical_digest,
            latest_allocation_epoch=latest_allocation,
            execution_epoch=authority.execution_epoch,
            execution_state=cast(
                Literal["shadow", "prepared", "active", "drain-only"],
                authority.execution_state,
            ),
            execution_manifest_sha256=authority.execution_manifest_sha256,
            executable_new_capacity_ceiling=authority.executable_new_capacity_ceiling,
            increase_freeze=authority.increase_freeze,
            items=tuple(
                {
                    "id": row.id,
                    "event_kind": row.event_kind,
                    "created_at": row.created_at.isoformat(),
                }
                for row in page
            ),
            next_cursor=next_cursor,
        )


__all__ = [
    "ActivatedConfiguration",
    "AuthorityRecoveryError",
    "CapacityManagementStore",
    "CapacityStatusPageV1",
    "CapacityStoreError",
    "CommittedShadowEpoch",
    "ConfigurationConflictError",
    "ExecutionConflictError",
    "ExecutionPreparationDisabledError",
    "IdempotencyConflictError",
    "IngestResult",
    "ProposedConfiguration",
    "RecordedShadowFailure",
    "ReportEquivocationError",
    "StaleAllocationInputError",
    "StaleReportError",
    "StaleWriterError",
    "UnknownReporterError",
    "WriterFence",
]
