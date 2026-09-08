"""Exact historical subject observations; neither readiness nor a release mutation."""

from __future__ import annotations

import json
from typing import Annotated, Literal, TypedDict, cast
from uuid import UUID

from pydantic import Field, TypeAdapter, model_validator
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.contracts import (
    ConfigurationGenerationRefV1,
    ConfigurationSnapshotV1,
    Digest,
    FleetManifestV1,
    PositiveQuantity,
    Quantity,
    StrictV1Model,
    SubjectConfigurationV1,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentBindingV2,
    canonical_executable_digest,
)
from loom_capacity_manager.membership_contracts import (
    ExecutionPreparationV3,
    PersonalApplicationMembershipMutationV1,
    PersonalApplicationMembershipResponseV1,
    PersonalApplicationMembershipResultV1,
)
from loom_capacity_manager.membership_current import (
    _immutable_base_subject,
    resolve_current_subject,
)
from loom_capacity_manager.membership_outcomes import (
    PersonalMembershipOperationCommittedV1,
    PersonalMembershipOperationOutcomeQueryV1,
    _bounded_json,
    query_membership_operation_outcome,
)
from loom_capacity_manager.membership_release import predecessor_release_sha256
from loom_capacity_manager.membership_store import _validated_membership_history
from loom_capacity_manager.models import (
    CapacityAuthorityState,
    CapacityConfigurationEpoch,
    CapacityExecutableIntent,
    CapacityExecutionEpoch,
    CapacityObservedCommitment,
    CapacityPersonalMembershipEvent,
    CapacityReservationTranche,
    CapacitySubject,
)
from loom_capacity_manager.store import (
    CapacityManagementStore,
    ConfigurationConflictError,
    _canonical_json_digest,
    _parse_contract,
    _subject_scalars_match,
    _write_transaction,
)


class PersonalMembershipSubjectQueryV1(StrictV1Model):
    membership_receipt: PersonalApplicationMembershipResponseV1


class PersonalMembershipCurrentObservationV1(StrictV1Model):
    authority_incarnation: UUID
    writer_epoch: PositiveQuantity
    execution_epoch: Quantity
    execution_manifest_sha256: Digest | None
    execution_state: Literal["shadow", "prepared", "active", "drain-only"]
    configuration_epoch: PositiveQuantity
    configuration_sha256: Digest
    membership_execution_epoch: Quantity
    membership_revision: Quantity
    membership_head_sha256: Digest
    subject: SubjectConfigurationV1 | None

    @model_validator(mode="after")
    def _coherent_position(self) -> PersonalMembershipCurrentObservationV1:
        if (self.membership_revision == 0) != (self.membership_head_sha256 == "0" * 64):
            raise ValueError("current membership head differs from revision")
        if self.membership_execution_epoch == 0 and self.membership_revision != 0:
            raise ValueError("current membership revision requires an execution epoch")
        if self.execution_state == "shadow":
            if self.execution_epoch != 0 or self.execution_manifest_sha256 is not None:
                raise ValueError("shadow observation retains executable authority")
        elif (
            self.execution_epoch == 0
            or self.execution_manifest_sha256 is None
            or self.membership_execution_epoch != self.execution_epoch
        ):
            raise ValueError("current execution observation is incomplete")
        return self


class PersonalMembershipWorkCountsV1(StrictV1Model):
    executable_intents: Quantity
    unreleased_executable_intents: Quantity
    legacy_reservations: Quantity
    unreleased_legacy_reservations: Quantity
    observed_commitments: Quantity

    @model_validator(mode="after")
    def _bounded_unreleased(self) -> PersonalMembershipWorkCountsV1:
        if (
            self.unreleased_executable_intents > self.executable_intents
            or self.unreleased_legacy_reservations > self.legacy_reservations
        ):
            raise ValueError("unreleased work exceeds total work")
        return self


class _HistoricalObservation(StrictV1Model):
    query_sha256: Digest
    membership_receipt: PersonalApplicationMembershipResponseV1
    current: PersonalMembershipCurrentObservationV1
    historical: Literal[True] = True
    worker_available: Literal[False] = False
    incarnation_work: PersonalMembershipWorkCountsV1

    @model_validator(mode="after")
    def _same_subject(self) -> _HistoricalObservation:
        if (
            self.current.subject is not None
            and self.current.subject.subject_id
            != self.membership_receipt.result.member.configuration.subject_id
        ):
            raise ValueError("current observation has a different subject")
        return self


class PersonalMembershipSubjectStatusV1(_HistoricalObservation):
    deployment_work: PersonalMembershipWorkCountsV1


class _ObservationValues(TypedDict):
    query_sha256: str
    membership_receipt: PersonalApplicationMembershipResponseV1
    current: PersonalMembershipCurrentObservationV1
    incarnation_work: PersonalMembershipWorkCountsV1


class _ReleaseObservation(_HistoricalObservation):
    @model_validator(mode="after")
    def _disabled_receipt(self) -> _ReleaseObservation:
        subject = self.membership_receipt.result.member.configuration
        if subject.lifecycle_state != "disabled" or subject.min_slots or subject.max_slots:
            raise ValueError("release requires a disabled zero-capacity receipt")
        current = self.current.subject
        if (
            current is not None
            and current.subject_incarnation == subject.subject_incarnation
            and current.configuration_generation < subject.configuration_generation
        ):
            raise ValueError("current subject generation regressed")
        return self

    def _expected_blockers(self) -> tuple[str, ...]:
        result = []
        current = self.current.subject
        if (
            current is not None
            and current.subject_incarnation
            == self.membership_receipt.result.member.configuration.subject_incarnation
            and (current.lifecycle_state != "disabled" or current.min_slots or current.max_slots)
        ):
            result.append("same-incarnation-enabled")
        if self.incarnation_work.unreleased_executable_intents:
            result.append("executable-intents")
        if self.incarnation_work.unreleased_legacy_reservations:
            result.append("legacy-reservations")
        if self.incarnation_work.observed_commitments:
            result.append("observed-commitments")
        return tuple(result)


class PersonalMembershipReleasePendingV1(_ReleaseObservation):
    outcome: Literal["pending"] = "pending"
    blockers: Annotated[
        tuple[
            Literal[
                "same-incarnation-enabled",
                "executable-intents",
                "legacy-reservations",
                "observed-commitments",
            ],
            ...,
        ],
        Field(min_length=1, max_length=4),
    ]

    @model_validator(mode="after")
    def _exact_blockers(self) -> PersonalMembershipReleasePendingV1:
        if self.blockers != self._expected_blockers():
            raise ValueError("pending release blockers differ from observed work")
        return self


class PersonalMembershipReleaseVerifiedV1(_ReleaseObservation):
    outcome: Literal["verified"] = "verified"
    release_set_sha256: Digest

    @model_validator(mode="after")
    def _no_blockers(self) -> PersonalMembershipReleaseVerifiedV1:
        if self._expected_blockers():
            raise ValueError("verified release retains outstanding work or enabled admission")
        return self


PersonalMembershipReleaseObservationV1 = Annotated[
    PersonalMembershipReleasePendingV1 | PersonalMembershipReleaseVerifiedV1,
    Field(discriminator="outcome"),
]
_RELEASE_ADAPTER: TypeAdapter[PersonalMembershipReleaseObservationV1] = TypeAdapter(
    PersonalMembershipReleaseObservationV1
)


def parse_membership_subject_query(payload: bytes | str) -> PersonalMembershipSubjectQueryV1:
    return PersonalMembershipSubjectQueryV1.model_validate_json(_bounded_json(payload))


def parse_membership_subject_status(payload: bytes | str) -> PersonalMembershipSubjectStatusV1:
    return PersonalMembershipSubjectStatusV1.model_validate_json(_bounded_json(payload))


def parse_membership_release_observation(
    payload: bytes | str,
) -> PersonalMembershipReleaseObservationV1:
    return _RELEASE_ADAPTER.validate_json(_bounded_json(payload))


async def _history(
    session: AsyncSession,
    management: CapacityManagementStore,
    epoch: CapacityExecutionEpoch,
) -> tuple[PersonalApplicationMembershipResultV1, ...]:
    preparation = management._execution_preparation_from_row(epoch)
    rows = (
        await session.scalars(
            select(CapacityPersonalMembershipEvent)
            .where(CapacityPersonalMembershipEvent.execution_epoch == epoch.execution_epoch)
            .order_by(CapacityPersonalMembershipEvent.revision)
        )
    ).all()
    if not isinstance(preparation, ExecutionPreparationV3):
        if rows:
            raise ConfigurationConflictError("undelegated subject history changed")
        return ()
    if any(row.actor != preparation.personal_membership.management_principal_id for row in rows):
        raise ConfigurationConflictError("historical subject delegation changed")
    return await _validated_membership_history(session, rows, epoch)


async def _exact_receipt(
    session: AsyncSession,
    management: CapacityManagementStore,
    query: PersonalMembershipSubjectQueryV1,
) -> PersonalApplicationMembershipResponseV1:
    supplied = query.membership_receipt
    row = await session.scalar(
        select(CapacityPersonalMembershipEvent).where(
            CapacityPersonalMembershipEvent.execution_epoch
            == supplied.checkpoint.execution.execution_epoch,
            CapacityPersonalMembershipEvent.revision == supplied.checkpoint.revision,
        )
    )
    if row is None:
        raise ConfigurationConflictError("historical membership receipt is unavailable")
    request = PersonalApplicationMembershipMutationV1.model_validate_json(
        json.dumps(row.request_payload)
    )
    outcome = await query_membership_operation_outcome(
        session,
        management,
        PersonalMembershipOperationOutcomeQueryV1(
            original_actor=row.actor,
            idempotency_key=row.idempotency_key,
            request=request,
        ),
    )
    normalized = supplied.model_copy(
        update={"result": supplied.result.model_copy(update={"replayed": False})}
    )
    if (
        not isinstance(outcome, PersonalMembershipOperationCommittedV1)
        or normalized != outcome.receipt
    ):
        raise ConfigurationConflictError("historical membership receipt changed")
    return outcome.receipt


async def _current_observation(
    session: AsyncSession,
    management: CapacityManagementStore,
    authority: CapacityAuthorityState,
    subject_id: UUID,
) -> PersonalMembershipCurrentObservationV1:
    if authority.execution_state not in {"shadow", "prepared", "active", "drain-only"}:
        raise ConfigurationConflictError("current subject authority is unavailable")
    configuration = await session.scalar(
        select(CapacityConfigurationEpoch)
        .order_by(CapacityConfigurationEpoch.configuration_epoch.desc())
        .limit(1)
    )
    if configuration is None:
        raise ConfigurationConflictError("current configuration is unavailable")
    snapshot = ConfigurationSnapshotV1(
        configuration_epoch=configuration.configuration_epoch,
        fleet=ConfigurationGenerationRefV1(
            scope="fleet",
            generation=configuration.fleet_generation,
            digest=configuration.fleet_digest,
        ),
        subjects=tuple(
            _parse_contract(ConfigurationGenerationRefV1, item)
            for item in configuration.subject_generation_manifest
        ),
    )
    if canonical_digest(snapshot) != configuration.canonical_digest:
        raise ConfigurationConflictError("current configuration manifest changed")
    fleet_row = await management._load_fleet_generation_row(session, configuration)
    fleet = _parse_contract(FleetManifestV1, fleet_row.payload)
    if (
        canonical_digest(fleet) != configuration.fleet_digest
        or fleet.fleet_generation != configuration.fleet_generation
    ):
        raise ConfigurationConflictError("current fleet manifest changed")
    expected: dict[UUID, SubjectConfigurationV1] = {}
    for ref in snapshot.subjects:
        if ref.subject_id is None:
            raise ConfigurationConflictError("current subject reference changed")
        subject = await _immutable_base_subject(session, configuration, subject_id=ref.subject_id)
        if subject is None:
            raise ConfigurationConflictError("current base subject is unavailable")
        expected[ref.subject_id] = subject
    epoch = None
    if authority.execution_state == "shadow":
        if (
            authority.execution_epoch != 0
            or authority.execution_manifest_sha256 is not None
            or authority.executable_new_capacity_ceiling != 0
        ):
            raise ConfigurationConflictError("shadow authority binding changed")
        # Until an explicit new base is projected, retired membership remains
        # materialized. Authenticate it rather than treating a missing base ref
        # as proof that this retained subject disappeared.
        epoch = await session.scalar(
            select(CapacityExecutionEpoch)
            .where(CapacityExecutionEpoch.configuration_epoch == configuration.configuration_epoch)
            .order_by(CapacityExecutionEpoch.execution_epoch.desc())
            .limit(1)
        )
        if epoch is not None:
            payload = epoch.retirement_request_payload
            if (
                epoch.state != "retired"
                or epoch.retired_at is None
                or epoch.effective_ceiling != 0
                or epoch.effective_rate_per_minute != 0
                or epoch.retirement_actor is None
                or epoch.retirement_idempotency_key is None
                or payload is None
                or _canonical_json_digest(payload) != epoch.retirement_request_digest
                or payload.get("execution_epoch") != epoch.execution_epoch
                or payload.get("execution_manifest_sha256") != epoch.execution_manifest_sha256
                or payload.get("authority_incarnation") != str(epoch.authority_incarnation)
            ):
                raise ConfigurationConflictError("shadow execution retirement evidence changed")
    else:
        epoch = await session.get(CapacityExecutionEpoch, authority.execution_epoch)
        if epoch is None or epoch.configuration_epoch != configuration.configuration_epoch:
            raise ConfigurationConflictError("current execution configuration changed")
        management._execution_context(authority, epoch)
    history = () if epoch is None else await _history(session, management, epoch)
    for result in history:
        expected[result.member.configuration.subject_id] = result.member.configuration
    rows = (
        await session.scalars(
            select(CapacitySubject).where(
                CapacitySubject.configuration_epoch == configuration.configuration_epoch
            )
        )
    ).all()
    materialized = {}
    for row in rows:
        value = _parse_contract(SubjectConfigurationV1, row.payload)
        if not _subject_scalars_match(row, value) or value.subject_id in materialized:
            raise ConfigurationConflictError("current subject materialization changed")
        materialized[value.subject_id] = value
    if materialized != expected:
        raise ConfigurationConflictError("current subject materialization differs from history")
    current = expected.get(subject_id)
    if current is not None and authority.execution_state == "active":
        assert epoch is not None
        current, _ack = await resolve_current_subject(
            session, epoch, subject_id=subject_id, allow_disabled=True
        )
    return PersonalMembershipCurrentObservationV1(
        authority_incarnation=authority.authority_incarnation,
        writer_epoch=authority.writer_epoch,
        execution_epoch=authority.execution_epoch,
        execution_manifest_sha256=authority.execution_manifest_sha256,
        execution_state=cast(
            Literal["shadow", "prepared", "active", "drain-only"], authority.execution_state
        ),
        configuration_epoch=configuration.configuration_epoch,
        configuration_sha256=configuration.canonical_digest,
        membership_execution_epoch=0 if epoch is None else epoch.execution_epoch,
        membership_revision=0 if not history else history[-1].revision,
        membership_head_sha256="0" * 64 if not history else history[-1].head_sha256,
        subject=current,
    )


async def _prove_successor(
    session: AsyncSession,
    management: CapacityManagementStore,
    receipt: PersonalApplicationMembershipResponseV1,
    current: SubjectConfigurationV1,
) -> None:
    old = receipt.result.member.configuration
    if current.account_id != old.account_id or current.display_name != old.display_name:
        raise ConfigurationConflictError("current subject ownership changed")
    epochs = (
        await session.scalars(
            select(CapacityExecutionEpoch)
            .where(
                CapacityExecutionEpoch.execution_epoch.in_(
                    select(CapacityPersonalMembershipEvent.execution_epoch).where(
                        CapacityPersonalMembershipEvent.subject_id == old.subject_id
                    )
                )
            )
            .order_by(CapacityExecutionEpoch.execution_epoch)
        )
    ).all()
    reachable = {old.subject_incarnation}
    for epoch in epochs:
        for result in await _history(session, management, epoch):
            evidence = result.member.reincarnation
            if evidence is None or evidence.predecessor.subject_id != old.subject_id:
                continue
            if evidence.predecessor.subject_incarnation == old.subject_incarnation and (
                evidence.predecessor != old
                or evidence.predecessor_revision != receipt.checkpoint.revision
                or evidence.predecessor_head_sha256 != receipt.checkpoint.head_sha256
                or evidence.execution_manifest_sha256
                != receipt.checkpoint.execution.execution_manifest_sha256
                or evidence.namespace_id != receipt.checkpoint.namespace_id
            ):
                raise ConfigurationConflictError("successor predecessor receipt changed")
            if evidence.predecessor.subject_incarnation in reachable:
                reachable.add(evidence.successor_incarnation)
    if current.subject_incarnation not in reachable:
        raise ConfigurationConflictError("current successor lacks immutable recreation proof")


async def _work_counts(
    session: AsyncSession,
    subject: SubjectConfigurationV1,
) -> tuple[PersonalMembershipWorkCountsV1, PersonalMembershipWorkCountsV1]:
    identity = (subject.subject_id, subject.subject_incarnation)
    totals = {
        "executable_intents": 0,
        "unreleased_executable_intents": 0,
        "legacy_reservations": 0,
        "unreleased_legacy_reservations": 0,
        "observed_commitments": 0,
    }
    deployment_counts = dict(totals)

    def count(name: str, matches_deployment: bool) -> None:
        totals[name] += 1
        if matches_deployment:
            deployment_counts[name] += 1

    # Limits on a single demand report are not limits on immutable lifetime
    # history. Stream every row in this authority-locked snapshot with bounded
    # driver buffering, retaining only the counters returned on the wire.
    intents = await session.stream_scalars(
        select(CapacityExecutableIntent)
        .where(
            CapacityExecutableIntent.subject_id == identity[0],
            CapacityExecutableIntent.subject_incarnation == identity[1],
        )
        .execution_options(yield_per=256)
    )
    async for intent in intents:
        binding = ExecutableIntentBindingV2.model_validate_json(json.dumps(intent.binding_payload))
        if (
            (binding.subject_id, binding.subject_incarnation) != identity
            or binding.intent_id != intent.intent_id
            or binding.shape_instance_id != intent.shape_instance_id
            or binding.execution.execution_epoch != intent.execution_epoch
            or binding.execution.execution_manifest_sha256 != intent.execution_manifest_sha256
            or canonical_executable_digest(binding) != intent.binding_digest
            or (intent.state == "released") != (intent.released_at is not None)
        ):
            raise ConfigurationConflictError("historical subject intent evidence changed")
        same_deployment = binding.deployment_generation == subject.deployment_generation
        count("executable_intents", same_deployment)
        if intent.state != "released":
            count("unreleased_executable_intents", same_deployment)
    legacy = await session.stream_scalars(
        select(CapacityReservationTranche)
        .where(
            CapacityReservationTranche.subject_id == identity[0],
            CapacityReservationTranche.subject_incarnation == identity[1],
        )
        .execution_options(yield_per=256)
    )
    async for reservation in legacy:
        if (reservation.state == "closed") != (reservation.closed_at is not None):
            raise ConfigurationConflictError("historical legacy reservation closure changed")
        same_deployment = reservation.deployment_generation == subject.deployment_generation
        count("legacy_reservations", same_deployment)
        if reservation.state != "closed":
            count("unreleased_legacy_reservations", same_deployment)
    observed = await session.stream_scalars(
        select(CapacityObservedCommitment)
        .where(
            or_(
                and_(
                    CapacityObservedCommitment.subject_id == identity[0],
                    CapacityObservedCommitment.subject_incarnation == identity[1],
                ),
                and_(
                    CapacityObservedCommitment.binding_payload["observed_contract"][
                        "subject_id"
                    ].astext
                    == str(identity[0]),
                    CapacityObservedCommitment.binding_payload["observed_contract"][
                        "subject_incarnation"
                    ].astext
                    == str(identity[1]),
                ),
            )
        )
        .execution_options(yield_per=256)
    )
    async for commitment in observed:
        same_deployment = (
            commitment.deployment_generation == subject.deployment_generation
            or commitment.binding_payload.get("observed_contract", {}).get("deployment_generation")
            == subject.deployment_generation
        )
        count("observed_commitments", same_deployment)
    return (
        PersonalMembershipWorkCountsV1.model_validate(totals),
        PersonalMembershipWorkCountsV1.model_validate(deployment_counts),
    )


async def query_membership_subject(
    session: AsyncSession,
    management: CapacityManagementStore,
    query: PersonalMembershipSubjectQueryV1,
    *,
    release: bool = False,
) -> PersonalMembershipSubjectStatusV1 | PersonalMembershipReleaseObservationV1:
    try:
        async with _write_transaction(session):
            authority = (
                await session.execute(
                    select(CapacityAuthorityState)
                    .where(CapacityAuthorityState.singleton_id == 1)
                    .with_for_update(read=True)
                )
            ).scalar_one_or_none()
            if authority is None:
                raise ConfigurationConflictError("current subject authority is unavailable")
            receipt = await _exact_receipt(session, management, query)
            subject = receipt.result.member.configuration
            current = await _current_observation(session, management, authority, subject.subject_id)
            incarnation_work, deployment_work = await _work_counts(session, subject)
            values = _ObservationValues(
                query_sha256=canonical_digest(query),
                membership_receipt=receipt,
                current=current,
                incarnation_work=incarnation_work,
            )
            if not release:
                return PersonalMembershipSubjectStatusV1(**values, deployment_work=deployment_work)
            if (
                subject.lifecycle_state != "disabled"
                or subject.min_slots != 0
                or subject.max_slots != 0
            ):
                raise ConfigurationConflictError(
                    "release requires an exact disabled membership receipt"
                )
            blockers: list[str] = []
            if current.subject is not None:
                if current.subject.subject_incarnation == subject.subject_incarnation:
                    if current.subject.configuration_generation < subject.configuration_generation:
                        raise ConfigurationConflictError("current disabled generation regressed")
                    if (
                        current.subject.lifecycle_state != "disabled"
                        or current.subject.min_slots != 0
                        or current.subject.max_slots != 0
                    ):
                        blockers.append("same-incarnation-enabled")
                else:
                    await _prove_successor(session, management, receipt, current.subject)
            if incarnation_work.unreleased_executable_intents:
                blockers.append("executable-intents")
            if incarnation_work.unreleased_legacy_reservations:
                blockers.append("legacy-reservations")
            if incarnation_work.observed_commitments:
                blockers.append("observed-commitments")
            if blockers:
                return PersonalMembershipReleasePendingV1.model_validate(
                    {**values, "blockers": tuple(blockers)}
                )
            digest = await predecessor_release_sha256(session, subject)
            return PersonalMembershipReleaseVerifiedV1(**values, release_set_sha256=digest)
    except ValueError as exc:
        raise ConfigurationConflictError("historical subject evidence is malformed") from exc
