"""Durable active personal-membership persistence."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import cast
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.contracts import (
    AccountPolicyV1,
    ConfigurationGenerationRefV1,
    DynamicDevelopmentSubjectProjectionV1,
    FleetManifestV1,
    SubjectConfigurationV1,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutionAuthorityV2,
    SubjectExecutionAcknowledgementV2,
)
from loom_capacity_manager.membership_contracts import (
    ExecutionPreparationV3,
    PersonalApplicationMembershipMutationV1,
    PersonalApplicationMembershipResultV1,
    PersonalApplicationMemberV1,
    PersonalMembershipCheckpointV1,
    PersonalMembershipSnapshotV1,
    PersonalReincarnationEvidenceV1,
    parse_execution_preparation,
)
from loom_capacity_manager.models import (
    CapacityAccountPolicy,
    CapacityAuthorityState,
    CapacityCandidate,
    CapacityConfigGeneration,
    CapacityConfigurationEpoch,
    CapacityDemandReporter,
    CapacityDeploymentGeneration,
    CapacityExecutionEpoch,
    CapacityPersonalMembershipEvent,
    CapacitySubject,
)
from loom_capacity_manager.store import (
    CapacityManagementStore,
    ConfigurationConflictError,
    ExecutionConflictError,
    IdempotencyConflictError,
    WriterFence,
    _derive_development_subject,
    _derive_owner_account,
    _parse_contract,
    _write_transaction,
)

_ZERO_DIGEST = "0" * 64


class PersonalMembershipRevisionConflictError(ConfigurationConflictError):
    """Only the membership revision changed within the supplied exact authority."""


def _subject_reference(subject: SubjectConfigurationV1) -> ConfigurationGenerationRefV1:
    return ConfigurationGenerationRefV1(
        scope="subject",
        subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation,
        generation=subject.configuration_generation,
        digest=canonical_digest(subject),
    )


def _json_digest(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _head_digest(
    *,
    actor: str,
    execution_epoch: int,
    idempotency_key: UUID,
    operation_id: UUID,
    previous_sha256: str,
    request_digest: str,
    request_payload: dict[str, object],
    member: PersonalApplicationMemberV1,
    revision: int,
) -> str:
    return _json_digest(
        {
            "actor": actor,
            "execution_epoch": execution_epoch,
            "idempotency_key": str(idempotency_key),
            "operation_id": str(operation_id),
            "previous_sha256": previous_sha256,
            "request_digest": request_digest,
            "request_payload": request_payload,
            "result_member": member.model_dump(mode="json", exclude_none=False),
            "revision": revision,
        }
    )


def _event_result(row: CapacityPersonalMembershipEvent) -> PersonalApplicationMembershipResultV1:
    try:
        request = PersonalApplicationMembershipMutationV1.model_validate_json(
            json.dumps(row.request_payload, sort_keys=True, separators=(",", ":"))
        )
        result = PersonalApplicationMembershipResultV1.model_validate_json(
            json.dumps(row.result_payload, sort_keys=True, separators=(",", ":"))
        )
    except ValueError as exc:
        raise ConfigurationConflictError("personal membership event payload is invalid") from exc
    member = result.member
    configuration = member.configuration
    projection = request.projection
    if (
        canonical_digest(request) != row.request_digest
        or request.namespace_id != row.namespace_id
        or request.expected_revision != row.revision - 1
        or request.execution.execution_epoch != row.execution_epoch
        or request.execution.execution_manifest_sha256 != row.execution_manifest_sha256
        or request.execution.authority_incarnation != row.authority_incarnation
        or request.execution.writer_epoch != row.writer_epoch
        or request.execution.execution_state != "active"
        or projection.operation_id != row.operation_id
        or projection.subject_id != row.subject_id
        or projection.subject_incarnation != row.subject_incarnation
        or projection.owner_id != row.owner_id
        or projection.configuration_generation != row.configuration_generation
        or projection.deployment_generation != row.deployment_generation
        or projection.demand_reporter_incarnation != row.reporter_incarnation
        or result.replayed
        or result.revision != row.revision
        or result.head_sha256 != row.head_sha256
        or member.revision != row.revision
        or configuration.subject_id != row.subject_id
        or configuration.subject_incarnation != row.subject_incarnation
        or member.owner_id != row.owner_id
        or configuration.configuration_generation != row.configuration_generation
        or configuration.deployment_generation != row.deployment_generation
        or configuration.candidate_generation != projection.candidate_generation
        or configuration.demand_reporter_incarnation != row.reporter_incarnation
        or configuration.display_name != f"dev-{projection.environment_name}"
        or configuration.account_id != f"dev-owner-{projection.owner_id.hex}"
        or configuration.min_slots
        != (0 if projection.operation_kind == "destroy" else projection.min_slots)
        or configuration.max_slots
        != (0 if projection.operation_kind == "destroy" else projection.max_slots)
        or configuration.lifecycle_state
        != ("disabled" if projection.operation_kind == "destroy" else "active")
        or request.acknowledgement != member.acknowledgement
        or not _acknowledgement_matches(member.acknowledgement, configuration)
        or member.acknowledgement.candidate.algorithm != "source-sha256"
        or member.acknowledgement.candidate.identity != projection.candidate_sha256
        or member.acknowledgement.candidate.publication_sha256
        != projection.candidate_publication_sha256
        or member.acknowledgement.protected_admission_sha256
        != projection.protected_admission_sha256
    ):
        raise ConfigurationConflictError("personal membership event binding changed")
    return result


def _acknowledgement_matches(
    acknowledgement: SubjectExecutionAcknowledgementV2,
    subject: SubjectConfigurationV1,
) -> bool:
    return bool(
        acknowledgement.subject_id == subject.subject_id
        and acknowledgement.subject_incarnation == subject.subject_incarnation
        and acknowledgement.configuration_generation == subject.configuration_generation
        and acknowledgement.deployment_generation == subject.deployment_generation
        and acknowledgement.reporter_incarnation == subject.demand_reporter_incarnation
    )


def _validated_event_results(
    rows: Sequence[CapacityPersonalMembershipEvent],
    epoch: CapacityExecutionEpoch,
) -> tuple[PersonalApplicationMembershipResultV1, ...]:
    try:
        preparation = parse_execution_preparation(json.dumps(epoch.manifest_payload))
    except ValueError as exc:
        raise ConfigurationConflictError("personal membership execution is invalid") from exc
    if not isinstance(preparation, ExecutionPreparationV3):
        raise ConfigurationConflictError("execution does not delegate personal membership")
    if not rows:
        return ()

    expected_previous = _ZERO_DIGEST
    first = rows[0]
    results: list[PersonalApplicationMembershipResultV1] = []
    for expected_revision, row in enumerate(rows, start=1):
        result = _event_result(row)
        request = PersonalApplicationMembershipMutationV1.model_validate_json(
            json.dumps(row.request_payload, sort_keys=True, separators=(",", ":"))
        )
        if (
            row.execution_epoch != epoch.execution_epoch
            or row.execution_manifest_sha256 != epoch.execution_manifest_sha256
            or row.authority_incarnation != epoch.authority_incarnation
            or row.execution_manifest_sha256 != first.execution_manifest_sha256
            or row.authority_incarnation != first.authority_incarnation
            or row.writer_epoch != first.writer_epoch
            or row.namespace_id != first.namespace_id
            or row.namespace_id != preparation.personal_membership.namespace_id
            or request.projection.expected_configuration_epoch != epoch.configuration_epoch
            or row.revision != expected_revision
            or row.previous_sha256 != expected_previous
            or row.head_sha256
            != _head_digest(
                actor=row.actor,
                execution_epoch=row.execution_epoch,
                idempotency_key=row.idempotency_key,
                operation_id=row.operation_id,
                previous_sha256=row.previous_sha256,
                request_digest=row.request_digest,
                request_payload=row.request_payload,
                member=result.member,
                revision=row.revision,
            )
        ):
            raise ConfigurationConflictError("personal membership event chain changed")
        expected_previous = row.head_sha256
        results.append(result)
    return tuple(results)


async def _validated_membership_history(
    session: AsyncSession,
    rows: Sequence[CapacityPersonalMembershipEvent],
    epoch: CapacityExecutionEpoch,
) -> tuple[PersonalApplicationMembershipResultV1, ...]:
    """Authenticate recreation certificates against history and release facts."""

    from loom_capacity_manager.membership_release import predecessor_release_sha256

    results = _validated_event_results(rows, epoch)
    if not results:
        return results
    configuration = await session.get(CapacityConfigurationEpoch, epoch.configuration_epoch)
    if configuration is None:
        raise ConfigurationConflictError("personal membership origin configuration is absent")
    origins = {
        reference.subject_id: reference
        for reference in (
            _parse_contract(ConfigurationGenerationRefV1, payload)
            for payload in configuration.subject_generation_manifest
        )
    }
    previous: dict[UUID, tuple[CapacityPersonalMembershipEvent, PersonalApplicationMemberV1]] = {}
    used_incarnations = {reference.subject_incarnation for reference in origins.values()}
    for row, result in zip(rows, results, strict=True):
        member = result.member
        subject = member.configuration
        origin = origins.setdefault(subject.subject_id, _subject_reference(subject))
        prior = previous.get(subject.subject_id)
        evidence = member.reincarnation
        if prior is None:
            if evidence is not None:
                raise ConfigurationConflictError("reincarnation predecessor membership is absent")
        else:
            previous_row, old_member = prior
            old = old_member.configuration
            if (
                member.owner_id != old_member.owner_id
                or subject.display_name != old.display_name
                or subject.configuration_generation <= old.configuration_generation
            ):
                raise ConfigurationConflictError("personal membership historical identity changed")
            if subject.subject_incarnation != old.subject_incarnation:
                if subject.subject_incarnation in used_incarnations:
                    raise ConfigurationConflictError("reincarnation identity was already used")
                if (
                    evidence is None
                    or evidence.origin != origin
                    or evidence.predecessor != old
                    or evidence.predecessor_revision != previous_row.revision
                    or evidence.predecessor_head_sha256 != previous_row.head_sha256
                    or evidence.admission_revision != row.revision
                    or evidence.namespace_id != row.namespace_id
                    or evidence.execution_manifest_sha256 != epoch.execution_manifest_sha256
                    or row.request_payload["projection"]["operation_kind"] != "create"
                    or evidence.release_set_sha256 != await predecessor_release_sha256(session, old)
                ):
                    raise ConfigurationConflictError("personal reincarnation release chain changed")
            elif evidence != old_member.reincarnation or old.lifecycle_state == "disabled":
                raise ConfigurationConflictError(
                    "personal reincarnation evidence was replaced or dropped"
                )
        previous[subject.subject_id] = (row, member)
        used_incarnations.add(subject.subject_incarnation)
    return results


class CapacityMembershipStore:
    """Append personal membership without rewriting global execution authority."""

    def __init__(self, management: CapacityManagementStore) -> None:
        self._management = management

    async def checkpoint(
        self, session: AsyncSession, *, actor: str
    ) -> PersonalMembershipCheckpointV1:
        """Read authority and verified membership in one consistent transaction."""

        async with _write_transaction(session):
            authority = (
                await session.execute(
                    select(CapacityAuthorityState)
                    .where(CapacityAuthorityState.singleton_id == 1)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if authority is None or authority.execution_state != "active":
                raise ExecutionConflictError("personal membership authority is unavailable")
            epoch = (
                await session.execute(
                    select(CapacityExecutionEpoch)
                    .where(CapacityExecutionEpoch.execution_epoch == authority.execution_epoch)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if epoch is None:
                raise ExecutionConflictError("personal membership execution is unavailable")
            preparation = self._management._execution_preparation_from_row(epoch)
            if (
                not isinstance(preparation, ExecutionPreparationV3)
                or preparation.personal_membership.management_principal_id != actor
            ):
                raise ExecutionConflictError("execution does not delegate personal membership")
            current = self._management._execution_context(authority, epoch)
            if not isinstance(current, ExecutionAuthorityV2) or current.execution_state != "active":
                raise ExecutionConflictError("personal membership execution fence changed")
            await self._management.load_allocation_input(
                session,
                WriterFence(
                    authority_incarnation=current.authority_incarnation,
                    writer_epoch=current.writer_epoch,
                ),
            )
            snapshot = await self.snapshot(session, epoch)
            return PersonalMembershipCheckpointV1(
                execution=current,
                namespace_id=snapshot.namespace_id,
                revision=snapshot.revision,
                head_sha256=snapshot.head_sha256,
            )

    async def apply(
        self,
        session: AsyncSession,
        request: PersonalApplicationMembershipMutationV1,
        *,
        actor: str,
        idempotency_key: UUID,
    ) -> PersonalApplicationMembershipResultV1:
        request_digest = canonical_digest(request)
        request_payload = cast(
            dict[str, object], request.model_dump(mode="json", exclude_none=False)
        )
        projection = request.projection
        async with _write_transaction(session):
            authority = (
                await session.execute(
                    select(CapacityAuthorityState)
                    .where(CapacityAuthorityState.singleton_id == 1)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if authority is None:
                raise ExecutionConflictError("personal membership authority is unavailable")
            epoch = (
                await session.execute(
                    select(CapacityExecutionEpoch)
                    .where(CapacityExecutionEpoch.execution_epoch == authority.execution_epoch)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if epoch is None:
                raise ExecutionConflictError("personal membership execution is unavailable")
            try:
                preparation = parse_execution_preparation(
                    json.dumps(epoch.manifest_payload, sort_keys=True, separators=(",", ":"))
                )
            except ValueError as exc:
                raise ExecutionConflictError("personal membership execution is invalid") from exc
            if not isinstance(preparation, ExecutionPreparationV3):
                raise ExecutionConflictError("execution does not delegate personal membership")
            policy = preparation.personal_membership
            current = self._management._execution_context(authority, epoch)
            if (
                not isinstance(current, ExecutionAuthorityV2)
                or current.execution_state != "active"
                or request.execution != current
                or request.namespace_id != policy.namespace_id
                or actor != policy.management_principal_id
                or projection.expected_configuration_epoch != epoch.configuration_epoch
            ):
                raise ExecutionConflictError("personal membership execution fence changed")

            replays = (
                (
                    await session.execute(
                        select(CapacityPersonalMembershipEvent)
                        .where(
                            or_(
                                CapacityPersonalMembershipEvent.operation_id
                                == projection.operation_id,
                                CapacityPersonalMembershipEvent.idempotency_key == idempotency_key,
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
                    "personal membership identities belong to different requests"
                )
            if replays:
                replay = replays[0]
                if (
                    replay.execution_epoch != epoch.execution_epoch
                    or replay.execution_manifest_sha256 != epoch.execution_manifest_sha256
                    or replay.operation_id != projection.operation_id
                    or replay.idempotency_key != idempotency_key
                    or replay.actor != actor
                    or replay.request_digest != request_digest
                    or replay.request_payload != request_payload
                ):
                    raise IdempotencyConflictError(
                        "personal membership identity was reused with different input"
                    )
                await self.snapshot(session, epoch)
                return _event_result(replay).model_copy(update={"replayed": True})

            verified_input = await self._management.load_allocation_input(
                session,
                WriterFence(
                    authority_incarnation=current.authority_incarnation,
                    writer_epoch=current.writer_epoch,
                ),
            )

            prior_rows = (
                (
                    await session.execute(
                        select(CapacityPersonalMembershipEvent)
                        .where(
                            CapacityPersonalMembershipEvent.execution_epoch == epoch.execution_epoch
                        )
                        .order_by(CapacityPersonalMembershipEvent.revision)
                        .with_for_update()
                    )
                )
                .scalars()
                .all()
            )
            current_revision = 0 if not prior_rows else prior_rows[-1].revision
            previous_sha256 = _ZERO_DIGEST if not prior_rows else prior_rows[-1].head_sha256
            if request.expected_revision != current_revision:
                raise PersonalMembershipRevisionConflictError(
                    "personal membership revision is stale"
                )
            latest_members = {
                result.member.configuration.subject_id: result.member
                for result in (_event_result(row) for row in prior_rows)
            }

            fleet_row = (
                await session.execute(
                    select(CapacityConfigGeneration).where(
                        CapacityConfigGeneration.scope == "fleet",
                        CapacityConfigGeneration.digest == epoch.fleet_digest,
                    )
                )
            ).scalar_one_or_none()
            if fleet_row is None:
                raise ConfigurationConflictError("personal membership fleet is unavailable")
            fleet = _parse_contract(FleetManifestV1, fleet_row.payload)
            if fleet.development_subject_template is None or (
                policy.development_template_sha256
                != canonical_digest(fleet.development_subject_template)
            ):
                raise ConfigurationConflictError("personal membership template changed")

            materialized_rows = (
                (
                    await session.execute(
                        select(CapacitySubject)
                        .where(CapacitySubject.configuration_epoch == epoch.configuration_epoch)
                        .with_for_update()
                    )
                )
                .scalars()
                .all()
            )
            materialized = {
                row.subject_id: _parse_contract(SubjectConfigurationV1, row.payload)
                for row in materialized_rows
            }
            existing = latest_members.get(projection.subject_id)
            recreating = False
            base_ids = {item.subject_id for item in preparation.subject_acknowledgements}
            if existing is None and projection.subject_id in set(policy.managed_base_subject_ids):
                original = materialized.get(projection.subject_id)
                original_acknowledgement = next(
                    (
                        item
                        for item in preparation.subject_acknowledgements
                        if item.subject_id == projection.subject_id
                    ),
                    None,
                )
                prefix = "dev-owner-"
                encoded_owner = "" if original is None else original.account_id.removeprefix(prefix)
                try:
                    original_owner = UUID(hex=encoded_owner)
                except ValueError:
                    original_owner = UUID(int=0)
                if (
                    original is None
                    or original_acknowledgement is None
                    or not original.account_id.startswith(prefix)
                    or original.account_id != f"{prefix}{original_owner.hex}"
                    or original_owner.int == 0
                ):
                    raise ConfigurationConflictError("managed base personal application is invalid")
                existing = PersonalApplicationMemberV1(
                    revision=1,
                    owner_id=original_owner,
                    configuration=original,
                    acknowledgement=original_acknowledgement,
                )
            display_name = f"dev-{projection.environment_name}"
            if any(
                value.display_name == display_name and subject_id != projection.subject_id
                for subject_id, value in materialized.items()
            ):
                raise ConfigurationConflictError(
                    "dynamic development environment name is already active"
                )
            if existing is None:
                if projection.operation_kind != "create":
                    raise ConfigurationConflictError("personal application membership is absent")
                configured_identity = (
                    await session.execute(
                        select(CapacityConfigGeneration.id)
                        .where(
                            CapacityConfigGeneration.scope == "subject",
                            or_(
                                CapacityConfigGeneration.subject_id == projection.subject_id,
                                CapacityConfigGeneration.subject_incarnation
                                == projection.subject_incarnation,
                            ),
                        )
                        .limit(1)
                    )
                ).scalar_one_or_none()
                historical_identity = (
                    await session.execute(
                        select(CapacityPersonalMembershipEvent.id)
                        .where(
                            or_(
                                CapacityPersonalMembershipEvent.subject_id == projection.subject_id,
                                CapacityPersonalMembershipEvent.subject_incarnation
                                == projection.subject_incarnation,
                            )
                        )
                        .limit(1)
                    )
                ).scalar_one_or_none()
                if (
                    projection.subject_id in base_ids
                    or configured_identity is not None
                    or historical_identity is not None
                    or any(
                        row.subject_id == projection.subject_id
                        or row.subject_incarnation == projection.subject_incarnation
                        for row in materialized_rows
                    )
                ):
                    raise ConfigurationConflictError(
                        "personal application identity was already used"
                    )
                if (
                    len(
                        set(policy.managed_base_subject_ids)
                        | set(latest_members)
                        | {projection.subject_id}
                    )
                    > policy.max_subjects
                ):
                    raise ConfigurationConflictError(
                        "personal membership exceeds its subject bound"
                    )
            else:
                old = existing.configuration
                recreating = (
                    old.lifecycle_state == "disabled" and projection.operation_kind == "create"
                )
                if old.lifecycle_state == "disabled" and not recreating:
                    raise ConfigurationConflictError(
                        "disabled personal application cannot be reactivated"
                    )
                if projection.operation_kind == "create" and not recreating:
                    raise ConfigurationConflictError(
                        "personal application membership already exists"
                    )
                if (
                    (projection.subject_incarnation != old.subject_incarnation and not recreating)
                    or projection.owner_id != existing.owner_id
                    or display_name != old.display_name
                ):
                    raise ConfigurationConflictError("personal application identity changed")
                if projection.configuration_generation <= old.configuration_generation:
                    raise ConfigurationConflictError(
                        "personal application configuration generation is not monotonic"
                    )
                if recreating:
                    if (
                        projection.subject_incarnation == old.subject_incarnation
                        or projection.candidate_generation != 1
                        or projection.deployment_generation != 1
                    ):
                        raise ConfigurationConflictError(
                            "reincarnation requires a fresh deployment identity"
                        )
                    used_configuration = (
                        await session.execute(
                            select(CapacityConfigGeneration.id)
                            .where(
                                CapacityConfigGeneration.subject_incarnation
                                == projection.subject_incarnation,
                            )
                            .limit(1)
                        )
                    ).scalar_one_or_none()
                    used_membership = (
                        await session.execute(
                            select(CapacityPersonalMembershipEvent.id)
                            .where(
                                CapacityPersonalMembershipEvent.subject_incarnation
                                == projection.subject_incarnation,
                            )
                            .limit(1)
                        )
                    ).scalar_one_or_none()
                    if (
                        used_configuration is not None
                        or used_membership is not None
                        or any(
                            row.subject_incarnation == projection.subject_incarnation
                            for row in materialized_rows
                        )
                    ):
                        raise ConfigurationConflictError("reincarnation identity was already used")
                elif projection.operation_kind == "update":
                    if projection.deployment_generation <= old.deployment_generation:
                        raise ConfigurationConflictError(
                            "personal application deployment generation is not monotonic"
                        )
                    if projection.demand_reporter_incarnation == old.demand_reporter_incarnation:
                        raise ConfigurationConflictError(
                            "a new deployment must rotate the demand reporter incarnation"
                        )
                elif (
                    projection.deployment_generation != old.deployment_generation
                    or projection.candidate_generation != old.candidate_generation
                    or projection.demand_reporter_incarnation != old.demand_reporter_incarnation
                ):
                    raise ConfigurationConflictError(
                        "non-deployment membership must retain deployment evidence"
                    )

            account = _derive_owner_account(fleet, projection.owner_id)
            subject = _derive_development_subject(fleet, projection)
            acknowledgement = request.acknowledgement
            if not _acknowledgement_matches(acknowledgement, subject) or (
                acknowledgement.candidate.algorithm != "source-sha256"
                or acknowledgement.candidate.identity != projection.candidate_sha256
                or acknowledgement.candidate.publication_sha256
                != projection.candidate_publication_sha256
                or acknowledgement.protected_admission_sha256
                != projection.protected_admission_sha256
            ):
                raise ConfigurationConflictError(
                    "personal application execution acknowledgement changed"
                )

            if existing is not None and projection.operation_kind in {"capacity", "destroy"}:
                await self._require_retained_evidence(session, projection, existing.configuration)
            else:
                await self._require_unused_reporter(session, projection)

            next_subjects = dict(materialized)
            next_subjects[subject.subject_id] = subject
            owner_subjects = [
                value
                for value in next_subjects.values()
                if value.account_id == account.account_id and value.lifecycle_state != "disabled"
            ]
            if len(owner_subjects) > account.max_live_subjects:
                raise ConfigurationConflictError("development owner exceeds max_live_subjects")
            if sum(value.min_slots for value in owner_subjects) > account.min_reservation_slots:
                raise ConfigurationConflictError(
                    "development owner minimum aggregate exceeds its reservation"
                )
            fleet_account_ids = {value.account_id for value in fleet.account_policies}
            derived_accounts: dict[str, AccountPolicyV1] = {}
            for value in verified_input.effective_account_policies:
                if value.account_id in fleet_account_ids:
                    continue
                if value.kind != "owner" or value.owner_id is None:
                    raise ConfigurationConflictError("personal base owner account is invalid")
                expected_account = _derive_owner_account(fleet, value.owner_id)
                if value != expected_account:
                    raise ConfigurationConflictError("personal base owner account changed")
                derived_accounts[value.account_id] = expected_account
            derived_accounts[account.account_id] = account
            self._management._validate_activation(
                fleet,
                tuple(next_subjects.values()),
                tuple(derived_accounts.values()),
            )

            reincarnation = None if existing is None else existing.reincarnation
            if recreating:
                from loom_capacity_manager.membership_release import predecessor_release_sha256

                assert existing is not None
                predecessor_row = next(
                    (row for row in reversed(prior_rows) if row.subject_id == subject.subject_id),
                    None,
                )
                if predecessor_row is None:
                    raise ConfigurationConflictError(
                        "reincarnation predecessor membership is absent"
                    )
                origin: ConfigurationGenerationRefV1 | None
                if existing.reincarnation is not None:
                    origin = existing.reincarnation.origin
                else:
                    origin = next(
                        (
                            reference
                            for reference in verified_input.configuration.subjects
                            if reference.subject_id == subject.subject_id
                        ),
                        None,
                    )
                    if origin is None:
                        origin = _subject_reference(
                            next(
                                _event_result(row).member.configuration
                                for row in prior_rows
                                if row.subject_id == subject.subject_id
                            )
                        )
                reincarnation = PersonalReincarnationEvidenceV1(
                    namespace_id=policy.namespace_id,
                    execution_manifest_sha256=epoch.execution_manifest_sha256,
                    origin=origin,
                    predecessor=existing.configuration,
                    predecessor_revision=predecessor_row.revision,
                    predecessor_head_sha256=predecessor_row.head_sha256,
                    admission_revision=current_revision + 1,
                    successor_incarnation=subject.subject_incarnation,
                    release_set_sha256=await predecessor_release_sha256(
                        session, existing.configuration
                    ),
                )
            await self._persist_generation_evidence(session, projection, subject, existing)
            await self._materialize_subject(
                session,
                epoch.configuration_epoch,
                subject,
                account,
                materialized_rows,
            )
            revision = current_revision + 1
            member = PersonalApplicationMemberV1(
                revision=revision,
                owner_id=projection.owner_id,
                configuration=subject,
                acknowledgement=acknowledgement,
                reincarnation=reincarnation,
            )
            head_sha256 = _head_digest(
                actor=actor,
                execution_epoch=epoch.execution_epoch,
                idempotency_key=idempotency_key,
                operation_id=projection.operation_id,
                previous_sha256=previous_sha256,
                request_digest=request_digest,
                request_payload=request_payload,
                member=member,
                revision=revision,
            )
            result = PersonalApplicationMembershipResultV1(
                revision=revision,
                head_sha256=head_sha256,
                member=member,
                replayed=False,
            )
            session.add(
                CapacityPersonalMembershipEvent(
                    execution_epoch=epoch.execution_epoch,
                    execution_manifest_sha256=epoch.execution_manifest_sha256,
                    authority_incarnation=authority.authority_incarnation,
                    writer_epoch=authority.writer_epoch,
                    namespace_id=policy.namespace_id,
                    revision=revision,
                    previous_sha256=previous_sha256,
                    head_sha256=head_sha256,
                    actor=actor,
                    idempotency_key=idempotency_key,
                    operation_id=projection.operation_id,
                    request_digest=request_digest,
                    request_payload=request_payload,
                    subject_id=subject.subject_id,
                    subject_incarnation=subject.subject_incarnation,
                    owner_id=projection.owner_id,
                    configuration_generation=subject.configuration_generation,
                    deployment_generation=subject.deployment_generation,
                    reporter_incarnation=subject.demand_reporter_incarnation,
                    result_payload=result.model_dump(mode="json", exclude_none=False),
                )
            )
            await session.flush()
            return result

    async def snapshot(
        self,
        session: AsyncSession,
        epoch: CapacityExecutionEpoch | int,
        *,
        through_revision: int | None = None,
    ) -> PersonalMembershipSnapshotV1:
        """Read the current head or an exact immutable historical log prefix."""

        if through_revision is not None and (
            type(through_revision) is not int or through_revision < 0
        ):
            raise ConfigurationConflictError("personal membership revision is invalid")
        execution_epoch = epoch if isinstance(epoch, int) else epoch.execution_epoch
        statement = select(CapacityPersonalMembershipEvent).where(
            CapacityPersonalMembershipEvent.execution_epoch == execution_epoch
        )
        if through_revision is not None:
            statement = statement.where(
                CapacityPersonalMembershipEvent.revision <= through_revision
            )
        rows = (
            (await session.execute(statement.order_by(CapacityPersonalMembershipEvent.revision)))
            .scalars()
            .all()
        )
        if (
            through_revision is not None
            and (0 if not rows else rows[-1].revision) != through_revision
        ):
            raise ConfigurationConflictError("personal membership revision is unavailable")
        if not rows:
            epoch_row = (
                epoch
                if isinstance(epoch, CapacityExecutionEpoch)
                else (
                    await session.execute(
                        select(CapacityExecutionEpoch).where(
                            CapacityExecutionEpoch.execution_epoch == execution_epoch
                        )
                    )
                ).scalar_one()
            )
            preparation = parse_execution_preparation(json.dumps(epoch_row.manifest_payload))
            if not isinstance(preparation, ExecutionPreparationV3):
                raise ConfigurationConflictError("execution does not delegate personal membership")
            return PersonalMembershipSnapshotV1(
                namespace_id=preparation.personal_membership.namespace_id,
                revision=0,
                head_sha256=_ZERO_DIGEST,
                members=(),
            )
        epoch_row = (
            epoch
            if isinstance(epoch, CapacityExecutionEpoch)
            else (
                await session.execute(
                    select(CapacityExecutionEpoch).where(
                        CapacityExecutionEpoch.execution_epoch == execution_epoch
                    )
                )
            ).scalar_one()
        )
        latest: dict[UUID, PersonalApplicationMemberV1] = {}
        for row, result in zip(
            rows,
            await _validated_membership_history(session, rows, epoch_row),
            strict=True,
        ):
            latest[row.subject_id] = result.member
        return PersonalMembershipSnapshotV1(
            namespace_id=rows[-1].namespace_id,
            revision=rows[-1].revision,
            head_sha256=rows[-1].head_sha256,
            members=tuple(latest.values()),
        )

    async def verify_snapshot_materialization(
        self,
        session: AsyncSession,
        epoch: CapacityExecutionEpoch,
        snapshot: PersonalMembershipSnapshotV1,
    ) -> None:
        """Verify every latest member's retained execution evidence."""

        rows = (
            (
                await session.execute(
                    select(CapacityPersonalMembershipEvent)
                    .where(CapacityPersonalMembershipEvent.execution_epoch == epoch.execution_epoch)
                    .order_by(CapacityPersonalMembershipEvent.revision.desc())
                )
            )
            .scalars()
            .all()
        )
        latest: dict[UUID, CapacityPersonalMembershipEvent] = {}
        for row in rows:
            latest.setdefault(row.subject_id, row)
        if set(latest) != {member.configuration.subject_id for member in snapshot.members}:
            raise ConfigurationConflictError(
                "personal membership evidence differs from its event log"
            )
        for member in snapshot.members:
            row = latest[member.configuration.subject_id]
            result = _event_result(row)
            if result.member != member:
                raise ConfigurationConflictError(
                    "personal membership evidence differs from its event log"
                )
            request = PersonalApplicationMembershipMutationV1.model_validate_json(
                json.dumps(row.request_payload, sort_keys=True, separators=(",", ":"))
            )
            await self._require_retained_evidence(
                session,
                request.projection,
                member.configuration,
            )

    async def _require_unused_reporter(self, session, projection) -> None:  # type: ignore[no-untyped-def]
        conflict = (
            await session.execute(
                select(CapacityDemandReporter)
                .where(
                    or_(
                        CapacityDemandReporter.token_sha256
                        == projection.demand_reporter_token_sha256,
                        CapacityDemandReporter.reporter_incarnation
                        == projection.demand_reporter_incarnation,
                    )
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        if conflict is not None:
            raise ConfigurationConflictError(
                "dynamic development demand reporter identity was already used"
            )

    async def _require_retained_evidence(
        self,
        session: AsyncSession,
        projection: DynamicDevelopmentSubjectProjectionV1,
        existing: SubjectConfigurationV1,
    ) -> None:
        candidate = (
            await session.execute(
                select(CapacityCandidate).where(
                    CapacityCandidate.subject_id == existing.subject_id,
                    CapacityCandidate.subject_incarnation == existing.subject_incarnation,
                    CapacityCandidate.candidate_generation == existing.candidate_generation,
                )
            )
        ).scalar_one_or_none()
        deployment = (
            await session.execute(
                select(CapacityDeploymentGeneration).where(
                    CapacityDeploymentGeneration.subject_id == existing.subject_id,
                    CapacityDeploymentGeneration.subject_incarnation
                    == existing.subject_incarnation,
                    CapacityDeploymentGeneration.deployment_generation
                    == existing.deployment_generation,
                )
            )
        ).scalar_one_or_none()
        reporter = (
            await session.execute(
                select(CapacityDemandReporter).where(
                    CapacityDemandReporter.subject_id == existing.subject_id,
                    CapacityDemandReporter.subject_incarnation == existing.subject_incarnation,
                    CapacityDemandReporter.reporter_incarnation
                    == existing.demand_reporter_incarnation,
                    CapacityDemandReporter.state == "current",
                )
            )
        ).scalar_one_or_none()
        expected_cutover = {
            "local_activation_sha256": projection.local_activation_sha256,
            "candidate_publication_sha256": projection.candidate_publication_sha256,
            "protected_admission_sha256": projection.protected_admission_sha256,
            "capacity_agent_installation_sha256": projection.capacity_agent_installation_sha256,
        }
        expected_profiles = [
            profile.model_dump(mode="json", exclude_none=False) for profile in existing.profiles
        ]
        if (
            candidate is None
            or deployment is None
            or reporter is None
            or candidate.candidate_digest != projection.candidate_sha256
            or candidate.candidate_identity_algorithm != "source-sha256"
            or candidate.candidate_identity != projection.candidate_sha256
            or candidate.source_payload
            != {"publication_sha256": projection.candidate_publication_sha256}
            or candidate.artifact_payload != {"candidate_sha256": projection.candidate_sha256}
            or candidate.architecture_payload
            != {
                "supported_architectures": list(projection.supported_architectures),
                "supported_pool_ids": list(projection.supported_pool_ids),
            }
            or candidate.launcher_payload
            != {"local_activation_sha256": projection.local_activation_sha256}
            or candidate.protocol_payload != projection.protocol_versions
            or deployment.candidate_digest != projection.candidate_sha256
            or deployment.required_profiles != expected_profiles
            or deployment.readiness_state != "ready"
            or deployment.lifecycle_state != "active"
            or deployment.cutover_payload != expected_cutover
            or reporter.token_sha256 != projection.demand_reporter_token_sha256
            or reporter.configuration_generation != existing.configuration_generation
            or reporter.deployment_generation != existing.deployment_generation
        ):
            raise ConfigurationConflictError(
                "non-deployment membership cannot change candidate or reporter evidence"
            )
        await self._management._require_preserved_subject_bindings(session, existing)

    async def _persist_generation_evidence(
        self,
        session: AsyncSession,
        projection: DynamicDevelopmentSubjectProjectionV1,
        subject: SubjectConfigurationV1,
        existing: PersonalApplicationMemberV1 | None,
    ) -> None:
        if projection.operation_kind in {"create", "update"}:
            session.add(
                CapacityCandidate(
                    subject_id=subject.subject_id,
                    subject_incarnation=subject.subject_incarnation,
                    candidate_generation=subject.candidate_generation,
                    candidate_digest=projection.candidate_sha256,
                    candidate_identity_algorithm="source-sha256",
                    candidate_identity=projection.candidate_sha256,
                    source_payload={"publication_sha256": projection.candidate_publication_sha256},
                    artifact_payload={"candidate_sha256": projection.candidate_sha256},
                    architecture_payload={
                        "supported_architectures": list(projection.supported_architectures),
                        "supported_pool_ids": list(projection.supported_pool_ids),
                    },
                    launcher_payload={
                        "local_activation_sha256": projection.local_activation_sha256
                    },
                    attestation_payload={
                        "operation_id": str(projection.operation_id),
                        "operation_epoch": projection.operation_epoch,
                        "protected_admission_sha256": projection.protected_admission_sha256,
                        "capacity_agent_installation_sha256": (
                            projection.capacity_agent_installation_sha256
                        ),
                    },
                    protocol_payload=projection.protocol_versions,
                )
            )
            session.add(
                CapacityDeploymentGeneration(
                    subject_id=subject.subject_id,
                    subject_incarnation=subject.subject_incarnation,
                    deployment_generation=subject.deployment_generation,
                    candidate_digest=projection.candidate_sha256,
                    required_profiles=[
                        profile.model_dump(mode="json", exclude_none=False)
                        for profile in subject.profiles
                    ],
                    readiness_state="ready",
                    lifecycle_state="active",
                    cutover_payload={
                        "local_activation_sha256": projection.local_activation_sha256,
                        "candidate_publication_sha256": (projection.candidate_publication_sha256),
                        "protected_admission_sha256": projection.protected_admission_sha256,
                        "capacity_agent_installation_sha256": (
                            projection.capacity_agent_installation_sha256
                        ),
                    },
                )
            )
            await session.flush()
            for profile in subject.profiles:
                await self._management._persist_worker_profile(session, subject, profile)
        await self._management._register_demand_reporter(
            session,
            subject,
            token_sha256=(
                None
                if projection.operation_kind == "destroy"
                else projection.demand_reporter_token_sha256
            ),
        )

    async def _materialize_subject(
        self,
        session: AsyncSession,
        configuration_epoch: int,
        subject: SubjectConfigurationV1,
        account: AccountPolicyV1,
        rows: Sequence[CapacitySubject],
    ) -> None:
        row = next((item for item in rows if item.subject_id == subject.subject_id), None)
        values = dict(
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
        if row is None:
            session.add(
                CapacitySubject(
                    configuration_epoch=configuration_epoch,
                    subject_id=subject.subject_id,
                    **values,
                )
            )
        else:
            for name, value in values.items():
                setattr(row, name, value)
        account_row = (
            await session.execute(
                select(CapacityAccountPolicy).where(
                    CapacityAccountPolicy.configuration_epoch == configuration_epoch,
                    CapacityAccountPolicy.account_id == account.account_id,
                )
            )
        ).scalar_one_or_none()
        account_values = dict(
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
        if account_row is None:
            session.add(
                CapacityAccountPolicy(
                    configuration_epoch=configuration_epoch,
                    account_id=account.account_id,
                    **account_values,
                )
            )
        else:
            for name, value in account_values.items():
                setattr(account_row, name, value)


async def resolve_subject_acknowledgement(
    session: AsyncSession,
    epoch: CapacityExecutionEpoch,
    *,
    subject_id: UUID,
    subject_incarnation: UUID,
    configuration_generation: int,
    deployment_generation: int,
    reporter_incarnation: UUID,
) -> SubjectExecutionAcknowledgementV2:
    """Resolve only the exact immutable base or delegated generation evidence."""

    preparation = parse_execution_preparation(json.dumps(epoch.manifest_payload))
    for acknowledgement in preparation.subject_acknowledgements:
        if (
            acknowledgement.subject_id == subject_id
            and acknowledgement.subject_incarnation == subject_incarnation
            and acknowledgement.configuration_generation == configuration_generation
            and acknowledgement.deployment_generation == deployment_generation
            and acknowledgement.reporter_incarnation == reporter_incarnation
        ):
            return acknowledgement
    rows = (
        (
            await session.execute(
                select(CapacityPersonalMembershipEvent)
                .where(
                    CapacityPersonalMembershipEvent.execution_epoch == epoch.execution_epoch,
                )
                .order_by(CapacityPersonalMembershipEvent.revision)
            )
        )
        .scalars()
        .all()
    )
    results = await _validated_membership_history(session, rows, epoch)
    for row, result in reversed(tuple(zip(rows, results, strict=True))):
        if (
            row.subject_id == subject_id
            and row.subject_incarnation == subject_incarnation
            and row.configuration_generation == configuration_generation
            and row.deployment_generation == deployment_generation
            and row.reporter_incarnation == reporter_incarnation
        ):
            return result.member.acknowledgement
    raise ConfigurationConflictError("subject execution acknowledgement is unavailable")


__all__ = [
    "CapacityMembershipStore",
    "PersonalMembershipRevisionConflictError",
    "resolve_subject_acknowledgement",
]
