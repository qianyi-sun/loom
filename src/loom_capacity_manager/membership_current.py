"""Resolve one exact current subject under active execution authority."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.contracts import (
    ConfigurationGenerationRefV1,
    DynamicDevelopmentSubjectProjectionV1,
    SubjectConfigurationV1,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import SubjectExecutionAcknowledgementV2
from loom_capacity_manager.membership_contracts import ExecutionPreparationV3
from loom_capacity_manager.membership_store import CapacityMembershipStore
from loom_capacity_manager.models import (
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
    _parse_contract,
    _subject_scalars_match,
)


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


async def _immutable_base_subject(
    session: AsyncSession,
    configuration: CapacityConfigurationEpoch,
    *,
    subject_id: UUID,
) -> SubjectConfigurationV1 | None:
    references = tuple(
        _parse_contract(ConfigurationGenerationRefV1, payload)
        for payload in configuration.subject_generation_manifest
    )
    reference = next(
        (item for item in references if item.scope == "subject" and item.subject_id == subject_id),
        None,
    )
    if reference is None:
        return None
    row = (
        await session.execute(
            select(CapacityConfigGeneration).where(
                CapacityConfigGeneration.scope == "subject",
                CapacityConfigGeneration.subject_id == reference.subject_id,
                CapacityConfigGeneration.subject_incarnation == reference.subject_incarnation,
                CapacityConfigGeneration.scope_generation == reference.generation,
                CapacityConfigGeneration.digest == reference.digest,
                CapacityConfigGeneration.state == "active",
            )
        )
    ).scalar_one_or_none()
    if row is None:
        raise ConfigurationConflictError("current base subject generation is unavailable")
    subject = _parse_contract(SubjectConfigurationV1, row.payload)
    if canonical_digest(subject) != reference.digest:
        raise ConfigurationConflictError("current base subject generation changed")
    return subject


async def _require_current_evidence(
    session: AsyncSession,
    management: CapacityManagementStore,
    subject: SubjectConfigurationV1,
    acknowledgement: SubjectExecutionAcknowledgementV2,
    *,
    require_deployment: bool,
) -> None:
    if not _acknowledgement_matches(acknowledgement, subject):
        raise ConfigurationConflictError("current subject acknowledgement changed")
    await management._require_preserved_subject_bindings(session, subject)
    candidate = (
        await session.execute(
            select(CapacityCandidate).where(
                CapacityCandidate.subject_id == subject.subject_id,
                CapacityCandidate.subject_incarnation == subject.subject_incarnation,
                CapacityCandidate.candidate_generation == subject.candidate_generation,
            )
        )
    ).scalar_one_or_none()
    if candidate is None or (
        candidate.candidate_identity_algorithm != acknowledgement.candidate.algorithm
        or candidate.candidate_identity != acknowledgement.candidate.identity
        or candidate.source_payload.get("publication_sha256")
        != acknowledgement.candidate.publication_sha256
    ):
        raise ConfigurationConflictError("current subject candidate binding changed")
    reporter = (
        await session.execute(
            select(CapacityDemandReporter).where(
                CapacityDemandReporter.subject_id == subject.subject_id,
                CapacityDemandReporter.subject_incarnation == subject.subject_incarnation,
                CapacityDemandReporter.reporter_incarnation == subject.demand_reporter_incarnation,
                CapacityDemandReporter.state == "current",
            )
        )
    ).scalar_one_or_none()
    if reporter is None or (
        reporter.configuration_generation != subject.configuration_generation
        or reporter.deployment_generation != subject.deployment_generation
    ):
        raise ConfigurationConflictError("current subject reporter binding changed")
    deployment = (
        await session.execute(
            select(CapacityDeploymentGeneration).where(
                CapacityDeploymentGeneration.subject_id == subject.subject_id,
                CapacityDeploymentGeneration.subject_incarnation == subject.subject_incarnation,
                CapacityDeploymentGeneration.deployment_generation == subject.deployment_generation,
            )
        )
    ).scalar_one_or_none()
    if deployment is None:
        if require_deployment:
            raise ConfigurationConflictError("current subject deployment binding is missing")
        return
    if (
        deployment.candidate_digest != candidate.candidate_digest
        or deployment.required_profiles
        != [profile.model_dump(mode="json", exclude_none=False) for profile in subject.profiles]
        or deployment.readiness_state != "ready"
        or deployment.lifecycle_state != "active"
    ):
        raise ConfigurationConflictError("current subject deployment binding changed")


async def resolve_current_subject(
    session: AsyncSession,
    epoch: CapacityExecutionEpoch,
    *,
    subject_id: UUID,
    allow_disabled: bool = False,
) -> tuple[SubjectConfigurationV1, SubjectExecutionAcknowledgementV2]:
    """Authenticate one active current subject without pinning another owner's head."""

    if epoch.state != "active":
        raise ConfigurationConflictError("current subject execution is not active")
    management = CapacityManagementStore()
    try:
        preparation = management._execution_preparation_from_row(epoch)
    except ExecutionConflictError as exc:
        raise ConfigurationConflictError("current subject execution manifest is invalid") from exc
    configuration = await session.get(CapacityConfigurationEpoch, epoch.configuration_epoch)
    if configuration is None or (
        configuration.fleet_generation != epoch.fleet_generation
        or configuration.fleet_digest != epoch.fleet_digest
    ):
        raise ConfigurationConflictError("current subject configuration changed")

    member = None
    if isinstance(preparation, ExecutionPreparationV3):
        snapshot = await CapacityMembershipStore(management).snapshot(session, epoch)
        member = next(
            (item for item in snapshot.members if item.configuration.subject_id == subject_id),
            None,
        )
    if member is not None:
        subject = member.configuration
        acknowledgement = member.acknowledgement
    else:
        base_subject = await _immutable_base_subject(session, configuration, subject_id=subject_id)
        if base_subject is None:
            raise ConfigurationConflictError("current subject is unavailable")
        base_acknowledgement = next(
            (
                item
                for item in preparation.subject_acknowledgements
                if _acknowledgement_matches(item, base_subject)
            ),
            None,
        )
        if base_acknowledgement is None:
            raise ConfigurationConflictError("current subject acknowledgement is unavailable")
        subject = base_subject
        acknowledgement = base_acknowledgement

    if subject.lifecycle_state != "active" and not (
        allow_disabled and subject.lifecycle_state == "disabled"
    ):
        raise ConfigurationConflictError("current subject is not active")
    materialized_rows = (
        (
            await session.execute(
                select(CapacitySubject).where(
                    CapacitySubject.configuration_epoch == epoch.configuration_epoch,
                    CapacitySubject.subject_id == subject.subject_id,
                )
            )
        )
        .scalars()
        .all()
    )
    if len(materialized_rows) != 1:
        raise ConfigurationConflictError("current subject materialization is unavailable")
    materialized = materialized_rows[0]
    try:
        payload = _parse_contract(SubjectConfigurationV1, materialized.payload)
    except ValueError as exc:
        raise ConfigurationConflictError("current subject materialization is invalid") from exc
    if payload != subject or not _subject_scalars_match(materialized, subject):
        raise ConfigurationConflictError("current subject materialization changed")
    await _require_current_evidence(
        session,
        management,
        subject,
        acknowledgement,
        require_deployment=member is not None,
    )
    if member is not None:
        event = await session.scalar(
            select(CapacityPersonalMembershipEvent).where(
                CapacityPersonalMembershipEvent.execution_epoch == epoch.execution_epoch,
                CapacityPersonalMembershipEvent.revision == member.revision,
            )
        )
        if event is None:
            raise ConfigurationConflictError("current subject membership evidence is unavailable")
        projection = _parse_contract(
            DynamicDevelopmentSubjectProjectionV1, event.request_payload["projection"]
        )
        await CapacityMembershipStore(management)._require_retained_evidence(
            session, projection, subject
        )
    return subject, acknowledgement


__all__ = ["resolve_current_subject"]
