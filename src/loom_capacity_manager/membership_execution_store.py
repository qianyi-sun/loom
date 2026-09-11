"""Authenticate an allocation's original subject generation before execution."""

from __future__ import annotations

import json
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.build_membership_contracts import PersonalMembershipSnapshotV2
from loom_capacity_manager.contracts import SubjectConfigurationV1, canonical_digest
from loom_capacity_manager.executable_contracts import (
    ExecutionPreparationV2,
    SubjectExecutionAcknowledgementV2,
)
from loom_capacity_manager.membership_contracts import (
    ExecutionPreparationV3,
    PersonalMembershipSnapshotV1,
)
from loom_capacity_manager.membership_execution import (
    ExecutableEpochV3,
    ExecutableEpochV4,
    parse_executable_epoch,
)
from loom_capacity_manager.membership_store import (
    CapacityMembershipStore,
    resolve_subject_acknowledgement,
)
from loom_capacity_manager.models import (
    CapacityAllocationEpoch,
    CapacityConfigGeneration,
    CapacityConfigurationEpoch,
    CapacityDemandReporter,
    CapacityExecutionEpoch,
)
from loom_capacity_manager.store import (
    CapacityManagementStore,
    ConfigurationConflictError,
    ExecutionConflictError,
)


async def resolve_allocation_subject(
    session: AsyncSession,
    epoch: CapacityExecutionEpoch,
    allocation: CapacityAllocationEpoch,
    *,
    subject_id: UUID,
    require_current: bool,
) -> tuple[SubjectConfigurationV1, SubjectExecutionAcknowledgementV2]:
    """Resolve pinned evidence; only exact current generations may increase work.

    Callers retain their existing authority-first transaction lock and must still
    validate the intent, reporter, pool and operation-specific execution fence.
    Historical resolution authenticates evidence, not permission to launch.
    """

    try:
        return await _resolve_allocation_subject(
            session, epoch, allocation, subject_id=subject_id, require_current=require_current
        )
    except (ValueError, ConfigurationConflictError) as exc:
        raise ExecutionConflictError("allocation subject generation evidence is invalid") from exc


async def _resolve_allocation_subject(
    session: AsyncSession,
    epoch: CapacityExecutionEpoch,
    allocation: CapacityAllocationEpoch,
    *,
    subject_id: UUID,
    require_current: bool,
) -> tuple[SubjectConfigurationV1, SubjectExecutionAcknowledgementV2]:
    preparation: ExecutionPreparationV2
    typed_history = None
    if epoch.manifest_payload.get("schema_version") == 4:
        from loom_capacity_manager.typed_membership_store import _load_typed_immutable_history

        typed_history = await _load_typed_immutable_history(session, epoch.execution_epoch)
        preparation = typed_history.preparation
    else:
        preparation = CapacityManagementStore._execution_preparation_from_row(epoch)
    payload = parse_executable_epoch(json.dumps(allocation.complete_payload))
    fence = payload.execution
    if (
        not allocation.sealed
        or not allocation.executable
        or allocation.status != "executable"
        or allocation.execution_epoch != epoch.execution_epoch
        or allocation.execution_manifest_sha256 != epoch.execution_manifest_sha256
        or allocation.configuration_epoch != epoch.configuration_epoch
        or allocation.allocation_epoch != fence.allocation_epoch
        or allocation.writer_epoch != fence.writer_epoch
        or allocation.input_digest != payload.input_digest
        or allocation.allocation_count != len(payload.allocations)
        or fence.authority_incarnation != epoch.authority_incarnation
        or fence.execution_epoch != epoch.execution_epoch
        or fence.execution_manifest_sha256 != epoch.execution_manifest_sha256
        or fence.configuration_epoch != epoch.configuration_epoch
        or fence.trusted_fleet_release_sha256 != epoch.trusted_fleet_release_sha256
        or fence.executable_new_capacity_ceiling > preparation.requested_ceiling
        or fence.executable_new_capacity_rate_per_minute > preparation.requested_rate_per_minute
        or preparation.schema_version != payload.schema_version
    ):
        raise ExecutionConflictError("allocation subject generation fence changed")
    base = await session.get(CapacityConfigurationEpoch, epoch.configuration_epoch)
    if (
        base is None
        or canonical_digest(payload.configuration) != base.canonical_digest
        or payload.configuration.fleet.generation != epoch.fleet_generation
        or payload.configuration.fleet.digest != epoch.fleet_digest
        or [item.model_dump(mode="json") for item in payload.configuration.subjects]
        != base.subject_generation_manifest
    ):
        raise ExecutionConflictError("allocation immutable base generation changed")

    subject: SubjectConfigurationV1 | None = None
    acknowledgement: SubjectExecutionAcknowledgementV2 | None = None
    if isinstance(payload, ExecutableEpochV4):
        if typed_history is None or typed_history.snapshot(payload.membership.revision) != payload.membership:
            raise ExecutionConflictError("allocation typed membership generation changed")
        typed_member = next((item for item in payload.membership.members if item.configuration.subject_id == subject_id), None)
        if typed_member is not None:
            subject, acknowledgement = typed_member.configuration, typed_member.acknowledgement
    if isinstance(payload, ExecutableEpochV3):
        snapshot = await CapacityMembershipStore(CapacityManagementStore()).snapshot(
            session, epoch, through_revision=payload.membership.revision
        )
        if snapshot != payload.membership:
            raise ExecutionConflictError("allocation membership generation changed")
        member = next(
            (item for item in snapshot.members if item.configuration.subject_id == subject_id),
            None,
        )
        if member is not None:
            subject, acknowledgement = member.configuration, member.acknowledgement
    if subject is None:
        reference = next(
            (item for item in payload.configuration.subjects if item.subject_id == subject_id),
            None,
        )
        if reference is None:
            raise ExecutionConflictError("allocation subject generation is unavailable")
        row = await session.scalar(
            select(CapacityConfigGeneration).where(
                CapacityConfigGeneration.scope == "subject",
                CapacityConfigGeneration.subject_id == reference.subject_id,
                CapacityConfigGeneration.subject_incarnation == reference.subject_incarnation,
                CapacityConfigGeneration.scope_generation == reference.generation,
                CapacityConfigGeneration.digest == reference.digest,
            )
        )
        if row is None:
            raise ExecutionConflictError("allocation base subject generation is unavailable")
        subject = SubjectConfigurationV1.model_validate_json(json.dumps(row.payload))
        if (
            canonical_digest(subject) != reference.digest
            or subject.subject_id != reference.subject_id
            or subject.subject_incarnation != reference.subject_incarnation
            or subject.configuration_generation != reference.generation
        ):
            raise ExecutionConflictError("allocation base subject generation changed")
        if typed_history is not None:
            from loom_capacity_manager.membership_current import _acknowledgement_matches

            acknowledgement = next((ack for ack in preparation.subject_acknowledgements if _acknowledgement_matches(ack, subject)), None)
            if acknowledgement is None:
                raise ExecutionConflictError("allocation typed base acknowledgement is unavailable")
        else:
            acknowledgement = await resolve_subject_acknowledgement(
                session,
                epoch,
                subject_id=subject.subject_id,
                subject_incarnation=subject.subject_incarnation,
                configuration_generation=subject.configuration_generation,
                deployment_generation=subject.deployment_generation,
                reporter_incarnation=subject.demand_reporter_incarnation,
            )
    assert acknowledgement is not None
    if require_current:
        from loom_capacity_manager.membership_current import resolve_current_subject

        current = await resolve_current_subject(session, epoch, subject_id=subject_id)
        if current != (subject, acknowledgement):
            raise ExecutionConflictError("allocation subject generation was superseded")
    return subject, acknowledgement


async def allocation_subject_is_current(
    session: AsyncSession,
    epoch: CapacityExecutionEpoch,
    allocation: CapacityAllocationEpoch,
    *,
    subject_id: UUID,
) -> bool:
    """Identify durable target supersession, never treating corrupt evidence as stale."""

    pinned = await resolve_allocation_subject(
        session, epoch, allocation, subject_id=subject_id, require_current=False
    )
    # Both matching and differing events require verified materialized evidence.
    # Disabled generations are readable only for this comparison, never admission.
    from loom_capacity_manager.membership_current import resolve_current_subject

    try:
        current = await resolve_current_subject(
            session, epoch, subject_id=subject_id, allow_disabled=True, allow_equivocal=True
        )
    except (ValueError, ConfigurationConflictError) as exc:
        raise ExecutionConflictError(
            "allocation current subject generation evidence changed"
        ) from exc
    reporter_state = await session.scalar(select(CapacityDemandReporter.state).where(
        CapacityDemandReporter.subject_id == subject_id,
        CapacityDemandReporter.subject_incarnation == current[0].subject_incarnation,
        CapacityDemandReporter.reporter_incarnation == current[0].demand_reporter_incarnation,
    ))
    return (
        current == pinned and current[0].lifecycle_state == "active"
        and reporter_state == "current"
    )


async def resolve_allocation_reporter(
    session: AsyncSession,
    epoch: CapacityExecutionEpoch,
    allocation: CapacityAllocationEpoch,
    *,
    subject_id: UUID,
    reporter_incarnation: UUID,
) -> CapacityDemandReporter:
    """Authenticate the exact pinned reporter for retained work, never an increase."""

    subject, acknowledgement = await resolve_allocation_subject(
        session, epoch, allocation, subject_id=subject_id, require_current=False
    )
    if acknowledgement.reporter_incarnation != reporter_incarnation:
        raise ExecutionConflictError("allocation historical reporter changed")
    reporter = await session.scalar(
        select(CapacityDemandReporter)
        .where(
            CapacityDemandReporter.subject_id == subject_id,
            CapacityDemandReporter.subject_incarnation == subject.subject_incarnation,
            CapacityDemandReporter.reporter_incarnation == reporter_incarnation,
        )
        .execution_options(populate_existing=True)
        .with_for_update(read=True)
    )
    if reporter is None or reporter.state not in {"current", "fenced"}:
        raise ExecutionConflictError("allocation historical reporter is unavailable")
    if (
        reporter.deployment_generation != subject.deployment_generation
        or reporter.configuration_generation < subject.configuration_generation
    ):
        raise ExecutionConflictError("allocation historical reporter generation changed")
    try:
        await resolve_subject_acknowledgement(
            session,
            epoch,
            subject_id=subject_id,
            subject_incarnation=subject.subject_incarnation,
            configuration_generation=reporter.configuration_generation,
            deployment_generation=reporter.deployment_generation,
            reporter_incarnation=reporter.reporter_incarnation,
        )
    except ConfigurationConflictError as exc:
        raise ExecutionConflictError(
            "allocation historical reporter generation is unavailable"
        ) from exc
    if reporter.state == "fenced":
        snapshot: PersonalMembershipSnapshotV1 | PersonalMembershipSnapshotV2
        if epoch.manifest_payload.get("schema_version") == 4:
            from loom_capacity_manager.typed_membership_store import _load_typed_history

            snapshot = (await _load_typed_history(session, epoch.execution_epoch)).snapshot()
        else:
            preparation = CapacityManagementStore._execution_preparation_from_row(epoch)
            if not isinstance(preparation, ExecutionPreparationV3):
                raise ExecutionConflictError("allocation historical reporter is fenced")
            snapshot = await CapacityMembershipStore(CapacityManagementStore()).snapshot(session, epoch)
        successor = next(
            (item for item in snapshot.members if item.configuration.subject_id == subject_id),
            None,
        )
        if (
            successor is None
            or successor.configuration.configuration_generation <= subject.configuration_generation
            or successor.acknowledgement.reporter_incarnation == reporter_incarnation
        ):
            raise ExecutionConflictError("allocation reporter lacks authenticated rollover")
    return reporter
