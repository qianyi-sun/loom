"""Read authenticated retired member origins, without activating a successor.

The immutable history reader authenticates full event prefixes and retained
installations, including recreation releases. Current reporters/materialization
are deliberately not historical evidence. Admission must additionally join this
export to the successor's complete configuration, policy and source graph.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.application_origin_contracts import ManagedApplicationOriginV1
from loom_capacity_manager.build_membership_contracts import PersonalMembershipSnapshotV2
from loom_capacity_manager.build_value_contracts import PersonalBuildProjectionV1
from loom_capacity_manager.contracts import (
    DynamicDevelopmentSubjectProjectionV1,
    canonical_bytes,
)
from loom_capacity_manager.models import CapacityAuthorityState
from loom_capacity_manager.retired_application_import import _reference, _require_retired
from loom_capacity_manager.retired_member_origin_contracts import (
    PersonalMemberEventAnchorV1,
    RetiredMembershipSnapshotReferenceV1,
    RetiredPersonalMemberOriginV1,
)
from loom_capacity_manager.store import (
    ConfigurationConflictError,
    ExecutionConflictError,
    _write_transaction,
)
from loom_capacity_manager.successor_origin_contracts import (
    ManagedApplicationOriginV2,
    ManagedBuildOriginV1,
)
from loom_capacity_manager.typed_membership_commands import (
    PersonalApplicationCommandV2,
    PersonalBuildCommandV2,
    parse_typed_membership_mutation,
)
from loom_capacity_manager.typed_membership_store import _load_typed_immutable_history


@dataclass(frozen=True)
class RetiredMemberOrigins:
    source: RetiredMembershipSnapshotReferenceV1
    applications: tuple[ManagedApplicationOriginV1 | ManagedApplicationOriginV2, ...]
    builds: tuple[ManagedBuildOriginV1, ...]


async def export_retired_member_origins(
    session: AsyncSession, *, execution_epoch: int, expected_snapshot: PersonalMembershipSnapshotV2,
) -> RetiredMemberOrigins:
    """Derive all managed origins from one exact retired source in a transaction.

    No candidate/proposal/activation is written, no caller-selected subset or
    installation is accepted, and no event is invented for untouched pinned bases.
    This is an internal read for a separately authenticated operator workflow.
    """
    # begin_nested flushes pending edits even with autoflush disabled. A read must
    # neither persist those edits nor discard them while refreshing cached rows.
    if session.new or session.dirty or session.deleted:
        raise ConfigurationConflictError("retired member export requires a session without pending edits")
    try:
        async with _write_transaction(session):
            authority = (await session.scalars(select(CapacityAuthorityState).where(
                CapacityAuthorityState.singleton_id == 1).with_for_update()
                .execution_options(populate_existing=True))).one_or_none()
            history = await _load_typed_immutable_history(session, execution_epoch)
            _require_retired(history.epoch)
            if authority is None or authority.authority_incarnation != history.epoch.authority_incarnation:
                raise ExecutionConflictError("retired member export authority incarnation changed")
            snapshot = history.snapshot()
            if canonical_bytes(snapshot) != canonical_bytes(expected_snapshot):
                raise ConfigurationConflictError("retired member export requires the complete final snapshot")
            source = RetiredMembershipSnapshotReferenceV1(namespace_id=snapshot.namespace_id,
                execution_epoch=execution_epoch, execution_manifest_sha256=history.epoch.execution_manifest_sha256,
                revision=snapshot.revision, head_sha256=snapshot.head_sha256)
            applications: dict[UUID, ManagedApplicationOriginV1 | ManagedApplicationOriginV2] = {
                origin.configuration.subject_id: origin for origin in history.preparation.managed_application_origins}
            builds: dict[UUID, ManagedBuildOriginV1] = {}
            roots = {identity: _reference(origin.configuration) for identity, origin in applications.items()}
            installations: dict[tuple[UUID, UUID, int], DynamicDevelopmentSubjectProjectionV1 | PersonalBuildProjectionV1] = {
                (origin.configuration.subject_id, origin.configuration.subject_incarnation,
                    origin.configuration.deployment_generation): origin.installation_projection
                for origin in applications.values()}
            for row, result in zip(history.events, history.results, strict=True):
                request = parse_typed_membership_mutation(json.dumps(row.request_payload))
                member, command = result.member, request.command
                subject = member.configuration
                root = roots.setdefault(subject.subject_id, _reference(subject))
                key = (subject.subject_id, subject.subject_incarnation, subject.deployment_generation)
                if command.projection.operation_kind in {"create", "update"}:
                    installations[key] = command.projection
                installed = installations.get(key)
                inherited = RetiredPersonalMemberOriginV1(source=source,
                    anchor=PersonalMemberEventAnchorV1(execution_epoch=execution_epoch,
                        execution_manifest_sha256=history.epoch.execution_manifest_sha256,
                        revision=row.revision, head_sha256=row.head_sha256, member=member),
                    original_origin=root)
                if isinstance(command, PersonalApplicationCommandV2) and isinstance(installed, DynamicDevelopmentSubjectProjectionV1):
                    applications[subject.subject_id] = ManagedApplicationOriginV2(configuration=subject,
                        acknowledgement=member.acknowledgement, installation_projection=installed,
                        base_projection=command.projection, inherited=inherited)
                elif isinstance(command, PersonalBuildCommandV2) and isinstance(installed, PersonalBuildProjectionV1):
                    builds[subject.subject_id] = ManagedBuildOriginV1(configuration=subject,
                        acknowledgement=member.acknowledgement, installation_projection=installed,
                        base_projection=command.projection, template=history.preparation.personal_builds,
                        trusted_fleet_release_sha256=history.preparation.trusted_fleet_release_sha256,
                        inherited=inherited)
                else:
                    raise ConfigurationConflictError("retired member installation purpose is unavailable")
            return RetiredMemberOrigins(source=source,
                applications=tuple(applications[identity] for identity in sorted(applications, key=lambda item: item.int)),
                builds=tuple(builds[identity] for identity in sorted(builds, key=lambda item: item.int)))
    except ValueError as exc:
        raise ConfigurationConflictError("retired member source evidence is invalid") from exc
