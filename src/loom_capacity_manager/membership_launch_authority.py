"""Derive launch provenance from authenticated immutable allocation history.

This producer preserves typed build/application provenance as well as legacy
application evidence. It does not sign a launch or expand execution endpoints.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.build_membership_contracts import PersonalMembershipSnapshotV2
from loom_capacity_manager.contracts import (
    ConfigurationGenerationRefV1,
    SubjectConfigurationV1,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    SubjectExecutionAcknowledgementV2,
    canonical_executable_digest,
)
from loom_capacity_manager.membership_contracts import PersonalMembershipSnapshotV1
from loom_capacity_manager.membership_execution import (
    ExecutableEpochV3,
    ExecutableEpochV4,
    parse_executable_epoch,
)
from loom_capacity_manager.membership_execution_store import resolve_allocation_subject
from loom_capacity_manager.membership_store import CapacityMembershipStore
from loom_capacity_manager.models import CapacityAllocationEpoch, CapacityExecutionEpoch
from loom_capacity_manager.store import (
    CapacityManagementStore,
    ConfigurationConflictError,
    ExecutionConflictError,
)
from loom_capacity_manager.typed_ownership_contracts import (
    ExecutableSubjectAuthorityV3,
    PersonalMembershipLaunchReferenceV3,
)


@dataclass(frozen=True, slots=True)
class ResolvedAllocationLaunchSubject:
    """Authenticated configuration and whole acknowledgement, plus signed references."""

    configuration: SubjectConfigurationV1
    acknowledgement: SubjectExecutionAcknowledgementV2
    authority: ExecutableSubjectAuthorityV3


async def resolve_allocation_launch_subject(
    session: AsyncSession,
    epoch: CapacityExecutionEpoch,
    allocation: CapacityAllocationEpoch,
    *,
    subject_id: UUID,
    require_current: bool,
) -> ResolvedAllocationLaunchSubject:
    """Select base/member provenance, never accepting caller-selected authority.

The caller retains the authority-first transaction lock and operation-specific
intent, pool, executor and fence checks. New capacity requires require_current;
historical resolution is exclusively evidence for accounting/recovery/cleanup.
The eventual renderer must compare the complete acknowledgement candidate with
the intent, not merely the configuration's candidate generation.
"""
    try:
        configuration, acknowledgement = await resolve_allocation_subject(
            session, epoch, allocation, subject_id=subject_id, require_current=require_current,
        )
        payload = parse_executable_epoch(json.dumps(allocation.complete_payload))
        event = None
        purpose: Literal["application-worker", "personal-build-worker"] = "application-worker"
        if isinstance(payload, (ExecutableEpochV3, ExecutableEpochV4)):
            member = next((item for item in payload.membership.members if item.configuration.subject_id == subject_id), None)
            if member is not None:
                # The allocation reader authenticates its complete snapshot. Read
                # the selected member's own prefix to retain that event's head,
                # independent of unrelated later membership changes.
                history: PersonalMembershipSnapshotV1 | PersonalMembershipSnapshotV2
                if isinstance(payload, ExecutableEpochV4):
                    from loom_capacity_manager.typed_membership_store import (
                        CapacityTypedMembershipStore,
                    )

                    history = await CapacityTypedMembershipStore().snapshot(session, epoch, through_revision=member.revision)
                else:
                    history = await CapacityMembershipStore(CapacityManagementStore()).snapshot(
                        session, epoch, through_revision=member.revision,
                    )
                selected = next((item for item in history.members if item.configuration.subject_id == subject_id), None)
                if (
                    selected != member or history.namespace_id != payload.membership.namespace_id
                    or member.configuration != configuration or member.acknowledgement != acknowledgement
                ):
                    raise ExecutionConflictError("launch subject member event binding changed")
                event = PersonalMembershipLaunchReferenceV3(
                    namespace_id=history.namespace_id, owner_id=member.owner_id,
                    revision=member.revision, head_sha256=history.head_sha256,
                    execution_manifest_sha256=epoch.execution_manifest_sha256,
                )
                if member.purpose == "personal-build-worker":
                    purpose = "personal-build-worker"
        authority = ExecutableSubjectAuthorityV3(
            source="immutable-base" if event is None else "personal-membership",
            purpose=purpose,
            configuration=ConfigurationGenerationRefV1(
                scope="subject", subject_id=configuration.subject_id,
                subject_incarnation=configuration.subject_incarnation,
                generation=configuration.configuration_generation, digest=canonical_digest(configuration),
            ),
            acknowledgement_sha256=canonical_executable_digest(acknowledgement),
            membership=event,
        )
        return ResolvedAllocationLaunchSubject(configuration=configuration, acknowledgement=acknowledgement, authority=authority)
    except (ValueError, ConfigurationConflictError) as exc:
        raise ExecutionConflictError("allocation launch subject provenance is invalid") from exc
