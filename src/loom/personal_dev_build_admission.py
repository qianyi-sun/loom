"""Join manager-authored placements to current, durable native build requests.

This pure validator does not authenticate caller inputs, write assignments, emit
acknowledgements or issue capabilities. The protected store must load these rows
under locks and persist the result before any acknowledgement publication.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from loom.db.schema import PersonalDevBuildPlatformRequest
from loom.personal_dev_build_platform_requests import _installation, _member, _values
from loom.personal_dev_build_runtime_installation import PersonalBuildRuntimeInstallation
from loom.personal_dev_candidate import CandidateRegistration, PersonalDevPlatform
from loom_capacity_manager.build_value_contracts import PersonalBuildMemberV1
from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionAllowanceV2,
    ExecutableAdmissionPlanProposalV2,
    ExecutableAdmissionShapeV2,
    ExecutionFenceV2,
)


@dataclass(frozen=True, slots=True)
class PersonalBuildAssignmentBinding:
    request_id: UUID
    platform: PersonalDevPlatform
    source_binding_sha256: str
    runtime_installation_sha256: str
    lease_not_after: datetime
    allowance: ExecutableAdmissionAllowanceV2
    shape: ExecutableAdmissionShapeV2


def bind_personal_build_admission(
    *, member: PersonalBuildMemberV1, runtime: PersonalBuildRuntimeInstallation,
    execution: ExecutionFenceV2, proposal: ExecutableAdmissionPlanProposalV2,
    requests: tuple[tuple[PersonalDevBuildPlatformRequest, CandidateRegistration], ...], now: datetime,
) -> tuple[PersonalBuildAssignmentBinding, ...]:
    """Require exactly one native work item per cold manager-owned shape slot."""
    member = _member(member)
    installation = _installation(member, runtime)
    execution = ExecutionFenceV2.model_validate_json(execution.model_dump_json())
    proposal = ExecutableAdmissionPlanProposalV2.model_validate_json(proposal.model_dump_json())
    if (
        now.tzinfo is None or proposal.lease_not_after <= now or execution.execution_state != "active"
        or execution.execution_manifest_sha256 != runtime.execution_manifest_sha256
        or execution.trusted_fleet_release_sha256 != runtime.trusted_fleet_release_sha256
        or proposal.reporter_incarnation != member.configuration.demand_reporter_incarnation
        or proposal.protected_admission_sha256 != member.acknowledgement.protected_admission_sha256
        or len(proposal.allowances) != len(proposal.shapes)
        or len(proposal.allowances) > member.configuration.max_slots
        or not isinstance(requests, tuple) or len(requests) != len(proposal.allowances)
        or len({row.id for row, _ in requests}) != len(requests)
    ):
        raise ValueError("build admission authority, lease or complete work set changed")
    by_id = {row.id: (row, registration) for row, registration in requests}
    if set(by_id) != {item.protected_attempt_id for item in proposal.allowances}:
        raise ValueError("build admission request set differs from manager allowances")
    shapes = {item.binding.shape_instance_id: item for item in proposal.shapes}
    config = member.configuration
    result = []
    for allowance in proposal.allowances:
        shape = shapes[allowance.shape_instance_id]
        binding = shape.binding
        pool = next((item for item in runtime.pools if item.pool_id == binding.pool_id), None)
        reference = next((item for item in runtime.profiles if item.pool_id == binding.pool_id), None)
        if pool is None or reference is None:
            raise ValueError("build admission native pool is not installed")
        if (
            binding.execution != execution
            or (binding.subject_id, binding.subject_incarnation, binding.account_id, binding.tier_id,
                binding.candidate_generation, binding.deployment_generation)
            != (config.subject_id, config.subject_incarnation, config.account_id, config.tier_id,
                config.candidate_generation, config.deployment_generation)
            or binding.candidate != runtime.candidate
            or binding.pool_generation != reference.pool_generation
            or binding.profile_id != pool.profile_id
            or binding.profile_generation != reference.profile_generation
            or binding.profile_digest != reference.profile_digest
            or binding.executor_id != pool.executor_id
            or str(binding.executor_incarnation) != pool.executor_incarnation
            or binding.concurrency_slots != 1 or allowance.shape_slot_index != 0
            or binding.rollout_surge_slots != 0 or binding.old_shape_backing_id is not None
            or len(binding.node_ids) != 1 or not set(binding.node_ids) <= set(pool.node_ids)
            or shape.worker_shape != reference.worker_shapes[0]
            or binding.shape_id != reference.worker_shapes[0].shape_id
            or shape.protocol_generation != reference.protocol_generation
            or shape.protocol_digest != reference.protocol_digest
        ):
            raise ValueError("build admission service, native shape or execution binding changed")
        row, registration = by_id[allowance.protected_attempt_id]
        expected = _values(registration, member, installation, pool.platform, now)
        if row.cancelled_at is not None or any(getattr(row, field) != value for field, value in expected.items()):
            raise ValueError("build admission request source, installation or cancellation changed")
        attempt = registration.build_attempt
        assert attempt is not None and attempt.lease_expires_at is not None
        result.append(PersonalBuildAssignmentBinding(request_id=row.id, platform=pool.platform,
            source_binding_sha256=row.source_binding_sha256, runtime_installation_sha256=installation,
            lease_not_after=min(proposal.lease_not_after, attempt.lease_expires_at),
            allowance=allowance, shape=shape))
    return tuple(result)
