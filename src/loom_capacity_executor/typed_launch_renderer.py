"""Render authenticated typed allocation facts without enabling submission.

The caller must obtain the resolved subject from the authenticated manager and
retain current intent/admission fences. Structural consistency and a signature
are not substitutes for that authorization. Legacy renderers reject this context.
"""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from datetime import datetime

from loom_capacity_executor.keys import ExecutorOwnershipKey
from loom_capacity_executor.launch_policy_set import (
    PoolLaunchPolicyV3,
    full_launch_profile_digest,
    resolve_typed_runtime_profile,
)
from loom_capacity_executor.launch_renderer import OperatorLaunchProfileV2, _render_slurm_request
from loom_capacity_executor.slurm_contracts import SlurmLaunchRequestV2
from loom_capacity_manager.contracts import (
    ConfigurationGenerationRefV1,
    SubjectConfigurationV1,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentBindingV2,
    PoolControllerAuthorityV2,
    SubjectExecutionAcknowledgementV2,
    canonical_executable_digest,
)
from loom_capacity_manager.membership_current import _acknowledgement_matches
from loom_capacity_manager.membership_launch_authority import ResolvedAllocationLaunchSubject
from loom_capacity_manager.ownership import sign_typed_executable_ownership
from loom_capacity_manager.typed_ownership_contracts import (
    ExecutableOwnershipMetadataV3,
    ExecutableSubjectAuthorityV3,
    SignedExecutableOwnershipProofV3,
    canonical_typed_ownership_bytes,
)


@dataclass(frozen=True, slots=True)
class TrustedLaunchContextV3:
    binding: ExecutableIntentBindingV2
    subject: ResolvedAllocationLaunchSubject
    profiles: tuple[OperatorLaunchProfileV2, ...]
    policy: PoolLaunchPolicyV3
    controller_authority: PoolControllerAuthorityV2
    ownership_key: ExecutorOwnershipKey
    submitted_at: datetime
    candidate_diagnostic: str = ""
    display_diagnostic: str = ""


@dataclass(frozen=True, slots=True)
class RenderedTrustedLaunchV3:
    request: SlurmLaunchRequestV2
    ownership_proof: SignedExecutableOwnershipProofV3


def render_typed_signed_launch(context: TrustedLaunchContextV3) -> RenderedTrustedLaunchV3:
    """Join full subject facts to the exact intent before signing its purpose."""
    if not isinstance(context, TrustedLaunchContextV3):
        raise TypeError("typed launch rendering requires TrustedLaunchContextV3")
    if not isinstance(context.subject, ResolvedAllocationLaunchSubject) or not isinstance(context.ownership_key, ExecutorOwnershipKey):
        raise ValueError("typed launch subject or key is invalid")
    binding = ExecutableIntentBindingV2.model_validate_json(context.binding.model_dump_json())
    subject = SubjectConfigurationV1.model_validate_json(context.subject.configuration.model_dump_json())
    ack = SubjectExecutionAcknowledgementV2.model_validate_json(context.subject.acknowledgement.model_dump_json())
    authority = ExecutableSubjectAuthorityV3.model_validate_json(context.subject.authority.model_dump_json())
    reference = ConfigurationGenerationRefV1(scope="subject", subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation, generation=subject.configuration_generation,
        digest=canonical_digest(subject))
    if (
        authority.configuration != reference
        or authority.acknowledgement_sha256 != canonical_executable_digest(ack)
        or not _acknowledgement_matches(ack, subject)
        or binding.subject_id != subject.subject_id or binding.subject_incarnation != subject.subject_incarnation
        or binding.account_id != subject.account_id or binding.tier_id != subject.tier_id
        or binding.candidate_generation != subject.candidate_generation
        or binding.deployment_generation != subject.deployment_generation
        or binding.candidate != ack.candidate or subject.lifecycle_state != "active"
        or context.controller_authority.pool_id != binding.pool_id
    ):
        raise ValueError("typed launch subject, candidate or acknowledgement binding changed")
    profile = resolve_typed_runtime_profile(binding, context.profiles, policy=context.policy,
        purpose=authority.purpose, controller_authority_sha256=context.controller_authority.controller_authority_sha256)
    pinned = next((item for item in subject.profiles if item.pool_id == binding.pool_id), None)
    shape = None if pinned is None else next((item for item in pinned.worker_shapes if item.shape_id == binding.shape_id), None)
    if (
        pinned is None or shape is None or pinned.pool_generation != binding.pool_generation
        or shape.shape_id != binding.profile_id or pinned.profile_generation != binding.profile_generation
        or pinned.profile_digest != binding.profile_digest or shape.total_resources != binding.resources
        or shape.concurrency_slots != binding.concurrency_slots
        or len(shape.node_resources) != len(binding.node_ids)
    ):
        raise ValueError("typed launch profile differs from authenticated subject")
    domain = next(domain for domain in profile.resource_domains if set(binding.node_ids) <= set(domain.node_ids))
    if domain.domain_id not in shape.compatible_domain_ids or domain.domain_id not in pinned.eligible_resource_domains:
        raise ValueError("typed launch domain differs from authenticated subject")
    metadata = ExecutableOwnershipMetadataV3(binding=binding, subject_authority=authority,
        launch_profile_sha256=full_launch_profile_digest(profile),
        controller_authority_sha256=context.controller_authority.controller_authority_sha256,
        trusted_launcher_sha256=binding.execution.trusted_fleet_release_sha256,
        slurm_cluster=profile.slurm_cluster, submitter_identity=profile.submitter,
        association=profile.association, submitted_at=context.submitted_at)
    proof = sign_typed_executable_ownership(context.ownership_key.private_key,
        signing_key_id=context.ownership_key.signing_key_id, metadata=metadata)
    token = base64.urlsafe_b64encode(hashlib.sha256(canonical_typed_ownership_bytes(proof)).digest()).rstrip(b"=").decode("ascii")
    request = _render_slurm_request(binding=binding, profile=profile, domain=domain,
        ownership_token=token, candidate_diagnostic=context.candidate_diagnostic,
        display_diagnostic=context.display_diagnostic)
    return RenderedTrustedLaunchV3(request=request, ownership_proof=proof)
