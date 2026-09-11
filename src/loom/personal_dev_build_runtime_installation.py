"""Join trusted builder publication to prepared, purpose-tagged pool policies.

Inputs must come from the approved publication loader and execution preparation,
not feature source or caller-supplied authority. These are installation facts, not
proof of an installed admission adapter, worker certification, or launch authority.
No worker allocation is required: build services must start from zero idle slots.
"""

from __future__ import annotations

from dataclasses import dataclass

from loom.personal_dev_build_runtime_publication import PersonalDevBuildRuntimePublication
from loom.personal_dev_candidate import PersonalDevPlatform
from loom_capacity_executor.launch_policy_set import (
    PoolLaunchPolicyV3,
    full_launch_profile_digest,
    validate_typed_runtime_profiles,
)
from loom_capacity_executor.launch_renderer import OperatorLaunchProfileV2
from loom_capacity_manager.build_membership_contracts import ExecutionPreparationV4
from loom_capacity_manager.contracts import ProfileReferenceV1, canonical_digest
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    canonical_executable_digest,
)


@dataclass(frozen=True, slots=True)
class PersonalBuildPoolInstallation:
    pool_id: str
    platform: PersonalDevPlatform
    executor_id: str
    executor_incarnation: str
    profile_id: str
    node_ids: tuple[str, ...]
    controller_authority_sha256: str
    launch_profile_sha256: str
    builder_image: str
    agent_image: str


@dataclass(frozen=True, slots=True)
class PersonalBuildRuntimeInstallation:
    candidate: CandidateBindingV2
    execution_manifest_sha256: str
    trusted_fleet_release_sha256: str
    template_sha256: str
    release_evidence_sha256: str
    profiles: tuple[ProfileReferenceV1, ...]
    pools: tuple[PersonalBuildPoolInstallation, ...]


def resolve_personal_build_runtime_installation(
    publication: PersonalDevBuildRuntimePublication, *,
    preparation: ExecutionPreparationV4,
    pool_profiles: tuple[tuple[PoolLaunchPolicyV3, tuple[OperatorLaunchProfileV2, ...]], ...],
) -> PersonalBuildRuntimeInstallation:
    """Resolve both native images against complete, independently pinned policies.

The policy root covers physical nodes and launcher configuration as well as every
application/build profile. The template separately constrains native shape and
eligible domains. Neither a matching image alone nor a partial policy is enough.
"""
    preparation = ExecutionPreparationV4.model_validate_json(preparation.model_dump_json())
    template = preparation.personal_builds
    if (
        publication.candidate != template.runtime_candidate
        or len(publication.platforms) != 2
        or {item.platform for item in publication.platforms} != {"linux/amd64", "linux/arm64"}
        or not isinstance(pool_profiles, tuple) or len(pool_profiles) != 2
        or {policy.pool_id for policy, _ in pool_profiles} != {"gb10", "oldlab"}
    ):
        raise ValueError("build installation publication or native pool set differs")
    result = []
    for policy, profiles in sorted(pool_profiles, key=lambda item: item[0].pool_id):
        executor = next(item for item in preparation.executors if item.pool_id == policy.pool_id)
        validate_typed_runtime_profiles(profiles, policy=policy,
            controller_authority_sha256=executor.controller_authority_sha256)
        reference = next(item for item in template.profiles if item.pool_id == policy.pool_id)
        shape = reference.worker_shapes[0]
        platform: PersonalDevPlatform = "linux/arm64" if policy.pool_id == "gb10" else "linux/amd64"
        images = next(item for item in publication.platforms if item.platform == platform)
        entries = {item.profile_sha256: item.purpose for item in policy.entries}
        matches = [profile for profile in profiles if
            entries[full_launch_profile_digest(profile)] == "personal-build-worker"
            and profile.profile_id == shape.shape_id
            and profile.profile_generation == reference.profile_generation
            and profile.profile_digest == reference.profile_digest
            and profile.shape_id == shape.shape_id]
        if len(matches) != 1:
            raise ValueError("build installation does not resolve one native purpose profile")
        profile = matches[0]
        domains = {item.domain_id for item in profile.resource_domains}
        if (
            executor.pool_generation != policy.pool_generation
            or reference.pool_generation != policy.pool_generation
            or profile.image_digest != images.agent_image
            or profile.trusted_launcher_release_sha256 != preparation.trusted_fleet_release_sha256
            or profile.resources != shape.total_resources
            or profile.concurrency_slots != shape.concurrency_slots
            or domains != set(reference.eligible_resource_domains) & set(shape.compatible_domain_ids)
        ):
            raise ValueError("build installation native image, resources or authority differs")
        result.append(PersonalBuildPoolInstallation(pool_id=policy.pool_id, platform=platform,
            executor_id=executor.executor_id, executor_incarnation=str(executor.executor_incarnation),
            profile_id=profile.profile_id,
            node_ids=tuple(sorted({node for domain in profile.resource_domains for node in domain.node_ids})),
            controller_authority_sha256=executor.controller_authority_sha256,
            launch_profile_sha256=full_launch_profile_digest(profile),
            builder_image=images.builder_image, agent_image=images.agent_image))
    return PersonalBuildRuntimeInstallation(candidate=publication.candidate,
        execution_manifest_sha256=canonical_executable_digest(preparation),
        trusted_fleet_release_sha256=preparation.trusted_fleet_release_sha256,
        template_sha256=canonical_digest(template), release_evidence_sha256=publication.release_evidence_sha256,
        profiles=template.profiles, pools=tuple(result))
