"""Authenticated typed subject facts bind the native image and signed launch."""

from dataclasses import replace
from importlib import import_module

import pytest

from loom_capacity_executor.launch_policy_set import (
    PoolLaunchPolicyV3,
    PurposeLaunchPolicyV3,
    canonical_pool_launch_policy_digest,
    full_launch_profile_digest,
)
from loom_capacity_executor.launch_renderer import OperatorResourceDomainV2, render_signed_launch
from loom_capacity_manager.contracts import ConfigurationGenerationRefV1, canonical_digest
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from loom_capacity_manager.membership_launch_authority import ResolvedAllocationLaunchSubject
from loom_capacity_manager.ownership import OwnershipKeyring
from loom_capacity_manager.typed_ownership_contracts import (
    ExecutableSubjectAuthorityV3,
    PersonalMembershipLaunchReferenceV3,
)
from tests.unit.test_capacity_build_membership import build_membership_input
from tests.unit.test_capacity_executor_launch_renderer import launch_context_fixture


def typed_context(*, purpose="personal-build-worker", pool="oldlab", resolved=None, execution=None):
    module = import_module("loom_capacity_executor.typed_launch_renderer")
    value = build_membership_input()
    member = next(item for item in value.membership.members if
        (item.purpose == "personal-build-worker") == (purpose == "personal-build-worker"))
    subject, ack = member.configuration, member.acknowledgement
    if resolved is not None:
        subject, ack = resolved.configuration, resolved.acknowledgement
    base = launch_context_fixture()
    execution = execution or base.binding.execution.model_copy(update={
        "trusted_fleet_release_sha256": value.preparation.trusted_fleet_release_sha256,
        "execution_manifest_sha256": canonical_executable_digest(value.preparation),
    })
    profile_ref = next(item for item in subject.profiles if item.pool_id == pool)
    shape = profile_ref.worker_shapes[0]
    nodes = ("trt-gb10-3",) if pool == "gb10" else ("oldlab-5",)
    profile = base.profile.model_copy(update={
        "pool_id": pool, "pool_generation": profile_ref.pool_generation,
        "profile_id": shape.shape_id, "profile_generation": profile_ref.profile_generation,
        "profile_digest": profile_ref.profile_digest, "shape_id": shape.shape_id,
        "resources": shape.total_resources, "concurrency_slots": shape.concurrency_slots,
        "cpus": shape.total_resources.cpu_millicores // 1000, "generic_tres": (),
        "resource_domains": (OperatorResourceDomainV2(domain_id=next(domain for domain in profile_ref.eligible_resource_domains if domain in shape.compatible_domain_ids), node_ids=nodes),),
        "trusted_launcher_release_sha256": execution.trusted_fleet_release_sha256,
        "image_digest": f"ghcr.io/qianyi-sun/loom-{purpose}@sha256:" + "e" * 64,
    })
    policy = PoolLaunchPolicyV3(pool_id=pool, pool_generation=profile.pool_generation,
        entries=(PurposeLaunchPolicyV3(purpose=purpose, profile_sha256=full_launch_profile_digest(profile)),))
    root = canonical_pool_launch_policy_digest(policy)
    profile = profile.model_copy(update={"controller_authority_sha256": root})
    binding = base.binding.model_copy(update={
        "execution": execution,
        "subject_id": subject.subject_id, "subject_incarnation": subject.subject_incarnation,
        "account_id": subject.account_id, "tier_id": subject.tier_id,
        "candidate": ack.candidate, "candidate_generation": subject.candidate_generation,
        "deployment_generation": subject.deployment_generation,
        "pool_id": pool, "pool_generation": profile.pool_generation, "profile_id": profile.profile_id,
        "profile_generation": profile.profile_generation, "profile_digest": profile.profile_digest,
        "shape_id": profile.shape_id, "resources": profile.resources,
        "concurrency_slots": profile.concurrency_slots, "node_ids": nodes,
    })
    authority = ExecutableSubjectAuthorityV3(source="personal-membership", purpose=purpose,
        configuration=ConfigurationGenerationRefV1(scope="subject", subject_id=subject.subject_id,
            subject_incarnation=subject.subject_incarnation, generation=subject.configuration_generation,
            digest=canonical_digest(subject)), acknowledgement_sha256=canonical_executable_digest(ack),
        membership=PersonalMembershipLaunchReferenceV3(namespace_id=value.membership.namespace_id,
            owner_id=member.owner_id, revision=member.revision, head_sha256="a" * 64,
            execution_manifest_sha256=binding.execution.execution_manifest_sha256))
    return module.TrustedLaunchContextV3(binding=binding,
        subject=resolved or ResolvedAllocationLaunchSubject(configuration=subject, acknowledgement=ack, authority=authority),
        profiles=(profile,), policy=policy,
        controller_authority=base.controller_authority.model_copy(update={"pool_id": pool, "controller_authority_sha256": root}),
        ownership_key=base.ownership_key, submitted_at=base.submitted_at)


@pytest.mark.parametrize("pool", ("oldlab", "gb10"))
@pytest.mark.parametrize("purpose", ("application-worker", "personal-build-worker"))
def test_typed_render_signs_exact_subject_native_profile_and_candidate(pool, purpose):
    context = typed_context(pool=pool, purpose=purpose)
    module = import_module("loom_capacity_executor.typed_launch_renderer")
    rendered = module.render_typed_signed_launch(context)
    metadata = rendered.ownership_proof.metadata
    assert metadata.binding == context.binding
    assert metadata.subject_authority == context.subject.authority
    assert metadata.launch_profile_sha256 == full_launch_profile_digest(context.profiles[0])
    assert rendered.request.image_digest == context.profiles[0].image_digest
    assert rendered.request.nodes == context.binding.node_ids
    assert rendered.request.memory_bytes == context.binding.resources.memory_bytes
    assert rendered.request.operation_id == context.binding.intent_id
    keys = OwnershipKeyring({context.ownership_key.signing_key_id: context.ownership_key.private_key.public_key()})
    assert keys.verify_typed_executable(rendered.ownership_proof, expected_public_key_sha256=context.ownership_key.public_key_sha256)
    assert not keys.verify_executable(rendered.ownership_proof, expected_public_key_sha256=context.ownership_key.public_key_sha256)
    with pytest.raises(TypeError):
        render_signed_launch(context)


@pytest.mark.parametrize("tamper", ("candidate", "ack", "reference", "purpose", "profile", "deployment"))
def test_typed_render_rejects_substitution_before_signing(tamper):
    context = typed_context()
    module = import_module("loom_capacity_executor.typed_launch_renderer")
    if tamper == "candidate":
        context = replace(context, binding=context.binding.model_copy(update={
            "candidate": context.binding.candidate.model_copy(update={"publication_sha256": "f" * 64})}))
    elif tamper == "ack":
        context = replace(context, subject=replace(context.subject, acknowledgement=context.subject.acknowledgement.model_copy(update={"acknowledgement_sha256": "f" * 64})))
    elif tamper == "reference":
        context = replace(context, subject=replace(context.subject, authority=context.subject.authority.model_copy(update={
            "configuration": context.subject.authority.configuration.model_copy(update={"digest": "f" * 64})})))
    elif tamper == "purpose":
        context = replace(context, subject=replace(context.subject, authority=context.subject.authority.model_copy(update={"purpose": "application-worker"})))
    elif tamper == "profile":
        context = replace(context, profiles=(context.profiles[0].model_copy(update={"image_digest": "ghcr.io/foreign/worker@sha256:" + "f" * 64}),))
    else:
        context = replace(context, binding=context.binding.model_copy(update={"deployment_generation": context.binding.deployment_generation + 1}))
    with pytest.raises(ValueError):
        module.render_typed_signed_launch(context)


def test_typed_application_render_rejects_more_nodes_than_authenticated_shape():
    context = typed_context(purpose="application-worker")
    profile = context.profiles[0]
    nodes = ("oldlab-5", "oldlab-6")
    profile = profile.model_copy(update={"resource_domains": (
        profile.resource_domains[0].model_copy(update={"node_ids": nodes}),)})
    policy = context.policy.model_copy(update={"entries": (
        PurposeLaunchPolicyV3(purpose="application-worker", profile_sha256=full_launch_profile_digest(profile)),)})
    root = canonical_pool_launch_policy_digest(policy)
    context = replace(context, binding=context.binding.model_copy(update={"node_ids": nodes}),
        profiles=(profile.model_copy(update={"controller_authority_sha256": root}),), policy=policy,
        controller_authority=context.controller_authority.model_copy(update={"controller_authority_sha256": root}))
    module = import_module("loom_capacity_executor.typed_launch_renderer")
    with pytest.raises(ValueError, match="profile differs"):
        module.render_typed_signed_launch(context)
