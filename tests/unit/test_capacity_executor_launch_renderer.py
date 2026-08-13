from __future__ import annotations

import base64
import hashlib
from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from pydantic import ValidationError

from loom_capacity_executor.keys import ExecutorOwnershipKey
from loom_capacity_executor.launch_renderer import (
    OperatorGenericTresMappingV2,
    OperatorLaunchProfileV2,
    OperatorResourceDomainV2,
    TrustedLaunchContextV2,
    TrustedLaunchRenderError,
    canonical_launch_policy_digest,
    render_launch_request,
    render_signed_launch,
)
from loom_capacity_executor.slurm_contracts import SlurmExecutableIdentityV2
from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableIntentBindingV2,
    ExecutionFenceV2,
    PoolControllerAuthorityV2,
    canonical_executable_bytes,
)
from loom_capacity_manager.ownership import (
    OwnershipKeyring,
    public_key_fingerprint,
    verify_executable_ownership,
)

_SUBMITTED_AT = datetime(2026, 8, 13, 16, 0, tzinfo=UTC)


def operator_profile_fixture() -> OperatorLaunchProfileV2:
    profile = OperatorLaunchProfileV2(
        pool_id="oldlab",
        pool_generation=8,
        profile_id="oldlab-a100",
        profile_generation=9,
        profile_digest="8" * 64,
        shape_id="oldlab-a100-one-slot",
        concurrency_slots=1,
        controller_authority_sha256="0" * 64,
        slurm_cluster="oldlab",
        controller_host="ctl.oldlab.internal",
        partition="loom",
        association="loom-executor",
        submitter="loom-oldlab",
        qos="loom",
        job_name_prefix="loom-worker",
        resource_domains=(
            OperatorResourceDomainV2(
                domain_id="oldlab-x86-a100",
                node_ids=("oldlab-5", "oldlab-6"),
                features=("avx2", "x86_64"),
            ),
        ),
        cpus=16,
        resources=ResourceVectorV1(
            slots=1,
            cpu_millicores=16_000,
            memory_bytes=68_719_476_736,
            gpu_count=2,
            generic={"fpga": 1, "gpu_a100": 2},
        ),
        generic_tres=(
            OperatorGenericTresMappingV2(
                resource_name="fpga",
                tres_name="gres/fpga",
            ),
            OperatorGenericTresMappingV2(
                resource_name="gpu_a100",
                tres_name="gres/gpu:a100",
            ),
        ),
        time_limit_seconds=3_600,
        launcher=SlurmExecutableIdentityV2(
            path="/opt/loom/bin/trusted-worker-launcher",
            sha256="2" * 64,
            owner_uid=0,
        ),
        trusted_launcher_release_sha256="3" * 64,
        image_digest="registry.internal/loom/worker@sha256:" + "4" * 64,
    )
    return profile.model_copy(
        update={
            "controller_authority_sha256": canonical_launch_policy_digest(profile),
        }
    )


def intent_fixture(profile: OperatorLaunchProfileV2) -> ExecutableIntentBindingV2:
    return ExecutableIntentBindingV2(
        execution=ExecutionFenceV2(
            authority_incarnation=UUID(int=1),
            writer_epoch=2,
            configuration_epoch=3,
            execution_epoch=4,
            execution_manifest_sha256="5" * 64,
            execution_state="active",
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
            trusted_fleet_release_sha256=profile.trusted_launcher_release_sha256,
            allocation_epoch=5,
        ),
        tranche_id=UUID(int=10),
        intent_id=UUID("00000000-0000-0000-0000-000000000101"),
        shape_instance_id="shape-oldlab-a100-0001",
        subject_id=UUID(int=11),
        subject_incarnation=UUID(int=12),
        account_id="owner-1",
        tier_id="development",
        candidate=CandidateBindingV2(
            algorithm="source-sha256",
            identity="6" * 64,
            publication_sha256="7" * 64,
        ),
        candidate_generation=6,
        deployment_generation=7,
        pool_id=profile.pool_id,
        pool_generation=profile.pool_generation,
        executor_id="oldlab-executor",
        executor_incarnation=UUID(int=13),
        shape_id=profile.shape_id,
        profile_id=profile.profile_id,
        profile_generation=profile.profile_generation,
        profile_digest=profile.profile_digest,
        concurrency_slots=profile.concurrency_slots,
        resources=profile.resources,
        node_ids=("oldlab-5",),
    )


def launch_context_fixture(
    *,
    candidate_diagnostic: str = "candidate display value",
    display_diagnostic: str = "owner/project display value",
) -> TrustedLaunchContextV2:
    profile = operator_profile_fixture()
    private_key = Ed25519PrivateKey.from_private_bytes(bytes(range(32)))
    return TrustedLaunchContextV2(
        binding=intent_fixture(profile),
        profile=profile,
        controller_authority=PoolControllerAuthorityV2(
            pool_id="oldlab",
            controller_authority_sha256=profile.controller_authority_sha256,
        ),
        ownership_key=ExecutorOwnershipKey(
            signing_key_id="oldlab-key-1",
            private_key=private_key,
            public_key_sha256=public_key_fingerprint(private_key.public_key()),
        ),
        submitted_at=_SUBMITTED_AT,
        candidate_diagnostic=candidate_diagnostic,
        display_diagnostic=display_diagnostic,
    )


def test_render_round_trips_exact_resources_and_operator_authority() -> None:
    context = launch_context_fixture()
    request = render_launch_request(context)

    assert request.cluster == "oldlab"
    assert request.controller_host == "ctl.oldlab.internal"
    assert request.partition == "loom"
    assert request.account == "loom-executor"
    assert request.submitter == "loom-oldlab"
    assert request.qos == "loom"
    assert request.operation_id == UUID("00000000-0000-0000-0000-000000000101")
    assert request.cpus == 16
    assert request.memory_bytes == 68_719_476_736
    assert request.gpus == 2
    assert tuple((item.name, item.value) for item in request.generic_tres) == (
        ("gres/fpga", 1),
        ("gres/gpu:a100", 2),
    )
    assert request.nodes == ("oldlab-5",)
    assert request.features == ("avx2", "x86_64")
    assert request.time_limit_seconds == 3_600
    assert request.launcher == context.profile.launcher
    assert request.launcher_release_sha256 == "3" * 64
    assert request.image_digest == "registry.internal/loom/worker@sha256:" + "4" * 64


@pytest.mark.parametrize("value", ["$(id)", "a;scancel 1", "a\n--uid=root"])
def test_candidate_and_display_text_never_enters_scheduler_or_launcher_argv(value: str) -> None:
    request = render_launch_request(
        launch_context_fixture(candidate_diagnostic=value, display_diagnostic=value)
    )

    assert value not in request.job_name
    diagnostic_digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
    assert request.job_name == f"loom-worker-{diagnostic_digest}-{diagnostic_digest}"
    assert all(value not in argument for argument in request.trusted_launcher_argv())
    assert (
        request.ownership_token
        == render_launch_request(
            launch_context_fixture(candidate_diagnostic="other", display_diagnostic="other")
        ).ownership_token
    )
    assert len(request.job_name) <= 128


def test_render_returns_task6_request_and_signed_complete_ownership_evidence() -> None:
    context = launch_context_fixture()
    rendered = render_signed_launch(context)

    assert render_launch_request(context) == rendered.request
    assert rendered.ownership_proof.metadata.binding == context.binding
    assert (
        rendered.ownership_proof.metadata.controller_authority_sha256
        == canonical_launch_policy_digest(context.profile)
    )
    assert rendered.ownership_proof.metadata.trusted_launcher_sha256 == "2" * 64
    assert rendered.ownership_proof.metadata.slurm_cluster == "oldlab"
    assert rendered.ownership_proof.metadata.submitter_identity == "loom-oldlab"
    assert rendered.ownership_proof.metadata.association == "loom-executor"
    assert rendered.ownership_proof.metadata.submitted_at == _SUBMITTED_AT
    expected_token = base64.urlsafe_b64encode(
        hashlib.sha256(canonical_executable_bytes(rendered.ownership_proof)).digest()
    ).rstrip(b"=")
    assert rendered.request.ownership_token == expected_token.decode("ascii")

    public_key = context.ownership_key.private_key.public_key()
    keyring = OwnershipKeyring({"oldlab-key-1": public_key})
    assert verify_executable_ownership(
        rendered.ownership_proof,
        keyring=keyring,
        expected_public_key_sha256=public_key_fingerprint(public_key),
    )
    assert not verify_executable_ownership(
        rendered.ownership_proof,
        keyring=keyring,
        expected_public_key_sha256="f" * 64,
    )


def test_proof_verification_rejects_every_changed_binding_group_and_unregistered_key() -> None:
    context = launch_context_fixture()
    proof = render_signed_launch(context).ownership_proof
    binding = proof.metadata.binding
    fingerprint = context.ownership_key.public_key_sha256
    keyring = OwnershipKeyring(
        {proof.signing_key_id: context.ownership_key.private_key.public_key()}
    )

    changed_executions = (
        binding.execution.model_copy(update={"authority_incarnation": UUID(int=91)}),
        binding.execution.model_copy(update={"writer_epoch": 91}),
        binding.execution.model_copy(update={"configuration_epoch": 91}),
        binding.execution.model_copy(update={"allocation_epoch": 91}),
        binding.execution.model_copy(update={"execution_epoch": 91}),
        binding.execution.model_copy(update={"execution_manifest_sha256": "8" * 64}),
        binding.execution.model_copy(update={"execution_state": "drain-only"}),
        binding.execution.model_copy(update={"executable_new_capacity_ceiling": 91}),
        binding.execution.model_copy(update={"executable_new_capacity_rate_per_minute": 91}),
        binding.execution.model_copy(update={"trusted_fleet_release_sha256": "8" * 64}),
    )
    changed_bindings = [
        binding.model_copy(update={"execution": item}) for item in changed_executions
    ]
    changed_bindings.extend(
        (
            binding.model_copy(update={"pool_id": "gb10"}),
            binding.model_copy(update={"pool_generation": 91}),
            binding.model_copy(update={"executor_id": "other-executor"}),
            binding.model_copy(update={"executor_incarnation": UUID(int=92)}),
            binding.model_copy(update={"subject_id": UUID(int=93)}),
            binding.model_copy(update={"subject_incarnation": UUID(int=94)}),
            binding.model_copy(
                update={"candidate": binding.candidate.model_copy(update={"identity": "9" * 64})}
            ),
            binding.model_copy(
                update={
                    "candidate": binding.candidate.model_copy(
                        update={"algorithm": "git-sha1", "identity": "9" * 40}
                    )
                }
            ),
            binding.model_copy(
                update={
                    "candidate": binding.candidate.model_copy(
                        update={"publication_sha256": "9" * 64}
                    )
                }
            ),
            binding.model_copy(update={"candidate_generation": 91}),
            binding.model_copy(update={"deployment_generation": 91}),
            binding.model_copy(update={"tranche_id": UUID(int=95)}),
            binding.model_copy(update={"intent_id": UUID(int=96)}),
            binding.model_copy(update={"shape_instance_id": "shape-other"}),
            binding.model_copy(update={"shape_id": "shape-other"}),
            binding.model_copy(update={"profile_id": "profile-other"}),
            binding.model_copy(update={"profile_generation": 91}),
            binding.model_copy(update={"profile_digest": "9" * 64}),
            binding.model_copy(update={"concurrency_slots": 2}),
            binding.model_copy(
                update={"resources": binding.resources.model_copy(update={"slots": 2})}
            ),
            binding.model_copy(update={"node_ids": ("oldlab-6",)}),
            binding.model_copy(
                update={"rollout_surge_slots": 1, "old_shape_backing_id": "shape-old"}
            ),
            binding.model_copy(update={"account_id": "owner-2"}),
            binding.model_copy(update={"tier_id": "staging"}),
        )
    )
    changed_metadata = [
        proof.metadata.model_copy(update={"binding": item}) for item in changed_bindings
    ]
    changed_metadata.extend(
        (
            proof.metadata.model_copy(update={"controller_authority_sha256": "9" * 64}),
            proof.metadata.model_copy(update={"trusted_launcher_sha256": "9" * 64}),
            proof.metadata.model_copy(update={"slurm_cluster": "other"}),
            proof.metadata.model_copy(update={"submitter_identity": "other"}),
            proof.metadata.model_copy(update={"association": "other"}),
            proof.metadata.model_copy(
                update={"submitted_at": datetime(2026, 8, 13, 16, 1, tzinfo=UTC)}
            ),
        )
    )

    for metadata in changed_metadata:
        tampered = proof.model_copy(update={"metadata": metadata})
        assert not verify_executable_ownership(
            tampered,
            keyring=keyring,
            expected_public_key_sha256=fingerprint,
        )

    other_key = Ed25519PrivateKey.generate()
    assert not verify_executable_ownership(
        proof,
        keyring=OwnershipKeyring({"other-key": other_key.public_key()}),
        expected_public_key_sha256=fingerprint,
    )


def test_launcher_content_and_trusted_release_cannot_substitute_for_each_other() -> None:
    context = launch_context_fixture()
    proof = render_signed_launch(context).ownership_proof
    public_key = context.ownership_key.private_key.public_key()
    keyring = OwnershipKeyring({proof.signing_key_id: public_key})
    fingerprint = public_key_fingerprint(public_key)

    substituted_launcher = proof.model_copy(
        update={
            "metadata": proof.metadata.model_copy(
                update={
                    "trusted_launcher_sha256": (
                        proof.metadata.binding.execution.trusted_fleet_release_sha256
                    )
                }
            )
        }
    )
    substituted_execution = proof.metadata.binding.execution.model_copy(
        update={"trusted_fleet_release_sha256": proof.metadata.trusted_launcher_sha256}
    )
    substituted_release = proof.model_copy(
        update={
            "metadata": proof.metadata.model_copy(
                update={
                    "binding": proof.metadata.binding.model_copy(
                        update={"execution": substituted_execution}
                    )
                }
            )
        }
    )

    assert proof.metadata.trusted_launcher_sha256 == context.profile.launcher.sha256
    assert (
        proof.metadata.binding.execution.trusted_fleet_release_sha256
        == context.profile.trusted_launcher_release_sha256
    )
    assert proof.metadata.trusted_launcher_sha256 != (
        proof.metadata.binding.execution.trusted_fleet_release_sha256
    )
    assert not verify_executable_ownership(
        substituted_launcher,
        keyring=keyring,
        expected_public_key_sha256=fingerprint,
    )
    assert not verify_executable_ownership(
        substituted_release,
        keyring=keyring,
        expected_public_key_sha256=fingerprint,
    )


def test_existing_profile_digest_and_controller_commitment_remain_distinct() -> None:
    context = launch_context_fixture()

    assert context.binding.profile_digest == "8" * 64
    assert context.profile.profile_digest == "8" * 64
    assert context.controller_authority.controller_authority_sha256 != (
        context.binding.profile_digest
    )
    assert canonical_launch_policy_digest(context.profile) == (
        context.controller_authority.controller_authority_sha256
    )
    assert render_launch_request(context).operation_id == context.binding.intent_id


def test_caller_cannot_replace_policy_under_registered_controller_authority() -> None:
    context = launch_context_fixture()
    changed_profile = context.profile.model_copy(update={"partition": "attacker"})
    changed_context = replace(
        context,
        profile=changed_profile,
    )

    with pytest.raises(TrustedLaunchRenderError, match="controller authority"):
        render_launch_request(changed_context)


def test_profile_scoped_fields_must_match_manager_binding() -> None:
    context = launch_context_fixture()
    profile = context.profile
    two_slot_resources = profile.resources.model_copy(update={"slots": 2})
    changed_profiles = (
        profile.model_copy(update={"profile_id": "other-profile"}),
        profile.model_copy(update={"profile_generation": 90}),
        profile.model_copy(update={"profile_digest": "9" * 64}),
        profile.model_copy(update={"shape_id": "other-shape"}),
        profile.model_copy(update={"concurrency_slots": 2, "resources": two_slot_resources}),
        profile.model_copy(
            update={
                "resources": profile.resources.model_copy(update={"memory_bytes": 34_359_738_368})
            }
        ),
    )

    for changed_profile in changed_profiles:
        with pytest.raises(TrustedLaunchRenderError, match="profile identity"):
            render_launch_request(replace(context, profile=changed_profile))


def test_controller_commitment_field_and_execution_release_are_cross_checked() -> None:
    context = launch_context_fixture()
    changed_commitment = context.profile.model_copy(
        update={"controller_authority_sha256": "9" * 64}
    )
    with pytest.raises(TrustedLaunchRenderError, match="controller authority"):
        render_launch_request(replace(context, profile=changed_commitment))

    changed_release = context.profile.model_copy(
        update={"trusted_launcher_release_sha256": "9" * 64}
    )
    changed_policy_digest = canonical_launch_policy_digest(changed_release)
    changed_release = changed_release.model_copy(
        update={"controller_authority_sha256": changed_policy_digest}
    )
    changed_authority = context.controller_authority.model_copy(
        update={"controller_authority_sha256": changed_policy_digest}
    )
    with pytest.raises(TrustedLaunchRenderError, match="execution fence"):
        render_launch_request(
            replace(
                context,
                profile=changed_release,
                controller_authority=changed_authority,
            )
        )


def test_launch_policy_digest_excludes_controller_commitment() -> None:
    profile = operator_profile_fixture()
    changed_commitment = profile.model_copy(update={"controller_authority_sha256": "9" * 64})

    assert canonical_launch_policy_digest(changed_commitment) == (
        canonical_launch_policy_digest(profile)
    )


def test_distinct_profiles_and_shapes_share_one_pool_controller_authority() -> None:
    context = launch_context_fixture()
    second_resources = context.profile.resources.model_copy(
        update={"slots": 2, "cpu_millicores": 8_000}
    )
    second_profile = context.profile.model_copy(
        update={
            "profile_id": "oldlab-a100-batch",
            "profile_generation": 10,
            "profile_digest": "9" * 64,
            "shape_id": "oldlab-a100-two-slot",
            "concurrency_slots": 2,
            "cpus": 8,
            "resources": second_resources,
        }
    )
    second_context = replace(
        context,
        binding=intent_fixture(second_profile),
        profile=second_profile,
    )

    assert canonical_launch_policy_digest(second_profile) == (
        context.controller_authority.controller_authority_sha256
    )
    request = render_launch_request(second_context)
    assert request.cpus == 8
    assert request.operation_id == second_context.binding.intent_id


def test_every_pool_wide_scheduler_field_is_committed_by_launch_policy_digest() -> None:
    context = launch_context_fixture()
    profile = context.profile
    other_domain_features = (
        profile.resource_domains[0].model_copy(update={"features": ("x86_64",)}),
    )
    other_domain_id = (
        profile.resource_domains[0].model_copy(update={"domain_id": "other-domain"}),
    )
    other_domain_nodes = (
        profile.resource_domains[0].model_copy(update={"node_ids": ("oldlab-5",)}),
    )
    other_tres_name = (
        OperatorGenericTresMappingV2(resource_name="fpga", tres_name="gres/other"),
        profile.generic_tres[1],
    )
    other_resource_name = (
        OperatorGenericTresMappingV2(resource_name="other", tres_name="gres/fpga"),
        profile.generic_tres[1],
    )
    changed_fields: tuple[tuple[str, object], ...] = (
        ("pool_id", "gb10"),
        ("pool_generation", 90),
        ("slurm_cluster", "other"),
        ("controller_host", "other.internal"),
        ("partition", "other"),
        ("association", "other"),
        ("submitter", "other"),
        ("qos", "other"),
        ("job_name_prefix", "other"),
        ("resource_domains", other_domain_features),
        ("resource_domains", other_domain_id),
        ("resource_domains", other_domain_nodes),
        ("generic_tres", other_tres_name),
        ("generic_tres", other_resource_name),
        ("time_limit_seconds", 7_200),
        (
            "launcher",
            profile.launcher.model_copy(update={"path": "/opt/loom/bin/other-launcher"}),
        ),
        ("launcher", profile.launcher.model_copy(update={"sha256": "9" * 64})),
        ("launcher", profile.launcher.model_copy(update={"owner_uid": 1})),
        ("trusted_launcher_release_sha256", "9" * 64),
        ("image_digest", "registry.internal/loom/worker@sha256:" + "9" * 64),
    )

    assert canonical_launch_policy_digest(profile) == (
        context.controller_authority.controller_authority_sha256
    )
    for field, value in changed_fields:
        changed = profile.model_copy(update={field: value})
        with pytest.raises(TrustedLaunchRenderError, match="launch policy digest"):
            render_launch_request(
                TrustedLaunchContextV2(
                    binding=context.binding,
                    profile=changed,
                    controller_authority=context.controller_authority,
                    ownership_key=context.ownership_key,
                    submitted_at=context.submitted_at,
                    candidate_diagnostic=context.candidate_diagnostic,
                    display_diagnostic=context.display_diagnostic,
                )
            )


@pytest.mark.parametrize(
    "path",
    (
        "/opt//loom/bin/trusted-worker-launcher",
        "/opt/./loom/bin/trusted-worker-launcher",
        "/opt/loom/../bin/trusted-worker-launcher",
    ),
)
def test_operator_profile_rejects_noncanonical_absolute_launcher_paths(path: str) -> None:
    profile = operator_profile_fixture()
    payload = profile.model_dump(mode="python")
    payload["launcher"] = profile.launcher.model_copy(update={"path": path})

    with pytest.raises(ValidationError, match="canonical"):
        OperatorLaunchProfileV2.model_validate(payload)


def test_profile_rejects_resource_translation_or_node_domain_ambiguity() -> None:
    profile = operator_profile_fixture()
    payload = profile.model_dump(mode="python")
    payload["cpus"] = 15
    with pytest.raises(ValidationError, match="CPU"):
        OperatorLaunchProfileV2.model_validate(payload)

    payload = profile.model_dump(mode="python")
    payload["generic_tres"] = profile.generic_tres[:1]
    with pytest.raises(ValidationError, match="generic"):
        OperatorLaunchProfileV2.model_validate(payload)

    payload = profile.model_dump(mode="python")
    payload["resource_domains"] = (
        profile.resource_domains[0],
        OperatorResourceDomainV2(
            domain_id="other-domain",
            node_ids=("oldlab-5",),
            features=("x86_64",),
        ),
    )
    with pytest.raises(ValidationError, match="multiple resource domains"):
        OperatorLaunchProfileV2.model_validate(payload)


def test_key_rotation_retains_old_verification_key_for_nonterminal_proof() -> None:
    context = launch_context_fixture()
    proof = render_signed_launch(context).ownership_proof
    old_public_key = context.ownership_key.private_key.public_key()
    new_private_key = Ed25519PrivateKey.generate()
    keyring = OwnershipKeyring(
        {
            proof.signing_key_id: old_public_key,
            "oldlab-key-2": new_private_key.public_key(),
        }
    )

    assert verify_executable_ownership(
        proof,
        keyring=keyring,
        expected_public_key_sha256=public_key_fingerprint(old_public_key),
    )
