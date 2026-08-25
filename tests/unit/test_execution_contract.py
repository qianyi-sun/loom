from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from loom.execution_contract import (
    NEBIUS_CPU_EXECUTION_CLASS_V1,
    ExecutionClassV1,
    ExecutionTargetV1,
    ExecutionTopologyV1,
    ImageMaterialization,
    NetworkAccess,
    VerifierTopology,
    WorkloadRequirementsV1,
    evaluate_execution_admission,
    workload_requirements_from_task,
)
from loom.models.task import TaskConfig


def _requirements(**updates: object) -> WorkloadRequirementsV1:
    values: dict[str, object] = {
        "operating_system": "linux",
        "cpu_architecture": "x86_64",
        "gpu_vendor": "none",
        "gpu_count": 0,
        "cpu_millis": 2_000,
        "memory_mib": 8_192,
        "ephemeral_storage_mib": 20_480,
        "isolation_level": "sandboxed_runtime",
        "network_access": "gateway_only",
        "image_materialization": "immutable_oci",
        "image_ref": "registry.example/loom/task@sha256:" + "a" * 64,
        "sidecar_count": 0,
        "verifier_topology": "in_attempt",
        "custom_dns": False,
        "extra_hosts": False,
        "tmpfs": False,
        "privileged": False,
        "host_path": False,
        "host_network": False,
        "nested_containers": False,
        "host_devices": False,
        "host_specialized": False,
    }
    values.update(updates)
    return WorkloadRequirementsV1.model_validate(values)


def test_nebius_class_is_provider_neutral_and_admits_exact_cpu_contract() -> None:
    decision = evaluate_execution_admission(
        _requirements(),
        NEBIUS_CPU_EXECUTION_CLASS_V1,
    )
    assert decision.compatible is True
    assert decision.reasons == ()
    assert "provider" not in ExecutionClassV1.model_fields
    assert "provider" not in WorkloadRequirementsV1.model_fields


def test_provider_binding_lives_on_regional_execution_target() -> None:
    target = ExecutionTargetV1(
        target_id="nebius-eu-north1-production",
        logical_pool_id="nebius-cpu",
        execution_class_id="linux-amd64-cpu-pod-v1",
        environment="production",
        provider="nebius",
        region="eu-north1",
        failure_domain="eu-north1-primary",
        data_residency="eu",
        health_role="primary",
        health_check_id="nebius-eu-north1-production",
        health_check_interval_seconds=30,
        health_stale_after_seconds=90,
    )
    assert target.provider == "nebius"
    assert target.logical_pool_id == "nebius-cpu"


def test_topology_requires_environment_isolation_and_two_production_regions() -> None:
    base = {
        "schema_version": "loom.execution-target.v1",
        "logical_pool_id": "nebius-cpu",
        "execution_class_id": "linux-amd64-cpu-pod-v1",
        "provider": "nebius",
        "region": "eu-north1",
        "failure_domain": "north",
        "data_residency": "eu",
        "health_role": "primary",
        "health_check_interval_seconds": 30,
        "health_stale_after_seconds": 90,
    }

    def target(target_id: str, environment: str, **updates: object) -> dict[str, object]:
        values: dict[str, object] = {
            **base,
            "target_id": target_id,
            "environment": environment,
            "health_check_id": target_id,
        }
        values.update(updates)
        return values

    topology = ExecutionTopologyV1.model_validate(
        {
            "logical_pool_id": "nebius-cpu",
            "execution_class_id": "linux-amd64-cpu-pod-v1",
            "placement_policy": "environment-local-health-first",
            "targets": [
                target("nebius-dev", "development"),
                target("nebius-staging", "staging"),
                target("nebius-prod-north", "production"),
                target(
                    "nebius-prod-west",
                    "production",
                    region="eu-west1",
                    failure_domain="west",
                    health_role="secondary",
                ),
            ],
        }
    )
    assert len(topology.targets) == 4

    invalid = topology.model_dump()
    invalid["targets"] = [
        row for row in invalid["targets"] if row["target_id"] != "nebius-prod-west"
    ]
    with pytest.raises(ValidationError, match="at least 4 items"):
        ExecutionTopologyV1.model_validate(invalid)


def test_unknown_capability_fields_fail_closed() -> None:
    values = _requirements().model_dump()
    values["host_magic"] = True
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        WorkloadRequirementsV1.model_validate(values)


def test_shared_kernel_execution_class_is_never_valid_for_service_work() -> None:
    values = NEBIUS_CPU_EXECUTION_CLASS_V1.model_dump()
    values["isolation_level"] = "shared_kernel"
    with pytest.raises(ValidationError, match="shared-kernel Pods"):
        ExecutionClassV1.model_validate(values)


@pytest.mark.parametrize(
    ("updates", "reason"),
    [
        ({"operating_system": "windows"}, "operating_system_unsupported"),
        ({"cpu_architecture": "arm64"}, "cpu_architecture_unsupported"),
        ({"gpu_vendor": "nvidia", "gpu_count": 1}, "gpu_unsupported"),
        ({"isolation_level": "dedicated_ephemeral_node"}, "isolation_level_unsupported"),
        ({"network_access": "unrestricted_public"}, "network_access_unsupported"),
        (
            {"image_materialization": "mutable_oci", "image_ref": "ubuntu:latest"},
            "immutable_image_required",
        ),
        (
            {"image_materialization": "task_dockerfile", "image_ref": None},
            "task_image_build_unsupported",
        ),
        ({"sidecar_count": 9}, "sidecar_limit_exceeded"),
        ({"cpu_millis": None}, "cpu_limit_required"),
        ({"memory_mib": None}, "memory_limit_required"),
        ({"ephemeral_storage_mib": None}, "ephemeral_storage_limit_required"),
        ({"custom_dns": True}, "custom_dns_unsupported"),
        ({"extra_hosts": True}, "extra_hosts_unsupported"),
        ({"privileged": True}, "privileged_unsupported"),
        ({"host_path": True}, "host_path_unsupported"),
        ({"host_network": True}, "host_network_unsupported"),
        ({"nested_containers": True}, "nested_containers_unsupported"),
        ({"host_devices": True}, "host_devices_unsupported"),
        ({"host_specialized": True}, "host_specialized_unsupported"),
    ],
)
def test_material_rejection_reasons_are_structured(
    updates: dict[str, object],
    reason: str,
) -> None:
    decision = evaluate_execution_admission(
        _requirements(**updates),
        NEBIUS_CPU_EXECUTION_CLASS_V1,
    )
    assert decision.compatible is False
    assert reason in {item.code for item in decision.reasons}


def test_separate_verifier_rejects_when_class_cannot_create_second_execution() -> None:
    values = NEBIUS_CPU_EXECUTION_CLASS_V1.model_dump()
    values["supports_separate_verifier"] = False
    execution_class = ExecutionClassV1.model_validate(values)
    decision = evaluate_execution_admission(
        _requirements(verifier_topology="separate_execution"),
        execution_class,
    )
    assert {reason.code for reason in decision.reasons} == {
        "separate_verifier_unsupported",
    }


def test_explicit_execution_class_resource_maxima_are_enforced() -> None:
    values = NEBIUS_CPU_EXECUTION_CLASS_V1.model_dump()
    values.update(
        {
            "maximum_cpu_millis": 1_000,
            "maximum_memory_mib": 4_096,
            "maximum_ephemeral_storage_mib": 10_000,
        }
    )
    decision = evaluate_execution_admission(
        _requirements(),
        ExecutionClassV1.model_validate(values),
    )
    assert {reason.code for reason in decision.reasons} == {
        "cpu_limit_exceeded",
        "memory_limit_exceeded",
        "ephemeral_storage_limit_exceeded",
    }


def test_task_projection_preserves_mutable_build_and_network_incompatibilities() -> None:
    task = TaskConfig.model_validate(
        {
            "task": {"id": "contract-test", "name": "Contract test"},
            "environment": {
                "os": "linux",
                "cpu_arch": "x86_64",
                "gpu_vendor": "none",
                "dockerfile": "Dockerfile",
                "cpus": 2,
                "memory_mb": 4096,
                "storage_mb": 8192,
                "sidecars": [{"name": "db", "docker_image": "postgres:16"}],
            },
            "agent": {"name": "oracle"},
            "verifier": {"name": "script", "env_mode": "separate"},
        }
    )
    requirements = workload_requirements_from_task(task)
    assert requirements.image_materialization == ImageMaterialization.TASK_DOCKERFILE
    assert requirements.network_access == NetworkAccess.UNRESTRICTED_PUBLIC
    assert requirements.verifier_topology == VerifierTopology.SEPARATE_EXECUTION
    assert requirements.sidecar_count == 1
    decision = evaluate_execution_admission(
        requirements,
        NEBIUS_CPU_EXECUTION_CLASS_V1,
    )
    assert {reason.code for reason in decision.reasons} >= {
        "immutable_image_required",
        "network_access_unsupported",
        "task_image_build_unsupported",
    }


def test_contracts_do_not_accept_silent_schema_version_downgrade() -> None:
    values = deepcopy(_requirements().model_dump())
    values["schema_version"] = "loom.workload-requirements.v0"
    with pytest.raises(ValidationError, match=r"loom\.workload-requirements\.v1"):
        WorkloadRequirementsV1.model_validate(values)
