"""Task capability declarations record requirements without granting authority."""

from pathlib import PurePosixPath

import pytest
from pydantic import ValidationError

from loom.execution_contract import (
    NEBIUS_CPU_EXECUTION_CLASS_V1,
    ExecutionClassV1,
    evaluate_execution_admission,
    workload_requirements_from_task,
)
from loom.models.task import EnvironmentConfig, TaskConfig
from loom.service_execution_materialization import automatic_service_execution_rejections
from loom.terminal_bench_normalize import normalize_terminal_bench_task_toml
from tests.unit.test_service_execution_materialization import _provenance
from tests.unit.test_service_execution_terminus_plan import _inputs


@pytest.mark.parametrize("capability", [
    "nested_docker", "singularity_mounts", "isolated_kernel_settings",
    "external_cluster", "pkcs11_authentication", "dpdk_networking",
])
def test_declared_capability_survives_freezing_and_is_rejected_at_both_admission_boundaries(capability):
    task, trial, _ = _inputs()
    raw = task.model_dump(mode="json")
    raw["environment"]["execution_requirements"] = {"capabilities": [capability]}
    task = TaskConfig.model_validate(raw)

    requirements = workload_requirements_from_task(task)
    assert requirements.model_dump(mode="json")["execution_requirements"]["capabilities"] == [capability]
    decision = evaluate_execution_admission(requirements, NEBIUS_CPU_EXECUTION_CLASS_V1)
    assert not decision.compatible
    assert f"{capability}_unqualified" in {reason.code for reason in decision.reasons}
    reasons = automatic_service_execution_rejections(task, trial, source_provenance=_provenance())
    assert f"{capability}_unqualified" in reasons


def test_host_escape_flags_cannot_be_enabled_by_declaring_nested_runtime():
    raw = NEBIUS_CPU_EXECUTION_CLASS_V1.model_dump(mode="json")
    for flag in ("permits_nested_containers", "permits_host_devices", "permits_privileged"):
        with pytest.raises(ValidationError, match="host-escape"):
            ExecutionClassV1.model_validate({**raw, flag: True})


def test_prerequisite_references_are_declarations_not_live_qualification():
    from loom.execution_requirements import execution_requirement_diagnostics

    env = EnvironmentConfig.model_validate({
        "os": "linux", "execution_requirements": {
            "capabilities": ["external_cluster"],
            "prerequisites": [
                {"name": "cluster", "kind": "endpoint"},
                {"name": "cluster_auth", "kind": "managed_secret", "reference": "k8s-secret://team/cluster-auth"},
            ],
        },
    })
    diagnostics = execution_requirement_diagnostics(env.execution_requirements)
    assert {item.code for item in diagnostics} == {
        "external_cluster_unqualified", "execution_prerequisite_missing", "execution_prerequisite_unverified",
    }
    assert any(item.field == "prerequisites.cluster" for item in diagnostics)
    assert any(item.field == "prerequisites.cluster_auth" for item in diagnostics)


@pytest.mark.parametrize("requirement", [
    {"capabilities": ["privileged"]},
    {"prerequisites": [{"name": "auth", "kind": "managed_secret", "reference": "-----BEGIN PRIVATE KEY-----"}]},
    {"prerequisites": [{"name": "auth", "kind": "managed_secret", "value": "password"}]},
    {"capabilities": ["nested_docker", "nested_docker"]},
    {"prerequisites": [{"name": "x", "kind": "fixture"}, {"name": "x", "kind": "device"}]},
])
def test_unbounded_unknown_or_secret_literal_declarations_are_rejected(requirement):
    with pytest.raises(ValidationError):
        EnvironmentConfig.model_validate({"os": "linux", "execution_requirements": requirement})


def test_harbor_normalization_preserves_capabilities_and_does_not_infer_from_text():
    raw = {
        "task": {"name": "download-docker-script"},
        "environment": {"execution_requirements": {"capabilities": ["nested_docker"]}},
    }
    task = TaskConfig.model_validate(normalize_terminal_bench_task_toml(raw))
    assert task.environment.execution_requirements.capabilities == ("nested_docker",)
    plain = TaskConfig.model_validate(normalize_terminal_bench_task_toml({"task": {"name": "download-docker-script"}}))
    assert plain.environment.execution_requirements is None
    assert plain.environment.workdir == PurePosixPath("/app")
