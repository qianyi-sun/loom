import pytest
from pydantic import ValidationError

from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    ProcessPhaseV1,
)
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    MultiStepConfig,
    StepConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)
from tests.support.execution_image_admission import signed_image_admission_bundle


def _minimal_config(steps: list[StepConfig] | None = None) -> TaskConfig:
    return TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="t1", name="t1"),
        environment=EnvironmentConfig(os="linux", docker_image="alpine"),
        agent=AgentDefaults(name="litellm-agent"),
        verifier=VerifierDefaults(name="pytest"),
        steps=steps or [],
    )


def test_minimal_config_parses():
    cfg = _minimal_config()
    assert cfg.schema_version == "1"
    assert cfg.steps == []


def test_task_config_round_trips_required_agent_capabilities() -> None:
    raw = _minimal_config().model_dump(mode="json")
    raw["required_agent_capabilities"] = ["workspace_exec"]

    cfg = TaskConfig.model_validate(raw)

    assert cfg.required_agent_capabilities == frozenset({"workspace_exec"})
    assert cfg.model_dump(mode="json")["required_agent_capabilities"] == [
        "workspace_exec",
    ]


def test_task_config_serializes_required_agent_capabilities_canonically() -> None:
    cfg = _minimal_config().model_copy(
        update={
            "required_agent_capabilities": frozenset(
                {
                    "workspace_exec",
                    "browser",
                    "filesystem",
                    "network_proxy",
                    "database",
                    "shell",
                },
            ),
        },
    )

    assert cfg.model_dump(mode="json")["required_agent_capabilities"] == [
        "browser",
        "database",
        "filesystem",
        "network_proxy",
        "shell",
        "workspace_exec",
    ]


def test_multi_step_config_weights_optional_unless_weighted():
    _minimal_config()
    ms = MultiStepConfig(reward_strategy="mean")
    assert ms.weights is None


def test_multi_step_weighted_requires_weights():
    with pytest.raises(ValidationError):
        MultiStepConfig(reward_strategy="weighted")


def test_service_execution_binding_is_strict_and_matches_task_resources() -> None:
    task_image = "registry.example/task@sha256:" + "a" * 64
    runtime_image = "registry.example/runtime@sha256:" + "b" * 64
    resources = ContainerResourcesV1(
        cpu_millis=1000,
        memory_mib=1024,
        ephemeral_storage_mib=2048,
    )
    plan = ExecutionRuntimePlanV1(
        candidate_sha="1" * 40,
        task_revision_sha256="sha256:" + "2" * 64,
        command_identity_sha256="sha256:" + "3" * 64,
        execution_class_id="linux-amd64-cpu-pod-v1",
        composition="init_payload",
        task_image_ref=task_image,
        runtime_image_ref=runtime_image,
        runtime_binary_sha256="sha256:" + "4" * 64,
        image_admission=signed_image_admission_bundle((task_image, runtime_image)),
        task_resources=resources,
        workspace_mib=1024,
        runtime_volume_mib=32,
        main=ProcessPhaseV1(
            role="agent",
            argv=("/bin/true",),
            working_directory="/workspace",
            timeout_seconds=60,
        ),
        verifier_execution="in_attempt",
        verifier=ProcessPhaseV1(
            role="verifier",
            argv=("/bin/true",),
            working_directory="/workspace",
            timeout_seconds=60,
        ),
    )
    raw = _minimal_config().model_dump(mode="json")
    raw["environment"].update(
        {
            "docker_image": task_image,
            "cpus": 1,
            "memory_mb": 1024,
            "storage_mb": 2048,
            "baseline_network_policy": {"kind": "gateway-only"},
            "network_policies_supported": ["gateway-only"],
        }
    )
    raw["service_execution"] = {
        "schema_version": "loom.task-service-execution.v1",
        "logical_pool_id": "nebius-cpu",
        "runtime_template": plan.model_dump(mode="json", exclude={"task_revision_sha256"}),
    }

    task = TaskConfig.model_validate(raw)

    assert task.service_execution is not None
    assert "task_revision_sha256" not in task.service_execution.runtime_template
    assert task.environment.baseline_network_policy.kind == "gateway-only"
    with pytest.raises(ValidationError, match="resources do not match"):
        TaskConfig.model_validate(
            {
                **raw,
                "environment": {**raw["environment"], "memory_mb": 2048},
            }
        )
