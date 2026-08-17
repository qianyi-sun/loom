import pytest
from pydantic import ValidationError

from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    MultiStepConfig,
    StepConfig,
    TaskConfig,
    TaskMetadata,
    VerifierDefaults,
)


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
