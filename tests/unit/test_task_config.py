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


def test_multi_step_config_weights_optional_unless_weighted():
    _minimal_config()
    ms = MultiStepConfig(reward_strategy="mean")
    assert ms.weights is None


def test_multi_step_weighted_requires_weights():
    with pytest.raises(ValidationError):
        MultiStepConfig(reward_strategy="weighted")
