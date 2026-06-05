from pathlib import PurePosixPath

import pytest
from pydantic import ValidationError

from loom.models.networking import NoNetwork, Public
from loom.models.task import (
    AgentOverrides,
    StepConfig,
    StepNetworkPlan,
    VerifierOverrides,
)


def test_step_config_minimal():
    s = StepConfig(name="main")
    assert s.instruction_file == PurePosixPath("instruction.md")
    assert s.artifacts == []
    assert s.min_reward is None


def test_step_config_min_reward_scalar():
    s = StepConfig(name="main", min_reward=0.5)
    assert s.min_reward == 0.5


def test_step_config_min_reward_per_key():
    s = StepConfig(name="main", min_reward={"passed": 0.7, "speed": 0.5})
    assert isinstance(s.min_reward, dict)
    assert s.min_reward["passed"] == 0.7


def test_step_network_plan_per_phase():
    plan = StepNetworkPlan(agent_phase=NoNetwork(), verifier_phase=Public())
    assert plan.agent_phase == NoNetwork()
    assert plan.verifier_phase == Public()


def test_agent_overrides_partial():
    o = AgentOverrides(timeout_sec=2400)
    assert o.timeout_sec == 2400
    assert o.user is None


def test_verifier_overrides_env_mode():
    o = VerifierOverrides(env_mode="separate")
    assert o.env_mode == "separate"


def test_step_config_immutable():
    s = StepConfig(name="main")
    with pytest.raises(ValidationError):
        s.name = "other"  # type: ignore[misc]
