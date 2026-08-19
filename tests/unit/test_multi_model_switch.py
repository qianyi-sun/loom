"""Unit tests for student/teacher/student model switch (#1380)."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom.agent.terminus2.model_switch import (
    assert_terminus2_switch_contract,
    deterministic_episode_draw,
    install_role_router,
    redact_agent_llm_kwargs,
    role_for_beta_episode,
    role_for_episode,
)
from loom.errors import AgentError
from loom.models.trial import (
    MultiModelSwitchSpec,
    TrialConfig,
    materialize_multi_model_switch_episode,
)
from loom.models.types import ModelSpec
from loom_service.multi_model import (
    apply_multi_model_materialization,
    apply_plan_mode,
    parse_multi_model,
    validate_multi_model_for_batch,
)

_PRIMARY = ModelSpec(provider="openai", name="glm-5.1", source="api")
_SECONDARY = ModelSpec(provider="openai", name="qwen-test", source="api")


def test_multi_model_spec_requires_secondary_when_enabled() -> None:
    with pytest.raises(ValidationError):
        MultiModelSwitchSpec(enabled=True)


def test_multi_model_spec_rejects_switch_above_ceiling() -> None:
    with pytest.raises(ValidationError):
        MultiModelSwitchSpec(
            enabled=True,
            secondary_model=_SECONDARY,
            switch_episode=10,
            episode_ceiling=5,
        )


def test_trial_config_accepts_multi_model() -> None:
    c = TrialConfig(
        agent_name="terminus-2",
        agent_model=_PRIMARY,
        multi_model=MultiModelSwitchSpec(
            enabled=True,
            secondary_model=_SECONDARY,
            switch_episode=3,
            teacher_episodes=2,
        ),
    )
    assert c.multi_model is not None
    assert c.multi_model.switch_episode == 3
    assert c.multi_model.policy == "student_teacher_student"


def test_materialize_persists_k1_and_k2() -> None:
    rng = random.Random(0)
    raw = {
        "enabled": True,
        "secondary_model": _SECONDARY.model_dump(mode="json"),
        "episode_ceiling": 4,
        "teacher_episodes": 2,
    }
    first = materialize_multi_model_switch_episode(raw, rng=rng)
    assert first is not None
    assert first["switch_episode"] in {2, 3, 4}
    assert first["return_switch_episode"] == first["switch_episode"] + 2
    second = materialize_multi_model_switch_episode(first, rng=rng)
    assert second is not None
    assert second["switch_episode"] == first["switch_episode"]
    assert second["return_switch_episode"] == first["return_switch_episode"]


def test_apply_multi_model_materialization_idempotent() -> None:
    cfg = {
        "agent_name": "terminus-2",
        "agent_model": _PRIMARY.model_dump(mode="json"),
        "multi_model": {
            "enabled": True,
            "secondary_model": _SECONDARY.model_dump(mode="json"),
            "switch_episode": 3,
            "episode_ceiling": 50,
        },
    }
    out = apply_multi_model_materialization(cfg)
    assert out["multi_model"]["switch_episode"] == 3
    assert out["multi_model"]["return_switch_episode"] == 5
    assert out["multi_model"]["teacher_episodes"] == 2


def test_role_for_episode_two_cuts() -> None:
    assert role_for_episode(1, first_switch_episode=3, return_switch_episode=5) == "student"
    assert role_for_episode(3, first_switch_episode=3, return_switch_episode=5) == "teacher"
    assert role_for_episode(4, first_switch_episode=3, return_switch_episode=5) == "teacher"
    assert role_for_episode(5, first_switch_episode=3, return_switch_episode=5) == "student"


def test_beta_spec_rejects_k1() -> None:
    with pytest.raises(ValidationError):
        MultiModelSwitchSpec(
            enabled=True,
            policy="beta_mixture",
            secondary_model=_SECONDARY,
            beta=0.6,
            switch_episode=3,
        )


def test_beta_spec_requires_beta() -> None:
    with pytest.raises(ValidationError):
        MultiModelSwitchSpec(
            enabled=True,
            policy="beta_mixture",
            secondary_model=_SECONDARY,
        )


def test_schedule_spec_rejects_beta() -> None:
    with pytest.raises(ValidationError):
        MultiModelSwitchSpec(
            enabled=True,
            secondary_model=_SECONDARY,
            beta=0.6,
        )


def test_materialize_beta_does_not_sample_k1() -> None:
    raw = {
        "enabled": True,
        "policy": "beta_mixture",
        "secondary_model": _SECONDARY.model_dump(mode="json"),
        "beta": 0.6,
        "mix_seed": "42",
    }
    out = materialize_multi_model_switch_episode(raw)
    assert out is not None
    assert out["policy"] == "beta_mixture"
    assert out["beta"] == 0.6
    assert "switch_episode" not in out
    assert "return_switch_episode" not in out


def test_role_for_beta_teacher_if_draw_lt_beta() -> None:
    trial = "11111111-1111-1111-1111-111111111111"
    seed = "abc"
    draw = deterministic_episode_draw(seed, trial, 1)
    role = role_for_beta_episode(1, beta=draw + 1e-12, seed=seed, trial_id=trial)
    assert role == "teacher"
    role_s = role_for_beta_episode(1, beta=draw, seed=seed, trial_id=trial)
    assert role_s == "student"
    assert role_for_beta_episode(1, beta=1.0, seed=seed, trial_id=trial) == "teacher"
    assert role_for_beta_episode(1, beta=0.0, seed=seed, trial_id=trial) == "student"
    assert role_for_beta_episode(1, beta=0.6, seed=seed, trial_id=trial) == (
        role_for_beta_episode(1, beta=0.6, seed=seed, trial_id=trial)
    )


def test_apply_plan_mode_resample_keeps_beta() -> None:
    cfg = {
        "agent_name": "terminus-2",
        "agent_model": _PRIMARY.model_dump(mode="json"),
        "multi_model": {
            "enabled": True,
            "policy": "beta_mixture",
            "secondary_model": _SECONDARY.model_dump(mode="json"),
            "beta": 0.6,
            "mix_seed": "keep-me",
        },
    }
    resampled = apply_plan_mode(cfg, mode="resample")
    assert resampled["multi_model"]["beta"] == 0.6
    assert resampled["multi_model"]["policy"] == "beta_mixture"
    assert "switch_episode" not in resampled["multi_model"]


@dataclass
class _FakeLLM:
    name: str
    calls: list[str] = field(default_factory=list)
    last_kwargs: dict[str, Any] = field(default_factory=dict)

    async def call(self, prompt: str = "", **kwargs: Any) -> str:
        del prompt
        self.last_kwargs = dict(kwargs)
        self.calls.append(self.name)
        return self.name

    def get_model_context_limit(self) -> int:
        return 100 if self.name.endswith("student") else 200

    def get_model_output_limit(self) -> int | None:
        return 10


@dataclass
class _FakeAgent:
    _llm: Any
    _model_name: str
    _n_episodes: int = 0
    _init_llm: Any = True
    _llm_kwargs: dict[str, Any] = field(
        default_factory=lambda: {"api_key": "loom_step_secret", "timeout": 1},
    )


@pytest.mark.asyncio
async def test_role_router_student_teacher_student() -> None:
    student = _FakeLLM(name="openai/glm-5.1-student")
    teacher = _FakeLLM(name="openai/qwen-teacher")
    agent = _FakeAgent(_llm=student, _model_name="openai/glm-5.1-student")
    router = install_role_router(
        agent,
        teacher_model_name="openai/qwen-teacher",
        first_switch_episode=3,
        return_switch_episode=5,
        teacher_llm=teacher,
    )

    agent._n_episodes = 1
    await router.call(prompt="a", previous_response_id="resp-a")
    agent._n_episodes = 2
    await router.call(prompt="b", previous_response_id="resp-b")
    await router.call(prompt="b-retry", previous_response_id="resp-b2")
    assert student.calls == [
        "openai/glm-5.1-student",
        "openai/glm-5.1-student",
        "openai/glm-5.1-student",
    ]
    assert teacher.calls == []
    assert agent._model_name == "openai/glm-5.1-student"

    agent._n_episodes = 3
    await router.call(prompt="c", previous_response_id="resp-student")
    assert teacher.last_kwargs.get("previous_response_id") is None
    agent._n_episodes = 4
    await router.call(prompt="d", previous_response_id="resp-teacher")
    assert teacher.calls == ["openai/qwen-teacher", "openai/qwen-teacher"]

    agent._n_episodes = 5
    await router.call(prompt="e", previous_response_id="resp-teacher-end")
    assert student.last_kwargs.get("previous_response_id") is None
    assert student.calls[-1] == "openai/glm-5.1-student"
    assert [c["to_role"] for c in router.applied_switches] == ["teacher", "student"]
    assert agent._llm_kwargs == {"timeout": 1}
    assert router.get_model_context_limit() == 100


@pytest.mark.asyncio
async def test_role_router_beta_all_teacher() -> None:
    student = _FakeLLM(name="openai/glm-5.1-student")
    teacher = _FakeLLM(name="openai/qwen-teacher")
    agent = _FakeAgent(_llm=student, _model_name="openai/glm-5.1-student")
    router = install_role_router(
        agent,
        teacher_model_name="openai/qwen-teacher",
        mix_mode="beta_mixture",
        beta=1.0,
        seed="s",
        trial_id="trial-1",
        teacher_llm=teacher,
    )
    agent._n_episodes = 1
    await router.call(prompt="a")
    agent._n_episodes = 2
    await router.call(prompt="b")
    assert teacher.calls == ["openai/qwen-teacher", "openai/qwen-teacher"]
    assert student.calls == []
    assert [c["to_role"] for c in router.applied_switches] == ["teacher"]


def test_redact_agent_llm_kwargs() -> None:
    agent = _FakeAgent(_llm=_FakeLLM("x"), _model_name="x")
    redact_agent_llm_kwargs(agent)
    assert "api_key" not in agent._llm_kwargs
    assert agent._llm_kwargs["timeout"] == 1


def test_assert_terminus2_switch_contract_missing_attrs() -> None:
    with pytest.raises(AgentError):
        assert_terminus2_switch_contract(object())


def test_validate_multi_model_rejects_non_terminus() -> None:
    err = validate_multi_model_for_batch(
        trial_config={
            "multi_model": {
                "enabled": True,
                "secondary_model": _SECONDARY.model_dump(mode="json"),
                "switch_episode": 3,
            },
        },
        agent_name="litellm",
        agent_model=_PRIMARY,
        provider_connection_id=uuid4(),
        provider_connection=None,
    )
    assert err is not None
    assert "terminus-2" in err


def test_validate_multi_model_rejects_missing_connection() -> None:
    err = validate_multi_model_for_batch(
        trial_config={
            "multi_model": {
                "enabled": True,
                "secondary_model": _SECONDARY.model_dump(mode="json"),
                "switch_episode": 3,
            },
        },
        agent_name="terminus-2",
        agent_model=_PRIMARY,
        provider_connection_id=None,
        provider_connection=None,
    )
    assert err is not None
    assert "provider_connection_id" in err


def test_validate_multi_model_rejects_provider_mismatch() -> None:
    err = validate_multi_model_for_batch(
        trial_config={
            "multi_model": {
                "enabled": True,
                "secondary_model": ModelSpec(
                    provider="anthropic", name="claude", source="api",
                ).model_dump(mode="json"),
                "switch_episode": 3,
            },
        },
        agent_name="terminus-2",
        agent_model=_PRIMARY,
        provider_connection_id=uuid4(),
        provider_connection=None,
    )
    assert err is not None
    assert "provider" in err


@dataclass
class _FakeConnection:
    allowed_models: list[str] | None


def test_validate_multi_model_allowed_models() -> None:
    conn = _FakeConnection(allowed_models=["glm-5.1"])
    err = validate_multi_model_for_batch(
        trial_config={
            "multi_model": {
                "enabled": True,
                "secondary_model": _SECONDARY.model_dump(mode="json"),
                "switch_episode": 3,
            },
        },
        agent_name="terminus-2",
        agent_model=_PRIMARY,
        provider_connection_id=uuid4(),
        provider_connection=conn,  # type: ignore[arg-type]
    )
    assert err is not None
    assert "allowed_models" in err


def test_parse_multi_model_rejects_seed_internals() -> None:
    with pytest.raises(ValueError, match="seed internals"):
        parse_multi_model({"multi_model": {"enabled": False, "seed": "abc"}})


def test_apply_plan_mode_resample_redraws_k1() -> None:
    cfg = {
        "agent_name": "terminus-2",
        "agent_model": _PRIMARY.model_dump(mode="json"),
        "multi_model": {
            "enabled": True,
            "secondary_model": _SECONDARY.model_dump(mode="json"),
            "switch_episode": 3,
            "return_switch_episode": 5,
            "teacher_episodes": 2,
            "episode_ceiling": 4,
        },
    }
    inherited = apply_plan_mode(cfg, mode="inherit")
    assert inherited["multi_model"]["switch_episode"] == 3
    resampled = apply_plan_mode(cfg, mode="resample")
    assert resampled["multi_model"]["switch_episode"] in {2, 3, 4}
    assert resampled["multi_model"]["return_switch_episode"] == (
        resampled["multi_model"]["switch_episode"] + 2
    )


def test_required_capabilities_blocks_old_workers() -> None:
    from loom.models.capabilities import Capabilities, RequiredCapabilities

    req = RequiredCapabilities(
        os="linux",
        gpu_vendor="none",
        network_policies=frozenset(["public"]),
        terminus2_model_switch=True,
    )
    old = Capabilities(
        os="linux",
        gpu_vendor="none",
        network_policies=frozenset(["public"]),
        dynamic_network_policy=True,
        mounted_fs=True,
        resource_modes=frozenset(["auto"]),
        terminus2_model_switch=False,
    )
    new = old.model_copy(update={"terminus2_model_switch": True})
    assert not req.satisfied_by(old)
    assert req.satisfied_by(new)
