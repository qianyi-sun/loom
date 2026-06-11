import pytest
from pydantic import ValidationError

from loom.models.trial import RetryPolicy, RetryReason, TrialConfig
from loom.models.types import ModelSpec

# Plan 23: every TrialConfig must specify agent_name + agent_model
# (no fallback to TaskConfig). These stubs keep tests focused on the
# fields they're actually exercising.
_AGENT = "oracle"
_MODEL = ModelSpec(provider="local", name="stub")


def test_trial_config_defaults():
    c = TrialConfig(agent_name=_AGENT, agent_model=_MODEL)
    assert c.schema_version == "1"
    assert c.force_build is False
    assert c.delete_env is True
    assert c.skip_verifier is False
    assert c.verifier_env_mode is None
    assert c.agent_timeout_multiplier == 1.0
    assert c.submit_priority == 100
    assert c.retry == RetryPolicy()


def test_trial_config_override_agent_timeout():
    c = TrialConfig(
        agent_name=_AGENT, agent_model=_MODEL,
        override_agent_timeout_sec=900.0,
    )
    assert c.override_agent_timeout_sec == 900.0


def test_trial_config_priority_bounds():
    TrialConfig(agent_name=_AGENT, agent_model=_MODEL, submit_priority=0)
    TrialConfig(agent_name=_AGENT, agent_model=_MODEL, submit_priority=1000)
    with pytest.raises(ValidationError):
        TrialConfig(agent_name=_AGENT, agent_model=_MODEL, submit_priority=-1)
    with pytest.raises(ValidationError):
        TrialConfig(agent_name=_AGENT, agent_model=_MODEL, submit_priority=1001)


def test_trial_config_retry_on_set():
    c = TrialConfig(
        agent_name=_AGENT, agent_model=_MODEL,
        retry=RetryPolicy(
            max_attempts=3,
            retry_on=frozenset({RetryReason.WORKER_CRASH, RetryReason.AGENT_TIMEOUT}),
        ),
    )
    assert RetryReason.WORKER_CRASH in c.retry.retry_on


def test_trial_config_missing_agent_name_422s():
    with pytest.raises(ValidationError):
        TrialConfig(agent_model=_MODEL)  # type: ignore[call-arg]


def test_trial_config_missing_agent_model_422s():
    with pytest.raises(ValidationError):
        TrialConfig(agent_name=_AGENT)  # type: ignore[call-arg]


def test_trial_config_empty_agent_name_422s():
    with pytest.raises(ValidationError):
        TrialConfig(agent_name="", agent_model=_MODEL)
