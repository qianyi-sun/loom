"""LocalTrialRunner overrides SubprocessAgent-style agents' step-JWT
TTL so it outlives the task's declared `[agent].timeout_sec`.

Root cause context: SubprocessAgent snapshots the step-JWT into the
child's OPENAI_API_KEY / ANTHROPIC_API_KEY env at exec time and never
re-reads /run/loom/step-jwt. If the JWT's TTL is shorter than the
agent timeout, retries mid-step (e.g. after an upstream 502/504) can
outlive the token and get 401 from the gateway.

Unit-scope: `_apply_step_token_ttl` is pure; no CP, no driver, no
network.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

from loom_worker.trial_runner import (
    _STEP_JWT_TTL_BUFFER_SEC,
    _apply_step_token_ttl,
)


@dataclass
class _MutableSubprocessAgent:
    step_token_ttl_sec: int = 1800


@dataclass
class _OracleLikeAgent:
    """No step_token_ttl_sec attribute — mirrors OracleAgent / LiteLLMAgent."""

    task_dir: str = "/tmp/task"


def _task_config_with_agent_timeout(timeout_sec: float) -> SimpleNamespace:
    return SimpleNamespace(agent=SimpleNamespace(timeout_sec=timeout_sec))


def test_subprocess_agent_ttl_is_extended_to_cover_agent_timeout() -> None:
    agent = _MutableSubprocessAgent()  # default 1800
    task_config = _task_config_with_agent_timeout(2400.0)  # 40 min

    _apply_step_token_ttl(agent, task_config)

    assert agent.step_token_ttl_sec == 2400 + _STEP_JWT_TTL_BUFFER_SEC


def test_subprocess_agent_ttl_is_extended_even_for_short_timeouts() -> None:
    """Semantics: match the declared timeout + buffer, don't cap."""
    agent = _MutableSubprocessAgent(step_token_ttl_sec=1800)
    task_config = _task_config_with_agent_timeout(120.0)

    _apply_step_token_ttl(agent, task_config)

    assert agent.step_token_ttl_sec == 120 + _STEP_JWT_TTL_BUFFER_SEC


def test_oracle_like_agent_without_ttl_attr_is_unaffected() -> None:
    """Agents that don't carry step_token_ttl_sec must not be mutated."""
    agent = _OracleLikeAgent()
    task_config = _task_config_with_agent_timeout(2400.0)

    _apply_step_token_ttl(agent, task_config)

    assert not hasattr(agent, "step_token_ttl_sec")
    assert agent.task_dir == "/tmp/task"


def test_ttl_buffer_is_at_least_a_few_minutes() -> None:
    """Regression guard: shrinking the buffer to near-zero would
    re-introduce the exact race the fix targets (child clock skew +
    retry backoff + exec overhead)."""
    assert _STEP_JWT_TTL_BUFFER_SEC >= 60


def test_fractional_agent_timeout_is_rounded_down_before_add() -> None:
    """timeout_sec is float on the schema but the JWT TTL is int."""
    agent = _MutableSubprocessAgent()
    task_config = _task_config_with_agent_timeout(2400.9)

    _apply_step_token_ttl(agent, task_config)

    assert agent.step_token_ttl_sec == 2400 + _STEP_JWT_TTL_BUFFER_SEC


def test_helper_is_idempotent() -> None:
    agent = _MutableSubprocessAgent()
    task_config = _task_config_with_agent_timeout(1800.0)

    _apply_step_token_ttl(agent, task_config)
    first = agent.step_token_ttl_sec
    _apply_step_token_ttl(agent, task_config)

    assert agent.step_token_ttl_sec == first
