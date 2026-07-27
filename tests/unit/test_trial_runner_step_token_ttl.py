"""Step execution aligns subprocess-agent JWT TTL with effective timeout.

Root cause context: SubprocessAgent snapshots the step-JWT into the
child's OPENAI_API_KEY / ANTHROPIC_API_KEY env at exec time and never
re-reads it. The effective timeout must include step/trial overrides
and multipliers, rather than only the task default.

Unit-scope: helpers are pure; no CP, driver, or network.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

from loom.trial.step_runner import (
    _STEP_JWT_TTL_BUFFER_SEC,
    _apply_step_token_ttl,
    _resolve_agent_timeout,
)


@dataclass
class _MutableSubprocessAgent:
    step_token_ttl_sec: int = 1800


@dataclass
class _OracleLikeAgent:
    """No step_token_ttl_sec attribute — mirrors OracleAgent / LiteLLMAgent."""

    task_dir: str = "/tmp/task"


def test_subprocess_agent_ttl_is_extended_to_cover_agent_timeout() -> None:
    agent = _MutableSubprocessAgent()  # default 1800
    _apply_step_token_ttl(agent, 2400.0)

    assert agent.step_token_ttl_sec == 2400 + _STEP_JWT_TTL_BUFFER_SEC


def test_subprocess_agent_ttl_is_extended_even_for_short_timeouts() -> None:
    """Semantics: match the declared timeout + buffer, don't cap."""
    agent = _MutableSubprocessAgent(step_token_ttl_sec=1800)
    _apply_step_token_ttl(agent, 120.0)

    assert agent.step_token_ttl_sec == 120 + _STEP_JWT_TTL_BUFFER_SEC


def test_oracle_like_agent_without_ttl_attr_is_unaffected() -> None:
    """Agents that don't carry step_token_ttl_sec must not be mutated."""
    agent = _OracleLikeAgent()
    _apply_step_token_ttl(agent, 2400.0)

    assert not hasattr(agent, "step_token_ttl_sec")
    assert agent.task_dir == "/tmp/task"


def test_ttl_buffer_is_at_least_a_few_minutes() -> None:
    """Regression guard: shrinking the buffer to near-zero would
    re-introduce the exact race the fix targets (child clock skew +
    retry backoff + exec overhead)."""
    assert _STEP_JWT_TTL_BUFFER_SEC >= 60


def test_fractional_agent_timeout_is_rounded_up_before_add() -> None:
    """The static JWT must never expire before a fractional timeout."""
    agent = _MutableSubprocessAgent()
    _apply_step_token_ttl(agent, 2400.1)

    assert agent.step_token_ttl_sec == 2401 + _STEP_JWT_TTL_BUFFER_SEC


def test_helper_is_idempotent() -> None:
    agent = _MutableSubprocessAgent()
    _apply_step_token_ttl(agent, 1800.0)
    first = agent.step_token_ttl_sec
    _apply_step_token_ttl(agent, 1800.0)

    assert agent.step_token_ttl_sec == first


def test_trial_override_and_multiplier_drive_step_jwt_ttl() -> None:
    ctx = SimpleNamespace(
        task_config=SimpleNamespace(agent=SimpleNamespace(timeout_sec=1800.0)),
        trial_config=SimpleNamespace(
            override_agent_timeout_sec=9000.0,
            agent_timeout_multiplier=1.0,
        ),
    )
    step = SimpleNamespace(agent=SimpleNamespace(timeout_sec=3600.0))
    agent = _MutableSubprocessAgent()

    effective_timeout = _resolve_agent_timeout(ctx, step)
    _apply_step_token_ttl(agent, effective_timeout)

    assert effective_timeout == 9000.0
    assert agent.step_token_ttl_sec == 9300


def test_step_override_and_multiplier_drive_step_jwt_ttl() -> None:
    ctx = SimpleNamespace(
        task_config=SimpleNamespace(agent=SimpleNamespace(timeout_sec=1800.0)),
        trial_config=SimpleNamespace(
            override_agent_timeout_sec=None,
            agent_timeout_multiplier=1.5,
        ),
    )
    step = SimpleNamespace(agent=SimpleNamespace(timeout_sec=3600.0))
    agent = _MutableSubprocessAgent()

    effective_timeout = _resolve_agent_timeout(ctx, step)
    _apply_step_token_ttl(agent, effective_timeout)

    assert effective_timeout == 5400.0
    assert agent.step_token_ttl_sec == 5700
