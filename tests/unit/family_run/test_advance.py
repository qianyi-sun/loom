"""Family advance predicate plugins."""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import uuid4

from loom.family_run.advance import (
    AlwaysOnTerminalPredicate,
    SuccessOrRetryExhaustedPredicate,
)
from loom.family_run.spec import (
    AdvanceDecision,
    PluginRef,
    ResolvedFamilyRunSpec,
)


@dataclass
class _Trial:
    id: object = field(default_factory=uuid4)
    task_id: str = "t/1"
    state: str = "succeeded"
    reward: float | None = 1.0
    attempt_count: int = 1


@dataclass
class _Family:
    batch_id: object = field(default_factory=uuid4)
    family_key: str = "fam"
    task_sequence: list[str] = field(default_factory=list)
    current_index: int = 0
    attempt_count: int = 0


def _spec() -> ResolvedFamilyRunSpec:
    return ResolvedFamilyRunSpec(
        enabled=True,
        family_key_extractor=PluginRef(name="instance_id_prefix"),
        sequencer=PluginRef(name="alphabetical"),
        advance_predicate=PluginRef(name="always_on_terminal"),
        adapter=PluginRef(name="noop"),
        failure_policy=PluginRef(name="stall_family"),
        state_backend=PluginRef(name="s3_artifacts"),
    )


def test_always_on_terminal_advances_regardless_of_state():
    pred = AlwaysOnTerminalPredicate()
    for state in ("succeeded", "failed", "cancelled"):
        trial = _Trial(state=state)
        family = _Family(task_sequence=["t/1"])
        assert (
            pred.decide(trial=trial, family=family, spec=_spec(), params={})
            == AdvanceDecision.ADVANCE
        )


def test_success_or_retry_exhausted_advances_on_success():
    pred = SuccessOrRetryExhaustedPredicate()
    trial = _Trial(state="succeeded")
    family = _Family(task_sequence=["t/1"])
    assert (
        pred.decide(trial=trial, family=family, spec=_spec(), params={"retry_budget": 3})
        == AdvanceDecision.ADVANCE
    )


def test_success_or_retry_exhausted_retries_on_failure_within_budget():
    pred = SuccessOrRetryExhaustedPredicate()
    trial = _Trial(state="failed", attempt_count=1)
    family = _Family(task_sequence=["t/1"], attempt_count=0)
    assert (
        pred.decide(trial=trial, family=family, spec=_spec(), params={"retry_budget": 3})
        == AdvanceDecision.RETRY
    )


def test_success_or_retry_exhausted_advances_when_budget_exhausted():
    pred = SuccessOrRetryExhaustedPredicate()
    trial = _Trial(state="failed", attempt_count=3)
    family = _Family(task_sequence=["t/1"], attempt_count=2)
    assert (
        pred.decide(trial=trial, family=family, spec=_spec(), params={"retry_budget": 3})
        == AdvanceDecision.ADVANCE
    )
