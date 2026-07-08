"""Family failure-policy plugins."""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import uuid4

from loom.family_run.failure import (
    AbortFamilyPolicy,
    SkipAndAdvancePolicy,
    StallFamilyPolicy,
)


@dataclass
class _Family:
    batch_id: object = field(default_factory=uuid4)
    family_key: str = "fam"
    task_sequence: list[str] = field(default_factory=list)
    current_index: int = 0
    attempt_count: int = 0


def test_stall_retries_with_exponential_backoff():
    pol = StallFamilyPolicy()
    family = _Family(task_sequence=["t/1"], attempt_count=0)
    action = pol.on_adapter_failure(
        family=family,
        exception=RuntimeError("boom"),
        params={"max_retries": 3, "backoff_sec": 30},
    )
    assert action.kind == "retry_with_backoff"
    assert action.backoff_sec == 30.0

    family.attempt_count = 1
    action = pol.on_adapter_failure(
        family=family,
        exception=RuntimeError("boom"),
        params={"max_retries": 3, "backoff_sec": 30},
    )
    assert action.backoff_sec == 60.0


def test_stall_gives_up_after_max_retries():
    pol = StallFamilyPolicy()
    family = _Family(task_sequence=["t/1"], attempt_count=3)
    action = pol.on_adapter_failure(
        family=family,
        exception=RuntimeError("boom"),
        params={"max_retries": 3, "backoff_sec": 30},
    )
    assert action.kind == "abort_family"


def test_skip_and_advance_always_skips():
    pol = SkipAndAdvancePolicy()
    family = _Family(task_sequence=["t/1"], attempt_count=99)
    action = pol.on_adapter_failure(
        family=family,
        exception=RuntimeError("boom"),
        params={},
    )
    assert action.kind == "skip_and_advance"


def test_abort_family_always_aborts():
    pol = AbortFamilyPolicy()
    family = _Family(task_sequence=["t/1"], attempt_count=0)
    action = pol.on_adapter_failure(
        family=family,
        exception=RuntimeError("boom"),
        params={},
    )
    assert action.kind == "abort_family"
