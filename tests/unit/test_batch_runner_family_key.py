"""Unit test: batch_runner attaches family_key to pending units (#672 PR-3).

Verifies the ``_with_family_key`` helper preserves the other fields on
the ``PendingUnit`` immutable record while stamping the family_key. The
async submit path is exercised in the batch_runner integration test.
"""

from __future__ import annotations

from uuid import uuid4

from loom_service.batch_runner import PendingUnit, _with_family_key


def _unit() -> PendingUnit:
    return PendingUnit(
        task_id="benchmarks/x/family-a/task-1",
        combination_idx=None,
        trial_config={"agent_name": "oracle", "agent_model": None},
        sample_idx=0,
        required_worker_pool=None,
        provider_connection_id=uuid4(),
        provider_model_id="stub",
    )


def test_with_family_key_returns_copy_with_family_key() -> None:
    original = _unit()
    stamped = _with_family_key(original, "family-a")
    assert stamped is not original
    assert stamped.family_key == "family-a"
    assert stamped.task_id == original.task_id
    assert stamped.provider_connection_id == original.provider_connection_id
    assert original.family_key is None


def test_with_family_key_none_returns_original() -> None:
    """When there is no seeded family for the task, the helper returns
    the input untouched so the fanout path is a no-op."""
    original = _unit()
    result = _with_family_key(original, None)
    assert result is original
    assert result.family_key is None
