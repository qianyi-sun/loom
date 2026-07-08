"""CP finalize hook: advance predicate + family state transition.

The full DB round-trip is exercised by an integration test in
``tests/integration/test_family_run_end_to_end.py``. This suite pins
the pure decision logic via a mock session.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from loom.models.result import TrialState
from loom_control_plane.routes.state import _finalize_family


def _spec_dict(adapter_name: str = "noop") -> dict[str, Any]:
    return {
        "enabled": True,
        "family_key_extractor": {"name": "instance_id_prefix", "params": {}},
        "sequencer": {"name": "alphabetical", "params": {}},
        "advance_predicate": {"name": "always_on_terminal", "params": {}},
        "adapter": {"name": adapter_name, "params": {}},
        "failure_policy": {"name": "stall_family", "params": {}},
        "state_backend": {"name": "s3_artifacts", "params": {}},
        "mount_path": "/root/.skills",
    }


@dataclass
class _FakeResult:
    row: dict[str, Any] | None

    def mappings(self):  # type: ignore[no-untyped-def]
        outer = self

        class _M:
            def one_or_none(self_inner):  # type: ignore[no-untyped-def]
                return outer.row

        return _M()


class _FakeSession:
    def __init__(self, load_row: dict[str, Any] | None) -> None:
        self._load_row = load_row
        self.executed: list[tuple[Any, dict[str, Any]]] = []

    async def execute(self, statement: Any, params: dict[str, Any]) -> Any:
        self.executed.append((statement, params))
        # First call is the LOAD; subsequent calls are UPDATE/UPDATE
        if len(self.executed) == 1:
            return _FakeResult(self._load_row)
        return _FakeResult(None)


@pytest.mark.asyncio
async def test_finalize_family_noop_shortcut_bumps_index_when_not_last() -> None:
    trial_id = uuid4()
    batch_id = uuid4()
    row = {
        "family_key": "fam",
        "batch_id": batch_id,
        "task_id": "fam/a",
        "attempt_count": 1,
        "trial_state": "succeeded",
        "result": {"reward": 1.0},
        "spec": _spec_dict(adapter_name="noop"),
        "task_sequence": ["fam/a", "fam/b"],
        "current_index": 0,
        "family_attempt_count": 0,
        "family_state": "running",
    }
    session = _FakeSession(load_row=row)

    await _finalize_family(session, trial_id=trial_id, new_state=TrialState.SUCCEEDED)

    # Expect: LOAD + UPDATE (no cancel step for ADVANCE)
    assert len(session.executed) == 2
    _, update_params = session.executed[1]
    assert update_params["batch_id"] == batch_id
    assert update_params["family_key"] == "fam"
    assert update_params["new_state"] == "pending"
    assert update_params["new_current_index"] == 1
    assert update_params["new_attempt_count"] == 0


@pytest.mark.asyncio
async def test_finalize_family_noop_shortcut_done_on_last() -> None:
    trial_id = uuid4()
    batch_id = uuid4()
    row = {
        "family_key": "fam",
        "batch_id": batch_id,
        "task_id": "fam/b",
        "attempt_count": 1,
        "trial_state": "succeeded",
        "result": {"reward": 1.0},
        "spec": _spec_dict(adapter_name="noop"),
        "task_sequence": ["fam/a", "fam/b"],
        "current_index": 1,
        "family_attempt_count": 0,
        "family_state": "running",
    }
    session = _FakeSession(load_row=row)

    await _finalize_family(session, trial_id=trial_id, new_state=TrialState.SUCCEEDED)

    _, update_params = session.executed[1]
    assert update_params["new_state"] == "done"
    assert update_params["new_current_index"] == 2


@pytest.mark.asyncio
async def test_finalize_family_no_op_when_not_family_trial() -> None:
    session = _FakeSession(load_row=None)
    await _finalize_family(session, trial_id=uuid4(), new_state=TrialState.SUCCEEDED)
    # Only the LOAD was issued; no update.
    assert len(session.executed) == 1


@pytest.mark.asyncio
async def test_finalize_family_non_noop_goes_to_adapting() -> None:
    """When adapter is not ``noop``, ADVANCE transitions to ``adapting``
    for the future orchestrator (PR-2) to pick up.
    """
    trial_id = uuid4()
    batch_id = uuid4()
    row = {
        "family_key": "fam",
        "batch_id": batch_id,
        "task_id": "fam/a",
        "attempt_count": 1,
        "trial_state": "succeeded",
        "result": {"reward": 1.0},
        "spec": _spec_dict(adapter_name="skill_patcher_llm"),
        "task_sequence": ["fam/a", "fam/b"],
        "current_index": 0,
        "family_attempt_count": 0,
        "family_state": "running",
    }
    # Register a stub for the unknown adapter so registry doesn't need it.
    # (It doesn't need to exist for finalize; only the advance predicate
    # is resolved.)
    session = _FakeSession(load_row=row)
    await _finalize_family(session, trial_id=trial_id, new_state=TrialState.SUCCEEDED)
    _, update_params = session.executed[1]
    assert update_params["new_state"] == "adapting"
    assert update_params["new_current_index"] == 0
