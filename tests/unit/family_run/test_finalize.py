"""CP finalize hook: advance predicate + family state transition.

The full DB round-trip is exercised by an integration test. This
suite pins the pure decision logic via a mock session.

PR-2 update: the noop shortcut is gone. Every ADVANCE decision
transitions to ``adapting``; the orchestrator picks up all adapters
uniformly (including ``noop``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
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
            def one_or_none(self) -> Any:
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
async def test_finalize_family_advance_goes_to_adapting_for_noop() -> None:
    """PR-2: even the noop adapter routes through the orchestrator.

    ADVANCE always transitions to ``adapting`` regardless of adapter
    name; the orchestrator's noop path bumps the index from there.
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
    assert update_params["new_state"] == "adapting"
    assert update_params["new_current_index"] == 0
    assert update_params["new_attempt_count"] == 0


@pytest.mark.asyncio
async def test_finalize_family_advance_goes_to_adapting_on_last_task() -> None:
    """End of sequence also routes through ``adapting`` - the
    orchestrator's post-evolve bump decides ``done``.
    """
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
    assert update_params["new_state"] == "adapting"
    assert update_params["new_current_index"] == 1


@pytest.mark.asyncio
async def test_finalize_family_no_op_when_not_family_trial() -> None:
    session = _FakeSession(load_row=None)
    await _finalize_family(session, trial_id=uuid4(), new_state=TrialState.SUCCEEDED)
    # Only the LOAD was issued; no update.
    assert len(session.executed) == 1


@pytest.mark.asyncio
async def test_finalize_family_non_noop_goes_to_adapting() -> None:
    """Non-noop adapters also transition to ``adapting`` - same path,
    same orchestrator ownership.
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
    session = _FakeSession(load_row=row)
    await _finalize_family(session, trial_id=trial_id, new_state=TrialState.SUCCEEDED)
    _, update_params = session.executed[1]
    assert update_params["new_state"] == "adapting"
    assert update_params["new_current_index"] == 0
