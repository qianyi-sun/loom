import io
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from loom.models.trajectory import TrialEndEvent
from loom_service.delivery_export import (
    ObjectRef,
    SelectedTrial,
    TerminalStateMismatchError,
    _select_trials,
    _validate_atif_terminal_evidence,
    _validate_typed_terminal_evidence,
)


def _item() -> SelectedTrial:
    trial_id = uuid4()
    trial = SimpleNamespace(
        id=trial_id,
        state="succeeded",
        task_id="task-1",
        config={},
        result={"state": "succeeded", "reward": {"score": 1.0}},
    )
    batch = SimpleNamespace(id=uuid4())
    atif = ObjectRef(kind="atif", trial_id=trial_id, bucket="trajectories", key="atif.json")
    trajectory = ObjectRef(
        kind="trajectory",
        trial_id=trial_id,
        bucket="trajectories",
        key="events.jsonl",
    )
    return SelectedTrial(
        trial=cast(Any, trial),
        batch=cast(Any, batch),
        priority=0,
        selection_source="main",
        trajectory=trajectory,
        atif=atif,
        reward=None,
    )


def test_typed_trial_end_must_agree_with_trial_row() -> None:
    item = _item()
    terminal = TrialEndEvent(
        emitted_at=datetime.now(UTC),
        trial_id=item.trial.id,
        step_id="__trial__",
        seq=1,
        final_state="failed",
        reward={"score": 1.0},
        failure_reason="verifier_error",
    )

    with pytest.raises(TerminalStateMismatchError) as caught:
        _validate_typed_terminal_evidence(item=item, events=[terminal])

    conflicts = caught.value.detail["inconsistent_trials"][0]["conflicts"]
    assert {conflict["field"] for conflict in conflicts} == {
        "trajectory.trial_end.final_state",
        "trajectory.trial_end.failure_reason",
    }


def test_atif_terminal_state_must_agree_with_trial_row() -> None:
    item = _item()

    class FakeClient:
        def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803
            assert (Bucket, Key) == ("trajectories", "atif.json")
            return {
                "Body": io.BytesIO(b'{"metadata":{"final_state":"failed","reward":{"score":1.0}}}'),
            }

    with pytest.raises(TerminalStateMismatchError) as caught:
        _validate_atif_terminal_evidence(client=FakeClient(), item=item)

    assert caught.value.detail["inconsistent_trials"][0]["conflicts"] == [
        {
            "field": "atif.metadata.final_state",
            "expected": "succeeded",
            "actual": "failed",
        }
    ]


def test_missing_terminal_evidence_fails_closed() -> None:
    item = _item()
    with pytest.raises(TerminalStateMismatchError) as trajectory_error:
        _validate_typed_terminal_evidence(item=item, events=[])
    assert trajectory_error.value.detail["inconsistent_trials"][0]["conflicts"] == [
        {
            "field": "trajectory.trial_end_count",
            "expected": 1,
            "actual": 0,
        }
    ]

    class FakeClient:
        def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:  # noqa: N803
            return {"Body": io.BytesIO(b"{}")}

    with pytest.raises(TerminalStateMismatchError) as atif_error:
        _validate_atif_terminal_evidence(client=FakeClient(), item=item)
    assert atif_error.value.detail["inconsistent_trials"][0]["conflicts"] == [
        {"field": "atif.metadata", "expected": "object", "actual": "NoneType"}
    ]


def test_clean_supplement_supersedes_dirty_historical_main_candidate() -> None:
    team_id = uuid4()
    main_id = uuid4()
    supplement_id = uuid4()

    def trial(*, trial_id: UUID, result: dict[str, object]) -> SimpleNamespace:
        return SimpleNamespace(
            id=trial_id,
            team_id=team_id,
            task_id="task-1",
            sample_idx=0,
            combination_idx=0,
            state="succeeded",
            submitted_at=datetime.now(UTC),
            result=result,
            failure_reason=None,
            config={},
            trajectory_index={},
        )

    dirty = trial(
        trial_id=uuid4(),
        result={
            "state": "succeeded",
            "failure_reason": "verifier_error",
            "aggregate_reward": 1.0,
        },
    )
    clean = trial(
        trial_id=uuid4(),
        result={"state": "succeeded", "aggregate_reward": 1.0},
    )
    selected = _select_trials(
        main=cast(Any, SimpleNamespace(id=main_id)),
        supplements=[cast(Any, SimpleNamespace(id=supplement_id))],
        trials_by_batch={main_id: [dirty], supplement_id: [clean]},
        trajectories_bucket="trajectories",
    )
    assert [item.trial.id for item in selected] == [clean.id]
