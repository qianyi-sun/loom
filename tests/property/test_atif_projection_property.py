"""Property: projecting an event log twice yields ATIF docs with
identical metadata + steps (trajectory_id is a fresh UUID per call, so
that field is the only allowed divergence)."""

from datetime import UTC, datetime
from uuid import uuid4

from hypothesis import given
from hypothesis import strategies as st

from loom.models.trajectory import (
    StepEndEvent,
    StepStartEvent,
    TrialEndEvent,
    TrialStartEvent,
)
from loom.trajectory.atif import project_to_atif

_TRIAL_ID = uuid4()
_BASE_TS = datetime(2026, 6, 6, tzinfo=UTC)


def _trial_start() -> TrialStartEvent:
    return TrialStartEvent(
        emitted_at=_BASE_TS, trial_id=_TRIAL_ID, step_id="__trial__", seq=0,
        task_id="t", agent_name="o", agent_mode="out-of-box",
    )


def _step(name: str, seq: int) -> list[StepStartEvent | StepEndEvent]:
    return [
        StepStartEvent(
            emitted_at=_BASE_TS, trial_id=_TRIAL_ID,
            step_id=name, seq=seq, instruction_excerpt=f"step {name}",
        ),
        StepEndEvent(
            emitted_at=_BASE_TS, trial_id=_TRIAL_ID,
            step_id=name, seq=seq + 1, summary={"r": 1.0},
        ),
    ]


# Step names: short, distinct alphanumerics that won't collide with reserved
# "__trial__" sentinel.
_STEP_NAME = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd")),
    min_size=1, max_size=6,
)


@given(steps=st.lists(_STEP_NAME, min_size=0, max_size=5, unique=True))
def test_projection_metadata_and_steps_are_deterministic(
    steps: list[str],
) -> None:
    events: list = [_trial_start()]
    seq = 1
    for name in steps:
        events.extend(_step(name, seq))
        seq += 2
    events.append(TrialEndEvent(
        emitted_at=_BASE_TS, trial_id=_TRIAL_ID,
        step_id="__trial__", seq=seq, final_state="succeeded",
    ))

    a = project_to_atif(
        iter(events), task_id="t", agent_name="o", agent_version="1.0",
    )
    b = project_to_atif(
        iter(events), task_id="t", agent_name="o", agent_version="1.0",
    )
    assert a.metadata == b.metadata
    assert a.steps == b.steps
    # session_id is the trial_id, deterministic. trajectory_id is a fresh
    # UUID per call — the only allowed divergence.
    assert a.session_id == b.session_id
