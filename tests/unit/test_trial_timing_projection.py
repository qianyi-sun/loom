"""Read paths recover historical native start evidence without rewriting rows."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from loom.db.schema import Trial
from loom_service.routes.batches import _trial_projections_for_batch_ids
from loom_service.routes.trials import _trial_row

_STARTED = datetime(2026, 9, 13, 0, 11, 55, tzinfo=UTC)
_FINISHED = _STARTED + timedelta(seconds=49)
_NATIVE = {
    "schema_version": "loom.execution-runtime-result.v1",
    "execution_role": "attempt",
    "started_at": _STARTED.isoformat(),
    "finished_at": _FINISHED.isoformat(),
}


def _trial(*, result, started_at=None):
    return Trial(
        id=uuid4(),
        batch_id=uuid4(),
        team_id=uuid4(),
        task_id="task",
        config={},
        state="succeeded",
        result=result,
        started_at=started_at,
        submitted_at=_STARTED - timedelta(minutes=3),
        finished_at=_FINISHED,
        attempt_count=1,
        sample_idx=0,
        combination_idx=0,
    )


async def _batch_projection(trial):
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(all=lambda: [trial])))
    return (await _trial_projections_for_batch_ids(session, [trial.batch_id]))[0]


async def test_trial_and_batch_read_project_native_start_without_mutating_history():
    result = {"runtime_result": deepcopy(_NATIVE)}
    trial = _trial(result=result)
    original_result = deepcopy(result)

    actual = (_trial_row(trial)["started_at"], (await _batch_projection(trial)).started_at)
    assert actual == (_STARTED.isoformat(), _STARTED)
    assert trial.started_at is None
    assert trial.result == original_result


async def test_persisted_trial_start_takes_precedence_over_runtime_fallback():
    persisted = _STARTED - timedelta(seconds=2)
    trial = _trial(result={"runtime_result": _NATIVE}, started_at=persisted)

    assert _trial_row(trial)["started_at"] == persisted.isoformat()
    assert (await _batch_projection(trial)).started_at == persisted


@pytest.mark.parametrize(
    "runtime",
    [
        None,
        [],
        {"started_at": _STARTED.isoformat()},
        {**_NATIVE, "schema_version": "legacy"},
        {**_NATIVE, "execution_role": "verifier"},
        {**_NATIVE, "started_at": "not-a-timestamp"},
        {**_NATIVE, "started_at": 12345},
        {**_NATIVE, "started_at": "2026-09-13T00:11:55"},
        {**_NATIVE, "finished_at": None},
        {**_NATIVE, "finished_at": (_STARTED - timedelta(seconds=1)).isoformat()},
    ],
)
async def test_unrecognized_or_invalid_runtime_evidence_keeps_start_unknown(runtime):
    trial = _trial(result={"runtime_result": runtime})

    assert _trial_row(trial)["started_at"] is None
    assert (await _batch_projection(trial)).started_at is None


async def test_no_result_keeps_start_unknown():
    trial = _trial(result=None)

    assert _trial_row(trial)["started_at"] is None
    assert (await _batch_projection(trial)).started_at is None


async def test_native_utc_z_timestamps_are_supported():
    trial = _trial(
        result={
            "runtime_result": {
                **_NATIVE,
                "started_at": "2026-09-13T00:11:55Z",
                "finished_at": "2026-09-13T00:12:44Z",
            }
        }
    )

    assert _trial_row(trial)["started_at"] == _STARTED.isoformat()
    assert (await _batch_projection(trial)).started_at == _STARTED
