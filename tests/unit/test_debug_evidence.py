from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

from loom_service.debug_evidence import build_batch_debug_evidence


def test_batch_debug_evidence_counts_claimed_without_started_trials() -> None:
    team_id = uuid4()
    batch_id = uuid4()
    stuck_trial_id = uuid4()
    running_trial_id = uuid4()
    now = datetime.now(UTC)
    batch = SimpleNamespace(
        id=batch_id,
        team_id=team_id,
        state="running",
        result_status=None,
        failure_reason=None,
        failure_message=None,
        created_at=now,
        finished_at=None,
        backend="docker",
        trial_config={},
        combinations=[],
        provider_connection_id=None,
        provider_model_id=None,
        task_filter={"benchmark_id": "skilllearnbench"},
        expected_trial_count=2,
        n_per_task=1,
        fanout_errors=None,
    )
    stuck_trial = SimpleNamespace(
        id=stuck_trial_id,
        task_id="skilllearnbench/stuck/stuck-1",
        state="claimed",
        failure_reason=None,
        failure_message=None,
        result=None,
        config={},
        provider_connection_id=None,
        provider_model_id=None,
        claimed_at=now - timedelta(minutes=10),
        started_at=None,
    )
    running_trial = SimpleNamespace(
        id=running_trial_id,
        task_id="skilllearnbench/running/running-1",
        state="running",
        failure_reason=None,
        failure_message=None,
        result=None,
        config={},
        provider_connection_id=None,
        provider_model_id=None,
        claimed_at=now - timedelta(minutes=10),
        started_at=now - timedelta(minutes=9),
    )

    evidence = build_batch_debug_evidence(
        batch,  # type: ignore[arg-type]
        trials=[stuck_trial, running_trial],  # type: ignore[list-item]
        llm_calls=[],
    )

    assert evidence["trials"]["summary"]["claimed"] == 1
    assert evidence["trials"]["summary"]["running"] == 1
    assert evidence["trials"]["summary"]["claimed_without_started"] == 1
