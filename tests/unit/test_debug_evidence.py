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


def test_batch_debug_evidence_reports_terminal_worker_pool_coverage() -> None:
    team_id = uuid4()
    batch_id = uuid4()
    oldlab_worker_id = uuid4()
    gb10_worker_id = uuid4()
    now = datetime.now(UTC)
    batch = SimpleNamespace(
        id=batch_id,
        team_id=team_id,
        state="finished",
        result_status="partial_failed",
        failure_reason=None,
        failure_message=None,
        created_at=now,
        finished_at=now,
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
    oldlab_trial = SimpleNamespace(
        id=uuid4(),
        task_id="skilllearnbench/oldlab/oldlab-1",
        state="succeeded",
        failure_reason=None,
        failure_message=None,
        result={"reward": 1.0},
        config={},
        provider_connection_id=None,
        provider_model_id=None,
        worker_id=oldlab_worker_id,
        claimed_at=now - timedelta(minutes=2),
        started_at=now - timedelta(minutes=1),
    )
    gb10_trial = SimpleNamespace(
        id=uuid4(),
        task_id="skilllearnbench/gb10/gb10-1",
        state="failed",
        failure_reason="internal_error",
        failure_message="provider disconnected",
        result=None,
        config={},
        provider_connection_id=None,
        provider_model_id=None,
        worker_id=gb10_worker_id,
        claimed_at=now - timedelta(minutes=2),
        started_at=now - timedelta(minutes=1),
    )

    evidence = build_batch_debug_evidence(
        batch,  # type: ignore[arg-type]
        trials=[oldlab_trial, gb10_trial],  # type: ignore[list-item]
        llm_calls=[],
        worker_pool_names_by_id={
            oldlab_worker_id: "oldlab",
            gb10_worker_id: "gb10-arm64",
        },
    )

    assert evidence["trials"]["worker_pools"]["terminal"] == {
        "gb10-arm64": 1,
        "oldlab": 1,
    }
    assert evidence["trials"]["worker_pools"]["unknown_terminal"] == 0
