from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

from loom_service.debug_evidence import build_batch_debug_evidence, build_trial_debug_evidence


class _Request:
    def url_for(self, name: str, **values: object) -> str:
        return f"http://test/{name}/{values.get('trial_id', '')}"


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


def test_trial_debug_evidence_classifies_provider_transport_disconnect() -> None:
    now = datetime.now(UTC)
    trial = SimpleNamespace(
        id=uuid4(),
        team_id=uuid4(),
        batch_id=None,
        task_id="skilllearnbench/fix-security-bug/fix-security-bug-2",
        state="failed",
        failure_reason="provider_transport_disconnect",
        failure_message="Server disconnected without sending a response.",
        result=None,
        config={},
        trajectory_index={},
        provider_connection_id=None,
        provider_model_id=None,
        submitted_at=now - timedelta(minutes=2),
        claimed_at=now - timedelta(minutes=2),
        started_at=now - timedelta(minutes=1),
        finished_at=now,
        cancellation_requested_at=None,
        cancellation_observed_at=None,
        attempt_count=2,
        next_attempt_at=None,
        worker_id=None,
        requires_caps={},
    )

    evidence = build_trial_debug_evidence(
        _Request(),  # type: ignore[arg-type]
        trial,  # type: ignore[arg-type]
        task=None,
        llm_calls=[],
    )

    assert evidence["failure"]["reason_code"] == "trial.provider_transport_disconnect"
    assert evidence["failure"]["category"] == "gateway"
    assert evidence["failure"]["attribution"] == "provider"
    assert evidence["failure"]["failure_class"] == "platform_failure"
    assert evidence["failure"]["root_cause"] == "provider_transport"
    assert evidence["failure"]["rerun_recommendation"] == "auto_safe"
    assert "provider preflight" in " ".join(evidence["next_actions"]).lower()


def test_trial_debug_evidence_distinguishes_reward_zero_score_failure() -> None:
    now = datetime.now(UTC)
    trial = SimpleNamespace(
        id=uuid4(),
        team_id=uuid4(),
        batch_id=None,
        task_id="source-useful-frontier/reward-zero",
        state="succeeded",
        failure_reason=None,
        failure_message=None,
        result={"aggregate_reward": 0.0},
        config={},
        trajectory_index={},
        provider_connection_id=None,
        provider_model_id=None,
        submitted_at=now - timedelta(minutes=2),
        claimed_at=now - timedelta(minutes=2),
        started_at=now - timedelta(minutes=1),
        finished_at=now,
        cancellation_requested_at=None,
        cancellation_observed_at=None,
        attempt_count=1,
        next_attempt_at=None,
        worker_id=None,
        requires_caps={},
    )

    evidence = build_trial_debug_evidence(
        _Request(),  # type: ignore[arg-type]
        trial,  # type: ignore[arg-type]
        task=None,
        llm_calls=[],
    )

    assert evidence["failure"]["reason_code"] == "trial.score_failure"
    assert evidence["failure"]["failure_class"] == "score_failure"
    assert evidence["failure"]["platform_outcome"] == "success"
    assert evidence["failure"]["score_outcome"] == "failed"
    assert evidence["failure"]["rerun_recommendation"] == "not_rerunnable"


def test_batch_debug_evidence_includes_failure_taxonomy_summary() -> None:
    team_id = uuid4()
    batch_id = uuid4()
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
        task_filter={"benchmark_id": "source-useful-frontier"},
        expected_trial_count=3,
        n_per_task=1,
        fanout_errors=None,
    )
    trials = [
        SimpleNamespace(
            id=uuid4(),
            task_id="source-useful-frontier/reward-zero",
            state="succeeded",
            failure_reason=None,
            failure_message=None,
            result={"aggregate_reward": 0.0},
            config={},
            provider_connection_id=None,
            provider_model_id=None,
            worker_id=None,
            claimed_at=now - timedelta(minutes=2),
            started_at=now - timedelta(minutes=1),
            sample_idx=0,
            combination_idx=0,
        ),
        SimpleNamespace(
            id=uuid4(),
            task_id="source-useful-frontier/gateway",
            state="failed",
            failure_reason="gateway_error",
            failure_message="gateway 503",
            result=None,
            config={},
            provider_connection_id=None,
            provider_model_id=None,
            worker_id=None,
            claimed_at=now - timedelta(minutes=2),
            started_at=now - timedelta(minutes=1),
            sample_idx=0,
            combination_idx=0,
        ),
        SimpleNamespace(
            id=uuid4(),
            task_id="source-useful-frontier/task-compat",
            state="failed",
            failure_reason="task_compatibility",
            failure_message="task bundle cannot run",
            result=None,
            config={},
            provider_connection_id=None,
            provider_model_id=None,
            worker_id=None,
            claimed_at=now - timedelta(minutes=2),
            started_at=now - timedelta(minutes=1),
            sample_idx=0,
            combination_idx=0,
        ),
    ]

    evidence = build_batch_debug_evidence(
        batch,  # type: ignore[arg-type]
        trials=trials,  # type: ignore[list-item]
        llm_calls=[],
    )

    assert evidence["trials"]["classification_summary"] == {
        "platform_failure": 1,
        "score_failure": 1,
        "task_failure": 1,
    }
    assert evidence["trials"]["rerun_recommendation_summary"] == {
        "auto_safe": 1,
        "not_rerunnable": 2,
    }
    assert [row["failure_class"] for row in evidence["trials"]["failure_ledger"]] == [
        "platform_failure",
        "score_failure",
        "task_failure",
    ]
