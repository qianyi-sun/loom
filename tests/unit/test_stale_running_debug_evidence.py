from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

from loom_service.debug_evidence import build_batch_debug_evidence, build_trial_debug_evidence
from loom_service.stale_running_debug import _decision_for_trial, stale_running_debug_policy


class _Request:
    def url_for(self, name: str, **values: object) -> str:
        return f"http://test/{name}/{values.get('trial_id', '')}"


def test_stale_running_debug_policy_reads_service_settings() -> None:
    settings = SimpleNamespace(
        worker_heartbeat_expiry_sec=45,
        stale_running_trial_reclaim_enabled=False,
        stale_running_trial_timeout_multiplier=4.0,
        stale_running_trial_grace_sec=120.0,
        stale_running_trial_silence_sec=180.0,
    )

    policy = stale_running_debug_policy(settings)

    assert policy.worker_heartbeat_expiry_sec == 45.0
    assert policy.reclaim_enabled is False
    assert policy.timeout_multiplier == 4.0
    assert policy.grace_sec == 120.0
    assert policy.silence_sec == 180.0


def test_stale_running_debug_policy_disabled_keeps_trial() -> None:
    now = datetime(2026, 7, 3, 12, 0, tzinfo=UTC)
    settings = SimpleNamespace(
        worker_heartbeat_expiry_sec=45,
        stale_running_trial_reclaim_enabled=False,
        stale_running_trial_timeout_multiplier=2.0,
        stale_running_trial_grace_sec=60.0,
        stale_running_trial_silence_sec=60.0,
    )
    trial = SimpleNamespace(
        state="running",
        started_at=now - timedelta(seconds=5000),
        finished_at=None,
        config={"agent_timeout_multiplier": 1.0},
    )
    worker = SimpleNamespace(last_seen_at=now - timedelta(seconds=5))

    decision = _decision_for_trial(
        trial,
        task_config={"agent": {"timeout_sec": 100.0}},
        last_event_at=now - timedelta(seconds=4000),
        last_llm_call_at=None,
        worker=worker,  # type: ignore[arg-type]
        policy=stale_running_debug_policy(settings),
        now=now,
    )

    assert decision.decision == "keep"
    assert decision.reason == "stale_running_reclaim_disabled"
    assert decision.reclaimable is False


def test_trial_debug_evidence_includes_stale_running_activity_and_worker_heartbeat() -> None:
    now = datetime(2026, 7, 3, 12, 0, tzinfo=UTC)
    team_id = uuid4()
    batch_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    started_at = now - timedelta(seconds=4000)
    last_event_at = now - timedelta(seconds=3700)
    last_llm_at = now - timedelta(seconds=3600)
    worker_seen_at = now - timedelta(seconds=5)
    trial = SimpleNamespace(
        id=trial_id,
        team_id=team_id,
        batch_id=batch_id,
        task_id="source-useful-frontier-5003/task",
        state="running",
        failure_reason=None,
        failure_message=None,
        result=None,
        config={
            "schema_version": "1",
            "agent_name": "opencode",
            "agent_model": {"provider": "openai", "name": "glm-5.1-thinking"},
            "agent_timeout_multiplier": 1.0,
        },
        trajectory_index={},
        provider_connection_id=None,
        provider_model_id="glm-5.1-thinking",
        submitted_at=now - timedelta(seconds=4100),
        claimed_at=started_at,
        started_at=started_at,
        finished_at=None,
        cancellation_requested_at=None,
        cancellation_observed_at=None,
        attempt_count=1,
        next_attempt_at=None,
        worker_id=worker_id,
        requires_caps={"worker_pool": "gb10-arm64"},
    )
    task = SimpleNamespace(
        benchmark_id="source-useful-frontier-5003",
        checksum="0" * 64,
        source="taskset",
        config={
            "task": {"id": "source-useful-frontier-5003/task"},
            "agent": {"name": "opencode", "timeout_sec": 2400.0},
        },
    )
    worker = SimpleNamespace(
        id=worker_id,
        hostname="trt-gb10-4",
        pool_name="gb10-arm64",
        status="active",
        last_seen_at=worker_seen_at,
    )
    llm_call = SimpleNamespace(
        id=uuid4(),
        step_id="main",
        model="glm-5.1-thinking",
        dialect="openai_facade",
        input_tokens=100,
        output_tokens=50,
        provider_extras={},
        request_params=None,
        cost_usd=Decimal("0.10"),
        rate_card_hash="rate",
        captured_at=last_llm_at,
        attempt=1,
    )
    last_event = {
        "kind": "thought",
        "created_at": last_event_at,
        "payload": {
            "emitted_at": last_event_at.isoformat(),
            "step_id": "main",
        },
    }
    stale_decision = {
        "decision": "reclaim",
        "reason": "fresh_worker_timeout_and_silent",
        "reclaimable": True,
        "hard_deadline_sec": 7800.0,
        "silence_sec": 3600.0,
    }

    evidence = build_trial_debug_evidence(
        _Request(),  # type: ignore[arg-type]
        trial,  # type: ignore[arg-type]
        task=task,  # type: ignore[arg-type]
        llm_calls=[llm_call],  # type: ignore[list-item]
        worker=worker,  # type: ignore[arg-type]
        last_event=last_event,
        stale_running_decision=stale_decision,
        now=now,
    )

    assert evidence["lifecycle"]["runtime_sec"] == 4000.0
    assert evidence["agent"]["timeout"]["agent_timeout_sec"] == 2400.0
    assert evidence["activity"]["last_trial_event"]["kind"] == "thought"
    assert evidence["activity"]["last_trial_event"]["created_at"] == last_event_at.isoformat()
    assert evidence["activity"]["last_llm_call_at"] == last_llm_at.isoformat()
    assert evidence["activity"]["last_activity_at"] == last_llm_at.isoformat()
    assert evidence["activity"]["silence_sec"] == 3600.0
    assert evidence["worker"]["hostname"] == "trt-gb10-4"
    assert evidence["worker"]["pool_name"] == "gb10-arm64"
    assert evidence["worker"]["last_heartbeat_at"] == worker_seen_at.isoformat()
    assert evidence["worker"]["heartbeat_age_sec"] == 5.0
    assert evidence["worker"]["heartbeat_fresh"] is True
    assert evidence["stale_running"]["decision"] == "reclaim"
    assert evidence["stale_running"]["reason"] == "fresh_worker_timeout_and_silent"


def test_batch_debug_evidence_lists_stale_running_candidates() -> None:
    now = datetime(2026, 7, 3, 12, 0, tzinfo=UTC)
    batch_id = uuid4()
    team_id = uuid4()
    trial_id = uuid4()
    batch = SimpleNamespace(
        id=batch_id,
        team_id=team_id,
        state="running",
        result_status=None,
        failure_reason=None,
        failure_message=None,
        created_at=now - timedelta(hours=2),
        finished_at=None,
        backend="docker",
        trial_config={},
        combinations=[],
        provider_connection_id=None,
        provider_model_id="glm-5.1-thinking",
        task_filter={"benchmark_id": "source-useful-frontier-5003"},
        expected_trial_count=1,
        n_per_task=1,
        fanout_errors=None,
    )
    trial = SimpleNamespace(
        id=trial_id,
        task_id="source-useful-frontier-5003/task",
        state="running",
        failure_reason=None,
        failure_message=None,
        result=None,
        config={"agent_name": "opencode"},
        provider_connection_id=None,
        provider_model_id="glm-5.1-thinking",
        worker_id=uuid4(),
        claimed_at=now - timedelta(seconds=4000),
        started_at=now - timedelta(seconds=3900),
    )
    decision = {
        "decision": "reclaim",
        "reason": "fresh_worker_timeout_and_silent",
        "reclaimable": True,
        "runtime_sec": 3900.0,
        "silence_sec": 3600.0,
        "agent_timeout_sec": 2400.0,
        "hard_deadline_sec": 7800.0,
        "last_activity_at": (now - timedelta(seconds=3600)).isoformat(),
    }

    evidence = build_batch_debug_evidence(
        batch,  # type: ignore[arg-type]
        trials=[trial],  # type: ignore[list-item]
        llm_calls=[],
        stale_running_decisions_by_trial_id={trial_id: decision},
        now=now,
    )

    assert evidence["trials"]["stale_running_count"] == 1
    assert evidence["trials"]["stale_running"][0]["id"] == str(trial_id)
    assert evidence["trials"]["stale_running"][0]["decision"] == "reclaim"
    assert evidence["trials"]["stale_running"][0]["runtime_sec"] == 3900.0
