from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest

from loom.db.schema import WorkerPoolAutoscalerPolicy
from loom.worker_token import WORKER_AUTH_FINGERPRINT_ENV_KEY, worker_token_fingerprint
from loom_control_plane.elastic_slurm_worker_controller import (
    SlurmNodeResource,
    build_sbatch_request,
)
from loom_control_plane.worker_pool_autoscaler import (
    AutoscalerDecision,
    AutoscalerObservation,
    AutoscalerPolicyConfig,
    _apply_gb10_host_intent,
    _apply_gb10_scale_up,
    _apply_slurm_release_drained,
    _apply_slurm_scale_up,
    _clean_nonempty,
    _load_observation,
    _persist_decision,
    _policy_to_config,
    _queued_trial_matches_policy,
    _request_worker_drain,
    _slurm_config_from_policy,
    _validate_policy_fields,
    autoscaler_policy_to_dict,
    compute_autoscaler_decision,
    fetch_autoscaler_status,
    select_slurm_qos,
    upsert_autoscaler_policy,
)


def test_select_slurm_qos_uses_boost_below_min() -> None:
    assert (
        select_slurm_qos(
            active_plus_pending=2,
            min_slots=6,
            qos_boost="loom-boost",
            qos_normal="loom-normal",
        )
        == "loom-boost"
    )


def test_select_slurm_qos_uses_normal_at_or_above_min() -> None:
    assert (
        select_slurm_qos(
            active_plus_pending=6,
            min_slots=6,
            qos_boost="loom-boost",
            qos_normal="loom-normal",
        )
        == "loom-normal"
    )
    assert (
        select_slurm_qos(
            active_plus_pending=9,
            min_slots=6,
            qos_boost="loom-boost",
            qos_normal="loom-normal",
        )
        == "loom-normal"
    )


def test_select_slurm_qos_without_boost_returns_normal() -> None:
    assert (
        select_slurm_qos(
            active_plus_pending=0,
            min_slots=6,
            qos_boost="",
            qos_normal="loom-normal",
        )
        == "loom-normal"
    )


def test_policy_to_config_reads_qos_boost_and_normal() -> None:
    row = _policy_row(
        actuator_config={
            "backend": "docker",
            "cpu_arch": "x86_64",
            "allowed_nodes": ["oldlab-1"],
            "env_file": "/secure/.env.remote-worker",
            "repo_dir": "/opt/loom",
            "requested_concurrency": 6,
            "requested_cpus": 12,
            "requested_memory_mib": 58000,
            "max_jobs": 1,
            "pending_job_cap": 1,
            "qos_boost": "loom-boost",
            "qos_normal": "loom-staging-normal",
        },
    )
    config = _policy_to_config(row)
    assert config.qos_boost == "loom-boost"
    assert config.qos_normal == "loom-staging-normal"


def test_slurm_config_from_policy_threads_account_qos_reservation() -> None:
    row = _policy_row(
        actuator_config={
            "backend": "docker",
            "cpu_arch": "x86_64",
            "allowed_nodes": ["oldlab-1"],
            "env_file": "/secure/.env.remote-worker",
            "repo_dir": "/opt/loom",
            "requested_concurrency": 6,
            "requested_cpus": 12,
            "requested_memory_mib": 58000,
            "max_jobs": 1,
            "pending_job_cap": 1,
            "slurm_account": "loom-staging",
            "slurm_qos": "loom-staging-normal",
            "slurm_reservation": "loom-staging-min",
            "exclusive": False,
            "container_cpus": 2.0,
            "container_memory_mib": 4096,
            "container_pids": 512,
            "candidate_sha": "a" * 40,
        },
    )
    config = _slurm_config_from_policy(row)
    assert config.slurm_account == "loom-staging"
    assert config.slurm_qos == "loom-staging-normal"
    assert config.slurm_reservation == "loom-staging-min"


def _policy(**overrides: object) -> AutoscalerPolicyConfig:
    values: dict[str, object] = {
        "environment": "production",
        "pool_name": "oldlab",
        "actuator": "slurm",
        "enabled": True,
        "min_slots": 6,
        "max_slots": 30,
        "scale_up_threshold_slots": 1,
        "scale_down_idle_seconds": 600,
        "scale_up_cooldown_seconds": 60,
        "scale_down_cooldown_seconds": 300,
        "drain_timeout_seconds": 600,
        "force": False,
        "disabled_reason": None,
        "idle_since_at": None,
        "last_scale_up_at": None,
        "last_scale_down_at": None,
    }
    values.update(overrides)
    return AutoscalerPolicyConfig(**values)  # type: ignore[arg-type]


def _observation(**overrides: object) -> AutoscalerObservation:
    values: dict[str, object] = {
        "active_slots": 0,
        "pending_slots": 0,
        "draining_slots": 0,
        "occupied_slots": 0,
        "queued_slots": 0,
        "idle_worker_ids": (),
        "drained_worker_ids": (),
    }
    values.update(overrides)
    return AutoscalerObservation(**values)  # type: ignore[arg-type]


def _policy_row(**overrides: object) -> WorkerPoolAutoscalerPolicy:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    values: dict[str, object] = {
        "id": uuid4(),
        "environment": "production",
        "pool_name": "oldlab",
        "actuator": "slurm",
        "enabled": True,
        "min_slots": 6,
        "max_slots": 30,
        "scale_up_threshold_slots": 1,
        "scale_down_idle_seconds": 600,
        "scale_up_cooldown_seconds": 60,
        "scale_down_cooldown_seconds": 300,
        "drain_timeout_seconds": 600,
        "force": False,
        "disabled_reason": None,
        "actuator_config": {
            "backend": "docker",
            "cpu_arch": "x86_64",
            "allowed_nodes": ["oldlab-1", "oldlab-2", "oldlab-3"],
            "env_file": "/secure/.env.remote-worker",
            "repo_dir": "/opt/loom",
            "requested_concurrency": 6,
            "requested_cpus": 12,
            "requested_memory_mib": 58000,
            "max_jobs": 3,
            "pending_job_cap": 3,
        },
        "idle_since_at": now - timedelta(seconds=900),
        "last_decision": "noop",
        "last_decision_reason": "at_min_capacity",
        "last_desired_slots": 6,
        "last_actual_slots": 6,
        "last_pending_slots": 0,
        "last_draining_slots": 0,
        "last_occupied_slots": 0,
        "last_queued_slots": 0,
        "last_blocked_reason": None,
        "last_blocked_details": None,
        "last_error": None,
        "last_scale_up_at": now - timedelta(seconds=120),
        "last_scale_down_at": now - timedelta(seconds=800),
        "last_decision_at": now - timedelta(seconds=30),
        "created_at": now - timedelta(days=1),
        "updated_at": now,
    }
    actor_overrides = overrides.pop("actuator_config", None)
    values.update(overrides)
    if actor_overrides is not None:
        values["actuator_config"] = dict(cast(dict[str, Any], actor_overrides))
    actor_config = cast(dict[str, Any], values["actuator_config"])
    if values["actuator"] == "slurm":
        actor_config.setdefault("exclusive", False)
        actor_config.setdefault("container_cpus", 2.0)
        actor_config.setdefault("container_memory_mib", 4096)
        actor_config.setdefault("container_pids", 512)
        actor_config.setdefault("candidate_sha", "a" * 40)
        # #896: non-exclusive workers require a positive job cgroup PID ceiling
        # (>= container_pids * concurrency). Default generously to cover the
        # higher concurrencies some scale-up tests use (512 * 16); tests
        # exercising the guard pass 0 explicitly.
        actor_config.setdefault("job_pids_max", 8192)
    return WorkerPoolAutoscalerPolicy(**values)


class _FakeScalarResult:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def all(self) -> list[Any]:
        return list(self._rows)


class _FakeResult:
    def __init__(
        self,
        *,
        scalar: Any = None,
        scalars: list[Any] | None = None,
        rows: list[Any] | None = None,
    ) -> None:
        self._scalar = scalar
        self._scalars = list(scalars or [])
        self._rows = list(rows or [])

    def scalar_one_or_none(self) -> Any:
        return self._scalar

    def scalars(self) -> _FakeScalarResult:
        return _FakeScalarResult(self._scalars)

    def all(self) -> list[Any]:
        return list(self._rows)


class _FakeSession:
    def __init__(self, results: list[_FakeResult] | None = None) -> None:
        self._results = list(results or [])
        self.executed: list[Any] = []
        self.added: list[Any] = []
        self.flush_count = 0

    async def execute(self, stmt: Any) -> _FakeResult:
        self.executed.append(stmt)
        if not self._results:
            return _FakeResult()
        return self._results.pop(0)

    def add(self, row: Any) -> None:
        self.added.append(row)

    async def flush(self) -> None:
        self.flush_count += 1


class _FakeSlurmRunner:
    def __init__(self) -> None:
        self.submitted_nodes: list[str] = []
        self.submitted_configs: list[Any] = []
        self.cancelled_job_ids: list[str] = []
        self.fail_submit_nodes: set[str] = set()
        self.node_resources: dict[str, SlurmNodeResource] = {}

    async def submit_worker(self, *, node: str, config: Any) -> str:
        self.submitted_nodes.append(node)
        self.submitted_configs.append(config)
        if node in self.fail_submit_nodes:
            raise RuntimeError(f"sbatch failed for {node}")
        return f"job-{node}"

    async def cancel_job(self, job_id: str) -> None:
        self.cancelled_job_ids.append(job_id)

    async def query_node_resources(
        self,
        nodes: tuple[str, ...],
    ) -> dict[str, SlurmNodeResource]:
        return {node: resource for node, resource in self.node_resources.items() if node in nodes}


def test_decision_scales_up_for_queue_deficit_with_max_bound() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(max_slots=18),
        AutoscalerObservation(
            active_slots=6,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=6,
            queued_slots=20,
            idle_worker_ids=(),
            drained_worker_ids=(),
        ),
        now=now,
    )

    assert decision.action == "scale_up"
    assert decision.desired_slots == 18
    assert decision.reason == "queued_deficit"


def test_decision_respects_scale_up_cooldown() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(last_scale_up_at=now - timedelta(seconds=10)),
        AutoscalerObservation(
            active_slots=6,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=6,
            queued_slots=12,
            idle_worker_ids=(),
            drained_worker_ids=(),
        ),
        now=now,
    )

    assert decision.action == "blocked"
    assert decision.blocked_reason == "scale_up_cooldown"


def test_decision_sets_idle_clock_before_scale_down() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(idle_since_at=None),
        AutoscalerObservation(
            active_slots=18,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=0,
            queued_slots=0,
            idle_worker_ids=("worker-1", "worker-2"),
            drained_worker_ids=(),
        ),
        now=now,
    )

    assert decision.action == "noop"
    assert decision.reason == "idle_window_started"
    assert decision.idle_since_at == now


def test_decision_requests_drain_after_idle_window() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(idle_since_at=now - timedelta(seconds=601)),
        AutoscalerObservation(
            active_slots=18,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=0,
            queued_slots=0,
            idle_worker_ids=("worker-1", "worker-2"),
            drained_worker_ids=(),
        ),
        now=now,
    )

    assert decision.action == "request_drain"
    assert decision.desired_slots == 6
    assert decision.worker_ids_to_drain == ("worker-1", "worker-2")


def test_decision_releases_drained_workers_before_new_drain() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(idle_since_at=now - timedelta(seconds=601)),
        AutoscalerObservation(
            active_slots=6,
            pending_slots=0,
            draining_slots=6,
            occupied_slots=0,
            queued_slots=0,
            idle_worker_ids=(),
            drained_worker_ids=("worker-3",),
        ),
        now=now,
    )

    assert decision.action == "release_drained"
    assert decision.worker_ids_to_release == ("worker-3",)


def test_decision_reports_waiting_for_drain_before_release() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(idle_since_at=now - timedelta(seconds=601)),
        AutoscalerObservation(
            active_slots=6,
            pending_slots=0,
            draining_slots=6,
            occupied_slots=1,
            queued_slots=0,
            idle_worker_ids=(),
            drained_worker_ids=(),
        ),
        now=now,
    )

    assert decision.action == "noop"
    assert decision.reason == "waiting_for_drain"


def test_decision_uses_disabled_reason_without_idle_clock() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(enabled=False, disabled_reason="maintenance"),
        _observation(active_slots=18, queued_slots=20),
        now=now,
    )

    assert decision.action == "noop"
    assert decision.reason == "maintenance"
    assert decision.desired_slots == 6
    assert decision.idle_since_at is None


def test_decision_scales_up_for_min_warm_capacity() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(min_slots=6, max_slots=12),
        _observation(active_slots=0, queued_slots=0),
        now=now,
    )

    assert decision.action == "scale_up"
    assert decision.reason == "min_warm_capacity"
    assert decision.desired_slots == 6


def test_decision_blocks_when_max_slots_prevents_scale_up() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(min_slots=6, max_slots=12),
        _observation(active_slots=12, occupied_slots=12, queued_slots=6),
        now=now,
    )

    assert decision.action == "blocked"
    assert decision.reason == "max_slots_reached"
    assert decision.blocked_reason == "max_slots_reached"


def test_decision_reports_busy_when_queue_has_no_deficit() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(min_slots=6, max_slots=12),
        _observation(active_slots=12, occupied_slots=2, queued_slots=1),
        now=now,
    )

    assert decision.action == "noop"
    assert decision.reason == "busy"
    assert decision.idle_since_at is None


def test_decision_reports_at_min_capacity_when_no_excess_slots() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(min_slots=6),
        _observation(active_slots=6, occupied_slots=0, queued_slots=0),
        now=now,
    )

    assert decision.action == "noop"
    assert decision.reason == "at_min_capacity"


def test_decision_blocks_on_release_drift_slurm_capacity() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(min_slots=1),
        _observation(
            active_slots=0,
            pending_slots=0,
            release_drift_slots=1,
            release_drift_job_ids=("17928",),
        ),
        now=now,
    )

    assert decision.action == "blocked"
    assert decision.reason == "release_state_drift"
    assert decision.blocked_reason == "release_state_drift"
    assert decision.error_message == "release-state drift in Slurm job(s): 17928"


def test_decision_drains_linked_worker_with_release_drift() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(min_slots=1),
        _observation(
            active_slots=0,
            release_drift_slots=1,
            release_drift_job_ids=("17928",),
            release_drift_worker_ids_to_drain=("worker-7",),
        ),
        now=now,
    )

    assert decision.action == "request_drain"
    assert decision.reason == "release_state_drift"
    assert decision.worker_ids_to_drain == ("worker-7",)
    assert decision.blocked_reason == "release_state_drift"


def test_decision_releases_linked_worker_after_release_drift_drain() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(min_slots=1),
        _observation(
            active_slots=0,
            release_drift_slots=1,
            release_drift_job_ids=("17928",),
            release_drift_worker_ids_to_release=("worker-7",),
        ),
        now=now,
    )

    assert decision.action == "release_drained"
    assert decision.reason == "release_state_drift"
    assert decision.worker_ids_to_release == ("worker-7",)
    assert decision.blocked_reason is None


def test_decision_waits_for_idle_window_before_scale_down() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    idle_since_at = now - timedelta(seconds=60)

    decision = compute_autoscaler_decision(
        _policy(idle_since_at=idle_since_at, scale_down_idle_seconds=600),
        _observation(active_slots=18, idle_worker_ids=("worker-1",)),
        now=now,
    )

    assert decision.action == "noop"
    assert decision.reason == "idle_window_waiting"
    assert decision.idle_since_at == idle_since_at


def test_decision_respects_scale_down_cooldown() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(
            idle_since_at=now - timedelta(seconds=900),
            last_scale_down_at=now - timedelta(seconds=60),
        ),
        _observation(active_slots=18, idle_worker_ids=("worker-1",)),
        now=now,
    )

    assert decision.action == "blocked"
    assert decision.reason == "scale_down_cooldown"
    assert decision.blocked_reason == "scale_down_cooldown"


def test_decision_blocks_scale_down_until_idle_workers_exist() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)

    decision = compute_autoscaler_decision(
        _policy(idle_since_at=now - timedelta(seconds=900)),
        _observation(active_slots=18, idle_worker_ids=()),
        now=now,
    )

    assert decision.action == "blocked"
    assert decision.reason == "waiting_for_idle_workers"
    assert decision.blocked_reason == "waiting_for_idle_workers"


def test_policy_serialization_and_config_preserve_status_fields() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    row = _policy_row(updated_at=now, last_decision_at=now - timedelta(seconds=5))

    body = autoscaler_policy_to_dict(row)
    config = _policy_to_config(row)

    assert body["environment"] == "production"
    assert body["pool_name"] == "oldlab"
    assert body["idle_since_at"] == row.idle_since_at.isoformat()
    assert body["last_decision_at"] == (now - timedelta(seconds=5)).isoformat()
    assert body["updated_at"] == now.isoformat()
    assert body["last_blocked_details"] is None
    assert config.environment == "production"
    assert config.pool_name == "oldlab"
    assert config.actuator_config == row.actuator_config


@pytest.mark.parametrize(
    ("field", "overrides", "message"),
    [
        ("actuator", {"actuator": "ssh"}, "actuator"),
        ("min_slots", {"min_slots": -1}, "min_slots"),
        ("max_slots", {"min_slots": 10, "max_slots": 5}, "max_slots"),
        (
            "scale_up_threshold_slots",
            {"scale_up_threshold_slots": -1},
            "scale_up_threshold_slots",
        ),
        (
            "scale_down_idle_seconds",
            {"scale_down_idle_seconds": -1},
            "scale_down_idle_seconds",
        ),
        (
            "scale_up_cooldown_seconds",
            {"scale_up_cooldown_seconds": -1},
            "scale_up_cooldown_seconds",
        ),
        (
            "scale_down_cooldown_seconds",
            {"scale_down_cooldown_seconds": -1},
            "scale_down_cooldown_seconds",
        ),
        ("drain_timeout_seconds", {"drain_timeout_seconds": 0}, "drain_timeout"),
    ],
)
def test_policy_validation_rejects_invalid_fields(
    field: str,
    overrides: dict[str, object],
    message: str,
) -> None:
    values: dict[str, object] = {
        "actuator": "slurm",
        "min_slots": 0,
        "max_slots": 6,
        "scale_up_threshold_slots": 1,
        "scale_down_idle_seconds": 600,
        "scale_up_cooldown_seconds": 60,
        "scale_down_cooldown_seconds": 300,
        "drain_timeout_seconds": 600,
    }
    values.update(overrides)

    with pytest.raises(ValueError, match=message):
        _validate_policy_fields(**values)  # type: ignore[arg-type]

    assert field


def test_clean_nonempty_strips_values_and_rejects_blank() -> None:
    assert _clean_nonempty(" production ", "environment") == "production"
    with pytest.raises(ValueError, match="environment"):
        _clean_nonempty("  ", "environment")


def test_queued_trial_capability_matching_uses_policy_backend_and_arch() -> None:
    slurm = _policy_row(
        actuator="slurm",
        actuator_config={"backend": "docker", "cpu_arch": "x86_64"},
    )
    gb10 = _policy_row(
        actuator="gb10",
        pool_name="gb10",
        actuator_config={"backend": "docker"},
    )

    assert _queued_trial_matches_policy({}, slurm) is True
    assert _queued_trial_matches_policy(None, slurm) is True
    assert _queued_trial_matches_policy({"backend": "docker"}, slurm) is True
    assert _queued_trial_matches_policy({"backend": "k8s"}, slurm) is False
    assert _queued_trial_matches_policy({"cpu_arch": "any"}, slurm) is True
    assert _queued_trial_matches_policy({"cpu_arch": "arm64"}, slurm) is False
    assert _queued_trial_matches_policy({"cpu_arch": "arm64"}, gb10) is True
    assert _queued_trial_matches_policy({"worker_pool": "oldlab"}, slurm) is True
    assert _queued_trial_matches_policy({"worker_pool": "gb10"}, slurm) is False
    assert _queued_trial_matches_policy({"worker_pool": "gb10"}, gb10) is True
    assert _queued_trial_matches_policy({"worker_pool": "oldlab"}, gb10) is False


def test_persist_decision_updates_scale_up_and_scale_down_timestamps() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    row = _policy_row(last_scale_up_at=None, last_scale_down_at=None)

    _persist_decision(
        row,
        AutoscalerDecision(
            action="scale_up",
            reason="queued_deficit",
            desired_slots=12,
            actual_slots=6,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=6,
            queued_slots=6,
            blocked_reason=None,
            error_message=None,
        ),
        now=now,
    )

    assert row.last_decision == "scale_up"
    assert row.last_decision_reason == "queued_deficit"
    assert row.last_desired_slots == 12
    assert row.last_scale_up_at == now
    assert row.last_scale_down_at is None

    _persist_decision(
        row,
        AutoscalerDecision(
            action="release_drained",
            reason="drain_complete",
            desired_slots=6,
            actual_slots=0,
            pending_slots=0,
            draining_slots=6,
            occupied_slots=0,
            queued_slots=0,
            idle_since_at=now - timedelta(seconds=900),
            worker_ids_to_release=("worker-1",),
        ),
        now=now + timedelta(seconds=10),
    )

    assert row.last_decision == "release_drained"
    assert row.last_scale_down_at == now + timedelta(seconds=10)
    assert row.idle_since_at == now - timedelta(seconds=900)


def test_persist_decision_and_serialization_include_blocked_details() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    row = _policy_row(last_scale_up_at=None, last_blocked_details=None)
    details = {
        "reason": "no_safe_slurm_nodes",
        "node_exclusions": [
            {"hostname": "oldlab-1", "reason": "insufficient_memory"},
        ],
    }

    _persist_decision(
        row,
        AutoscalerDecision(
            action="blocked",
            reason="no_safe_slurm_nodes",
            desired_slots=1,
            actual_slots=0,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=0,
            queued_slots=1,
            blocked_reason="no_safe_slurm_nodes",
            blocked_details=details,
        ),
        now=now,
    )

    assert row.last_decision == "blocked"
    assert row.last_blocked_reason == "no_safe_slurm_nodes"
    assert row.last_blocked_details == details
    assert row.last_scale_up_at is None
    assert autoscaler_policy_to_dict(row)["last_blocked_details"] == details


def test_slurm_config_from_policy_uses_actuator_config_defaults_and_overrides() -> None:
    row = _policy_row(
        max_slots=18,
        actuator_config={
            "allowed_nodes": ["oldlab-1", "oldlab-2", "oldlab-3"],
            "env_file": "/secure/.env.remote-worker",
            "repo_dir": "/opt/loom",
            "partition": "cpu",
            "requested_concurrency": 6,
            "requested_cpus": 12,
            "requested_memory_mib": 58000,
            "pending_job_cap": 2,
            "stale_after_seconds": 120,
            "sbatch_path": "/usr/bin/sbatch",
            "squeue_path": "/usr/bin/squeue",
            "sacct_path": "/usr/bin/sacct",
            "scancel_path": "/usr/bin/scancel",
            "command_timeout_seconds": 5.5,
            "exclusive": False,
            "container_cpus": 2.0,
            "container_memory_mib": 4096,
            "container_pids": 512,
            "candidate_sha": "a" * 40,
        },
    )

    config = _slurm_config_from_policy(row)

    assert config.allowed_nodes == ("oldlab-1", "oldlab-2", "oldlab-3")
    assert config.max_jobs == 3
    assert config.pending_job_cap == 2
    assert config.partition == "cpu"
    assert config.command_timeout_seconds == 5.5
    assert config.sbatch_path == "/usr/bin/sbatch"

    csv_row = _policy_row(
        max_slots=6,
        actuator_config={
            "allowed_nodes": "oldlab-4",
            "env_file": "/secure/.env.remote-worker",
            "repo_dir": "/opt/loom",
            "requested_concurrency": 6,
            "requested_cpus": 12,
            "requested_memory_mib": 58000,
            "exclusive": False,
            "container_cpus": 2.0,
            "container_memory_mib": 4096,
            "container_pids": 512,
            "candidate_sha": "a" * 40,
        },
    )

    assert _slurm_config_from_policy(csv_row).allowed_nodes == ("oldlab-4",)


def test_slurm_config_from_policy_uses_nonexclusive_node_allocation() -> None:
    row = _policy_row(
        max_slots=1,
        actuator_config={
            "allowed_nodes": ["oldlab-4"],
            "env_file": "/secure/.env.remote-worker",
            "repo_dir": "/opt/loom",
            "requested_concurrency": 1,
            "requested_cpus": 2,
            "requested_memory_mib": 8000,
            "exclusive": False,
            "container_cpus": 2.0,
            "container_memory_mib": 8000,
            "container_pids": 512,
            "job_pids_max": 512,
            "candidate_sha": "a" * 40,
        },
    )

    request = build_sbatch_request(_slurm_config_from_policy(row), node="oldlab-4")

    assert "--exclusive" not in request.args
    assert "--comment=loom-cgroup-v1:pids=512" in request.args


def test_slurm_config_from_policy_rejects_missing_nonexclusive_job_pids_max() -> None:
    row = _policy_row(
        max_slots=1,
        actuator_config={
            "allowed_nodes": ["oldlab-4"],
            "env_file": "/secure/.env.remote-worker",
            "repo_dir": "/opt/loom",
            "requested_concurrency": 1,
            "requested_cpus": 2,
            "requested_memory_mib": 8000,
            "exclusive": False,
            "container_cpus": 2.0,
            "container_memory_mib": 8000,
            "container_pids": 512,
            "candidate_sha": "a" * 40,
            "job_pids_max": 0,
        },
    )

    with pytest.raises(ValueError, match="job_pids_max is required"):
        _slurm_config_from_policy(row)


def test_slurm_config_from_policy_rejects_exclusive_node_allocation() -> None:
    row = _policy_row(
        max_slots=1,
        actuator_config={
            "allowed_nodes": ["oldlab-4"],
            "env_file": "/secure/.env.remote-worker",
            "repo_dir": "/opt/loom",
            "requested_concurrency": 1,
            "requested_cpus": 2,
            "requested_memory_mib": 8000,
            "exclusive": True,
            "container_cpus": 2.0,
            "container_memory_mib": 8000,
            "container_pids": 512,
            "candidate_sha": "a" * 40,
        },
    )

    with pytest.raises(ValueError, match="exclusive Loom Slurm workers"):
        _slurm_config_from_policy(row)


async def test_policy_upsert_and_status_helpers_use_normalized_fields() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    session = _FakeSession([_FakeResult(scalar=None)])

    created = await upsert_autoscaler_policy(
        cast(Any, session),
        environment=" production ",
        pool_name=" oldlab ",
        actuator=" slurm ",
        enabled=True,
        min_slots=6,
        max_slots=12,
        scale_up_threshold_slots=1,
        scale_down_idle_seconds=600,
        scale_up_cooldown_seconds=60,
        scale_down_cooldown_seconds=300,
        drain_timeout_seconds=600,
        force=True,
        disabled_reason="manual pause",
        actuator_config={"allowed_nodes": ["oldlab-1"]},
        now=now,
    )

    assert created in session.added
    assert session.flush_count == 1
    assert created.environment == "production"
    assert created.pool_name == "oldlab"
    assert created.force is True
    assert created.actuator_config == {"allowed_nodes": ["oldlab-1"]}

    existing = _policy_row()
    update_session = _FakeSession([_FakeResult(scalar=existing)])
    updated = await upsert_autoscaler_policy(
        cast(Any, update_session),
        environment="production",
        pool_name="oldlab",
        actuator="gb10",
        enabled=False,
        min_slots=0,
        max_slots=20,
        scale_up_threshold_slots=2,
        scale_down_idle_seconds=120,
        scale_up_cooldown_seconds=30,
        scale_down_cooldown_seconds=90,
        drain_timeout_seconds=300,
        actuator_config={"hosts": ["trt-gb10-1"]},
        now=now,
    )

    assert updated is existing
    assert update_session.added == []
    assert updated.actuator == "gb10"
    assert updated.enabled is False
    assert updated.max_slots == 20

    status_session = _FakeSession([_FakeResult(scalars=[created, updated])])
    status = await fetch_autoscaler_status(
        cast(Any, status_session),
        environment="production",
        pool_name="oldlab",
    )

    assert [policy["pool_name"] for policy in status["policies"]] == [
        "oldlab",
        "oldlab",
    ]


async def test_load_observation_counts_slots_and_matching_queue() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    idle_worker_id = uuid4()
    busy_worker_id = uuid4()
    drained_worker_id = uuid4()
    workers = [
        SimpleNamespace(
            id=idle_worker_id,
            hostname="oldlab-1",
            max_concurrent=6,
            drain_state="active",
        ),
        SimpleNamespace(
            id=busy_worker_id,
            hostname="oldlab-2",
            max_concurrent=6,
            drain_state="active",
        ),
        SimpleNamespace(
            id=drained_worker_id,
            hostname="oldlab-3",
            max_concurrent=6,
            drain_state="draining",
        ),
    ]
    session = _FakeSession(
        [
            _FakeResult(scalars=workers),
            _FakeResult(rows=[(busy_worker_id,), (None,)]),
            _FakeResult(
                scalars=[
                    SimpleNamespace(
                        job_id="pending-1",
                        state="pending",
                        requested_concurrency=6,
                        worker_id=None,
                        nodelist="oldlab-1",
                        redacted_env={
                            "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/.env.remote-worker",
                            "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
                        },
                    ),
                ],
            ),
            _FakeResult(
                rows=[
                    ({"backend": "docker", "cpu_arch": "x86_64"},),
                    ({"backend": "docker", "cpu_arch": "arm64"},),
                    ({},),
                ],
            ),
        ]
    )

    observation = await _load_observation(
        cast(Any, session),
        _policy_row(min_slots=6, last_pending_slots=42),
        now=now,
        freshness_sec=120,
    )

    assert observation.active_slots == 12
    assert observation.pending_slots == 6
    assert observation.draining_slots == 6
    assert observation.occupied_slots == 1
    assert observation.queued_slots == 2
    assert observation.idle_worker_ids == (str(idle_worker_id),)
    assert observation.drained_worker_ids == (str(drained_worker_id),)


async def test_load_observation_slurm_release_excludes_unlinked_static_workers() -> None:
    # #1021: a fresh static worker with no linked Slurm job must never be a
    # drain candidate for the Slurm actuator (only Slurm-owned workers are),
    # though it still counts toward pool capacity.
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    owned_id = uuid4()
    static_id = uuid4()
    workers = [
        SimpleNamespace(id=owned_id, hostname="oldlab-1", max_concurrent=6, drain_state="active"),
        SimpleNamespace(
            id=static_id,
            hostname="gb10-static",
            max_concurrent=6,
            drain_state="active",
        ),
    ]
    session = _FakeSession(
        [
            _FakeResult(scalars=workers),
            _FakeResult(rows=[]),  # no in-flight trials → both idle
            _FakeResult(
                scalars=[
                    SimpleNamespace(
                        job_id="run-1",
                        state="running",
                        requested_concurrency=6,
                        worker_id=owned_id,
                        nodelist="oldlab-1",
                        redacted_env={
                            "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/.env.remote-worker",
                            "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
                        },
                    ),
                ],
            ),
            _FakeResult(rows=[]),  # no queued trials
        ]
    )

    observation = await _load_observation(
        cast(Any, session),
        _policy_row(min_slots=0),
        now=now,
        freshness_sec=120,
    )

    # Both workers count toward capacity, but only the Slurm-owned one is drainable.
    assert observation.active_slots == 12
    assert observation.idle_worker_ids == (str(owned_id),)


async def test_load_observation_excludes_release_drift_slurm_jobs() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    workers = [
        SimpleNamespace(
            id=worker_id,
            hostname="TRT-EAI-OLDLAB-1",
            max_concurrent=1,
            drain_state="active",
        ),
    ]
    session = _FakeSession(
        [
            _FakeResult(scalars=workers),
            _FakeResult(rows=[]),
            _FakeResult(
                scalars=[
                    SimpleNamespace(
                        job_id="17928",
                        state="running",
                        requested_concurrency=1,
                        worker_id=worker_id,
                        nodelist="TRT-EAI-OLDLAB-1",
                        redacted_env={
                            "LOOM_REMOTE_WORKER_ENV_FILE": (
                                "/shared_work/qianyi/loom-worker-capacity/"
                                "staging-oldlab-worker-staging-a88e33a4.env"
                            ),
                            "LOOM_REMOTE_WORKER_REPO_DIR": (
                                "/shared_work/qianyi/loom-remote-worker-staging-a88e33a4"
                            ),
                        },
                    ),
                ],
            ),
            _FakeResult(rows=[]),
        ]
    )

    observation = await _load_observation(
        cast(Any, session),
        _policy_row(
            min_slots=1,
            actuator_config={
                "allowed_nodes": ["TRT-EAI-OLDLAB-1"],
                "env_file": (
                    "/shared_work/qianyi/loom-worker-capacity/"
                    "staging-oldlab-worker-staging-a232312f.env"
                ),
                "repo_dir": ("/shared_work/qianyi/loom-remote-worker-staging-a232312f"),
                "requested_concurrency": 1,
                "requested_cpus": 2,
                "requested_memory_mib": 8192,
                "external_runner": True,
            },
        ),
        now=now,
        freshness_sec=120,
    )

    assert observation.active_slots == 0
    assert observation.pending_slots == 0
    assert observation.release_drift_slots == 1
    assert observation.release_drift_job_ids == ("17928",)
    assert observation.idle_worker_ids == ()


async def test_load_observation_quarantines_running_job_outside_allowed_nodes() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    worker = SimpleNamespace(
        id=worker_id,
        hostname="trt-gb10-7",
        max_concurrent=10,
        drain_state="active",
    )
    session = _FakeSession(
        [
            _FakeResult(scalars=[worker]),
            _FakeResult(rows=[]),
            _FakeResult(
                scalars=[
                    SimpleNamespace(
                        job_id="17928",
                        state="running",
                        requested_concurrency=10,
                        worker_id=worker_id,
                        nodelist="trt-gb10-7",
                        redacted_env={
                            "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/.env.remote-worker",
                            "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
                        },
                    ),
                ],
            ),
            _FakeResult(rows=[]),
        ],
    )

    observation = await _load_observation(
        cast(Any, session),
        _policy_row(
            pool_name="gb10",
            min_slots=0,
            actuator_config={
                "allowed_nodes": ["trt-gb10-1"],
                "env_file": "/secure/.env.remote-worker",
                "repo_dir": "/opt/loom",
                "requested_concurrency": 10,
                "requested_cpus": 20,
                "requested_memory_mib": 115000,
                "max_jobs": 1,
                "pending_job_cap": 1,
            },
        ),
        now=now,
        freshness_sec=120,
    )

    assert observation.active_slots == 0
    assert observation.pending_slots == 0
    assert observation.release_drift_slots == 10
    assert observation.release_drift_job_ids == ("17928",)
    assert observation.release_drift_worker_ids_to_drain == (str(worker_id),)
    assert observation.idle_worker_ids == ()


async def test_load_observation_does_not_drain_ambiguous_hostname_registration() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    workers = [
        SimpleNamespace(
            id=uuid4(),
            hostname="trt-gb10-7",
            max_concurrent=10,
            drain_state="active",
        ),
        SimpleNamespace(
            id=uuid4(),
            hostname="trt-gb10-7",
            max_concurrent=10,
            drain_state="active",
        ),
    ]
    session = _FakeSession(
        [
            _FakeResult(scalars=workers),
            _FakeResult(rows=[]),
            _FakeResult(
                scalars=[
                    SimpleNamespace(
                        job_id="17928",
                        state="running",
                        requested_concurrency=10,
                        worker_id=None,
                        nodelist="trt-gb10-7",
                        redacted_env={
                            "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/.env.remote-worker",
                            "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
                        },
                    ),
                ],
            ),
            _FakeResult(rows=[]),
        ],
    )

    observation = await _load_observation(
        cast(Any, session),
        _policy_row(
            pool_name="gb10",
            min_slots=0,
            actuator_config={
                "allowed_nodes": ["trt-gb10-1"],
                "env_file": "/secure/.env.remote-worker",
                "repo_dir": "/opt/loom",
                "requested_concurrency": 10,
                "requested_cpus": 20,
                "requested_memory_mib": 115000,
                "max_jobs": 1,
                "pending_job_cap": 1,
            },
        ),
        now=now,
        freshness_sec=120,
    )

    decision = compute_autoscaler_decision(
        _policy(min_slots=0),
        observation,
        now=now,
    )
    assert observation.active_slots == 0
    assert observation.release_drift_job_ids == ("17928",)
    assert observation.release_drift_worker_ids_to_drain == ()
    assert observation.release_drift_worker_ids_to_release == ()
    assert decision.action == "blocked"
    assert decision.blocked_reason == "release_state_drift"


async def test_load_observation_does_not_drain_null_link_with_other_job_on_host() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    jobs = [
        SimpleNamespace(
            job_id="stale-job",
            state="running",
            requested_cpus=10,
            requested_memory_mib=60000,
            requested_concurrency=5,
            worker_id=None,
            nodelist="trt-gb10-1",
            redacted_env={
                "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/stale.env",
                "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
            },
        ),
        SimpleNamespace(
            job_id="current-job",
            state="running",
            requested_cpus=20,
            requested_memory_mib=115000,
            requested_concurrency=10,
            worker_id=None,
            nodelist="trt-gb10-1",
            redacted_env={
                "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/.env.remote-worker",
                "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
            },
        ),
    ]
    session = _FakeSession(
        [
            _FakeResult(
                scalars=[
                    SimpleNamespace(
                        id=worker_id,
                        hostname="trt-gb10-1",
                        max_concurrent=10,
                        drain_state="active",
                    ),
                ],
            ),
            _FakeResult(rows=[]),
            _FakeResult(scalars=jobs),
            _FakeResult(rows=[]),
        ],
    )

    observation = await _load_observation(
        cast(Any, session),
        _policy_row(
            pool_name="gb10",
            min_slots=0,
            actuator_config={
                "allowed_nodes": ["trt-gb10-1"],
                "env_file": "/secure/.env.remote-worker",
                "repo_dir": "/opt/loom",
                "requested_concurrency": 10,
                "requested_cpus": 20,
                "requested_memory_mib": 115000,
                "max_jobs": 1,
                "pending_job_cap": 1,
            },
        ),
        now=now,
        freshness_sec=120,
    )

    decision = compute_autoscaler_decision(
        _policy(min_slots=0),
        observation,
        now=now,
    )
    assert observation.release_drift_job_ids == ("stale-job",)
    assert observation.release_drift_worker_ids_to_drain == ()
    assert observation.release_drift_worker_ids_to_release == ()
    assert decision.action == "blocked"
    assert decision.blocked_reason == "release_state_drift"


async def test_load_observation_does_not_drain_duplicate_active_worker_link() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    jobs = [
        SimpleNamespace(
            job_id="stale-job",
            state="running",
            requested_concurrency=10,
            worker_id=worker_id,
            nodelist="trt-gb10-7",
            redacted_env={
                "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/.env.remote-worker",
                "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
            },
        ),
        SimpleNamespace(
            job_id="current-job",
            state="running",
            requested_concurrency=10,
            worker_id=worker_id,
            nodelist="trt-gb10-1",
            redacted_env={
                "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/.env.remote-worker",
                "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
            },
        ),
    ]
    session = _FakeSession(
        [
            _FakeResult(
                scalars=[
                    SimpleNamespace(
                        id=worker_id,
                        hostname="trt-gb10-7",
                        max_concurrent=10,
                        drain_state="active",
                    ),
                ],
            ),
            _FakeResult(rows=[]),
            _FakeResult(scalars=jobs),
            _FakeResult(rows=[]),
        ],
    )

    observation = await _load_observation(
        cast(Any, session),
        _policy_row(
            pool_name="gb10",
            min_slots=0,
            actuator_config={
                "allowed_nodes": ["trt-gb10-1"],
                "env_file": "/secure/.env.remote-worker",
                "repo_dir": "/opt/loom",
                "requested_concurrency": 10,
                "requested_cpus": 20,
                "requested_memory_mib": 115000,
                "max_jobs": 1,
                "pending_job_cap": 1,
            },
        ),
        now=now,
        freshness_sec=120,
    )

    decision = compute_autoscaler_decision(
        _policy(min_slots=0),
        observation,
        now=now,
    )
    assert observation.release_drift_job_ids == ("stale-job",)
    assert observation.release_drift_worker_ids_to_drain == ()
    assert observation.release_drift_worker_ids_to_release == ()
    assert decision.action == "blocked"
    assert decision.blocked_reason == "release_state_drift"


async def test_load_observation_does_not_drain_worker_with_mismatched_hostname() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    session = _FakeSession(
        [
            _FakeResult(
                scalars=[
                    SimpleNamespace(
                        id=worker_id,
                        hostname="trt-gb10-1",
                        max_concurrent=10,
                        drain_state="active",
                    ),
                ],
            ),
            _FakeResult(rows=[]),
            _FakeResult(
                scalars=[
                    SimpleNamespace(
                        job_id="17928",
                        state="running",
                        requested_concurrency=10,
                        worker_id=worker_id,
                        nodelist="trt-gb10-7",
                        redacted_env={
                            "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/.env.remote-worker",
                            "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
                        },
                    ),
                ],
            ),
            _FakeResult(rows=[]),
        ],
    )

    observation = await _load_observation(
        cast(Any, session),
        _policy_row(
            pool_name="gb10",
            min_slots=0,
            actuator_config={
                "allowed_nodes": ["trt-gb10-1"],
                "env_file": "/secure/.env.remote-worker",
                "repo_dir": "/opt/loom",
                "requested_concurrency": 10,
                "requested_cpus": 20,
                "requested_memory_mib": 115000,
                "max_jobs": 1,
                "pending_job_cap": 1,
            },
        ),
        now=now,
        freshness_sec=120,
    )

    decision = compute_autoscaler_decision(
        _policy(min_slots=0),
        observation,
        now=now,
    )
    assert observation.release_drift_worker_ids_to_drain == ()
    assert observation.release_drift_worker_ids_to_release == ()
    assert decision.action == "blocked"
    assert decision.blocked_reason == "release_state_drift"


async def test_load_observation_excludes_worker_token_release_drift(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    env_file = tmp_path / "remote-worker.env"
    env_file.write_text('LOOM_WORKER_TOKEN="current-token"\n', encoding="utf-8")
    worker_id = uuid4()
    workers = [
        SimpleNamespace(
            id=worker_id,
            hostname="TRT-EAI-OLDLAB-1",
            max_concurrent=1,
            drain_state="active",
        ),
    ]
    session = _FakeSession(
        [
            _FakeResult(scalars=workers),
            _FakeResult(rows=[]),
            _FakeResult(
                scalars=[
                    SimpleNamespace(
                        job_id="17929",
                        state="running",
                        requested_concurrency=1,
                        worker_id=worker_id,
                        nodelist="TRT-EAI-OLDLAB-1",
                        redacted_env={
                            "LOOM_REMOTE_WORKER_ENV_FILE": str(env_file),
                            "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
                            WORKER_AUTH_FINGERPRINT_ENV_KEY: worker_token_fingerprint(
                                "stale-token",
                            ),
                        },
                    ),
                ],
            ),
            _FakeResult(rows=[]),
        ]
    )

    observation = await _load_observation(
        cast(Any, session),
        _policy_row(
            min_slots=1,
            actuator_config={
                "allowed_nodes": ["TRT-EAI-OLDLAB-1"],
                "env_file": str(env_file),
                "repo_dir": "/opt/loom",
                "requested_concurrency": 1,
                "requested_cpus": 2,
                "requested_memory_mib": 8192,
                "external_runner": True,
            },
        ),
        now=now,
        freshness_sec=120,
    )

    assert observation.active_slots == 0
    assert observation.release_drift_slots == 1
    assert observation.release_drift_job_ids == ("17929",)


async def test_request_worker_drain_skips_empty_and_executes_update() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    empty_session = _FakeSession()

    await _request_worker_drain(
        cast(Any, empty_session),
        worker_ids=(),
        now=now,
        reason="idle_excess_capacity",
    )

    assert empty_session.executed == []

    update_session = _FakeSession()
    await _request_worker_drain(
        cast(Any, update_session),
        worker_ids=("worker-1",),
        now=now,
        reason="idle_excess_capacity",
    )

    assert len(update_session.executed) == 1


async def test_apply_slurm_scale_up_records_success_and_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_token = "loom_w_current_environment_token"
    env_file = tmp_path / ".env.remote-worker"
    env_file.write_text(f'export LOOM_WORKER_TOKEN="{worker_token}"\n', encoding="utf-8")
    recorded_jobs: list[dict[str, Any]] = []

    async def fake_record_slurm_worker_job(*args: Any, **kwargs: Any) -> None:
        recorded_jobs.append(kwargs)

    monkeypatch.setattr(
        "loom_control_plane.worker_pool_autoscaler.record_slurm_worker_job",
        fake_record_slurm_worker_job,
    )
    session = _FakeSession([_FakeResult(scalars=[])])
    runner = _FakeSlurmRunner()
    runner.fail_submit_nodes.add("oldlab-2")

    result = await _apply_slurm_scale_up(
        cast(Any, session),
        _policy_row(
            min_slots=0,
            max_slots=12,
            actuator_config={
                "allowed_nodes": ["oldlab-1", "oldlab-2"],
                "env_file": str(env_file),
                "repo_dir": "/opt/loom",
                "requested_concurrency": 6,
                "requested_cpus": 12,
                "requested_memory_mib": 58000,
                "max_jobs": 2,
                "pending_job_cap": 2,
            },
        ),
        AutoscalerDecision(
            action="scale_up",
            reason="queued_deficit",
            desired_slots=12,
            actual_slots=0,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=0,
            queued_slots=12,
        ),
        runner=runner,
        now=now,
    )

    assert runner.submitted_nodes == ["oldlab-1", "oldlab-2"]
    assert result.error == "sbatch failed for oldlab-2"
    assert result.blocked_reason is None
    assert result.blocked_details is None
    assert [job["nodelist"] for job in recorded_jobs] == ["oldlab-1", "oldlab-2"]
    assert recorded_jobs[0]["job_id"] == "job-oldlab-1"
    assert recorded_jobs[1]["job_id"] is None
    assert recorded_jobs[1]["submission_error"] == "sbatch failed for oldlab-2"
    assert recorded_jobs[0]["env"][WORKER_AUTH_FINGERPRINT_ENV_KEY] == (
        worker_token_fingerprint(worker_token)
    )
    assert recorded_jobs[1]["env"][WORKER_AUTH_FINGERPRINT_ENV_KEY] == (
        worker_token_fingerprint(worker_token)
    )
    assert "LOOM_WORKER_TOKEN" not in recorded_jobs[0]["env"]


async def test_apply_slurm_scale_up_fail_closes_before_counting_drift_job_caps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded_jobs: list[dict[str, Any]] = []

    async def fake_record_slurm_worker_job(*args: Any, **kwargs: Any) -> None:
        recorded_jobs.append(kwargs)

    monkeypatch.setattr(
        "loom_control_plane.worker_pool_autoscaler.record_slurm_worker_job",
        fake_record_slurm_worker_job,
    )
    session = _FakeSession(
        [
            _FakeResult(
                scalars=[
                    SimpleNamespace(
                        nodelist="trt-gb10-7",
                        state="running",
                        job_id="old-job-7",
                        redacted_env={
                            "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/.env.remote-worker",
                            "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
                        },
                    ),
                ],
            ),
        ],
    )
    runner = _FakeSlurmRunner()

    result = await _apply_slurm_scale_up(
        cast(Any, session),
        _policy_row(
            pool_name="gb10",
            min_slots=0,
            max_slots=10,
            actuator_config={
                "allowed_nodes": ["trt-gb10-1"],
                "env_file": "/secure/.env.remote-worker",
                "repo_dir": "/opt/loom",
                "requested_concurrency": 10,
                "requested_cpus": 20,
                "requested_memory_mib": 115000,
                "max_jobs": 1,
                "pending_job_cap": 1,
            },
        ),
        AutoscalerDecision(
            action="scale_up",
            reason="queued_deficit",
            desired_slots=10,
            actual_slots=0,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=0,
            queued_slots=10,
        ),
        runner=runner,
        now=datetime(2026, 6, 27, 12, 0, tzinfo=UTC),
    )

    assert result.error == "release-state drift in Slurm job(s): old-job-7"
    assert result.blocked_reason == "release_state_drift"
    assert result.blocked_details == {
        "reason": "release_state_drift",
        "job_ids": ["old-job-7"],
        "nodes": ["trt-gb10-7"],
    }
    assert runner.submitted_nodes == []
    assert recorded_jobs == []


async def test_apply_slurm_scale_up_respects_pending_job_cap() -> None:
    session = _FakeSession(
        [
            _FakeResult(
                scalars=[
                    SimpleNamespace(
                        nodelist="oldlab-1",
                        state="pending",
                        job_id="pending-1",
                        redacted_env={
                            "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/.env.remote-worker",
                            "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
                        },
                    ),
                ]
            ),
        ]
    )
    runner = _FakeSlurmRunner()

    result = await _apply_slurm_scale_up(
        cast(Any, session),
        _policy_row(
            min_slots=0,
            max_slots=12,
            actuator_config={
                "allowed_nodes": ["oldlab-1", "oldlab-2"],
                "env_file": "/secure/.env.remote-worker",
                "repo_dir": "/opt/loom",
                "requested_concurrency": 6,
                "requested_cpus": 12,
                "requested_memory_mib": 58000,
                "max_jobs": 2,
                "pending_job_cap": 1,
            },
        ),
        AutoscalerDecision(
            action="scale_up",
            reason="queued_deficit",
            desired_slots=12,
            actual_slots=0,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=0,
            queued_slots=12,
        ),
        runner=runner,
        now=datetime(2026, 6, 27, 12, 0, tzinfo=UTC),
    )

    assert result.error is None
    assert result.blocked_reason is None
    assert result.blocked_details is None
    assert runner.submitted_nodes == []


async def test_apply_slurm_scale_up_records_resource_aware_concurrency_per_node(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    recorded_jobs: list[dict[str, Any]] = []

    async def fake_record_slurm_worker_job(*args: Any, **kwargs: Any) -> None:
        recorded_jobs.append(kwargs)

    monkeypatch.setattr(
        "loom_control_plane.worker_pool_autoscaler.record_slurm_worker_job",
        fake_record_slurm_worker_job,
    )
    session = _FakeSession([_FakeResult(scalars=[])])
    runner = _FakeSlurmRunner()
    runner.node_resources = {
        "oldlab-1": SlurmNodeResource("oldlab-1", "mixed", 24, 120_000, 4.0),
        "oldlab-2": SlurmNodeResource(
            "oldlab-2",
            "mixed",
            24,
            57_344,
            4.0,
            idle_cpus=12,
        ),
        "oldlab-3": SlurmNodeResource("oldlab-3", "mixed", 24, 8_000, 4.0),
    }

    result = await _apply_slurm_scale_up(
        cast(Any, session),
        _policy_row(
            min_slots=0,
            max_slots=12,
            actuator_config={
                "allowed_nodes": ["oldlab-1", "oldlab-2", "oldlab-3"],
                "env_file": "/secure/.env.remote-worker",
                "repo_dir": "/opt/loom",
                "requested_concurrency": 1,
                "requested_cpus": 2,
                "requested_memory_mib": 8192,
                "max_jobs": 3,
                "pending_job_cap": 3,
                "resource_aware": True,
                "cpu_per_slot": 2,
                "memory_mib_per_slot": 8192,
                "reserved_cpus": 4,
                "reserved_memory_mib": 24_576,
                "max_concurrency_per_node": 8,
            },
        ),
        AutoscalerDecision(
            action="scale_up",
            reason="queued_deficit",
            desired_slots=12,
            actual_slots=0,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=0,
            queued_slots=12,
        ),
        runner=runner,
        now=now,
    )

    assert result.error is None
    assert result.blocked_reason is None
    assert result.blocked_details is None
    assert runner.submitted_nodes == ["oldlab-1", "oldlab-2"]
    assert [config.requested_concurrency for config in runner.submitted_configs] == [8, 4]
    assert [job["requested_concurrency"] for job in recorded_jobs] == [8, 4]
    assert [job["requested_cpus"] for job in recorded_jobs] == [16, 8]
    assert [job["requested_memory_mib"] for job in recorded_jobs] == [65_536, 32_768]
    assert [job["env"]["LOOM_WORKER_MAX_CONCURRENT"] for job in recorded_jobs] == [
        "8",
        "4",
    ]


async def test_apply_slurm_scale_up_reports_no_safe_node_details() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    session = _FakeSession([_FakeResult(scalars=[])])
    runner = _FakeSlurmRunner()
    runner.node_resources = {
        "oldlab-1": SlurmNodeResource("oldlab-1", "mixed", 24, 8_000, 4.0),
        "oldlab-2": SlurmNodeResource("oldlab-2", "drain", 24, 120_000, 4.0),
        "oldlab-3": SlurmNodeResource("oldlab-3", "mixed", 24, 120_000, 30.0),
    }

    result = await _apply_slurm_scale_up(
        cast(Any, session),
        _policy_row(
            min_slots=0,
            max_slots=12,
            actuator_config={
                "allowed_nodes": ["oldlab-1", "oldlab-2", "oldlab-3"],
                "env_file": "/secure/.env.remote-worker",
                "repo_dir": "/opt/loom",
                "requested_concurrency": 1,
                "requested_cpus": 2,
                "requested_memory_mib": 8192,
                "max_jobs": 3,
                "pending_job_cap": 3,
                "resource_aware": True,
                "cpu_per_slot": 2,
                "memory_mib_per_slot": 8192,
                "reserved_cpus": 4,
                "reserved_memory_mib": 24_576,
                "max_concurrency_per_node": 8,
            },
        ),
        AutoscalerDecision(
            action="scale_up",
            reason="min_warm_capacity",
            desired_slots=6,
            actual_slots=0,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=0,
            queued_slots=0,
        ),
        runner=runner,
        now=now,
    )

    assert runner.submitted_nodes == []
    assert result.error is None
    assert result.blocked_reason == "no_safe_slurm_nodes"
    assert result.blocked_details == {
        "reason": "no_safe_slurm_nodes",
        "slurm_decision_reason": "no_safe_nodes",
        "resource_aware": True,
        "allowed_nodes": ["oldlab-1", "oldlab-2", "oldlab-3"],
        "node_exclusions": [
            {
                "hostname": "oldlab-1",
                "reason": "insufficient_memory",
                "safe_slots": 0,
                "state": "mixed",
                "cpus_total": 24,
                "idle_cpus": None,
                "cpu_load": 4.0,
                "free_memory_mib": 8000,
            },
            {
                "hostname": "oldlab-2",
                "reason": "unsafe_state",
                "safe_slots": 0,
                "state": "drain",
                "cpus_total": 24,
                "idle_cpus": None,
                "cpu_load": 4.0,
                "free_memory_mib": 120000,
            },
            {
                "hostname": "oldlab-3",
                "reason": "cpu_load_high",
                "safe_slots": 0,
                "state": "mixed",
                "cpus_total": 24,
                "idle_cpus": None,
                "cpu_load": 30.0,
                "free_memory_mib": 120000,
            },
        ],
    }


async def test_apply_slurm_scale_up_blocks_missing_allowed_nodes() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    session = _FakeSession()
    runner = _FakeSlurmRunner()

    result = await _apply_slurm_scale_up(
        cast(Any, session),
        _policy_row(
            actuator_config={
                "allowed_nodes": [],
                "env_file": "/secure/.env.remote-worker",
                "repo_dir": "/opt/loom",
                "requested_concurrency": 1,
                "requested_cpus": 2,
                "requested_memory_mib": 8192,
                "max_jobs": 1,
                "pending_job_cap": 1,
                "resource_aware": True,
            },
        ),
        AutoscalerDecision(
            action="scale_up",
            reason="min_warm_capacity",
            desired_slots=1,
            actual_slots=0,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=0,
            queued_slots=0,
        ),
        runner=runner,
        now=now,
    )

    assert runner.submitted_nodes == []
    assert result.error == ("allowed nodes are required when Slurm worker controller is enabled")
    assert result.blocked_reason == "missing_slurm_allowed_nodes"
    assert result.blocked_details == {
        "reason": "missing_slurm_allowed_nodes",
        "message": "allowed nodes are required when Slurm worker controller is enabled",
        "resource_aware": True,
        "allowed_nodes": [],
    }


async def test_apply_slurm_release_drained_cancels_jobs_after_drain() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    runner = _FakeSlurmRunner()
    empty_session = _FakeSession()

    await _apply_slurm_release_drained(
        cast(Any, empty_session),
        _policy_row(),
        AutoscalerDecision(
            action="release_drained",
            reason="drain_complete",
            desired_slots=6,
            actual_slots=0,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=0,
            queued_slots=0,
        ),
        runner=runner,
        now=now,
    )

    assert empty_session.executed == []

    worker_id = uuid4()
    policy = _policy_row()
    worker = SimpleNamespace(
        id=worker_id,
        hostname="oldlab-1",
        status="active",
        last_seen_at=now,
        drain_state="draining",
    )
    job = SimpleNamespace(
        job_id="9001",
        worker_id=worker_id,
        environment="production",
        pool_name="oldlab",
        nodelist="oldlab-1",
        state="running",
        slurm_state="RUNNING",
        pending_reason=None,
        finished_at=None,
        updated_at=None,
    )
    session = _FakeSession(
        [
            _FakeResult(scalar=policy),
            _FakeResult(scalars=[worker]),
            _FakeResult(rows=[]),
            _FakeResult(scalars=[job]),
        ],
    )

    result = await _apply_slurm_release_drained(
        cast(Any, session),
        policy,
        AutoscalerDecision(
            action="release_drained",
            reason="drain_complete",
            desired_slots=6,
            actual_slots=0,
            pending_slots=0,
            draining_slots=6,
            occupied_slots=0,
            queued_slots=0,
            worker_ids_to_release=(str(worker_id),),
        ),
        runner=runner,
        now=now,
    )

    assert runner.cancelled_job_ids == ["9001"]
    assert job.state == "cancelled"
    assert job.slurm_state == "CANCELLED"
    assert job.pending_reason == "cancelled after autoscaler drain"
    assert job.finished_at == now
    assert result.blocked_reason is None
    assert len(session.executed) == 5


async def test_apply_slurm_release_drained_accepts_job_that_naturally_finished() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    policy = _policy_row()
    worker = SimpleNamespace(
        id=worker_id,
        hostname="oldlab-1",
        status="active",
        last_seen_at=now,
        drain_state="draining",
    )
    session = _FakeSession(
        [
            _FakeResult(scalar=policy),
            _FakeResult(scalars=[worker]),
            _FakeResult(rows=[]),
            _FakeResult(scalars=[]),
        ],
    )
    runner = _FakeSlurmRunner()

    result = await _apply_slurm_release_drained(
        cast(Any, session),
        policy,
        AutoscalerDecision(
            action="release_drained",
            reason="drain_complete",
            desired_slots=6,
            actual_slots=0,
            pending_slots=0,
            draining_slots=6,
            occupied_slots=0,
            queued_slots=0,
            worker_ids_to_release=(str(worker_id),),
        ),
        runner=runner,
        now=now,
    )

    assert result.blocked_reason is None
    assert runner.cancelled_job_ids == []
    assert len(session.executed) == 5


async def test_apply_slurm_release_drained_blocks_duplicate_active_job_link() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    policy = _policy_row()
    worker = SimpleNamespace(
        id=worker_id,
        hostname="oldlab-1",
        status="active",
        last_seen_at=now,
        drain_state="draining",
    )
    jobs = [
        SimpleNamespace(worker_id=worker_id),
        SimpleNamespace(worker_id=worker_id),
    ]
    session = _FakeSession(
        [
            _FakeResult(scalar=policy),
            _FakeResult(scalars=[worker]),
            _FakeResult(rows=[]),
            _FakeResult(scalars=jobs),
        ],
    )
    runner = _FakeSlurmRunner()

    result = await _apply_slurm_release_drained(
        cast(Any, session),
        policy,
        AutoscalerDecision(
            action="release_drained",
            reason="release_state_drift",
            desired_slots=0,
            actual_slots=0,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=0,
            queued_slots=0,
            worker_ids_to_release=(str(worker_id),),
        ),
        runner=runner,
        now=now,
    )

    assert result.blocked_reason == "release_state_drift"
    assert result.blocked_details is not None
    assert result.blocked_details["guard_errors"] == [
        f"{worker_id}: expected at most one active Slurm job, found 2",
    ]
    assert runner.cancelled_job_ids == []
    assert len(session.executed) == 4


async def test_apply_slurm_release_drift_uses_locked_current_allowlist() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    stale_policy = _policy_row(
        pool_name="gb10",
        actuator_config={
            "backend": "docker",
            "cpu_arch": "arm64",
            "allowed_nodes": ["trt-gb10-1"],
            "env_file": "/secure/.env.remote-worker",
            "repo_dir": "/opt/loom",
            "requested_concurrency": 10,
            "requested_cpus": 20,
            "requested_memory_mib": 115000,
            "max_jobs": 1,
            "pending_job_cap": 1,
        },
    )
    current_policy = _policy_row(
        id=stale_policy.id,
        pool_name="gb10",
        actuator_config={
            **dict(stale_policy.actuator_config or {}),
            "allowed_nodes": ["trt-gb10-1", "trt-gb10-7"],
        },
    )
    worker = SimpleNamespace(
        id=worker_id,
        hostname="trt-gb10-7",
        status="active",
        last_seen_at=now,
        drain_state="draining",
    )
    job = SimpleNamespace(
        worker_id=worker_id,
        environment="production",
        pool_name="gb10",
        nodelist="trt-gb10-7",
        state="running",
        job_id="gb10-job-7",
        redacted_env={
            "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/.env.remote-worker",
            "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
        },
    )
    session = _FakeSession(
        [
            _FakeResult(scalar=current_policy),
            _FakeResult(scalars=[worker]),
            _FakeResult(rows=[]),
            _FakeResult(scalars=[job]),
        ],
    )
    runner = _FakeSlurmRunner()

    result = await _apply_slurm_release_drained(
        cast(Any, session),
        stale_policy,
        AutoscalerDecision(
            action="release_drained",
            reason="release_state_drift",
            desired_slots=0,
            actual_slots=0,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=0,
            queued_slots=0,
            worker_ids_to_release=(str(worker_id),),
        ),
        runner=runner,
        now=now,
    )

    assert result.blocked_reason == "release_state_drift"
    assert result.blocked_details is not None
    assert result.blocked_details["guard_errors"] == [
        f"{worker_id}: Slurm job no longer has release-state drift",
    ]
    assert runner.cancelled_job_ids == []
    assert len(session.executed) == 4


@pytest.mark.parametrize(
    ("case", "drain_state", "has_in_flight", "worker_hostname", "guard_message"),
    [
        (
            "in_flight_appeared",
            "draining",
            True,
            "oldlab-1",
            "worker still has in-flight trials",
        ),
        (
            "worker_reactivated",
            "active",
            False,
            "oldlab-1",
            "worker is not draining",
        ),
        (
            "hostname_changed",
            "draining",
            False,
            "oldlab-2",
            "Slurm job hostname does not match worker",
        ),
    ],
)
async def test_apply_slurm_release_revalidates_worker_state(
    case: str,
    drain_state: str,
    has_in_flight: bool,
    worker_hostname: str,
    guard_message: str,
) -> None:
    del case
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    worker_id = uuid4()
    policy = _policy_row()
    worker = SimpleNamespace(
        id=worker_id,
        hostname=worker_hostname,
        status="active",
        last_seen_at=now,
        drain_state=drain_state,
    )
    job = SimpleNamespace(
        worker_id=worker_id,
        environment="production",
        pool_name="oldlab",
        nodelist="oldlab-1",
        state="running",
        job_id="9001",
    )
    session = _FakeSession(
        [
            _FakeResult(scalar=policy),
            _FakeResult(scalars=[worker]),
            _FakeResult(rows=[(worker_id,)] if has_in_flight else []),
            _FakeResult(scalars=[job]),
        ],
    )
    runner = _FakeSlurmRunner()

    result = await _apply_slurm_release_drained(
        cast(Any, session),
        policy,
        AutoscalerDecision(
            action="release_drained",
            reason="drain_complete",
            desired_slots=6,
            actual_slots=0,
            pending_slots=0,
            draining_slots=6,
            occupied_slots=0,
            queued_slots=0,
            worker_ids_to_release=(str(worker_id),),
        ),
        runner=runner,
        now=now,
    )

    assert result.blocked_reason == "release_state_drift"
    assert result.blocked_details is not None
    assert result.blocked_details["guard_errors"] == [
        f"{worker_id}: {guard_message}",
    ]
    assert runner.cancelled_job_ids == []
    assert len(session.executed) == 4


async def test_apply_gb10_host_intent_creates_desired_state_and_updates_nodes() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    node = SimpleNamespace(
        hostname="trt-gb10-1",
        worker_id=None,
        desired_intent="active",
        updated_at=None,
    )
    session = _FakeSession(
        [
            _FakeResult(rows=[("worker-1", "trt-gb10-1")]),
            _FakeResult(scalar=None),
            _FakeResult(scalars=[node]),
        ]
    )

    await _apply_gb10_host_intent(
        cast(Any, session),
        _policy_row(
            actuator="gb10",
            pool_name="gb10",
            actuator_config={
                "image_tag": "gb10-image",
                "max_concurrent": 10,
                "env_config_version": "gb10-env",
            },
        ),
        AutoscalerDecision(
            action="request_drain",
            reason="idle_excess_capacity",
            desired_slots=0,
            actual_slots=10,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=0,
            queued_slots=0,
            worker_ids_to_drain=("worker-1",),
        ),
        worker_ids=("worker-1",),
        intent="draining",
        now=now,
    )

    desired = session.added[0]
    assert desired.host_intents == {"trt-gb10-1": "draining"}
    assert desired.target_slots == 0
    assert node.desired_intent == "draining"
    assert node.updated_at == now


async def test_apply_gb10_scale_up_creates_desired_state_and_selects_hosts() -> None:
    now = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)
    nodes = [
        SimpleNamespace(hostname="trt-gb10-1", current_intent="active"),
        SimpleNamespace(hostname="trt-gb10-2", current_intent="stopped"),
        SimpleNamespace(hostname="trt-gb10-3", current_intent="stopped"),
    ]
    session = _FakeSession(
        [
            _FakeResult(scalar=None),
            _FakeResult(scalars=nodes),
        ]
    )

    await _apply_gb10_scale_up(
        cast(Any, session),
        _policy_row(
            actuator="gb10",
            pool_name="gb10",
            actuator_config={
                "hosts": ["trt-gb10-1", "trt-gb10-2", "trt-gb10-3"],
                "image_tag": "gb10-image",
                "max_concurrent": 10,
                "env_config_version": "gb10-env",
            },
        ),
        AutoscalerDecision(
            action="scale_up",
            reason="queued_deficit",
            desired_slots=11,
            actual_slots=0,
            pending_slots=0,
            draining_slots=0,
            occupied_slots=0,
            queued_slots=11,
        ),
        now=now,
    )

    desired = session.added[0]
    assert session.flush_count == 1
    assert desired.target_slots == 11
    assert desired.host_intents == {
        "trt-gb10-1": "active",
        "trt-gb10-2": "active",
        "trt-gb10-3": "stopped",
    }
    assert len(session.executed) == 4


# ── Dev-instance pool admission (design phase 4 / #1166) ─────────────────────
# `upsert_autoscaler_policy` must reject any `dev-<name>` pool policy outside the
# dev envelope (slurm-only, PER_INSTANCE_CAP, DEV_FLEET_BUDGET) — enforced no
# matter which caller writes the policy. Base pools stay untouched.


async def _upsert_dev(
    session: _FakeSession,
    *,
    pool_name: str,
    actuator: str = "slurm",
    min_slots: int = 0,
    max_slots: int = 1,
) -> Any:
    return await upsert_autoscaler_policy(
        cast(Any, session),
        environment="development",
        pool_name=pool_name,
        actuator=actuator,
        enabled=False,
        min_slots=min_slots,
        max_slots=max_slots,
        scale_up_threshold_slots=1,
        scale_down_idle_seconds=600,
        scale_up_cooldown_seconds=60,
        scale_down_cooldown_seconds=300,
        drain_timeout_seconds=600,
        now=datetime(2026, 8, 5, 12, 0, tzinfo=UTC),
    )


async def test_dev_pool_within_envelope_upserts() -> None:
    from loom.dev_instance import PER_INSTANCE_CAP

    # First execute → the "other dev pools" query (none); second → get policy.
    session = _FakeSession([_FakeResult(rows=[]), _FakeResult(scalar=None)])
    created = await _upsert_dev(
        session,
        pool_name="dev-alice",
        max_slots=PER_INSTANCE_CAP,
    )
    assert created in session.added
    assert created.pool_name == "dev-alice"
    assert created.max_slots == PER_INSTANCE_CAP


async def test_dev_pool_exceeds_per_instance_cap_rejected() -> None:
    from loom.dev_instance import PER_INSTANCE_CAP

    session = _FakeSession([_FakeResult(rows=[])])
    with pytest.raises(ValueError, match="PER_INSTANCE_CAP"):
        await _upsert_dev(
            session,
            pool_name="dev-alice",
            max_slots=PER_INSTANCE_CAP + 1,
        )
    assert session.added == []  # rejected before any write


async def test_dev_pool_wrong_actuator_rejected() -> None:
    # `gb10` passes _validate_policy_fields but dev instances must use slurm.
    session = _FakeSession([_FakeResult(rows=[])])
    with pytest.raises(ValueError, match="slurm"):
        await _upsert_dev(session, pool_name="dev-alice", actuator="gb10")
    assert session.added == []


async def test_dev_pool_exceeds_fleet_budget_rejected() -> None:
    from loom.dev_instance import DEV_FLEET_BUDGET, PER_INSTANCE_CAP

    # Other live dev pools already commit the whole budget; one more slot tips it.
    committed = DEV_FLEET_BUDGET
    others = [
        ("dev-bob", PER_INSTANCE_CAP),
        ("dev-carol", PER_INSTANCE_CAP),
        ("dev-dan", PER_INSTANCE_CAP),
        ("dev-eve", committed - 3 * PER_INSTANCE_CAP),
    ]
    session = _FakeSession([_FakeResult(rows=others)])
    with pytest.raises(ValueError, match="fleet budget"):
        await _upsert_dev(session, pool_name="dev-alice", max_slots=PER_INSTANCE_CAP)
    assert session.added == []


async def test_dev_pool_ignores_malformed_peer_pool() -> None:
    from loom.dev_instance import PER_INSTANCE_CAP

    # A legacy/hand-written malformed dev pool must not 500 an unrelated valid
    # upsert — it's skipped in budget accounting rather than crashing.
    others = [("dev-Bad_Name", PER_INSTANCE_CAP), ("dev-", 99)]
    session = _FakeSession([_FakeResult(rows=others), _FakeResult(scalar=None)])
    created = await _upsert_dev(
        session,
        pool_name="dev-alice",
        max_slots=PER_INSTANCE_CAP,
    )
    assert created.pool_name == "dev-alice"


async def test_non_dev_pool_skips_dev_envelope() -> None:
    # A base pool is untouched by dev admission: gb10 actuator + large slot
    # counts are fine, and only the get-policy query runs (no dev-pool query).
    session = _FakeSession([_FakeResult(scalar=None)])
    created = await _upsert_dev(
        session,
        pool_name="oldlab",
        actuator="gb10",
        max_slots=99,
    )
    assert created.pool_name == "oldlab"
    assert created.max_slots == 99
    assert len(session.executed) == 1  # only get_autoscaler_policy ran
