from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

CapacityUsage = int | float


EXECUTION_ATTEMPT_METADATA_SCHEMA_VERSION = "execution-attempt-metadata-v1"


class SchedulerLeaseStatus(str, Enum):
    DISPATCHED = "dispatched"
    CLAIMED = "claimed"
    RECOVERED = "recovered"
    EXPIRED = "expired"
    CANCELED = "canceled"


class RunnerProcessStatus(str, Enum):
    CLAIMED = "claimed"
    EXECUTING = "executing"
    HEARTBEATING = "heartbeating"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


@dataclass(frozen=True)
class SchedulerCapacityBlock:
    run_id: str
    project_id: str
    scheduler_id: str
    execution_task_id: str
    dimension: str
    key: str
    active_count: CapacityUsage
    limit: CapacityUsage
    reason: str
    observed_at: datetime
    backend_key: str
    provider_key: str
    model_key: str
    agent_key: str
    benchmark_key: str
    metric: str = "active_runs"
    candidate_usage: CapacityUsage | None = None
    projected_usage: CapacityUsage | None = None

    def __post_init__(self) -> None:
        for name, value in {
            "run_id": self.run_id,
            "project_id": self.project_id,
            "scheduler_id": self.scheduler_id,
            "execution_task_id": self.execution_task_id,
            "dimension": self.dimension,
            "key": self.key,
            "reason": self.reason,
            "backend_key": self.backend_key,
            "provider_key": self.provider_key,
            "model_key": self.model_key,
            "agent_key": self.agent_key,
            "benchmark_key": self.benchmark_key,
            "metric": self.metric,
        }.items():
            _require_non_empty(name, value)
        if self.active_count < 0:
            raise ValueError("active_count must be non-negative")
        if self.limit <= 0:
            raise ValueError("limit must be positive")
        if self.candidate_usage is not None and self.candidate_usage < 0:
            raise ValueError("candidate_usage must be non-negative")
        if self.projected_usage is not None and self.projected_usage < 0:
            raise ValueError("projected_usage must be non-negative")
        _require_timezone("observed_at", self.observed_at)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "run_id": self.run_id,
            "project_id": self.project_id,
            "scheduler_id": self.scheduler_id,
            "execution_task_id": self.execution_task_id,
            "dimension": self.dimension,
            "key": self.key,
            "metric": self.metric,
            "active_count": self.active_count,
            "limit": self.limit,
            "reason": self.reason,
            "observed_at": _datetime_json(self.observed_at),
            "backend_key": self.backend_key,
            "provider_key": self.provider_key,
            "model_key": self.model_key,
            "agent_key": self.agent_key,
            "benchmark_key": self.benchmark_key,
        }
        if self.candidate_usage is not None:
            payload["candidate_usage"] = self.candidate_usage
        if self.projected_usage is not None:
            payload["projected_usage"] = self.projected_usage
        return payload


def scheduler_lease_status_value(status: SchedulerLeaseStatus | str) -> str:
    if isinstance(status, SchedulerLeaseStatus):
        return status.value
    if isinstance(status, str) and status.strip():
        return status
    raise ValueError("scheduler lease status must be a non-empty string")


def runner_process_status_value(status: RunnerProcessStatus | str) -> str:
    if isinstance(status, RunnerProcessStatus):
        return status.value
    if isinstance(status, str) and status.strip():
        return status
    raise ValueError("runner process status must be a non-empty string")


def scheduler_lease_metadata(
    metadata: dict[str, Any] | None,
    *,
    scheduler_id: str,
    lease_status: SchedulerLeaseStatus | str,
    observed_at: datetime,
    execution_task_id: str,
    backend_key: str,
    project_id: str,
    provider_key: str | None = None,
    model_key: str | None = None,
    agent_key: str | None = None,
    benchmark_key: str | None = None,
) -> dict[str, Any]:
    _require_non_empty("scheduler_id", scheduler_id)
    _require_non_empty("execution_task_id", execution_task_id)
    _require_non_empty("backend_key", backend_key)
    _require_non_empty("project_id", project_id)
    _require_timezone("observed_at", observed_at)
    for name, value in {
        "provider_key": provider_key,
        "model_key": model_key,
        "agent_key": agent_key,
        "benchmark_key": benchmark_key,
    }.items():
        if value is not None:
            _require_non_empty(name, value)

    updated = dict(metadata or {})
    execution = _execution_metadata(updated)
    scheduler = dict(execution.get("scheduler") or {})
    canonical_status = scheduler_lease_status_value(lease_status)
    scheduler.update(
        {
            "scheduler_id": scheduler_id,
            "lease_status": canonical_status,
            "execution_task_id": execution_task_id,
            "backend_key": backend_key,
            "project_id": project_id,
            "lease_updated_at": _datetime_json(observed_at),
        }
    )
    if provider_key is not None:
        scheduler["provider_key"] = provider_key
    if model_key is not None:
        scheduler["model_key"] = model_key
    if agent_key is not None:
        scheduler["agent_key"] = agent_key
    if benchmark_key is not None:
        scheduler["benchmark_key"] = benchmark_key
    if canonical_status == SchedulerLeaseStatus.DISPATCHED.value:
        scheduler.pop("capacity_blocked", None)
        scheduler["dispatched_at"] = _datetime_json(observed_at)
    execution["scheduler"] = scheduler
    updated["execution"] = execution
    return updated


def scheduler_capacity_blocked_metadata(
    metadata: dict[str, Any] | None,
    *,
    block: SchedulerCapacityBlock,
) -> dict[str, Any]:
    updated = dict(metadata or {})
    execution = _execution_metadata(updated)
    scheduler = dict(execution.get("scheduler") or {})
    scheduler["capacity_blocked"] = block.to_dict()
    scheduler["scheduler_id"] = block.scheduler_id
    scheduler["execution_task_id"] = block.execution_task_id
    scheduler["project_id"] = block.project_id
    scheduler["backend_key"] = block.backend_key
    scheduler["provider_key"] = block.provider_key
    scheduler["model_key"] = block.model_key
    scheduler["agent_key"] = block.agent_key
    scheduler["benchmark_key"] = block.benchmark_key
    scheduler["lease_updated_at"] = block.to_dict()["observed_at"]
    execution["scheduler"] = scheduler
    updated["execution"] = execution
    return updated


def runner_process_metadata(
    metadata: dict[str, Any] | None,
    *,
    worker_id: str,
    process_status: RunnerProcessStatus | str,
    heartbeat_status: str,
    observed_at: datetime,
    claimed_at: datetime | None = None,
    completed_at: datetime | None = None,
    process_id: int | None = None,
    return_code: int | None = None,
    execution_lock_id: str | None = None,
    execution_lock_acquired_at: datetime | None = None,
) -> dict[str, Any]:
    _require_non_empty("worker_id", worker_id)
    _require_non_empty("heartbeat_status", heartbeat_status)
    _require_timezone("observed_at", observed_at)
    if claimed_at is not None:
        _require_timezone("claimed_at", claimed_at)
    if completed_at is not None:
        _require_timezone("completed_at", completed_at)
    if execution_lock_acquired_at is not None:
        _require_timezone("execution_lock_acquired_at", execution_lock_acquired_at)
    if execution_lock_id is not None:
        _require_non_empty("execution_lock_id", execution_lock_id)

    updated = dict(metadata or {})
    execution = _execution_metadata(updated)
    runner = dict(execution.get("runner") or {})
    runner.update(
        {
            "worker_id": worker_id,
            "process_status": runner_process_status_value(process_status),
            "heartbeat_status": heartbeat_status,
            "last_heartbeat_at": _datetime_json(observed_at),
        }
    )
    if claimed_at is not None:
        runner["claimed_at"] = _datetime_json(claimed_at)
    if completed_at is not None:
        runner["completed_at"] = _datetime_json(completed_at)
    if process_id is not None:
        runner["process_id"] = process_id
    if return_code is not None:
        runner["return_code"] = return_code
    if execution_lock_id is not None:
        runner["execution_lock_id"] = execution_lock_id
    if execution_lock_acquired_at is not None:
        runner["execution_lock_acquired_at"] = _datetime_json(execution_lock_acquired_at)
    execution["runner"] = runner
    updated["execution"] = execution
    return updated


def _execution_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    execution = dict(metadata.get("execution") or {})
    execution["schema_version"] = EXECUTION_ATTEMPT_METADATA_SCHEMA_VERSION
    return execution


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _require_timezone(name: str, value: datetime) -> None:
    if value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")


def _datetime_json(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
