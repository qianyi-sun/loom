from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any


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
) -> dict[str, Any]:
    _require_non_empty("scheduler_id", scheduler_id)
    _require_non_empty("execution_task_id", execution_task_id)
    _require_non_empty("backend_key", backend_key)
    _require_non_empty("project_id", project_id)
    _require_timezone("observed_at", observed_at)

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
    if canonical_status == SchedulerLeaseStatus.DISPATCHED.value:
        scheduler["dispatched_at"] = _datetime_json(observed_at)
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
