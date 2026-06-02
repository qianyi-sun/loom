from __future__ import annotations

from enum import Enum
from typing import Any


class RunEventType(str, Enum):
    CREATED = "run.created"
    STATUS_CHANGED = "run.status_changed"
    DISPATCHED = "run.dispatched"
    CLAIMED = "run.claimed"
    STARTED = "run.started"
    EVALUATING = "run.evaluating"
    SUCCEEDED = "run.succeeded"
    FAILED = "run.failed"
    CANCELED = "run.canceled"
    RETRIED = "run.retried"
    RECOVERED = "run.recovered"
    WORKER_FAILED = "run.worker_failed"
    WORKER_SUBPROCESS_FAILED = "run.worker_subprocess_failed"
    SCHEDULER_CAPACITY_BLOCKED = "scheduler.capacity_blocked"
    ARTIFACT_CHUNK_RECORDED = "artifact.chunk_recorded"
    ARTIFACT_UPLOAD_EXPIRED = "artifact.upload_expired"
    LOG_CHUNK_RECORDED = "log.chunk_recorded"
    SANDBOX_CONTAINER_CLEANUP = "sandbox.container_cleanup"
    EVALUATOR_COMPLETED = "evaluator.completed"
    EVALUATOR_FAILED = "evaluator.failed"


class RecoveryReasonCode(str, Enum):
    STALE_DISPATCHED = "stale_dispatched"
    STALE_WORKER_HEARTBEAT = "stale_worker_heartbeat"
    TERMINAL_RESULT_MISMATCH = "terminal_result_mismatch"
    CANCELED_RESOURCE_CLEANUP = "canceled_resource_cleanup"
    DOCKER_CONTAINER_CLEANUP = "docker_container_cleanup"
    ARTIFACT_UPLOAD_EXPIRED = "artifact_upload_expired"
    PROJECTION_REFRESH_FAILED = "projection_refresh_failed"


def event_type_value(event_type: RunEventType | str) -> str:
    if isinstance(event_type, RunEventType):
        return event_type.value
    if isinstance(event_type, str) and event_type.strip():
        return event_type
    raise ValueError("event_type must be a non-empty string")


def recovery_reason_value(reason: RecoveryReasonCode | str) -> str:
    if isinstance(reason, RecoveryReasonCode):
        return reason.value
    if isinstance(reason, str) and reason.strip():
        return reason
    raise ValueError("recovery reason must be a non-empty string")


def recovery_event_metadata(reason: RecoveryReasonCode | str, **metadata: Any) -> dict[str, Any]:
    return {
        "recovery": recovery_reason_value(reason),
        **{key: value for key, value in metadata.items() if value is not None},
    }
