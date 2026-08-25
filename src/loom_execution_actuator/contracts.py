from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ActuatorContractError(RuntimeError):
    pass


class NormalizedJobState(StrEnum):
    ABSENT = "absent"
    MISSING = "missing"
    PENDING = "pending"
    UNSCHEDULABLE = "unschedulable"
    IMAGE_PULL_BACKOFF = "image_pull_backoff"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    OOM_KILLED = "oom_killed"
    EVICTED = "evicted"
    NODE_LOST = "node_lost"
    DEADLINE_EXCEEDED = "deadline_exceeded"
    TERMINATING = "terminating"
    DELETED = "deleted"


class KubernetesJobObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = Field(default="loom.kubernetes-job-observation.v1")
    namespace: str = Field(min_length=1, max_length=63)
    job_name: str = Field(min_length=1, max_length=63)
    lease_id: str = Field(min_length=36, max_length=36)
    resource_generation: int = Field(gt=0)
    target_id: str = Field(min_length=1, max_length=80)
    execution_unit_key: str = Field(min_length=36, max_length=36)
    normalized_state: NormalizedJobState
    job_uid: str | None = Field(default=None, min_length=1, max_length=128)
    pod_uid: str | None = Field(default=None, min_length=1, max_length=128)
    resource_version: str | None = Field(default=None, min_length=1, max_length=128)
    node_name: str | None = Field(default=None, min_length=1, max_length=253)
    scheduled_at: datetime | None = None
    started_at: datetime | None = None
    terminated_at: datetime | None = None
    reason: str | None = Field(default=None, max_length=120)
    message: str | None = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def _timestamps_are_ordered(self) -> KubernetesJobObservation:
        if (
            self.scheduled_at is not None
            and self.started_at is not None
            and self.started_at < self.scheduled_at
        ):
            raise ValueError("started_at precedes scheduled_at")
        if (
            self.started_at is not None
            and self.terminated_at is not None
            and self.terminated_at < self.started_at
        ):
            raise ValueError("terminated_at precedes started_at")
        return self

    def event_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"schema_version", "namespace", "job_name"})


class KubernetesApiError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after_seconds: int | None = None,
        ambiguous: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds
        self.ambiguous = ambiguous


@dataclass(frozen=True)
class KubernetesJobInventory:
    observations: tuple[KubernetesJobObservation, ...]
    rejected_count: int = 0

    def __post_init__(self) -> None:
        if self.rejected_count < 0:
            raise ValueError("rejected_count cannot be negative")


class KubernetesJobApi(Protocol):
    async def get_job(
        self, *, namespace: str, job_name: str
    ) -> KubernetesJobObservation | None: ...

    async def create_job(
        self, *, namespace: str, manifest: dict[str, Any]
    ) -> KubernetesJobObservation: ...

    async def delete_job(
        self,
        *,
        namespace: str,
        job_name: str,
        expected_uid: str,
        grace_period_seconds: int,
    ) -> None: ...

    async def list_jobs(self, *, namespace: str, label_selector: str) -> KubernetesJobInventory: ...

    async def watch_jobs(
        self,
        *,
        namespace: str,
        label_selector: str,
        resource_version: str | None,
        timeout_seconds: int,
    ) -> tuple[KubernetesJobObservation, ...]: ...
