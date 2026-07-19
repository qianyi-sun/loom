"""TrialResult and friends — what gets persisted (spec §4.5)."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.models.verifier import VerifierResult


class TrialState(StrEnum):
    QUEUED = "queued"
    CLAIMED = "claimed"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class FailureReason(StrEnum):
    AGENT_ERROR = "agent_error"
    AGENT_TIMEOUT = "agent_timeout"
    ENV_START_FAILURE = "env_start_failure"
    ENV_HEALTHCHECK_FAILED = "env_healthcheck_failed"
    VERIFIER_ERROR = "verifier_error"
    VERIFIER_TIMEOUT = "verifier_timeout"
    ARTIFACT_UPLOAD_FAILED = "artifact_upload_failed"
    MISSING_REQUIRED_ARTIFACTS = "missing_required_artifacts"
    TRAJECTORY_FLUSH_FAILED = "trajectory_flush_failed"
    TASK_IMAGE_BUILD_TIMEOUT = "task_image_build_timeout"
    NODE_SETUP_HEALTH = "node_setup_health"
    TASK_COMPATIBILITY = "task_compatibility"
    EXHAUSTED_RETRIES = "exhausted_retries"
    WORKER_LOST_CLAIM = "worker_lost_claim"
    INTERNAL_ERROR = "internal_error"
    PROVIDER_ERROR = "provider_error"   # upstream provider returned 4xx
    GATEWAY_ERROR = "gateway_error"     # loom gateway itself returned 5xx
    PROVIDER_TRANSPORT_DISCONNECT = "provider_transport_disconnect"


class AgentInfo(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str
    version: str
    mode: Literal["out-of-box", "in-box"]
    model: ModelSpec | None = None


class StepError(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    phase: Literal["prepare", "agent", "artifacts", "verifier"]
    reason: Literal["timeout", "exception", "missing_artifacts", "cancelled"]
    message: str
    traceback: str | None = None
    occurred_at: datetime


class ArtifactRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    step_name: str
    bucket: str
    key: str
    size: int = Field(ge=0)
    content_hash: str | None = None
    share_status: Literal["pending_scan", "shared", "blocked"] = "pending_scan"
    blocked_reason: str | None = None


class StepResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    step_name: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    verifier_result: VerifierResult | None = None
    error: StepError | None = None
    artifacts_uri: str | None = None
    artifacts: list[ArtifactRef] = []


class TrialResult(BaseModel):
    model_config = ConfigDict(extra="forbid")  # NOT frozen — built incrementally by Trial.run()
    schema_version: Literal["1"] = "1"
    id: UUID = Field(default_factory=uuid4)
    task_id: str
    task_checksum: str
    team_id: UUID
    agent: AgentInfo
    config: TrialConfig
    state: TrialState
    failure_reason: FailureReason | None = None
    failure_message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    reward: dict[str, float] | None = None
    steps: list[StepResult] = []
    trajectory_uri: str | None = None
    trajectory_sha256: str | None = None
    trajectory_size_bytes: int | None = None
    atif_uri: str | None = None
    atif_sha256: str | None = None
    atif_size_bytes: int | None = None
    atif_schema_version: str | None = None
    artifacts_prefix: str | None = None
