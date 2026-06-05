"""Trajectory event catalog — JSONL events written during a trial.

Spec §4.4 (catalog) + §4.4.1 (LLMCallEvent — the training-data load-bearing one).
All events share an envelope and discriminate via `kind`. Per-event-type
classes follow. ATIF v1.7 projection consumes this stream at finalize.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class EventKind(StrEnum):
    # Trial-level
    TRIAL_START = "trial_start"
    TRIAL_END = "trial_end"
    TRIAL_ERROR = "trial_error"
    TRIAL_CANCELLED = "trial_cancelled"
    # Step-level
    STEP_START = "step_start"
    STEP_END = "step_end"
    # Environment-level
    ENV_START = "env_start"
    ENV_READY = "env_ready"
    ENV_STOP = "env_stop"
    ENV_EXEC = "env_exec"
    # File ops
    FILE_UPLOAD = "file_upload"
    FILE_DOWNLOAD = "file_download"
    # Agent-level
    LLM_CALL = "llm_call"
    TOOL_USE = "tool_use"
    AGENT_THOUGHT = "agent_thought"
    # Verifier-level
    VERIFIER_START = "verifier_start"
    VERIFIER_END = "verifier_end"
    VERIFIER_CHECK = "verifier_check"
    # Net/policy
    NETWORK_POLICY_CHANGE = "network_policy_change"
    # Sys
    WORKER_LOST_CLAIM = "worker_lost_claim"
    WORKER_DRAIN_INTERRUPTED = "worker_drain_interrupted"


class _EventBase(BaseModel):
    """Common envelope fields. All events carry these."""
    model_config = ConfigDict(frozen=True, extra="forbid")
    emitted_at: datetime
    trial_id: UUID
    step_id: str
    seq: int = Field(ge=0)


class TrialStartEvent(_EventBase):
    kind: Literal[EventKind.TRIAL_START] = EventKind.TRIAL_START
    task_id: str
    agent_name: str
    agent_mode: Literal["out-of-box", "in-box"]


# Trial-level ──────────────────────────────────────────────────────────────────

class TrialEndEvent(_EventBase):
    kind: Literal[EventKind.TRIAL_END] = EventKind.TRIAL_END
    final_state: Literal["succeeded", "failed", "cancelled"]
    reward: dict[str, float] | None = None
    failure_reason: str | None = None


class TrialErrorEvent(_EventBase):
    kind: Literal[EventKind.TRIAL_ERROR] = EventKind.TRIAL_ERROR
    error_type: str
    message: str
    traceback: str


class TrialCancelledEvent(_EventBase):
    kind: Literal[EventKind.TRIAL_CANCELLED] = EventKind.TRIAL_CANCELLED
    cancellation_requested_at: datetime
    observed_at: datetime


# Step-level ───────────────────────────────────────────────────────────────────

class StepStartEvent(_EventBase):
    kind: Literal[EventKind.STEP_START] = EventKind.STEP_START
    instruction_excerpt: str


class StepEndEvent(_EventBase):
    kind: Literal[EventKind.STEP_END] = EventKind.STEP_END
    summary: dict[str, float] | None = None
    error_phase: Literal["prepare", "agent", "artifacts", "verifier"] | None = None


# Environment-level ────────────────────────────────────────────────────────────

class EnvStartEvent(_EventBase):
    kind: Literal[EventKind.ENV_START] = EventKind.ENV_START
    image_ref: str
    build_time_sec: float


class EnvReadyEvent(_EventBase):
    kind: Literal[EventKind.ENV_READY] = EventKind.ENV_READY
    healthcheck_attempts: int


class EnvStopEvent(_EventBase):
    kind: Literal[EventKind.ENV_STOP] = EventKind.ENV_STOP
    duration_sec: float
    exit_status: int | None = None


class EnvExecEvent(_EventBase):
    kind: Literal[EventKind.ENV_EXEC] = EventKind.ENV_EXEC
    cmd: str
    user: str | int | None
    cwd: str | None
    return_code: int
    stdout_bytes: int
    stderr_bytes: int
    truncated: bool
    duration_sec: float


# File ops ─────────────────────────────────────────────────────────────────────

class FileUploadEvent(_EventBase):
    kind: Literal[EventKind.FILE_UPLOAD] = EventKind.FILE_UPLOAD
    src_size_bytes: int
    dst_path: str
    duration_sec: float


class FileDownloadEvent(_EventBase):
    kind: Literal[EventKind.FILE_DOWNLOAD] = EventKind.FILE_DOWNLOAD
    src_path: str
    dst_size_bytes: int
    duration_sec: float


# Agent — LLM call (training-data load-bearing per spec §4.4.1) ────────────────

from typing import Any  # noqa: E402  (intentionally local to keep top imports tight)

from loom.models.types import ModelSpec  # noqa: E402


class ChatMessage(BaseModel):
    """OpenAI-compatible chat message used in LLMCallEvent.messages / .response."""
    model_config = ConfigDict(frozen=True, extra="allow")  # allow provider-specific fields
    role: Literal["system", "user", "assistant", "tool"]
    content: str | list[dict[str, Any]] | None = None
    name: str | None = None
    tool_calls: list[dict[str, Any]] | None = None
    tool_call_id: str | None = None


class ToolSpec(BaseModel):
    """Tool definition sent in LLMCallEvent.tools."""
    model_config = ConfigDict(frozen=True, extra="allow")
    name: str
    description: str | None = None
    parameters: dict[str, Any] = {}


class LLMCallEvent(_EventBase):
    """The training-data load-bearing event (spec §4.4.1)."""
    kind: Literal[EventKind.LLM_CALL] = EventKind.LLM_CALL

    # Model identification (frozen at call time)
    model: ModelSpec
    rate_card_hash: str

    # Input
    system_prompt: str | None
    messages: list[ChatMessage]
    tools: list[ToolSpec] | None = None
    tool_choice: str | dict[str, Any] | None = None

    # Output
    response: ChatMessage
    finish_reason: str

    # Usage — RAW, NOT derived (spec H5)
    input_tokens: int = Field(ge=0)
    cached_input_tokens: int = Field(ge=0)
    cache_write_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    thinking_tokens: int = Field(ge=0)
    provider_extras: dict[str, int]                 # NAMED counters (int-valued)

    # Derived (recomputable)
    cost_usd_snapshot: float = Field(ge=0)

    # Timing
    duration_sec: float = Field(ge=0)
    streamed: bool
    time_to_first_token_sec: float | None

    # Attribution
    gateway_request_id: str
    cache_keys: list[str] = []
