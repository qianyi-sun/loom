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
