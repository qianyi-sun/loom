"""Immutable Terminus-2 student/teacher switch plan (#1380)."""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from loom.models.types import ModelSpec

PRNG_VERSION = "model_switch_plan.v2"


class ModelSwitchPlanSnapshot(BaseModel):
    """Worker/API view of a persisted ``model_switch_plans`` row."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: UUID
    trial_id: UUID
    combination_idx: int = 0
    mix_mode: Literal["student_teacher_student", "beta_mixture"] = (
        "student_teacher_student"
    )
    k1: int | None = Field(default=None, ge=2)
    k2: int | None = Field(default=None, ge=3)
    teacher_episodes: int | None = Field(default=None, ge=1)
    beta: float | None = Field(default=None, ge=0.0, le=1.0)
    seed: str
    prng_version: str = PRNG_VERSION
    student_model: ModelSpec
    teacher_model: ModelSpec
    provider_connection_id: UUID | None = None
    pricing_snapshot: dict[str, Any] = Field(default_factory=dict)
    capability_snapshot: dict[str, Any] = Field(default_factory=dict)
    inherited_from_plan_id: UUID | None = None


class TerminusRecoveryState(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    agent_execution_id: UUID
    agent_run_attempt_id: UUID
    attempt_number: int = Field(ge=1)
    recovery: Literal["fresh", "resumed", "recovery_failed"]
    last_episode: int | None = None
    active_role: Literal["student", "teacher"] | None = None
    last_call_ordinal: int = 0
    last_seq: int = 0
    checksum: str | None = None
