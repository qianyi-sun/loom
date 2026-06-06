"""ATIF v1.7 schema models + project_to_atif (spec §4.8).

Schema is intentionally compact: trajectory → metadata + steps[]. Each step
records whether an LLM call happened, the messages exchanged, metrics, and
reasoning content.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class AtifMetadata(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")
    task_id: str
    agent_name: str
    agent_version: str
    final_state: Literal["succeeded", "failed", "cancelled"] | None = None
    reward: dict[str, float] | None = None
    error: dict[str, Any] | None = None
    verifier_name: str | None = None
    verifier_rewards: dict[str, float] | None = None


class AtifStepMetrics(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")
    input_tokens: int
    output_tokens: int
    cached_input_tokens: int
    cache_write_tokens: int
    thinking_tokens: int
    cost_usd: float


class AtifStep(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    step_id: str
    llm_call_count: int = Field(ge=0)
    is_copied_context: bool
    messages: list[dict[str, Any]] | None = None
    reasoning_content: str | None = None
    tool_calls: list[dict[str, Any]] | None = None
    metrics: AtifStepMetrics | None = None

    @model_validator(mode="after")
    def _consistency(self) -> AtifStep:
        if self.llm_call_count == 0:
            for field_name in ("messages", "reasoning_content", "metrics"):
                if getattr(self, field_name) is not None:
                    raise ValueError(
                        f"llm_call_count == 0 → `{field_name}` must be absent",
                    )
        if self.llm_call_count > 0 and self.metrics is None:
            raise ValueError("llm_call_count > 0 → `metrics` required")
        return self


class AtifTrajectory(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["1.7"] = "1.7"
    trajectory_id: str
    session_id: str
    metadata: AtifMetadata
    steps: list[AtifStep]
