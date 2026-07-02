"""VerifierResult and supporting types (spec §4.6)."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class CheckResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str
    passed: bool
    score: float | None = None
    message: str | None = None
    detail: dict[str, Any] = Field(default_factory=dict)
    duration_sec: float | None = None


class VerifierError(BaseModel):
    """Structured failure on `VerifierResult.error` — NOT a Python exception.

    Used so missing-tests / parse-failures are inspectable result data rather
    than opaque crashes (spec §2.4 wart-fix).
    """
    model_config = ConfigDict(frozen=True, extra="forbid")
    kind: Literal["missing_tests", "parse_failure", "exec_failure", "timeout", "internal"]
    message: str
    detail: dict[str, Any] = {}


class VerifierResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    rewards: dict[str, float]
    checks: list[CheckResult] = []
    confidence: float | None = None
    structured: dict[str, Any] | None = None
    error: VerifierError | None = None

    @field_validator("confidence")
    @classmethod
    def _confidence_range(cls, v: float | None) -> float | None:
        if v is not None and not 0.0 <= v <= 1.0:
            raise ValueError("confidence must be in [0, 1]")
        return v
