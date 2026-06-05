"""Primitive types and scalar enums shared across Loom models.

Spec §2.3 (Capabilities), §4.1 (Task), §4.2 (Supporting types), §4.5 (TrialState).
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

# Scalar fields that match Postgres claim-query semantics exactly.
OS = Literal["linux", "windows"]
GPUVendor = Literal["none", "nvidia"]

# Verifier semantics.
VerifierEnvMode = Literal["shared", "separate"]

# Multi-step aggregation strategy.
MultiStepRewardStrategy = Literal["mean", "min", "weighted", "final"]

# Resource enforcement (trimmed from Harbor's 5 to 3 per spec §2.3).
ResourceMode = Literal["auto", "limit", "guarantee"]

# Capability-axis tag for NetworkPolicy. Matches NetworkPolicy subclass `kind` field.
NetworkPolicyKind = Literal["public", "no-network", "allowlist"]

# Logging level (spec §7.2).
LogLevel = Literal["debug", "info", "warn", "error", "fatal"]


class ModelSpec(BaseModel):
    """Identifies an LLM model the agent should call (spec §4.2)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: str
    name: str
    tier: str | None = None
    region: str | None = None
    max_input_tokens: int | None = None
    max_output_tokens: int | None = None
