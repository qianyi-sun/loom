"""Pydantic models for the family-run spec (#672)."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class PluginRef(BaseModel):
    """Reference to a plugin by entry-point name plus configuration params."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)


class FamilyRunSpec(BaseModel):
    """Partial spec: any subset of roles may be provided.

    Used by the catalog (``family_run_defaults``) and per-batch overrides
    (``trial_config.family_run``). Resolver merges these two into a
    :class:`ResolvedFamilyRunSpec` at batch-accept time.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)
    enabled: bool | None = None
    family_key_extractor: PluginRef | None = None
    sequencer: PluginRef | None = None
    advance_predicate: PluginRef | None = None
    adapter: PluginRef | None = None
    failure_policy: PluginRef | None = None
    state_backend: PluginRef | None = None
    mount_path: str | None = None


class ResolvedFamilyRunSpec(BaseModel):
    """Fully-resolved spec persisted on ``batches.family_run_spec``."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    enabled: bool
    family_key_extractor: PluginRef
    sequencer: PluginRef
    advance_predicate: PluginRef
    adapter: PluginRef
    failure_policy: PluginRef
    state_backend: PluginRef
    mount_path: str = "/root/.skills"


class AdvanceDecision(StrEnum):
    """Returned by advance_predicate.decide() after a trial terminates."""

    ADVANCE = "advance"
    RETRY = "retry"
    SKIP = "skip"
    ABORT = "abort"


class FailureAction(BaseModel):
    """Returned by failure_policy.on_adapter_failure().

    ``kind == "retry_with_backoff"`` uses ``backoff_sec``; other kinds ignore it.
    """

    model_config = ConfigDict(frozen=True)
    kind: str = Field(pattern=r"^(retry_with_backoff|skip_and_advance|abort_family)$")
    backoff_sec: float | None = None

    @classmethod
    def retry_with_backoff(cls, backoff_sec: float) -> FailureAction:
        return cls(kind="retry_with_backoff", backoff_sec=backoff_sec)

    @classmethod
    def skip_and_advance(cls) -> FailureAction:
        return cls(kind="skip_and_advance")

    @classmethod
    def abort_family(cls) -> FailureAction:
        return cls(kind="abort_family")
