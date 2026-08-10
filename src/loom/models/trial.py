"""TrialConfig — runtime configuration provided at trial submission (spec §4.3)."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from loom.family_run.spec import FamilyRunSpec
from loom.models.mcp import MCPConnection
from loom.models.networking import NetworkPolicy
from loom.models.skill import SkillRef
from loom.models.types import ModelSpec, VerifierEnvMode
from loom.request_params import sanitize_request_extras


class RetryReason(StrEnum):
    WORKER_CRASH = "worker_crash"
    ENV_START_FAILURE = "env_start_failure"
    AGENT_TIMEOUT = "agent_timeout"
    VERIFIER_TIMEOUT = "verifier_timeout"
    TRAJECTORY_FLUSH_FAILED = "trajectory_flush_failed"
    GATEWAY_ERROR = "gateway_error"
    PROVIDER_TRANSPORT_DISCONNECT = "provider_transport_disconnect"
    NODE_SETUP_HEALTH = "node_setup_health"


class BackoffSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    base_sec: float = Field(default=30, gt=0)
    max_sec: float = Field(default=600, gt=0)
    multiplier: float = Field(default=2.0, gt=0)
    jitter: float = Field(default=0.2, ge=0, le=1)


class RetryPolicy(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    max_attempts: int = Field(default=3, ge=1)
    retry_on: frozenset[RetryReason] = frozenset({
        RetryReason.GATEWAY_ERROR,
        RetryReason.PROVIDER_TRANSPORT_DISCONNECT,
        RetryReason.NODE_SETUP_HEALTH,
    })
    backoff: BackoffSpec = BackoffSpec()


class TrialConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal["1"] = "1"

    # Build/runtime
    force_build: bool = False
    delete_env: bool = True
    skip_verifier: bool = False
    verifier_env_mode: VerifierEnvMode | None = None

    # Timeouts — `override_*` replaces task default; `*_multiplier` scales the resolved value
    override_agent_timeout_sec: float | None = Field(default=None, gt=0)
    override_verifier_timeout_sec: float | None = Field(default=None, gt=0)
    override_env_build_timeout_sec: float | None = Field(default=None, gt=0)
    agent_timeout_multiplier: float = Field(default=1.0, gt=0)
    verifier_timeout_multiplier: float = Field(default=1.0, gt=0)
    env_build_timeout_multiplier: float = Field(default=1.0, gt=0)

    # Retry policy
    retry: RetryPolicy = RetryPolicy()

    # Scheduling
    submit_priority: int = Field(default=100, ge=0, le=1000)

    # Safe, user-controlled provider generation parameters. Prompt payloads,
    # headers, credentials, and unknown extras are stripped before execution.
    request_params: dict[str, Any] = Field(default_factory=dict)

    # Per-trial overrides on the task's defaults
    extra_mcp_servers: list[MCPConnection] = []
    extra_skills: list[SkillRef] = []
    baseline_network_policy_override: NetworkPolicy | None = None

    # Plan 23: agent + model are REQUIRED at submission, no fallback
    # to TaskConfig. Every trial must explicitly state which agent
    # runs and which model it talks to. `agent_model` is required
    # but its value MAY be null for agents that don't call an LLM
    # (oracle, in-box runtimes) — the field must still appear in
    # the payload, just as a literal null.
    agent_name: str = Field(min_length=1)
    agent_model: ModelSpec | None

    # #672 family-runs: optional per-batch override merged with the
    # benchmark catalog's ``family_run_defaults`` at batch-accept time.
    # When neither this override nor the catalog opts in, the batch runs
    # in the classic mode - fully backward compatible.
    family_run: FamilyRunSpec | None = None

    # #1263: optional batch/trial selection of a named workspace staging
    # policy (e.g. Harbor-shaped packs that need TB21 private-path isolation
    # without a terminal-bench-2@tb2.1-r6/ task-id prefix).
    # ``tb21`` → canonical TB21_AGENT_WORKSPACE_POLICY; ``none`` → legacy
    # full upload; omitted → worker falls back to TB2.1 prefix / provenance.
    workspace_staging_policy_name: Literal["tb21", "none"] | None = None

    @field_validator("request_params", mode="before")
    @classmethod
    def _sanitize_request_params(cls, value: Any) -> dict[str, Any]:
        return sanitize_request_extras(value)
