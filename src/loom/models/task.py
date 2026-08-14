"""Task schema — parsed `task.toml` (spec §4.1).

This module defines the on-disk task config that lives in a task directory:
metadata + environment + per-step config + multi-step aggregation strategy.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.models.healthcheck import HealthcheckSpec
from loom.models.mcp import MCPConnection
from loom.models.networking import NetworkPolicy, Public
from loom.models.skill import SkillRef
from loom.models.types import (
    OS,
    GPUVendor,
    ModelSpec,
    MultiStepRewardStrategy,
    NetworkPolicyKind,
    RequiredCPUArch,
    VerifierEnvMode,
)


class TaskMetadata(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    id: str
    name: str
    description: str | None = None
    labels: list[str] = []


class TaskSidecarConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str
    docker_image: str | None = None
    dockerfile: PurePosixPath | None = None
    docker_build_context: PurePosixPath | None = None
    command: str | list[str] | None = None
    environment: dict[str, str] = {}
    hostname: str | None = None
    healthcheck: HealthcheckSpec | None = None
    depends_on: list[str] = []


class EnvironmentConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    os: OS
    cpu_arch: RequiredCPUArch = "x86_64"
    gpu_vendor: GPUVendor = "none"
    docker_image: str | None = None
    dockerfile: PurePosixPath | None = None
    docker_build_context: PurePosixPath | None = None
    environment: dict[str, str] = {}
    extra_hosts: dict[str, str] = {}
    dns: list[str] = []
    tmpfs: list[str] = []
    healthcheck: HealthcheckSpec | None = None
    workdir: PurePosixPath = PurePosixPath("/workspace")
    user: str | int = "agent"
    network_policies_supported: frozenset[NetworkPolicyKind] = frozenset({"public"})
    baseline_network_policy: NetworkPolicy = Public()
    skills_dir: PurePosixPath | None = None
    mcp_servers: list[MCPConnection] = []
    build_timeout_sec: float = Field(default=1200, gt=0)
    # Runtime resource contract. These values are optional for ordinary Loom
    # tasks, but audited benchmark profiles preserve their native limits here
    # so drivers can enforce them instead of treating provenance-only TOML as
    # executable policy.
    cpus: float | None = Field(default=None, gt=0)
    memory_mb: int | None = Field(default=None, gt=0)
    storage_mb: int | None = Field(default=None, gt=0)
    gpus: int = Field(default=0, ge=0)
    sidecars: list[TaskSidecarConfig] = []


class AgentDefaults(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str
    version: str | None = None
    model: ModelSpec | None = None
    timeout_sec: float = Field(default=1800, gt=0)
    setup_timeout_sec: float = Field(default=360, gt=0)
    user: str | int | None = None
    extra_mcp_servers: list[MCPConnection] = []
    skills: list[SkillRef] = []


class VerifierDefaults(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str
    args: dict[str, Any] = {}
    timeout_sec: float = Field(default=300, gt=0)
    env_mode: VerifierEnvMode = "shared"
    user: str | int | None = None


class AgentOverrides(BaseModel):
    """Per-step partial override of AgentDefaults. None fields inherit task default."""
    model_config = ConfigDict(frozen=True, extra="forbid")
    model: ModelSpec | None = None
    timeout_sec: float | None = None
    user: str | int | None = None
    extra_mcp_servers: list[MCPConnection] | None = None


class VerifierOverrides(BaseModel):
    """Per-step partial override of VerifierDefaults."""
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str | None = None
    args: dict[str, Any] | None = None
    timeout_sec: float | None = None
    env_mode: VerifierEnvMode | None = None
    user: str | int | None = None


class StepNetworkPlan(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    agent_phase: NetworkPolicy | None = None
    verifier_phase: NetworkPolicy | None = None


class StepConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str
    instruction_file: PurePosixPath = PurePosixPath("instruction.md")
    agent: AgentOverrides | None = None
    verifier: VerifierOverrides | None = None
    artifacts: list[str] = []                                       # POSIX globs
    required_artifacts: list[str] = []                              # verifier-required POSIX globs
    min_reward: dict[str, float] | float | None = None
    network: StepNetworkPlan | None = None
    healthcheck: HealthcheckSpec | None = None


class MultiStepConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    reward_strategy: MultiStepRewardStrategy = "mean"
    weights: dict[str, float] | None = None

    @model_validator(mode="after")
    def _weights_required_for_weighted(self) -> MultiStepConfig:
        if self.reward_strategy == "weighted" and not self.weights:
            raise ValueError(
                "reward_strategy='weighted' requires non-empty `weights`",
            )
        return self


class TaskConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["1"] = "1"
    required_agent_capabilities: frozenset[str] = frozenset()
    task: TaskMetadata
    environment: EnvironmentConfig
    agent: AgentDefaults
    verifier: VerifierDefaults
    steps: list[StepConfig] = []
    multi_step: MultiStepConfig | None = None


def normalize_steps(cfg: TaskConfig) -> TaskConfig:
    """Implicit single-step synthesis (spec §4.1).

    Tasks without explicit `steps` get a single synthesized step named "main"
    so the trial loop has exactly one code path to follow.
    """
    if cfg.steps:
        return cfg
    return cfg.model_copy(update={"steps": [StepConfig(name="main")]})
