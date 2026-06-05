"""Task schema — parsed `task.toml` (spec §4.1).

This module defines the on-disk task config that lives in a task directory:
metadata + environment + per-step config + multi-step aggregation strategy.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from loom.models.healthcheck import HealthcheckSpec
from loom.models.mcp import MCPConnection
from loom.models.networking import NetworkPolicy, Public
from loom.models.skill import SkillRef
from loom.models.types import (
    OS,
    GPUVendor,
    ModelSpec,
    NetworkPolicyKind,
    VerifierEnvMode,
)


class TaskMetadata(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    id: str
    name: str
    description: str | None = None
    labels: list[str] = []


class EnvironmentConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    os: OS
    gpu_vendor: GPUVendor = "none"
    docker_image: str | None = None
    dockerfile: PurePosixPath | None = None
    healthcheck: HealthcheckSpec | None = None
    workdir: PurePosixPath = PurePosixPath("/workspace")
    user: str | int = "agent"
    network_policies_supported: frozenset[NetworkPolicyKind] = frozenset({"public"})
    baseline_network_policy: NetworkPolicy = Public()
    skills_dir: PurePosixPath | None = None
    mcp_servers: list[MCPConnection] = []
    build_timeout_sec: float = Field(default=1200, gt=0)


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
    min_reward: dict[str, float] | float | None = None
    network: StepNetworkPlan | None = None
    healthcheck: HealthcheckSpec | None = None
