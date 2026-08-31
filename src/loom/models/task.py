"""Task schema — parsed `task.toml` (spec §4.1).

This module defines the on-disk task config that lives in a task directory:
metadata + environment + per-step config + multi-step aggregation strategy.
"""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

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

if TYPE_CHECKING:
    from loom.execution_runtime_contract import ExecutionRuntimePlanV1

_SERVICE_EXECUTION_REVISION_SENTINEL = "sha256:" + "0" * 64


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
    artifacts: list[str] = []  # POSIX globs
    required_artifacts: list[str] = []  # verifier-required POSIX globs
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


class TaskServiceExecutionV1(BaseModel):
    """Opt-in, immutable service-execution binding carried by a task revision.

    The runtime template deliberately omits ``task_revision_sha256`` because
    that digest covers the complete task directory, including task.toml. The
    scheduler binds the published Task checksum after materialization, avoiding
    an impossible self-referential digest while retaining strict validation.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal["loom.task-service-execution.v1"] = "loom.task-service-execution.v1"
    logical_pool_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    runtime_template: dict[str, Any]

    @field_validator("runtime_template")
    @classmethod
    def _runtime_template_is_canonical(cls, value: dict[str, Any]) -> dict[str, Any]:
        if "task_revision_sha256" in value:
            raise ValueError("runtime template cannot contain its enclosing task revision")
        plan = bind_service_execution_runtime_plan(
            value,
            task_revision_sha256=_SERVICE_EXECUTION_REVISION_SENTINEL,
        )
        if plan.execution_role != "attempt":
            raise ValueError("task service execution requires an attempt runtime plan")
        payload = plan.canonical_payload()
        del payload["task_revision_sha256"]
        return payload


def bind_service_execution_runtime_plan(
    runtime_template: dict[str, Any],
    *,
    task_revision_sha256: str,
) -> ExecutionRuntimePlanV1:
    """Bind a published task revision to its immutable runtime template."""

    from loom.execution_runtime_contract import ExecutionRuntimePlanV1

    return ExecutionRuntimePlanV1.model_validate(
        {**runtime_template, "task_revision_sha256": task_revision_sha256}
    )


class TaskConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["1"] = "1"
    required_agent_capabilities: frozenset[str] = frozenset()
    task: TaskMetadata
    environment: EnvironmentConfig
    agent: AgentDefaults
    verifier: VerifierDefaults
    service_execution: TaskServiceExecutionV1 | None = None
    steps: list[StepConfig] = []
    multi_step: MultiStepConfig | None = None

    @field_serializer("required_agent_capabilities", when_used="json")
    def _serialize_required_agent_capabilities(
        self,
        value: frozenset[str],
    ) -> list[str]:
        return sorted(value)

    @model_validator(mode="after")
    def _service_execution_matches_task(self) -> TaskConfig:
        if self.service_execution is None:
            return self

        from loom.execution_contract import workload_requirements_from_task
        from loom.execution_runtime_contract import validate_runtime_plan_requirements

        plan = bind_service_execution_runtime_plan(
            self.service_execution.runtime_template,
            task_revision_sha256=_SERVICE_EXECUTION_REVISION_SENTINEL,
        )
        requirements = workload_requirements_from_task(self)
        validate_runtime_plan_requirements(plan, requirements)
        return self


def normalize_steps(cfg: TaskConfig) -> TaskConfig:
    """Implicit single-step synthesis (spec §4.1).

    Tasks without explicit `steps` get a single synthesized step named "main"
    so the trial loop has exactly one code path to follow.
    """
    if cfg.steps:
        return cfg
    return cfg.model_copy(update={"steps": [StepConfig(name="main")]})
