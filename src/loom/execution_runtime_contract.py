"""Immutable Pod-native execution plan shared by admission, actuator, and runtime."""

from __future__ import annotations

import math
import re
from datetime import datetime
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.execution_contract import VerifierTopology, WorkloadRequirementsV1
from loom.execution_image_admission import ExecutionImageAdmissionBundleV1

_DIGEST_REF = re.compile(r"^.+@sha256:[0-9a-f]{64}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_CANDIDATE = re.compile(r"^[0-9a-f]{40}$")
_ENV_NAME = re.compile(r"^[A-Z_][A-Z0-9_]{0,127}$")
_ROLE_NAME = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
_SECRET_ENV = re.compile(r"(?:KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL|KUBECONFIG)")


def _confined_relative_path(value: str) -> str:
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ValueError("runtime output path must be a confined relative POSIX path")
    return value


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RuntimeComposition(StrEnum):
    PRECOMPOSED = "precomposed"
    INIT_PAYLOAD = "init_payload"


class VerifierExecution(StrEnum):
    IN_ATTEMPT = "in_attempt"
    SEPARATE_EXECUTION = "separate_execution"
    SKIPPED = "skipped"


class ContainerResourcesV1(_Strict):
    cpu_millis: int = Field(gt=0, le=128_000)
    memory_mib: int = Field(gt=0, le=1_048_576)
    ephemeral_storage_mib: int = Field(gt=0, le=1_048_576)


class ProcessPhaseV1(_Strict):
    role: Literal["setup", "agent", "verifier"]
    argv: tuple[str, ...] = Field(min_length=1, max_length=128)
    working_directory: str = Field(pattern=r"^/workspace(?:/[-A-Za-z0-9._]+)*$")
    timeout_seconds: int = Field(gt=0, le=86_400)
    environment: dict[str, str] = Field(default_factory=dict)

    @field_validator("argv")
    @classmethod
    def _argv_is_bounded(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item or "\x00" in item or len(item.encode("utf-8")) > 4096 for item in value):
            raise ValueError("process argv contains an invalid item")
        if sum(len(item.encode("utf-8")) for item in value) > 32_768:
            raise ValueError("process argv exceeds 32 KiB")
        return value

    @field_validator("environment")
    @classmethod
    def _environment_is_nonsecret(cls, value: dict[str, str]) -> dict[str, str]:
        if len(value) > 64:
            raise ValueError("process environment exceeds 64 entries")
        for name, item in value.items():
            if _ENV_NAME.fullmatch(name) is None or _SECRET_ENV.search(name):
                raise ValueError("process environment contains a forbidden name")
            if "\x00" in item or len(item.encode("utf-8")) > 4096:
                raise ValueError("process environment value is invalid")
        return value


class ProbeV1(_Strict):
    kind: Literal["http", "tcp", "exec"]
    timeout_seconds: int = Field(default=2, gt=0, le=30)
    period_seconds: int = Field(default=2, gt=0, le=60)
    failure_threshold: int = Field(default=30, gt=0, le=300)
    port: int | None = Field(default=None, gt=0, le=65535)
    path: str | None = Field(default=None, pattern=r"^/[ -~]{0,1023}$")
    argv: tuple[str, ...] = Field(default=(), max_length=32)

    @field_validator("argv")
    @classmethod
    def _argv_is_bounded(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return ProcessPhaseV1._argv_is_bounded(value)

    @model_validator(mode="after")
    def _shape_matches_kind(self) -> ProbeV1:
        if self.kind == "http" and (self.port is None or self.path is None or self.argv):
            raise ValueError("HTTP probe requires port/path only")
        if self.kind == "tcp" and (self.port is None or self.path is not None or self.argv):
            raise ValueError("TCP probe requires port only")
        if self.kind == "exec" and (
            not self.argv or self.port is not None or self.path is not None
        ):
            raise ValueError("exec probe requires argv only")
        return self


class SidecarContainerV1(_Strict):
    role_name: str = Field(pattern=_ROLE_NAME.pattern)
    image_ref: str
    argv: tuple[str, ...] = Field(min_length=1, max_length=128)
    environment: dict[str, str] = Field(default_factory=dict)
    resources: ContainerResourcesV1
    startup_probe: ProbeV1
    readiness_probe: ProbeV1
    depends_on: tuple[str, ...] = Field(default=(), max_length=32)

    @field_validator("image_ref")
    @classmethod
    def _immutable_image(cls, value: str) -> str:
        if _DIGEST_REF.fullmatch(value) is None:
            raise ValueError("sidecar image must be digest-pinned")
        return value

    @field_validator("argv")
    @classmethod
    def _argv_is_bounded(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return ProcessPhaseV1._argv_is_bounded(value)

    @field_validator("environment")
    @classmethod
    def _nonsecret_environment(cls, value: dict[str, str]) -> dict[str, str]:
        ProcessPhaseV1._environment_is_nonsecret(value)
        return value


class RuntimeTaskInputV1(_Strict):
    schema_version: Literal["loom.runtime-task-input.v1"] = "loom.runtime-task-input.v1"
    manifest_sha256: str = Field(pattern=_SHA256.pattern)
    file_count: int = Field(gt=0, le=10_000)
    total_bytes: int = Field(ge=0, le=10 * 1024**3)


class RuntimeOutputDeclarationV1(_Strict):
    """One immutable workspace file expected in the complete Trial bundle."""

    source_path: str = Field(min_length=1, max_length=4096)
    relative_path: str = Field(min_length=1, max_length=4096)
    kind: Literal[
        "task_artifact",
        "trajectory",
        "agent_native",
        "verifier",
        "usage",
        "diagnostic",
        "checkpoint",
    ]
    required: bool

    @field_validator("source_path", "relative_path")
    @classmethod
    def _safe_path(cls, value: str) -> str:
        return _confined_relative_path(value)

    @model_validator(mode="after")
    def _bundle_namespace(self) -> RuntimeOutputDeclarationV1:
        namespace = self.relative_path.split("/", 1)[0]
        if namespace not in {
            "artifacts",
            "trajectory",
            "agent",
            "verifier",
            "accounting",
            "diagnostics",
            "checkpoints",
        }:
            raise ValueError("runtime output has an unknown bundle namespace")
        return self


class RuntimeOutputEvidenceV1(RuntimeOutputDeclarationV1):
    state: Literal["captured", "missing"]
    size_bytes: int | None = Field(default=None, ge=0)
    sha256: str | None = Field(default=None, pattern=_SHA256.pattern)

    @model_validator(mode="after")
    def _state_matches_evidence(self) -> RuntimeOutputEvidenceV1:
        populated = self.size_bytes is not None and self.sha256 is not None
        if populated != (self.state == "captured"):
            raise ValueError("runtime output state does not match its evidence")
        return self


class ExecutionRuntimePlanV1(_Strict):
    schema_version: Literal["loom.execution-runtime-plan.v1"] = "loom.execution-runtime-plan.v1"
    candidate_sha: str = Field(pattern=_CANDIDATE.pattern)
    task_revision_sha256: str = Field(pattern=_SHA256.pattern)
    command_identity_sha256: str = Field(pattern=_SHA256.pattern)
    execution_role: Literal["attempt", "verifier"] = "attempt"
    execution_class_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    composition: RuntimeComposition
    task_image_ref: str
    runtime_image_ref: str
    runtime_binary_sha256: str = Field(pattern=_SHA256.pattern)
    image_admission: ExecutionImageAdmissionBundleV1
    run_as_user: int = Field(default=65532, gt=0, le=2_147_483_647)
    run_as_group: int = Field(default=65532, gt=0, le=2_147_483_647)
    fs_group: int = Field(default=65532, gt=0, le=2_147_483_647)
    task_resources: ContainerResourcesV1
    workspace_mib: int = Field(gt=0, le=1_048_576)
    runtime_volume_mib: int = Field(gt=0, le=4096)
    termination_grace_seconds: int = Field(default=30, ge=1, le=300)
    setup: tuple[ProcessPhaseV1, ...] = Field(default=(), max_length=32)
    main: ProcessPhaseV1
    verifier_execution: VerifierExecution
    verifier: ProcessPhaseV1 | None = None
    sidecars: tuple[SidecarContainerV1, ...] = Field(default=(), max_length=32)
    max_log_bytes_per_stream: int = Field(default=10 * 1024 * 1024, gt=0, le=100 * 1024 * 1024)
    max_artifact_bytes: int = Field(default=1024 * 1024 * 1024, gt=0, le=10 * 1024**3)
    task_input: RuntimeTaskInputV1 | None = None
    output_declarations: tuple[RuntimeOutputDeclarationV1, ...] = Field(
        default=(),
        max_length=10_000,
    )

    @field_validator("task_image_ref", "runtime_image_ref")
    @classmethod
    def _images_are_immutable(cls, value: str) -> str:
        if _DIGEST_REF.fullmatch(value) is None:
            raise ValueError("runtime plan images must be digest-pinned")
        return value

    @model_validator(mode="after")
    def _roles_and_dependencies_are_closed(self) -> ExecutionRuntimePlanV1:
        if any(phase.role != "setup" for phase in self.setup):
            raise ValueError("runtime phase roles do not match their positions")
        if self.execution_role == "attempt":
            if self.main.role != "agent":
                raise ValueError("attempt execution requires an agent main phase")
            if self.verifier_execution == VerifierExecution.IN_ATTEMPT:
                if self.verifier is None or self.verifier.role != "verifier":
                    raise ValueError("in-attempt verifier requires a verifier phase")
            elif self.verifier is not None:
                raise ValueError("separate/skipped verifier cannot run in the primary attempt")
        elif (
            self.main.role != "verifier"
            or self.verifier_execution != VerifierExecution.SKIPPED
            or self.verifier is not None
        ):
            raise ValueError("verifier execution requires one verifier main phase")
        names = [sidecar.role_name for sidecar in self.sidecars]
        if len(names) != len(set(names)):
            raise ValueError("sidecar role names must be unique")
        if {"execution", "runtime-materializer", "setup", "agent", "verifier"} & set(names):
            raise ValueError("sidecar role name collides with a reserved container role")
        known: set[str] = set()
        for sidecar in self.sidecars:
            if any(item not in known for item in sidecar.depends_on):
                raise ValueError("sidecar dependencies must reference earlier sidecars")
            known.add(sidecar.role_name)
        source_paths = [item.source_path for item in self.output_declarations]
        bundle_paths = [item.relative_path for item in self.output_declarations]
        if len(source_paths) != len(set(source_paths)) or len(bundle_paths) != len(
            set(bundle_paths)
        ):
            raise ValueError("runtime output declarations must be unique")
        return self

    def canonical_payload(self) -> dict[str, object]:
        return self.model_dump(mode="json")


class RuntimeStreamEvidenceV1(_Strict):
    path: str = Field(pattern=r"^[0-9]{2}-(?:setup|agent|verifier)\.(?:stdout|stderr)$")
    sha256: str = Field(pattern=_SHA256.pattern)
    bytes_seen: int = Field(ge=0)
    bytes_saved: int = Field(ge=0)
    truncated: bool

    @model_validator(mode="after")
    def _saved_bytes_are_bounded(self) -> RuntimeStreamEvidenceV1:
        if self.bytes_saved > self.bytes_seen or self.truncated != (
            self.bytes_saved < self.bytes_seen
        ):
            raise ValueError("stream truncation evidence is inconsistent")
        return self


class RuntimePhaseEvidenceV1(_Strict):
    role: Literal["setup", "agent", "verifier"]
    ordinal: int = Field(gt=0, le=64)
    started_at: datetime
    finished_at: datetime
    exit_code: int
    signal: str | None = Field(default=None, max_length=32)
    timed_out: bool
    stdout: RuntimeStreamEvidenceV1
    stderr: RuntimeStreamEvidenceV1

    @model_validator(mode="after")
    def _phase_time_is_ordered(self) -> RuntimePhaseEvidenceV1:
        if self.finished_at < self.started_at:
            raise ValueError("runtime phase timestamps are reversed")
        return self


class ExecutionRuntimeResultV1(_Strict):
    schema_version: Literal["loom.execution-runtime-result.v1"]
    runtime_contract_sha256: str = Field(pattern=_SHA256.pattern)
    candidate_sha: str = Field(pattern=_CANDIDATE.pattern)
    task_revision_sha256: str = Field(pattern=_SHA256.pattern)
    command_identity_sha256: str = Field(pattern=_SHA256.pattern)
    execution_role: Literal["attempt", "verifier"]
    container_roles: tuple[str, ...] = Field(min_length=2, max_length=66)
    task_image_ref: str
    runtime_image_ref: str
    runtime_binary_sha256: str = Field(pattern=_SHA256.pattern)
    execution_class_id: str
    status: Literal[
        "succeeded",
        "setup_error",
        "task_error",
        "verifier_error",
        "timed_out",
        "cancelled",
        "runtime_error",
        "artifact_upload_failed",
        "missing_required_artifacts",
        "trajectory_flush_failed",
    ]
    started_at: datetime
    finished_at: datetime
    phases: tuple[RuntimePhaseEvidenceV1, ...] = Field(max_length=64)
    outputs: tuple[RuntimeOutputEvidenceV1, ...] = Field(default=(), max_length=10_000)
    verifier_rewards: dict[str, float] | None = None
    partial_evidence: bool

    @field_validator("task_image_ref", "runtime_image_ref")
    @classmethod
    def _result_images_are_immutable(cls, value: str) -> str:
        if _DIGEST_REF.fullmatch(value) is None:
            raise ValueError("runtime result images must be digest-pinned")
        return value

    @model_validator(mode="after")
    def _terminal_result_is_consistent(self) -> ExecutionRuntimeResultV1:
        if self.finished_at < self.started_at:
            raise ValueError("runtime result timestamps are reversed")
        if self.partial_evidence != (self.status != "succeeded"):
            raise ValueError("runtime partial-evidence flag does not match status")
        if [phase.ordinal for phase in self.phases] != list(range(1, len(self.phases) + 1)):
            raise ValueError("runtime phase ordinals are not contiguous")
        sources = [item.source_path for item in self.outputs]
        paths = [item.relative_path for item in self.outputs]
        if len(sources) != len(set(sources)) or len(paths) != len(set(paths)):
            raise ValueError("runtime output evidence must be unique")
        if self.status == "succeeded" and any(
            item.required and item.state != "captured" for item in self.outputs
        ):
            raise ValueError("successful runtime result is missing required output")
        if self.verifier_rewards is not None:
            if not self.verifier_rewards or any(
                not key or len(key.encode("utf-8")) > 256 or not math.isfinite(value)
                for key, value in self.verifier_rewards.items()
            ):
                raise ValueError("runtime verifier rewards are invalid")
        return self


def validate_runtime_plan_requirements(
    plan: ExecutionRuntimePlanV1,
    requirements: WorkloadRequirementsV1,
) -> None:
    """Reject semantic drift between admission requirements and the runtime plan."""

    if requirements.image_ref != plan.task_image_ref:
        raise ValueError("runtime plan task image does not match workload requirements")
    expected_resources = (
        requirements.cpu_millis,
        requirements.memory_mib,
        requirements.ephemeral_storage_mib,
    )
    actual_resources = (
        plan.task_resources.cpu_millis,
        plan.task_resources.memory_mib,
        plan.task_resources.ephemeral_storage_mib,
    )
    if expected_resources != actual_resources:
        raise ValueError("runtime plan resources do not match workload requirements")
    if requirements.sidecar_count != len(plan.sidecars):
        raise ValueError("runtime plan sidecars do not match workload requirements")
    if plan.execution_role == "verifier":
        if plan.verifier_execution != VerifierExecution.SKIPPED:
            raise ValueError("a verifier execution unit cannot schedule another verifier")
    else:
        expected_verifier = {
            VerifierTopology.IN_ATTEMPT: VerifierExecution.IN_ATTEMPT,
            VerifierTopology.SEPARATE_EXECUTION: VerifierExecution.SEPARATE_EXECUTION,
        }[requirements.verifier_topology]
        if plan.verifier_execution != expected_verifier:
            raise ValueError("runtime plan verifier topology does not match workload requirements")


__all__ = [
    "ContainerResourcesV1",
    "ExecutionRuntimePlanV1",
    "ExecutionRuntimeResultV1",
    "ProbeV1",
    "ProcessPhaseV1",
    "RuntimeComposition",
    "RuntimeOutputDeclarationV1",
    "RuntimeOutputEvidenceV1",
    "RuntimePhaseEvidenceV1",
    "RuntimeStreamEvidenceV1",
    "RuntimeTaskInputV1",
    "SidecarContainerV1",
    "VerifierExecution",
    "validate_runtime_plan_requirements",
]
