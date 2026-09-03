"""Durable compilation of ordinary TaskSets into Nebius execution plans."""

from __future__ import annotations

import hashlib
import json
import re
import stat
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.execution_image_admission import ExecutionImageAdmissionBundleV1
from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    ProcessPhaseV1,
    RuntimeOutputDeclarationV1,
    RuntimeTaskInputV1,
)
from loom.models.task import TaskConfig, normalize_steps
from loom.models.trial import TrialConfig
from loom.pipeline.keys import canonical_digest

_DIGEST_REF = re.compile(r"^.+@sha256:[0-9a-f]{64}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_GLOB_MAGIC = re.compile(r"[*?[]")
MAX_INPUT_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_INPUT_FILES = 10_000
MAX_INPUT_BYTES = 10 * 1024**3


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ServiceExecutionInputFileV1(_Strict):
    relative_path: str = Field(min_length=1, max_length=4096)
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=_SHA256.pattern)
    mode: Literal["0644", "0755"]

    @field_validator("relative_path")
    @classmethod
    def safe_relative_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
            raise ValueError("service execution input path must be safe and relative")
        return value


class ServiceExecutionInputManifestV1(_Strict):
    schema_version: Literal["loom.service-execution-input-manifest.v1"] = (
        "loom.service-execution-input-manifest.v1"
    )
    task_revision_sha256: str = Field(pattern=_SHA256.pattern)
    files: tuple[ServiceExecutionInputFileV1, ...] = Field(
        min_length=1,
        max_length=MAX_INPUT_FILES,
    )

    @model_validator(mode="after")
    def canonical_inventory(self) -> ServiceExecutionInputManifestV1:
        paths = [item.relative_path for item in self.files]
        if paths != sorted(paths, key=lambda item: item.encode("utf-8")):
            raise ValueError("service execution input files must be sorted")
        if len(paths) != len(set(paths)):
            raise ValueError("service execution input paths must be unique")
        return self

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")


class ServiceExecutionInputBindingV1(_Strict):
    schema_version: Literal["loom.service-execution-input.v1"] = "loom.service-execution-input.v1"
    manifest_uri: str = Field(pattern=r"^s3://[^/]+/.+$", max_length=4096)
    manifest_sha256: str = Field(pattern=_SHA256.pattern)
    file_count: int = Field(gt=0, le=MAX_INPUT_FILES)
    total_bytes: int = Field(ge=0, le=MAX_INPUT_BYTES)


class ServiceExecutionRuntimeProfileV1(_Strict):
    """Deployment-owned immutable inputs for automatic plan compilation."""

    schema_version: Literal["loom.service-execution-runtime-profile.v1"] = (
        "loom.service-execution-runtime-profile.v1"
    )
    logical_pool_id: Literal["nebius-cpu"] = "nebius-cpu"
    candidate_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    execution_class_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    task_image_ref: str
    runtime_image_ref: str
    runtime_binary_sha256: str = Field(pattern=_SHA256.pattern)
    image_admission: ExecutionImageAdmissionBundleV1
    run_as_user: int = Field(default=65532, gt=0)
    run_as_group: int = Field(default=65532, gt=0)
    fs_group: int = Field(default=65532, gt=0)
    runtime_volume_mib: int = Field(default=32, gt=0, le=4096)
    termination_grace_seconds: int = Field(default=30, ge=1, le=300)
    max_log_bytes_per_stream: int = Field(default=10 * 1024 * 1024, gt=0)
    max_artifact_bytes: int = Field(default=1024 * 1024 * 1024, gt=0)

    @field_validator("task_image_ref", "runtime_image_ref")
    @classmethod
    def immutable_images(cls, value: str) -> str:
        if _DIGEST_REF.fullmatch(value) is None:
            raise ValueError("runtime profile images must be digest-pinned")
        return value


def load_service_execution_runtime_profile(
    raw: str,
) -> ServiceExecutionRuntimeProfileV1 | None:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("service execution runtime profile is not valid JSON") from exc
    if value == {}:
        return None
    return ServiceExecutionRuntimeProfileV1.model_validate(value)


def build_service_execution_input_manifest(
    bundle_dir: Path,
    *,
    task_checksum: str,
) -> ServiceExecutionInputManifestV1:
    files: list[ServiceExecutionInputFileV1] = []
    for path in sorted(bundle_dir.rglob("*")):
        if path.is_dir():
            continue
        if path.is_symlink() or not path.is_file():
            raise ValueError("service execution input contains a non-regular file")
        body = path.read_bytes()
        mode = "0755" if stat.S_IMODE(path.stat().st_mode) & 0o111 else "0644"
        files.append(
            ServiceExecutionInputFileV1(
                relative_path=path.relative_to(bundle_dir).as_posix(),
                size_bytes=len(body),
                sha256="sha256:" + hashlib.sha256(body).hexdigest(),
                mode=mode,
            )
        )
    return ServiceExecutionInputManifestV1(
        task_revision_sha256="sha256:" + task_checksum.removeprefix("sha256:"),
        files=tuple(files),
    )


def service_execution_input_binding(
    provenance: dict[str, Any],
) -> ServiceExecutionInputBindingV1 | None:
    raw = provenance.get("service_execution_input")
    if raw is None:
        return None
    return ServiceExecutionInputBindingV1.model_validate(raw)


def automatic_service_execution_rejections(
    task: TaskConfig,
    trial: TrialConfig,
    *,
    source_provenance: dict[str, Any],
) -> tuple[str, ...]:
    """Return stable reasons why the v1 ordinary-TaskSet compiler cannot run a task."""

    task = normalize_steps(task)
    env = task.environment
    reasons: list[str] = []
    if service_execution_input_binding(source_provenance) is None:
        reasons.append("immutable_task_input_unavailable")
    if env.os != "linux" or env.cpu_arch not in {"x86_64", "any"}:
        reasons.append("linux_x86_64_required")
    if env.gpu_vendor != "none" or env.gpus:
        reasons.append("gpu_unsupported")
    if (
        env.dockerfile is not None
        or env.docker_image is None
        or _DIGEST_REF.fullmatch(env.docker_image) is None
    ):
        reasons.append("immutable_task_image_required")
    if env.cpus is None or env.memory_mb is None or env.storage_mb is None:
        reasons.append("resource_limits_required")
    elif env.cpus > 128 or env.memory_mb > 1_048_576 or env.storage_mb > 1_048_576:
        reasons.append("resource_limits_out_of_range")
    if env.workdir != PurePosixPath("/workspace") or env.user != "agent":
        reasons.append("standard_workspace_identity_required")
    if env.baseline_network_policy.kind != "gateway-only":
        reasons.append("gateway_only_network_required")
    if (
        env.environment
        or env.sidecars
        or env.extra_hosts
        or env.dns
        or env.tmpfs
        or env.healthcheck is not None
        or env.skills_dir is not None
        or env.mcp_servers
    ):
        reasons.append("extended_environment_unsupported")
    if task.required_agent_capabilities:
        reasons.append("agent_capabilities_unsupported")
    if task.agent.extra_mcp_servers or task.agent.skills or task.agent.user is not None:
        reasons.append("extended_agent_runtime_unsupported")
    if task.verifier.user is not None:
        reasons.append("custom_verifier_identity_unsupported")
    if len(task.steps) != 1 or task.multi_step is not None:
        reasons.append("single_step_required")
    if trial.agent_name not in {"direct-completion", "litellm"}:
        reasons.append("direct_completion_required")
    if trial.agent_model is None or trial.agent_model.source != "api":
        reasons.append("api_model_required")
    if trial.extra_mcp_servers or trial.extra_skills or trial.multi_model is not None:
        reasons.append("extended_agent_runtime_unsupported")
    if task.verifier.name != "script" or task.verifier.env_mode != "shared":
        reasons.append("shared_script_verifier_required")
    verifier_path = task.verifier.args.get("script_path")
    if (
        not isinstance(verifier_path, str)
        or not verifier_path
        or PurePosixPath(verifier_path).is_absolute()
        or ".." in PurePosixPath(verifier_path).parts
        or _GLOB_MAGIC.search(verifier_path)
    ):
        reasons.append("exact_verifier_path_required")
    if trial.skip_verifier or trial.verifier_env_mode not in {None, "shared"}:
        reasons.append("shared_verifier_required")
    if task.steps:
        step = task.steps[0]
        if (
            step.agent is not None
            or step.verifier is not None
            or step.network is not None
            or step.healthcheck is not None
        ):
            reasons.append("step_overrides_unsupported")
        instruction_path = PurePosixPath(step.instruction_file)
        if (
            instruction_path.is_absolute()
            or ".." in instruction_path.parts
            or _GLOB_MAGIC.search(str(instruction_path))
        ):
            reasons.append("exact_instruction_path_required")
        artifacts = [*step.artifacts, *step.required_artifacts]
        if any(
            PurePosixPath(item).is_absolute()
            or ".." in PurePosixPath(item).parts
            or _GLOB_MAGIC.search(item)
            or PurePosixPath(item).parts[:1] == (".loom",)
            for item in artifacts
        ):
            reasons.append("exact_artifact_paths_required")
    return tuple(dict.fromkeys(reasons))


def compile_service_execution_plan(
    *,
    task: TaskConfig,
    trial: TrialConfig,
    task_revision_sha256: str,
    source_provenance: dict[str, Any],
    profile: ServiceExecutionRuntimeProfileV1,
) -> ExecutionRuntimePlanV1:
    task = normalize_steps(task)
    reasons = automatic_service_execution_rejections(
        task,
        trial,
        source_provenance=source_provenance,
    )
    if reasons:
        raise ValueError("automatic service execution is incompatible: " + ",".join(reasons))
    if task.environment.docker_image != profile.task_image_ref:
        raise ValueError("task image is not provided by the active runtime profile")
    binding = service_execution_input_binding(source_provenance)
    assert binding is not None
    assert task.environment.cpus is not None
    assert task.environment.memory_mb is not None
    assert task.environment.storage_mb is not None
    step = task.steps[0]
    assert trial.agent_model is not None
    # The in-Pod runner targets Loom's attributed chat route.  It revalidates
    # the service-execution lease and supports both a JWT-bound provider
    # connection and the platform route, whose model identity is provider/name.
    model = trial.agent_model.to_gateway_model_string()
    main_environment = {
        "LOOM_TASK_MODEL": model,
        "LOOM_TASK_INSTRUCTION_FILE": str(step.instruction_file),
        "LOOM_TASK_ARTIFACTS_JSON": json.dumps(step.artifacts, separators=(",", ":")),
        "LOOM_TASK_REQUEST_PARAMS_JSON": json.dumps(
            trial.request_params, sort_keys=True, separators=(",", ":")
        ),
    }
    verifier_path = str(task.verifier.args.get("script_path", ""))
    verifier = ProcessPhaseV1(
        role="verifier",
        argv=("/bin/sh", verifier_path),
        working_directory="/workspace",
        timeout_seconds=round(
            (trial.override_verifier_timeout_sec or task.verifier.timeout_sec)
            * trial.verifier_timeout_multiplier
        ),
        environment={
            "LOOM_TASK_DIR": "/workspace",
            "LOOM_VERIFIER_OUTPUT": "/workspace/.loom/verifier/output.json",
            **(
                {"LOOM_AGENT_OUTPUT": f"/workspace/{step.artifacts[0]}"}
                if len(step.artifacts) == 1
                else {}
            ),
        },
    )
    command_identity = canonical_digest(
        {
            "schema_version": "loom.automatic-service-execution-command.v1",
            "task_revision_sha256": task_revision_sha256,
            "agent": trial.agent_name,
            "model": trial.agent_model.model_dump(mode="json"),
            "request_params": trial.request_params,
            "instruction_file": str(step.instruction_file),
            "artifacts": step.artifacts,
            "required_artifacts": step.required_artifacts,
            "verifier": task.verifier.model_dump(mode="json"),
        }
    )
    output_paths = list(dict.fromkeys((*step.artifacts, *step.required_artifacts)))
    output_declarations = (
        *(
            RuntimeOutputDeclarationV1(
                source_path=path,
                relative_path=f"artifacts/{path}",
                kind="task_artifact",
                required=True,
            )
            for path in output_paths
        ),
        RuntimeOutputDeclarationV1(
            source_path=".loom/agent/trajectory.jsonl",
            relative_path="trajectory/events.jsonl",
            kind="trajectory",
            required=True,
        ),
        RuntimeOutputDeclarationV1(
            source_path=".loom/agent/usage.json",
            relative_path="accounting/usage.json",
            kind="usage",
            required=True,
        ),
        RuntimeOutputDeclarationV1(
            source_path=".loom/verifier/output.json",
            relative_path="verifier/output.json",
            kind="verifier",
            required=True,
        ),
    )
    return ExecutionRuntimePlanV1(
        candidate_sha=profile.candidate_sha,
        task_revision_sha256=task_revision_sha256,
        command_identity_sha256=command_identity,
        execution_class_id=profile.execution_class_id,
        composition="init_payload",
        task_image_ref=profile.task_image_ref,
        runtime_image_ref=profile.runtime_image_ref,
        runtime_binary_sha256=profile.runtime_binary_sha256,
        image_admission=profile.image_admission,
        run_as_user=profile.run_as_user,
        run_as_group=profile.run_as_group,
        fs_group=profile.fs_group,
        task_resources=ContainerResourcesV1(
            cpu_millis=round(task.environment.cpus * 1000),
            memory_mib=task.environment.memory_mb,
            ephemeral_storage_mib=task.environment.storage_mb,
        ),
        workspace_mib=task.environment.storage_mb,
        runtime_volume_mib=profile.runtime_volume_mib,
        termination_grace_seconds=profile.termination_grace_seconds,
        task_input=RuntimeTaskInputV1(
            manifest_sha256=binding.manifest_sha256,
            file_count=binding.file_count,
            total_bytes=binding.total_bytes,
        ),
        output_declarations=output_declarations,
        main=ProcessPhaseV1(
            role="agent",
            argv=("python", "-m", "loom.service_execution_task", "direct-completion"),
            working_directory="/workspace",
            timeout_seconds=round(
                (trial.override_agent_timeout_sec or task.agent.timeout_sec)
                * trial.agent_timeout_multiplier
            ),
            environment=main_environment,
        ),
        verifier_execution="in_attempt",
        verifier=verifier,
        max_log_bytes_per_stream=profile.max_log_bytes_per_stream,
        max_artifact_bytes=profile.max_artifact_bytes,
    )


__all__ = [
    "MAX_INPUT_BYTES",
    "MAX_INPUT_FILES",
    "MAX_INPUT_MANIFEST_BYTES",
    "RuntimeTaskInputV1",
    "ServiceExecutionInputBindingV1",
    "ServiceExecutionInputFileV1",
    "ServiceExecutionInputManifestV1",
    "ServiceExecutionRuntimeProfileV1",
    "automatic_service_execution_rejections",
    "build_service_execution_input_manifest",
    "compile_service_execution_plan",
    "load_service_execution_runtime_profile",
    "service_execution_input_binding",
]
