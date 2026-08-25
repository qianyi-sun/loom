"""Provider-neutral service execution and workload admission contracts.

The contracts in this module define what a service workload needs without
choosing a provider or physical target.  Provider/region binding belongs to an
execution target (and, later, a durable execution lease), while admission is a
strict comparison between immutable workload requirements and an execution
class.

Issue #1548 deliberately keeps these models independent from the current
``backend``/worker-pool scheduler fields.  They are the versioned boundary that
the durable execution control plane introduced by #1540 will persist.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.models.task import TaskConfig

_IMMUTABLE_OCI_REF = re.compile(r"^.+@sha256:[0-9a-f]{64}$")


class _StrictContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class IsolationLevel(StrEnum):
    """Minimum hostile-code isolation supplied by an execution class."""

    SHARED_KERNEL = "shared_kernel"
    SANDBOXED_RUNTIME = "sandboxed_runtime"
    DEDICATED_EPHEMERAL_NODE = "dedicated_ephemeral_node"


class NetworkAccess(StrEnum):
    NONE = "none"
    GATEWAY_ONLY = "gateway_only"
    APPROVED_ALLOWLIST = "approved_allowlist"
    UNRESTRICTED_PUBLIC = "unrestricted_public"


class ImageMaterialization(StrEnum):
    IMMUTABLE_OCI = "immutable_oci"
    TASK_DOCKERFILE = "task_dockerfile"
    MUTABLE_OCI = "mutable_oci"
    UNSPECIFIED = "unspecified"


class VerifierTopology(StrEnum):
    IN_ATTEMPT = "in_attempt"
    SEPARATE_EXECUTION = "separate_execution"


class ExecutionClassV1(_StrictContract):
    """Versioned, provider-neutral capabilities for one execution class."""

    schema_version: Literal["loom.execution-class.v1"] = "loom.execution-class.v1"
    class_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    operating_system: Literal["linux"]
    cpu_architecture: Literal["x86_64", "arm64"]
    gpu_vendor: Literal["none", "nvidia"]
    isolation_level: IsolationLevel
    network_access: frozenset[NetworkAccess]
    maximum_cpu_millis: int | None = Field(default=None, gt=0)
    maximum_memory_mib: int | None = Field(default=None, gt=0)
    maximum_ephemeral_storage_mib: int | None = Field(default=None, gt=0)
    maximum_sidecars: int = Field(ge=0)
    supports_separate_verifier: bool
    supports_custom_dns: bool
    supports_extra_hosts: bool
    supports_tmpfs: bool
    supports_task_image_build: bool
    requires_immutable_image: bool
    permits_privileged: bool
    permits_host_path: bool
    permits_host_network: bool
    permits_nested_containers: bool
    permits_host_devices: bool

    @model_validator(mode="after")
    def _hostile_code_must_not_use_shared_kernel(self) -> ExecutionClassV1:
        if self.isolation_level == IsolationLevel.SHARED_KERNEL:
            raise ValueError(
                "service execution classes must use a sandboxed runtime or "
                "a dedicated ephemeral node; shared-kernel Pods are not an "
                "accepted hostile-code boundary",
            )
        forbidden = {
            "permits_privileged": self.permits_privileged,
            "permits_host_path": self.permits_host_path,
            "permits_host_network": self.permits_host_network,
            "permits_nested_containers": self.permits_nested_containers,
            "permits_host_devices": self.permits_host_devices,
        }
        enabled = sorted(name for name, value in forbidden.items() if value)
        if enabled:
            raise ValueError(
                "service execution classes cannot enable host-escape capabilities: "
                + ", ".join(enabled),
            )
        return self


class ExecutionTargetV1(_StrictContract):
    """Provider-bound regional target kept outside trial requirements."""

    schema_version: Literal["loom.execution-target.v1"] = "loom.execution-target.v1"
    target_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    logical_pool_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    execution_class_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    environment: Literal["development", "staging", "production"]
    provider: Literal["nebius"]
    region: str = Field(min_length=1, max_length=80)
    failure_domain: str = Field(min_length=1, max_length=120)
    data_residency: Literal["eu"]
    health_role: Literal["primary", "secondary"]
    health_check_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    health_check_interval_seconds: int = Field(ge=5, le=300)
    health_stale_after_seconds: int = Field(ge=10, le=900)

    @model_validator(mode="after")
    def _health_freshness_exceeds_probe_interval(self) -> ExecutionTargetV1:
        if self.health_stale_after_seconds <= self.health_check_interval_seconds:
            raise ValueError(
                "health_stale_after_seconds must exceed the independent probe interval",
            )
        return self


class ExecutionTopologyV1(_StrictContract):
    """Checked environment and regional bindings for one logical pool."""

    schema_version: Literal["loom.execution-topology.v1"] = "loom.execution-topology.v1"
    logical_pool_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    execution_class_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,79}$")
    placement_policy: Literal["environment-local-health-first"]
    targets: tuple[ExecutionTargetV1, ...] = Field(min_length=4)

    @model_validator(mode="after")
    def _targets_form_an_isolated_multi_region_topology(self) -> ExecutionTopologyV1:
        target_ids = [target.target_id for target in self.targets]
        health_ids = [target.health_check_id for target in self.targets]
        if len(target_ids) != len(set(target_ids)):
            raise ValueError("execution target ids must be unique")
        if len(health_ids) != len(set(health_ids)):
            raise ValueError("every execution target needs an independent health check")
        for target in self.targets:
            if target.logical_pool_id != self.logical_pool_id:
                raise ValueError("every target must bind the declared logical pool")
            if target.execution_class_id != self.execution_class_id:
                raise ValueError("every target must bind the declared execution class")

        by_environment = {
            environment: [target for target in self.targets if target.environment == environment]
            for environment in ("development", "staging", "production")
        }
        if not by_environment["development"] or not by_environment["staging"]:
            raise ValueError("development and staging require isolated targets")
        production = by_environment["production"]
        if len(production) < 2:
            raise ValueError("production requires at least two regional targets")
        if {target.health_role for target in production} != {"primary", "secondary"}:
            raise ValueError("production requires primary and secondary health roles")
        if len({target.region for target in production}) < 2:
            raise ValueError("production targets must use distinct regions")
        if len({target.failure_domain for target in production}) < 2:
            raise ValueError("production targets must use distinct failure domains")
        return self


class WorkloadRequirementsV1(_StrictContract):
    """Complete material capability declaration for one execution unit."""

    schema_version: Literal["loom.workload-requirements.v1"] = "loom.workload-requirements.v1"
    operating_system: Literal["linux", "windows"]
    cpu_architecture: Literal["x86_64", "arm64", "any"]
    gpu_vendor: Literal["none", "nvidia"]
    gpu_count: int = Field(ge=0)
    cpu_millis: int | None = Field(gt=0)
    memory_mib: int | None = Field(gt=0)
    ephemeral_storage_mib: int | None = Field(gt=0)
    isolation_level: IsolationLevel
    network_access: NetworkAccess
    image_materialization: ImageMaterialization
    image_ref: str | None
    sidecar_count: int = Field(ge=0)
    verifier_topology: VerifierTopology
    custom_dns: bool
    extra_hosts: bool
    tmpfs: bool
    privileged: bool
    host_path: bool
    host_network: bool
    nested_containers: bool
    host_devices: bool
    host_specialized: bool

    @model_validator(mode="after")
    def _image_identity_matches_materialization(self) -> WorkloadRequirementsV1:
        if self.image_materialization == ImageMaterialization.IMMUTABLE_OCI:
            if self.image_ref is None or not _IMMUTABLE_OCI_REF.fullmatch(self.image_ref):
                raise ValueError(
                    "immutable_oci requires an image_ref pinned by @sha256:<64 hex>",
                )
        return self


class CompatibilityReasonV1(_StrictContract):
    code: str = Field(pattern=r"^[a-z0-9][a-z0-9_]{0,79}$")
    message: str = Field(min_length=1, max_length=500)


class ExecutionAdmissionV1(_StrictContract):
    schema_version: Literal["loom.execution-admission.v1"] = "loom.execution-admission.v1"
    execution_class_id: str
    compatible: bool
    reasons: tuple[CompatibilityReasonV1, ...]

    @model_validator(mode="after")
    def _decision_matches_reasons(self) -> ExecutionAdmissionV1:
        if self.compatible == bool(self.reasons):
            raise ValueError("compatible must be true exactly when reasons is empty")
        return self


def workload_requirements_from_task(task: TaskConfig) -> WorkloadRequirementsV1:
    """Project a version-1 task into the explicit service workload contract.

    Existing task fields called ``public`` mean unrestricted network access;
    the Nebius service class therefore rejects them until the task is converted
    to gateway-only or an approved allowlist.  Mutable tags and Dockerfile
    builds are likewise represented honestly instead of being auto-repaired.
    """

    env = task.environment
    if env.dockerfile is not None:
        materialization = ImageMaterialization.TASK_DOCKERFILE
        image_ref: str | None = None
    elif env.docker_image is None:
        materialization = ImageMaterialization.UNSPECIFIED
        image_ref = None
    elif _IMMUTABLE_OCI_REF.fullmatch(env.docker_image):
        materialization = ImageMaterialization.IMMUTABLE_OCI
        image_ref = env.docker_image
    else:
        materialization = ImageMaterialization.MUTABLE_OCI
        image_ref = env.docker_image

    policy_kind = env.baseline_network_policy.kind
    network_access = {
        "no-network": NetworkAccess.NONE,
        "allowlist": NetworkAccess.APPROVED_ALLOWLIST,
        "public": NetworkAccess.UNRESTRICTED_PUBLIC,
    }[policy_kind]
    verifier_topology = (
        VerifierTopology.SEPARATE_EXECUTION
        if task.verifier.env_mode == "separate"
        else VerifierTopology.IN_ATTEMPT
    )
    return WorkloadRequirementsV1(
        operating_system=env.os,
        cpu_architecture=env.cpu_arch,
        gpu_vendor=env.gpu_vendor,
        gpu_count=env.gpus,
        cpu_millis=round(env.cpus * 1000) if env.cpus is not None else None,
        memory_mib=env.memory_mb,
        ephemeral_storage_mib=env.storage_mb,
        isolation_level=IsolationLevel.SANDBOXED_RUNTIME,
        network_access=network_access,
        image_materialization=materialization,
        image_ref=image_ref,
        sidecar_count=len(env.sidecars),
        verifier_topology=verifier_topology,
        custom_dns=bool(env.dns),
        extra_hosts=bool(env.extra_hosts),
        tmpfs=bool(env.tmpfs),
        privileged=False,
        host_path=False,
        host_network=False,
        nested_containers=False,
        host_devices=False,
        host_specialized=False,
    )


def evaluate_execution_admission(
    requirements: WorkloadRequirementsV1,
    execution_class: ExecutionClassV1,
) -> ExecutionAdmissionV1:
    """Return every material incompatibility; never weaken requirements."""

    reasons: list[CompatibilityReasonV1] = []

    def reject(code: str, message: str) -> None:
        reasons.append(CompatibilityReasonV1(code=code, message=message))

    if requirements.operating_system != execution_class.operating_system:
        reject("operating_system_unsupported", "execution class only supports Linux")
    if requirements.cpu_architecture not in {"any", execution_class.cpu_architecture}:
        reject(
            "cpu_architecture_unsupported",
            f"execution class provides {execution_class.cpu_architecture}",
        )
    if requirements.gpu_vendor != execution_class.gpu_vendor or requirements.gpu_count:
        reject("gpu_unsupported", "CPU execution class cannot satisfy a GPU workload")
    if requirements.isolation_level != execution_class.isolation_level:
        reject(
            "isolation_level_unsupported",
            "workload isolation requirement does not exactly match the admitted class",
        )
    if requirements.network_access not in execution_class.network_access:
        reject(
            "network_access_unsupported",
            f"network mode {requirements.network_access.value} is not admitted",
        )
    if requirements.image_materialization == ImageMaterialization.TASK_DOCKERFILE:
        if not execution_class.supports_task_image_build:
            reject("task_image_build_unsupported", "runtime Dockerfile builds are not admitted")
    if execution_class.requires_immutable_image:
        if requirements.image_materialization != ImageMaterialization.IMMUTABLE_OCI:
            reject(
                "immutable_image_required", "attempt image must be digest-pinned before admission"
            )
    if requirements.sidecar_count > execution_class.maximum_sidecars:
        reject("sidecar_limit_exceeded", "declared sidecars exceed the execution class limit")
    if (
        requirements.verifier_topology == VerifierTopology.SEPARATE_EXECUTION
        and not execution_class.supports_separate_verifier
    ):
        reject("separate_verifier_unsupported", "separate verifier execution is required")

    resource_pairs = (
        ("cpu", requirements.cpu_millis, execution_class.maximum_cpu_millis),
        ("memory", requirements.memory_mib, execution_class.maximum_memory_mib),
        (
            "ephemeral_storage",
            requirements.ephemeral_storage_mib,
            execution_class.maximum_ephemeral_storage_mib,
        ),
    )
    for name, requested, maximum in resource_pairs:
        if requested is None:
            reject(f"{name}_limit_required", f"{name} must have an explicit positive limit")
        elif maximum is not None and requested > maximum:
            reject(f"{name}_limit_exceeded", f"{name} exceeds the execution class maximum")

    feature_pairs = (
        (requirements.custom_dns, execution_class.supports_custom_dns, "custom_dns"),
        (requirements.extra_hosts, execution_class.supports_extra_hosts, "extra_hosts"),
        (requirements.tmpfs, execution_class.supports_tmpfs, "tmpfs"),
        (requirements.privileged, execution_class.permits_privileged, "privileged"),
        (requirements.host_path, execution_class.permits_host_path, "host_path"),
        (requirements.host_network, execution_class.permits_host_network, "host_network"),
        (
            requirements.nested_containers,
            execution_class.permits_nested_containers,
            "nested_containers",
        ),
        (requirements.host_devices, execution_class.permits_host_devices, "host_devices"),
    )
    for required, supported, name in feature_pairs:
        if required and not supported:
            reject(f"{name}_unsupported", f"{name} is forbidden by the execution class")
    if requirements.host_specialized:
        reject("host_specialized_unsupported", "host-specialized workloads are not portable Pods")

    return ExecutionAdmissionV1(
        execution_class_id=execution_class.class_id,
        compatible=not reasons,
        reasons=tuple(reasons),
    )


NEBIUS_CPU_EXECUTION_CLASS_V1 = ExecutionClassV1(
    class_id="linux-amd64-cpu-pod-v1",
    operating_system="linux",
    cpu_architecture="x86_64",
    gpu_vendor="none",
    isolation_level=IsolationLevel.SANDBOXED_RUNTIME,
    network_access=frozenset(
        {
            NetworkAccess.NONE,
            NetworkAccess.GATEWAY_ONLY,
            NetworkAccess.APPROVED_ALLOWLIST,
        }
    ),
    maximum_sidecars=8,
    supports_separate_verifier=True,
    supports_custom_dns=False,
    supports_extra_hosts=False,
    supports_tmpfs=True,
    supports_task_image_build=False,
    requires_immutable_image=True,
    permits_privileged=False,
    permits_host_path=False,
    permits_host_network=False,
    permits_nested_containers=False,
    permits_host_devices=False,
)
