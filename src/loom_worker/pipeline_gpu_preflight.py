"""Attested 120-second GPU container preflight for Pipeline Attempts."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from loom.models.worker_capabilities import (
    SlurmGpuAllocationEvidenceV1,
    WorkerCapabilitySnapshotV1,
)
from loom.pipeline.image_runtime import driver_version_satisfies
from loom.pipeline.work_protocol import (
    ImageRuntimeContractV1,
    ResourceExecutionVariantV1,
    ResourceProfileV1,
)
from loom_worker.pipeline_container_runner import (
    MaterializedInputView,
    PipelineContainerSpec,
)


class PipelineGpuPreflightError(RuntimeError):
    def __init__(self, reason: str, *, retryable: bool = False) -> None:
        super().__init__(reason)
        self.reason = reason
        self.retryable = retryable


@dataclass(frozen=True)
class GpuContainerPreflightPlan:
    variant_id: str
    image_index_digest: str
    platform_manifest_digest: str
    preflight_argv: tuple[str, ...]
    preflight_digest: str
    cuda_userspace_version: str
    timeout_seconds: Literal[120]
    docker_device_uuids: tuple[str, ...]
    sim_gpu_index: int
    vla_gpu_index: int | None
    container_memory_bytes: int
    require_concurrent_vla_isaac: bool
    requires_vla: bool


@dataclass(frozen=True)
class GpuContainerPreflightObservation:
    cpu_arch: str
    platform_manifest_digest: str
    cuda_userspace_version: str
    egl_healthy: bool
    visible_device_uuids: tuple[str, ...]
    device_models: tuple[str, ...]
    isaac_healthy: bool
    omnigibson_healthy: bool
    vla_healthy: bool
    concurrent_vla_isaac_healthy: bool


GpuPreflightObserver = Callable[
    [UUID, GpuContainerPreflightPlan, PipelineContainerSpec, MaterializedInputView],
    Awaitable[GpuContainerPreflightObservation],
]


@dataclass(frozen=True)
class AttestedGpuExecutionPreflight:
    """Run the attested probe with the same closed spec before stage argv."""

    plan: GpuContainerPreflightPlan
    observe: GpuPreflightObserver

    async def attest(
        self,
        *,
        attempt_id: UUID,
        spec: PipelineContainerSpec,
        input_view: MaterializedInputView,
    ) -> None:
        if (
            spec.gpu_device_uuids != self.plan.docker_device_uuids
            or spec.limits.memory_bytes != self.plan.container_memory_bytes
        ):
            raise PipelineGpuPreflightError("gpu_allocation_mismatch")
        try:
            observation = await asyncio.wait_for(
                self.observe(attempt_id, self.plan, spec, input_view),
                timeout=self.plan.timeout_seconds,
            )
        except TimeoutError as exc:
            raise PipelineGpuPreflightError("gpu_preflight_timeout") from exc
        validate_gpu_container_preflight(self.plan, observation)


def _selected_variant(
    profile: ResourceProfileV1, variant_id: str
) -> ResourceExecutionVariantV1:
    matches = [item for item in profile.execution_variants if item.variant_id == variant_id]
    if len(matches) != 1:
        raise PipelineGpuPreflightError("resource_profile_variant_mismatch")
    return matches[0]


def build_gpu_container_preflight_plan(
    *,
    profile: ResourceProfileV1,
    variant_id: str,
    image_contract: ImageRuntimeContractV1,
    capability: WorkerCapabilitySnapshotV1,
    allocation: SlurmGpuAllocationEvidenceV1,
    requires_vla: bool = True,
) -> GpuContainerPreflightPlan:
    variant = _selected_variant(profile, variant_id)
    devices = capability.gpu_devices
    if (
        variant.gpu_count_exact == 0
        or variant.device_roles is None
        or image_contract.gpu_vendor != "nvidia"
        or allocation.variant_id != variant.variant_id
        or len(devices) != variant.gpu_count_exact
        or {item.allocation_id for item in devices} != {allocation.allocation_id}
        or tuple(item.device_uuid for item in devices) != tuple(allocation.device_uuids)
    ):
        raise PipelineGpuPreflightError("gpu_allocation_mismatch")
    if capability.cpu_arch != variant.cpu_arch or image_contract.cpu_arch != variant.cpu_arch:
        raise PipelineGpuPreflightError("image_contract_mismatch")
    if any(item.model not in variant.allowed_gpu_models for item in devices):
        raise PipelineGpuPreflightError("gpu_model_mismatch")
    if variant.same_gpu_model_required and len({item.model for item in devices}) != 1:
        raise PipelineGpuPreflightError("gpu_model_mismatch")
    if variant.gpu_memory_kind == "dedicated":
        if any(
            item.memory_kind != "dedicated"
            or item.memory_mb is None
            or variant.gpu_memory_mb_min is None
            or item.memory_mb < variant.gpu_memory_mb_min
            for item in devices
        ):
            raise PipelineGpuPreflightError("gpu_memory_mismatch")
    elif variant.gpu_memory_kind == "unified":
        if any(
            item.memory_kind != "unified"
            or item.unified_memory_mb is None
            or variant.gpu_unified_memory_mb_min is None
            or item.unified_memory_mb < variant.gpu_unified_memory_mb_min
            for item in devices
        ):
            raise PipelineGpuPreflightError("gpu_memory_mismatch")
    else:
        raise PipelineGpuPreflightError("gpu_memory_mismatch")
    minimum_driver = image_contract.min_nvidia_driver_version
    if minimum_driver is None or any(
        not driver_version_satisfies(item.nvidia_driver_version, minimum_driver)
        for item in devices
    ):
        raise PipelineGpuPreflightError("nvidia_driver_mismatch")
    if image_contract.cuda_userspace_version is None:
        raise PipelineGpuPreflightError("image_contract_mismatch")
    if not set(profile.required_image_features) <= set(image_contract.application_features):
        raise PipelineGpuPreflightError("image_contract_mismatch")
    if not set(profile.required_host_runtime_features) <= set(
        capability.container_runtime_features
    ):
        raise PipelineGpuPreflightError("container_runtime_feature_mismatch")
    memory_limit = variant.container_memory_bytes_override or profile.memory_bytes
    if capability.memory_bytes < memory_limit:
        raise PipelineGpuPreflightError("host_memory_mismatch")
    return GpuContainerPreflightPlan(
        variant_id=variant.variant_id,
        image_index_digest=image_contract.image_index_digest,
        platform_manifest_digest=image_contract.platform_manifest_digest,
        preflight_argv=tuple(image_contract.preflight_argv),
        preflight_digest=image_contract.preflight_digest,
        cuda_userspace_version=image_contract.cuda_userspace_version,
        timeout_seconds=120,
        docker_device_uuids=tuple(allocation.device_uuids),
        sim_gpu_index=variant.device_roles.sim_gpu_index,
        vla_gpu_index=(variant.device_roles.vla_gpu_index if requires_vla else None),
        container_memory_bytes=memory_limit,
        require_concurrent_vla_isaac=(
            requires_vla and variant.variant_id == "gb10-shared-1gpu"
        ),
        requires_vla=requires_vla,
    )


def validate_gpu_container_preflight(
    plan: GpuContainerPreflightPlan,
    observation: GpuContainerPreflightObservation,
) -> None:
    expected_arch = "arm64" if plan.variant_id == "gb10-shared-1gpu" else "x86_64"
    expected_model = (
        "NVIDIA GB10"
        if plan.variant_id == "gb10-shared-1gpu"
        else "NVIDIA GeForce RTX 5080"
    )
    if (
        observation.cpu_arch != expected_arch
        or observation.platform_manifest_digest != plan.platform_manifest_digest
        or observation.cuda_userspace_version != plan.cuda_userspace_version
        or not observation.egl_healthy
        or observation.visible_device_uuids != plan.docker_device_uuids
        or observation.device_models != (expected_model,) * len(plan.docker_device_uuids)
        or not observation.isaac_healthy
        or not observation.omnigibson_healthy
        or (plan.requires_vla and not observation.vla_healthy)
        or (
            plan.require_concurrent_vla_isaac
            and not observation.concurrent_vla_isaac_healthy
        )
    ):
        raise PipelineGpuPreflightError("gpu_preflight_incompatible")


def classify_gpu_preflight_runtime_error(*, transient_runtime_setup: bool) -> str:
    """Only a matching host's transient Docker/NVIDIA/EGL setup may retry."""

    if transient_runtime_setup:
        return "container_start_transient"
    return "gpu_preflight_incompatible"
