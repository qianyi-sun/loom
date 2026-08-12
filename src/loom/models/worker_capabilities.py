"""Closed worker GPU capability and Slurm allocation contracts for Pipeline v1."""

from __future__ import annotations

import re
from typing import Annotated, Literal

from pydantic import StringConstraints, field_validator, model_validator

from loom.pipeline.keys import canonical_digest
from loom.pipeline.spec import NonNegativeSafeInt, PipelineModel, PositiveSafeInt

_ALLOCATION_RE = re.compile(r"^(oldlab|gb10):([1-9][0-9]*)$")
_DRIVER_RE = re.compile(r"^[0-9]+(?:\.[0-9]+)+$")

CapabilityText = Annotated[str, StringConstraints(min_length=1, max_length=128)]
DeviceUuid = Annotated[
    str,
    StringConstraints(pattern=r"^GPU-[A-Za-z0-9][A-Za-z0-9_-]{0,122}$"),
]


def _bytewise_unique(values: list[str], label: str) -> list[str]:
    if values != sorted(values, key=lambda value: value.encode("utf-8")):
        raise ValueError(f"{label} must be bytewise sorted")
    if len(values) != len(set(values)):
        raise ValueError(f"{label} must be unique")
    return values


class GpuDeviceCapabilityV1(PipelineModel):
    """One NVIDIA device proven to belong to the worker's Slurm allocation."""

    allocation_id: CapabilityText
    device_uuid: DeviceUuid
    vendor: Literal["nvidia"]
    model: Literal["NVIDIA GeForce RTX 5080", "NVIDIA GB10"]
    memory_kind: Literal["dedicated", "unified"]
    memory_mb: NonNegativeSafeInt | None
    unified_memory_mb: NonNegativeSafeInt | None
    nvidia_driver_version: CapabilityText
    mig_mode: Literal["disabled", "not_supported"]

    @field_validator("allocation_id")
    @classmethod
    def allocation_is_cluster_qualified(cls, value: str) -> str:
        if _ALLOCATION_RE.fullmatch(value) is None:
            raise ValueError("allocation_id must be <oldlab|gb10>:<jobid>")
        return value

    @field_validator("nvidia_driver_version")
    @classmethod
    def driver_is_dotted_integer(cls, value: str) -> str:
        if _DRIVER_RE.fullmatch(value) is None:
            raise ValueError("NVIDIA driver version must contain dotted integers only")
        return value

    @model_validator(mode="after")
    def device_shape_is_exact(self) -> GpuDeviceCapabilityV1:
        cluster = self.allocation_id.split(":", 1)[0]
        if self.model == "NVIDIA GeForce RTX 5080":
            if (
                cluster != "oldlab"
                or self.memory_kind != "dedicated"
                or self.memory_mb is None
                or self.memory_mb < 16_000
                or self.unified_memory_mb is not None
                or self.mig_mode != "disabled"
            ):
                raise ValueError("RTX 5080 capability does not match the OLDLAB contract")
        elif (
            cluster != "gb10"
            or self.memory_kind != "unified"
            or self.memory_mb is not None
            or self.unified_memory_mb is None
            or self.unified_memory_mb < 120_000
            or self.mig_mode != "not_supported"
        ):
            raise ValueError("GB10 capability does not match the unified-memory contract")
        return self


class WorkerCapabilitySnapshotV1(PipelineModel):
    """Canonical worker identity used by registration, claims, and acceptance fences."""

    schema_version: Literal["loom.worker-capabilities.v1"]
    cpu_arch: Literal["x86_64", "arm64"]
    cpu_cores: PositiveSafeInt
    memory_bytes: PositiveSafeInt
    scratch_bytes: PositiveSafeInt
    network_profiles: list[Literal["gateway", "none"]]
    container_runtime_features: list[CapabilityText]
    gpu_devices: list[GpuDeviceCapabilityV1]
    input_cache_capacity_bytes: NonNegativeSafeInt
    input_cache_reserved_bytes: NonNegativeSafeInt
    input_cache_ready_bytes: NonNegativeSafeInt

    @field_validator("network_profiles")
    @classmethod
    def networks_are_canonical(cls, values: list[str]) -> list[str]:
        if not values:
            raise ValueError("at least one network profile is required")
        return _bytewise_unique(values, "network profiles")

    @field_validator("container_runtime_features")
    @classmethod
    def runtime_features_are_canonical(cls, values: list[str]) -> list[str]:
        return _bytewise_unique(values, "container runtime features")

    @field_validator("gpu_devices")
    @classmethod
    def devices_are_canonical(
        cls, values: list[GpuDeviceCapabilityV1]
    ) -> list[GpuDeviceCapabilityV1]:
        identities = [(item.allocation_id, item.device_uuid) for item in values]
        if identities != sorted(
            identities,
            key=lambda item: (item[0].encode("utf-8"), item[1].encode("utf-8")),
        ):
            raise ValueError("GPU devices must be sorted by allocation ID then UUID")
        if len(identities) != len(set(identities)):
            raise ValueError("GPU devices must be unique")
        uuids = [item.device_uuid for item in values]
        if len(uuids) != len(set(uuids)):
            raise ValueError("GPU UUIDs must be unique")
        allocation_ids = {item.allocation_id for item in values}
        if len(allocation_ids) > 1:
            raise ValueError("one worker may advertise only its one active Slurm allocation")
        return values

    @model_validator(mode="after")
    def accounting_and_allocation_are_coherent(self) -> WorkerCapabilitySnapshotV1:
        if not (
            0 <= self.input_cache_reserved_bytes <= self.input_cache_capacity_bytes
            and 0 <= self.input_cache_ready_bytes <= self.input_cache_capacity_bytes
        ):
            raise ValueError("input cache accounting exceeds capacity")
        if self.gpu_devices:
            cluster = self.gpu_devices[0].allocation_id.split(":", 1)[0]
            expected_arch = "x86_64" if cluster == "oldlab" else "arm64"
            if self.cpu_arch != expected_arch:
                raise ValueError("GPU allocation cluster and CPU architecture drift")
            if len(self.gpu_devices) != (2 if cluster == "oldlab" else 1):
                raise ValueError("GPU device count does not match its Slurm cluster")
            models = {device.model for device in self.gpu_devices}
            if len(models) != 1:
                raise ValueError("mixed GPU models are forbidden")
        return self

    @property
    def digest(self) -> str:
        return canonical_digest(self)


class SlurmGpuAllocationEvidenceV1(PipelineModel):
    """Immutable join between one Slurm job, its node, and visible devices."""

    allocation_id: CapabilityText
    slurm_cluster_id: Literal["oldlab", "gb10"]
    job_id: Annotated[str, StringConstraints(pattern=r"^[1-9][0-9]*$")]
    node_name: CapabilityText
    partition: Literal["all", "gb10"]
    gpu_tres: Literal["gpu:rtx5080:2", "gpu:gb10:1"]
    allocated_device_ids: list[NonNegativeSafeInt]
    device_uuids: list[DeviceUuid]
    variant_id: Literal["oldlab-rtx5080-2gpu", "gb10-shared-1gpu"]

    @field_validator("allocated_device_ids")
    @classmethod
    def device_ids_are_canonical(cls, values: list[int]) -> list[int]:
        if values != sorted(values) or len(values) != len(set(values)):
            raise ValueError("allocated device IDs must be sorted and unique")
        return values

    @field_validator("device_uuids")
    @classmethod
    def device_uuids_are_canonical(cls, values: list[str]) -> list[str]:
        return _bytewise_unique(values, "device UUIDs")

    @model_validator(mode="after")
    def allocation_join_is_exact(self) -> SlurmGpuAllocationEvidenceV1:
        if self.allocation_id != f"{self.slurm_cluster_id}:{self.job_id}":
            raise ValueError("allocation_id does not join the Slurm cluster and job")
        expected = {
            "oldlab": (
                "all",
                "gpu:rtx5080:2",
                "oldlab-rtx5080-2gpu",
                2,
                re.compile(r"^(?:TRT-EAI-OLDLAB|trt-eai-oldlab)-[1-5]$"),
            ),
            "gb10": (
                "gb10",
                "gpu:gb10:1",
                "gb10-shared-1gpu",
                1,
                re.compile(r"^trt-gb10-(?:[1-9]|1[0-5])$"),
            ),
        }[self.slurm_cluster_id]
        partition, gpu_tres, variant, count, node_pattern = expected
        if (
            self.partition != partition
            or self.gpu_tres != gpu_tres
            or self.variant_id != variant
            or len(self.allocated_device_ids) != count
            or len(self.device_uuids) != count
            or node_pattern.fullmatch(self.node_name) is None
        ):
            raise ValueError("Slurm GPU allocation evidence does not match its cluster")
        return self

    @property
    def digest(self) -> str:
        return canonical_digest(self)
