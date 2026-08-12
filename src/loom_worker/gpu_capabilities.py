"""Fail-closed NVIDIA capability discovery inside one exclusive Slurm job."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

from loom.models.worker_capabilities import (
    GpuDeviceCapabilityV1,
    SlurmGpuAllocationEvidenceV1,
    WorkerCapabilitySnapshotV1,
)

_JOB_ID_RE = re.compile(r"^[1-9][0-9]*$")
_INTEGER_RE = re.compile(r"^[0-9]+$")
_RANGE_RE = re.compile(r"^(?P<first>[0-9]+)-(?P<last>[0-9]+)$")
_OLDLAB_NODE_RE = re.compile(r"^(?:TRT-EAI-OLDLAB|trt-eai-oldlab)-[1-5]$")


class GpuCapabilityProbeError(RuntimeError):
    """The local process cannot prove an issue-1213 GPU allocation."""


@dataclass(frozen=True)
class NvidiaSmiRow:
    index: int
    uuid: str
    model: str
    memory_total: int | None
    driver_version: str
    mig_mode: str | None


def parse_slurm_gpu_ids(raw: str) -> tuple[int, ...]:
    """Parse only comma-separated integer IDs and inclusive integer ranges."""

    if not raw or raw != raw.strip():
        raise GpuCapabilityProbeError("SLURM_JOB_GPUS is missing or malformed")
    result: list[int] = []
    for token in raw.split(","):
        if _INTEGER_RE.fullmatch(token):
            result.append(int(token))
            continue
        match = _RANGE_RE.fullmatch(token)
        if match is None:
            raise GpuCapabilityProbeError("SLURM_JOB_GPUS contains a non-integer device")
        first, last = int(match["first"]), int(match["last"])
        if first >= last:
            raise GpuCapabilityProbeError("SLURM_JOB_GPUS range is not increasing")
        result.extend(range(first, last + 1))
    if result != sorted(result) or len(result) != len(set(result)):
        raise GpuCapabilityProbeError("SLURM_JOB_GPUS must be sorted and unique")
    return tuple(result)


def parse_nvidia_smi_csv(value: str) -> tuple[NvidiaSmiRow, ...]:
    """Parse the fixed no-header CSV query used by the worker probe."""

    rows: list[NvidiaSmiRow] = []
    for line in value.splitlines():
        if not line.strip():
            continue
        fields = [field.strip() for field in line.split(",")]
        if len(fields) != 6:
            raise GpuCapabilityProbeError("nvidia-smi row does not have six fields")
        index_text, uuid, model, memory_text, driver, mig_text = fields
        if _INTEGER_RE.fullmatch(index_text) is None:
            raise GpuCapabilityProbeError("nvidia-smi GPU index is not an integer")
        memory: int | None
        if memory_text in {"N/A", "[N/A]"}:
            memory = None
        else:
            normalized_memory = memory_text.removesuffix(" MiB").strip()
            if _INTEGER_RE.fullmatch(normalized_memory) is None:
                raise GpuCapabilityProbeError("nvidia-smi memory.total is invalid")
            memory = int(normalized_memory)
        mig = None if mig_text in {"N/A", "[N/A]"} else mig_text.lower()
        rows.append(
            NvidiaSmiRow(
                index=int(index_text),
                uuid=uuid,
                model=model,
                memory_total=memory,
                driver_version=driver,
                mig_mode=mig,
            )
        )
    indexes = [row.index for row in rows]
    if indexes != sorted(indexes) or len(indexes) != len(set(indexes)):
        raise GpuCapabilityProbeError("nvidia-smi rows must be sorted by unique index")
    return tuple(rows)


def parse_memtotal_mib(meminfo: str) -> int:
    matches = re.findall(r"^MemTotal:\s+([1-9][0-9]*)\s+kB$", meminfo, flags=re.MULTILINE)
    if len(matches) != 1:
        raise GpuCapabilityProbeError("/proc/meminfo has no unique MemTotal kB row")
    return int(matches[0]) // 1024


def validate_oldlab_cpu_allocation(environment: Mapping[str, str]) -> None:
    """Require a zero-GPU OLDLAB Slurm identity for CPU-data workers."""

    if (
        environment.get("LOOM_SLURM_CLUSTER_ID") != "oldlab"
        or _JOB_ID_RE.fullmatch(environment.get("SLURM_JOB_ID", "")) is None
        or _OLDLAB_NODE_RE.fullmatch(environment.get("SLURMD_NODENAME", "")) is None
        or environment.get("SLURM_JOB_PARTITION") != "all"
        or environment.get("SLURM_JOB_GPUS")
        or environment.get("CUDA_VISIBLE_DEVICES")
    ):
        raise GpuCapabilityProbeError(
            "CPU-data workers require a zero-GPU OLDLAB Slurm allocation"
        )


def discover_slurm_gpu_allocation(
    *,
    environment: Mapping[str, str],
    cpu_arch: str,
    nvidia_smi_csv: str,
    meminfo: str,
) -> tuple[tuple[GpuDeviceCapabilityV1, ...], SlurmGpuAllocationEvidenceV1]:
    """Return capabilities only when Slurm, CUDA, hardware, and inventory agree."""

    cluster = environment.get("LOOM_SLURM_CLUSTER_ID", "")
    if cluster not in {"oldlab", "gb10"}:
        raise GpuCapabilityProbeError("LOOM_SLURM_CLUSTER_ID is missing or unsupported")
    job_id = environment.get("SLURM_JOB_ID", "")
    node_name = environment.get("SLURMD_NODENAME", "")
    partition = environment.get("SLURM_JOB_PARTITION", "")
    if _JOB_ID_RE.fullmatch(job_id) is None or not node_name or not partition:
        raise GpuCapabilityProbeError("mandatory Slurm allocation identity is missing")
    allocated_ids = parse_slurm_gpu_ids(environment.get("SLURM_JOB_GPUS", ""))
    rows = parse_nvidia_smi_csv(nvidia_smi_csv)
    if tuple(row.index for row in rows) != allocated_ids:
        raise GpuCapabilityProbeError("visible NVIDIA devices differ from the Slurm allocation")

    expected_count = 2 if cluster == "oldlab" else 1
    if len(rows) != expected_count:
        raise GpuCapabilityProbeError("GPU count does not match the selected Slurm cluster")
    visible = environment.get("CUDA_VISIBLE_DEVICES", "")
    expected_visible = ",".join(str(index) for index in allocated_ids)
    expected_uuids = ",".join(row.uuid for row in rows)
    if visible not in {expected_visible, expected_uuids}:
        raise GpuCapabilityProbeError("CUDA visibility does not match the Slurm allocation")

    allocation_id = f"{cluster}:{job_id}"
    devices: list[GpuDeviceCapabilityV1] = []
    if cluster == "oldlab":
        if cpu_arch != "x86_64" or partition != "all":
            raise GpuCapabilityProbeError("OLDLAB requires x86_64 and partition all")
        for row in rows:
            if (
                row.model != "NVIDIA GeForce RTX 5080"
                or row.memory_total is None
                or row.memory_total < 16_000
                or row.mig_mode != "disabled"
            ):
                raise GpuCapabilityProbeError("OLDLAB RTX 5080 probe mismatch")
            devices.append(
                GpuDeviceCapabilityV1(
                    allocation_id=allocation_id,
                    device_uuid=row.uuid,
                    vendor="nvidia",
                    model="NVIDIA GeForce RTX 5080",
                    memory_kind="dedicated",
                    memory_mb=row.memory_total,
                    unified_memory_mb=None,
                    nvidia_driver_version=row.driver_version,
                    mig_mode="disabled",
                )
            )
        gpu_tres = "gpu:rtx5080:2"
        variant_id = "oldlab-rtx5080-2gpu"
    else:
        if cpu_arch != "arm64" or partition != "gb10":
            raise GpuCapabilityProbeError("GB10 requires arm64 and partition gb10")
        row = rows[0]
        unified_mib = parse_memtotal_mib(meminfo)
        if (
            row.model != "NVIDIA GB10"
            or row.memory_total is not None
            or row.mig_mode is not None
            or unified_mib < 120_000
        ):
            raise GpuCapabilityProbeError("GB10 unified-memory probe mismatch")
        devices.append(
            GpuDeviceCapabilityV1(
                allocation_id=allocation_id,
                device_uuid=row.uuid,
                vendor="nvidia",
                model="NVIDIA GB10",
                memory_kind="unified",
                memory_mb=None,
                unified_memory_mb=unified_mib,
                nvidia_driver_version=row.driver_version,
                mig_mode="not_supported",
            )
        )
        gpu_tres = "gpu:gb10:1"
        variant_id = "gb10-shared-1gpu"

    devices.sort(key=lambda item: (item.allocation_id.encode(), item.device_uuid.encode()))
    uuids = sorted((item.device_uuid for item in devices), key=lambda item: item.encode())
    evidence = SlurmGpuAllocationEvidenceV1.model_validate(
        {
            "allocation_id": allocation_id,
            "slurm_cluster_id": cluster,
            "job_id": job_id,
            "node_name": node_name,
            "partition": partition,
            "gpu_tres": gpu_tres,
            "allocated_device_ids": list(allocated_ids),
            "device_uuids": uuids,
            "variant_id": variant_id,
        }
    )
    return tuple(devices), evidence


def build_worker_capability_snapshot(
    *,
    cpu_arch: str,
    cpu_cores: int,
    memory_bytes: int,
    scratch_bytes: int,
    network_profiles: list[str],
    container_runtime_features: list[str],
    input_cache_capacity_bytes: int,
    input_cache_reserved_bytes: int,
    input_cache_ready_bytes: int,
    gpu_devices: tuple[GpuDeviceCapabilityV1, ...] = (),
) -> WorkerCapabilitySnapshotV1:
    return WorkerCapabilitySnapshotV1.model_validate(
        {
            "schema_version": "loom.worker-capabilities.v1",
            "cpu_arch": cpu_arch,
            "cpu_cores": cpu_cores,
            "memory_bytes": memory_bytes,
            "scratch_bytes": scratch_bytes,
            "network_profiles": sorted(set(network_profiles), key=str.encode),
            "container_runtime_features": sorted(
                set(container_runtime_features), key=str.encode
            ),
            "gpu_devices": [item.model_dump(mode="json") for item in gpu_devices],
            "input_cache_capacity_bytes": input_cache_capacity_bytes,
            "input_cache_reserved_bytes": input_cache_reserved_bytes,
            "input_cache_ready_bytes": input_cache_ready_bytes,
        }
    )
