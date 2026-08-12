from __future__ import annotations

import pytest
from pydantic import ValidationError

from loom.models.worker_capabilities import WorkerCapabilitySnapshotV1
from loom_worker.gpu_capabilities import (
    GpuCapabilityProbeError,
    discover_slurm_gpu_allocation,
    parse_slurm_gpu_ids,
    validate_oldlab_cpu_allocation,
)


def _oldlab_env() -> dict[str, str]:
    return {
        "LOOM_SLURM_CLUSTER_ID": "oldlab",
        "SLURM_JOB_ID": "401",
        "SLURMD_NODENAME": "trt-eai-oldlab-3",
        "SLURM_JOB_PARTITION": "all",
        "SLURM_JOB_GPUS": "0-1",
        "CUDA_VISIBLE_DEVICES": "0,1",
    }


def _oldlab_smi() -> str:
    return "\n".join(
        (
            "0, GPU-AAAA, NVIDIA GeForce RTX 5080, 16384, 580.12.0, Disabled",
            "1, GPU-BBBB, NVIDIA GeForce RTX 5080, 16384, 580.12.0, Disabled",
        )
    )


def test_oldlab_probe_requires_exact_two_allocated_rtx5080_devices() -> None:
    devices, evidence = discover_slurm_gpu_allocation(
        environment=_oldlab_env(),
        cpu_arch="x86_64",
        nvidia_smi_csv=_oldlab_smi(),
        meminfo="MemTotal:       65536000 kB\n",
    )
    assert [device.device_uuid for device in devices] == ["GPU-AAAA", "GPU-BBBB"]
    assert evidence.allocation_id == "oldlab:401"
    assert evidence.gpu_tres == "gpu:rtx5080:2"

    with pytest.raises(GpuCapabilityProbeError, match="GPU count"):
        discover_slurm_gpu_allocation(
            environment={**_oldlab_env(), "SLURM_JOB_GPUS": "0", "CUDA_VISIBLE_DEVICES": "0"},
            cpu_arch="x86_64",
            nvidia_smi_csv=_oldlab_smi().splitlines()[0],
            meminfo="MemTotal:       65536000 kB\n",
        )


def test_gb10_probe_uses_meminfo_and_rejects_nvidia_memory_value() -> None:
    env = {
        "LOOM_SLURM_CLUSTER_ID": "gb10",
        "SLURM_JOB_ID": "401",
        "SLURMD_NODENAME": "trt-gb10-7",
        "SLURM_JOB_PARTITION": "gb10",
        "SLURM_JOB_GPUS": "0",
        "CUDA_VISIBLE_DEVICES": "GPU-GB10",
    }
    devices, evidence = discover_slurm_gpu_allocation(
        environment=env,
        cpu_arch="arm64",
        nvidia_smi_csv="0, GPU-GB10, NVIDIA GB10, N/A, 580.12.0, N/A\n",
        meminfo="MemTotal:       126976000 kB\n",
    )
    assert devices[0].unified_memory_mb == 124000
    assert evidence.allocation_id == "gb10:401"
    with pytest.raises(GpuCapabilityProbeError, match="unified-memory"):
        discover_slurm_gpu_allocation(
            environment=env,
            cpu_arch="arm64",
            nvidia_smi_csv="0, GPU-GB10, NVIDIA GB10, 120000, 580.12.0, N/A\n",
            meminfo="MemTotal:       126976000 kB\n",
        )


def test_snapshot_is_closed_sorted_and_allocation_coherent() -> None:
    devices, _ = discover_slurm_gpu_allocation(
        environment=_oldlab_env(),
        cpu_arch="x86_64",
        nvidia_smi_csv=_oldlab_smi(),
        meminfo="MemTotal:       65536000 kB\n",
    )
    snapshot = WorkerCapabilitySnapshotV1(
        schema_version="loom.worker-capabilities.v1",
        cpu_arch="x86_64",
        cpu_cores=16,
        memory_bytes=64 << 30,
        scratch_bytes=150 << 30,
        network_profiles=["gateway", "none"],
        container_runtime_features=["egl", "loom-secret-tmpfs-v1", "nvidia-container-runtime"],
        gpu_devices=list(devices),
        input_cache_capacity_bytes=1_649_267_441_664,
        input_cache_reserved_bytes=0,
        input_cache_ready_bytes=0,
    )
    assert snapshot.digest.startswith("sha256:")
    with pytest.raises(ValidationError, match="Extra inputs"):
        WorkerCapabilitySnapshotV1.model_validate({**snapshot.model_dump(), "gpu_vendor": "nvidia"})
    with pytest.raises(GpuCapabilityProbeError):
        parse_slurm_gpu_ids("gpu:0")


def test_cpu_data_allocation_is_oldlab_slurm_only_and_has_no_gpu_visibility() -> None:
    environment = {
        "LOOM_SLURM_CLUSTER_ID": "oldlab",
        "SLURM_JOB_ID": "88",
        "SLURMD_NODENAME": "trt-eai-oldlab-4",
        "SLURM_JOB_PARTITION": "all",
    }
    validate_oldlab_cpu_allocation(environment)
    with pytest.raises(GpuCapabilityProbeError, match="zero-GPU"):
        validate_oldlab_cpu_allocation(
            {**environment, "CUDA_VISIBLE_DEVICES": "0"}
        )
    with pytest.raises(GpuCapabilityProbeError, match="zero-GPU"):
        validate_oldlab_cpu_allocation(
            {**environment, "SLURMD_NODENAME": "trt-eai-oldlab-6"}
        )
