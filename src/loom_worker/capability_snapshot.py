"""Canonical resource snapshots for local and disposable workers."""

from loom.models.worker_capabilities import GpuDeviceCapabilityV1, WorkerCapabilitySnapshotV1


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
