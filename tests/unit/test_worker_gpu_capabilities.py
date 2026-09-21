from __future__ import annotations

import pytest
from pydantic import ValidationError

from loom.models.worker_capabilities import WorkerCapabilitySnapshotV1


@pytest.mark.parametrize("cpu_arch", ["x86_64", "arm64"])
def test_local_gpu_snapshot_has_no_cluster_or_model_restriction(cpu_arch: str) -> None:
    from loom.models.worker_capabilities import GpuDeviceCapabilityV1

    device = GpuDeviceCapabilityV1(
        allocation_id="local:device-0", device_uuid="GPU-LOCAL", vendor="nvidia",
        model="NVIDIA local GPU", memory_kind="dedicated", memory_mb=8192,
        unified_memory_mb=None, nvidia_driver_version="580.12.0", mig_mode="disabled",
    )
    snapshot = WorkerCapabilitySnapshotV1(
        schema_version="loom.worker-capabilities.v1", cpu_arch=cpu_arch,
        cpu_cores=2, memory_bytes=16384, scratch_bytes=16384,
        network_profiles=["none"], container_runtime_features=[], gpu_devices=[device],
        input_cache_capacity_bytes=0, input_cache_reserved_bytes=0, input_cache_ready_bytes=0,
    )
    assert snapshot.gpu_devices == [device]
    with pytest.raises(ValidationError, match="dedicated GPU memory"):
        GpuDeviceCapabilityV1.model_validate({**device.model_dump(), "memory_mb": 0})
    with pytest.raises(ValidationError, match="GPU devices must be unique"):
        WorkerCapabilitySnapshotV1.model_validate({**snapshot.model_dump(), "gpu_devices": [device, device]})
