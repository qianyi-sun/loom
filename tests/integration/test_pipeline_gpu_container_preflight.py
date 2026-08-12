from __future__ import annotations

import pytest

from loom.pipeline.resource_profiles import load_resource_profiles
from loom.pipeline.work_protocol import ImageRuntimeContractV1
from loom_worker.gpu_capabilities import (
    build_worker_capability_snapshot,
    discover_slurm_gpu_allocation,
)
from loom_worker.pipeline_gpu_preflight import (
    GpuContainerPreflightObservation,
    PipelineGpuPreflightError,
    build_gpu_container_preflight_plan,
    validate_gpu_container_preflight,
)

DIGEST = "sha256:" + "a" * 64
IMAGE = "registry.example.com/loom/behavior-sim@sha256:" + "b" * 64


def _image_contract(*, arch: str) -> ImageRuntimeContractV1:
    platform = "linux/arm64" if arch == "arm64" else "linux/amd64"
    return ImageRuntimeContractV1.model_validate(
        {
            "image_index_digest": IMAGE,
            "platform": platform,
            "platform_manifest_digest": DIGEST,
            "cpu_arch": arch,
            "gpu_vendor": "nvidia",
            "cuda_userspace_version": "13.0",
            "min_nvidia_driver_version": "580.1",
            "application_features": ["isaac-sim-5.1", "omnigibson-3.8"],
            "provider_assets": [],
            "preflight_argv": ["/opt/loom/gpu-preflight"],
            "preflight_digest": DIGEST,
            "sbom_digest": DIGEST,
            "attestation_digest": DIGEST,
        }
    )


def _allocation(cluster: str):  # type: ignore[no-untyped-def]
    if cluster == "gb10":
        environment = {
            "LOOM_SLURM_CLUSTER_ID": "gb10",
            "SLURM_JOB_ID": "81",
            "SLURMD_NODENAME": "trt-gb10-8",
            "SLURM_JOB_PARTITION": "gb10",
            "SLURM_JOB_GPUS": "0",
            "CUDA_VISIBLE_DEVICES": "GPU-GB10",
        }
        devices, evidence = discover_slurm_gpu_allocation(
            environment=environment,
            cpu_arch="arm64",
            nvidia_smi_csv="0, GPU-GB10, NVIDIA GB10, N/A, 580.12.0, N/A\n",
            meminfo="MemTotal:       126976000 kB\n",
        )
        arch = "arm64"
    else:
        environment = {
            "LOOM_SLURM_CLUSTER_ID": "oldlab",
            "SLURM_JOB_ID": "81",
            "SLURMD_NODENAME": "trt-eai-oldlab-2",
            "SLURM_JOB_PARTITION": "all",
            "SLURM_JOB_GPUS": "0-1",
            "CUDA_VISIBLE_DEVICES": "0,1",
        }
        devices, evidence = discover_slurm_gpu_allocation(
            environment=environment,
            cpu_arch="x86_64",
            nvidia_smi_csv=(
                "0, GPU-AAAA, NVIDIA GeForce RTX 5080, 16384, 580.12.0, Disabled\n"
                "1, GPU-BBBB, NVIDIA GeForce RTX 5080, 16384, 580.12.0, Disabled\n"
            ),
            meminfo="MemTotal:       67108864 kB\n",
        )
        arch = "x86_64"
    capability = build_worker_capability_snapshot(
        cpu_arch=arch,
        cpu_cores=20,
        memory_bytes=128 << 30,
        scratch_bytes=200 << 30,
        network_profiles=["gateway", "none"],
        container_runtime_features=[
            "egl",
            "loom-secret-tmpfs-v1",
            "nvidia-container-runtime",
        ],
        input_cache_capacity_bytes=1_649_267_441_664,
        input_cache_reserved_bytes=0,
        input_cache_ready_bytes=0,
        gpu_devices=devices,
    )
    return capability, evidence


@pytest.mark.parametrize(
    ("cluster", "variant", "arch", "uuids", "memory_limit"),
    [
        ("gb10", "gb10-shared-1gpu", "arm64", ("GPU-GB10",), 125_829_120_000),
        (
            "oldlab",
            "oldlab-rtx5080-2gpu",
            "x86_64",
            ("GPU-AAAA", "GPU-BBBB"),
            64 << 30,
        ),
    ],
)
def test_attested_gpu_preflight_binds_exact_platform_and_uuid_set(
    cluster: str,
    variant: str,
    arch: str,
    uuids: tuple[str, ...],
    memory_limit: int,
) -> None:
    profile = load_resource_profiles().get("behavior-sim-local-none@1").profile
    capability, evidence = _allocation(cluster)
    plan = build_gpu_container_preflight_plan(
        profile=profile,
        variant_id=variant,
        image_contract=_image_contract(arch=arch),
        capability=capability,
        allocation=evidence,
    )
    assert plan.timeout_seconds == 120
    assert plan.docker_device_uuids == uuids
    assert plan.container_memory_bytes == memory_limit
    observation = GpuContainerPreflightObservation(
        cpu_arch=arch,
        platform_manifest_digest=DIGEST,
        cuda_userspace_version="13.0",
        egl_healthy=True,
        visible_device_uuids=uuids,
        device_models=(
            ("NVIDIA GB10",)
            if cluster == "gb10"
            else ("NVIDIA GeForce RTX 5080",) * 2
        ),
        isaac_healthy=True,
        omnigibson_healthy=True,
        vla_healthy=True,
        concurrent_vla_isaac_healthy=True,
    )
    validate_gpu_container_preflight(plan, observation)
    with pytest.raises(PipelineGpuPreflightError, match="gpu_preflight_incompatible"):
        validate_gpu_container_preflight(
            plan,
            GpuContainerPreflightObservation(
                **{
                    **observation.__dict__,
                    "visible_device_uuids": (*uuids, "GPU-EXTRA"),
                }
            ),
        )


def test_gb10_requires_concurrent_vla_but_primitive_starts_no_vla() -> None:
    profile = load_resource_profiles().get("behavior-sim-local-none@1").profile
    capability, evidence = _allocation("gb10")
    kwargs = {
        "profile": profile,
        "variant_id": "gb10-shared-1gpu",
        "image_contract": _image_contract(arch="arm64"),
        "capability": capability,
        "allocation": evidence,
    }
    full_plan = build_gpu_container_preflight_plan(**kwargs)
    primitive_plan = build_gpu_container_preflight_plan(**kwargs, requires_vla=False)
    observation = GpuContainerPreflightObservation(
        cpu_arch="arm64",
        platform_manifest_digest=DIGEST,
        cuda_userspace_version="13.0",
        egl_healthy=True,
        visible_device_uuids=("GPU-GB10",),
        device_models=("NVIDIA GB10",),
        isaac_healthy=True,
        omnigibson_healthy=True,
        vla_healthy=False,
        concurrent_vla_isaac_healthy=False,
    )
    with pytest.raises(PipelineGpuPreflightError, match="gpu_preflight_incompatible"):
        validate_gpu_container_preflight(full_plan, observation)
    validate_gpu_container_preflight(primitive_plan, observation)
    assert primitive_plan.vla_gpu_index is None
