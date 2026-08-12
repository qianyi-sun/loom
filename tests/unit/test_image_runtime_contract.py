from __future__ import annotations

import pytest
from pydantic import ValidationError

from loom.pipeline.image_runtime import (
    ImageRuntimeRecord,
    ImageRuntimeRegistry,
    ImageRuntimeRegistryError,
    driver_version_satisfies,
)
from loom.pipeline.keys import canonical_digest
from loom.pipeline.work_protocol import ImageRuntimeContractV1

D = "sha256:" + "a" * 64
IMAGE = "registry.example.com/loom/behavior@sha256:" + "b" * 64


def _contract(platform: str, arch: str, *, marker: str = "a") -> dict[str, object]:
    return {
        "image_index_digest": IMAGE,
        "platform": platform,
        "platform_manifest_digest": "sha256:" + marker * 64,
        "cpu_arch": arch,
        "gpu_vendor": "nvidia",
        "cuda_userspace_version": "13.0",
        "min_nvidia_driver_version": "580.12.0",
        "application_features": ["isaac-sim-5.1", "omnigibson-3.8"],
        "provider_assets": [],
        "preflight_argv": ["/opt/loom/gpu-preflight"],
        "preflight_digest": D,
        "sbom_digest": D,
        "attestation_digest": D,
    }


def test_multi_arch_contract_is_closed_and_platform_bound() -> None:
    amd64 = ImageRuntimeContractV1.model_validate(_contract("linux/amd64", "x86_64"))
    arm64 = ImageRuntimeContractV1.model_validate(
        _contract("linux/arm64", "arm64", marker="b")
    )
    assert amd64.image_index_digest == arm64.image_index_digest
    with pytest.raises(ValidationError, match="platform and CPU"):
        ImageRuntimeContractV1.model_validate(_contract("linux/arm64", "x86_64"))
    with pytest.raises(ValidationError, match="Extra inputs"):
        ImageRuntimeContractV1.model_validate({**_contract("linux/amd64", "x86_64"), "tag": "latest"})


def test_platform_child_digest_cannot_be_substituted_across_architectures() -> None:
    amd64 = ImageRuntimeContractV1.model_validate(_contract("linux/amd64", "x86_64"))
    arm64 = ImageRuntimeContractV1.model_validate(_contract("linux/arm64", "arm64"))
    with pytest.raises(ImageRuntimeRegistryError, match="across platforms"):
        ImageRuntimeRegistry(
            {
                (IMAGE, "linux/amd64"): ImageRuntimeRecord(
                    amd64, canonical_digest(amd64)
                ),
                (IMAGE, "linux/arm64"): ImageRuntimeRecord(
                    arm64, canonical_digest(arm64)
                ),
            }
        )


def test_driver_comparison_is_numeric_not_lexicographic() -> None:
    assert driver_version_satisfies("580.12.0", "58.9")
    assert not driver_version_satisfies("579.99", "580.1")
    with pytest.raises(ValueError):
        driver_version_satisfies("580.beta", "580.1")
