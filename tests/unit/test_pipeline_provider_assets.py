from __future__ import annotations

import pytest
from pydantic import ValidationError

from loom.pipeline.image_runtime import validate_provider_asset_payloads
from loom.pipeline.work_protocol import ImageRuntimeContractV1

D = "sha256:" + "a" * 64
IMAGE = "registry.example.com/loom/behavior@sha256:" + "b" * 64


def _contract() -> ImageRuntimeContractV1:
    return ImageRuntimeContractV1.model_validate(
        {
            "image_index_digest": IMAGE,
            "platform": "linux/amd64",
            "platform_manifest_digest": D,
            "cpu_arch": "x86_64",
            "gpu_vendor": "none",
            "cuda_userspace_version": None,
            "min_nvidia_driver_version": None,
            "application_features": ["behavior-cpu-data"],
            "provider_assets": [
                {
                    "logical_name": "behavior_offline_judge",
                    "role": "judge",
                    "image_path": "/opt/behavior/provider-assets/behavior_offline_judge/manifest.json",
                    "sha256": D,
                }
            ],
            "preflight_argv": ["/opt/loom/preflight"],
            "preflight_digest": D,
            "sbom_digest": D,
            "attestation_digest": D,
        }
    )


def test_provider_assets_are_exact_image_files_with_no_runtime_fetch() -> None:
    contract = _contract()
    validate_provider_asset_payloads(
        contract,
        observed_sha256_by_path={contract.provider_assets[0].image_path: D},
    )
    with pytest.raises(ValueError, match="provider_asset_manifest_mismatch"):
        validate_provider_asset_payloads(contract, observed_sha256_by_path={})


def test_provider_asset_path_must_match_its_logical_name() -> None:
    value = _contract().model_dump()
    value["provider_assets"][0]["image_path"] = "/opt/behavior/provider-assets/other/file"
    with pytest.raises(ValidationError, match="logical-name"):
        ImageRuntimeContractV1.model_validate(value)
