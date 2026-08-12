from uuid import UUID

import pytest
from pydantic import ValidationError

from loom.pipeline.work_protocol import AcceptancePreflightGrantV1

D0 = "sha256:" + "0" * 64


def _grant() -> dict[str, object]:
    return {
        "authorization_id": UUID(int=20),
        "authorization_snapshot_sha256": D0,
        "action": "matrix",
        "candidate_sha256": D0,
        "preflight_input_set_id": "S02",
        "prerequisite_pipeline_run_id": UUID(int=1),
        "exclusive_fence_id": UUID(int=21),
        "node_key": "oldlab-rtx5080-2gpu_acceptance_preflight_cold",
        "backend_variant_id": "oldlab-rtx5080-2gpu",
        "cache_expectation": "cold_after_eviction",
        "sealed_input_descriptor_set_sha256": D0,
        "policy_id": "behavior-gpu-oldlab",
        "policy_config_sha256": D0,
        "policy_activation_epoch": 1,
        "slurm_cluster_id": "oldlab",
        "slurm_cluster_config_sha256": D0,
        "slurm_allocation_id": "oldlab:acceptance",
        "image_runtime_contract_digest": D0,
        "resource_profile_digest": D0,
        "network_profile": "none",
        "renderer_digest": D0,
    }


def test_preflight_grant_binds_variant_policy_and_cache_phase() -> None:
    assert AcceptancePreflightGrantV1.model_validate(_grant()).network_profile == "none"
    with pytest.raises(ValidationError, match="variant/policy/cluster drift"):
        AcceptancePreflightGrantV1.model_validate({**_grant(), "policy_id": "behavior-gpu-gb10"})
