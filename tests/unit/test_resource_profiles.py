from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from loom.pipeline.resource_profiles import (
    INPUT_CACHE_ALLOCATABLE_BYTES_MIN,
    INPUT_CACHE_RAW_BYTES_MIN,
    ResourceProfileRegistryError,
    load_resource_profiles,
    reject_user_resource_overrides,
)
from loom.pipeline.work_protocol import ResourceProfileV1


def test_checked_in_profiles_are_exact_and_have_no_unsuffixed_alias() -> None:
    registry = load_resource_profiles()
    identities = [record.identity for record in registry.list()]
    assert identities == [
        "behavior-export-io@1",
        "behavior-offline-gateway@1",
        "behavior-offline-none@1",
        "behavior-sim-local-gateway@1",
        "behavior-sim-local-none@1",
        "pipeline-test-cpu-gateway@1",
        "pipeline-test-cpu-none@1",
        "terminalgen-generate-gateway@1",
        "terminalgen-package-none@1",
        "terminalgen-plan-none@1",
        "terminalgen-validate-none@1",
    ]
    assert INPUT_CACHE_ALLOCATABLE_BYTES_MIN == 1_649_267_441_664
    assert INPUT_CACHE_RAW_BYTES_MIN == 1_940_314_637_252
    assert all(
        record.profile.input_cache_capacity_bytes_min == INPUT_CACHE_ALLOCATABLE_BYTES_MIN
        for record in registry.list()
        if record.identity.startswith("behavior-")
    )
    assert all(
        "pids_limit" not in record.profile.model_dump(mode="json")
        for record in registry.list()
        if not record.identity.startswith("terminalgen-")
    )


def test_gpu_variants_encode_split_and_shared_device_roles_exactly() -> None:
    profile = load_resource_profiles().get("behavior-sim-local-none@1").profile
    gb10, oldlab = profile.execution_variants
    assert gb10.variant_id == "gb10-shared-1gpu"
    assert (gb10.device_roles.sim_gpu_index, gb10.device_roles.vla_gpu_index) == (0, 0)  # type: ignore[union-attr]
    assert gb10.memory_accounting_kind == "unified_shared"
    assert gb10.container_memory_bytes_override == 125_829_120_000
    assert (oldlab.device_roles.sim_gpu_index, oldlab.device_roles.vla_gpu_index) == (0, 1)  # type: ignore[union-attr]
    assert oldlab.memory_accounting_kind == "separate"


def test_pipeline_test_profiles_have_an_unprovisioned_non_behavior_pool() -> None:
    registry = load_resource_profiles()
    for identity in (
        "pipeline-test-cpu-gateway@1",
        "pipeline-test-cpu-none@1",
    ):
        profile = registry.get(identity).profile
        assert profile.required_image_features == ["pipeline-test-cpu"]
        assert len(profile.execution_variants) == 1
        variant = profile.execution_variants[0]
        assert variant.variant_id == "pipeline-test-cpu-x86_64"
        assert variant.pool_class == "pipeline-test-cpu"
        assert variant.gpu_count_exact == 0
        assert profile.input_cache_capacity_bytes_min == 0


def test_terminalgen_profiles_are_closed_cpu_pools_with_profile_owned_pid_limits() -> None:
    registry = load_resource_profiles()
    expected = {
        "terminalgen-generate-gateway@1": (
            "terminalgen-generate-gateway",
            "gateway",
            "terminalgen-generator",
            1_024,
        ),
        "terminalgen-package-none@1": (
            "terminalgen-package-none",
            "none",
            "terminalgen-packager",
            512,
        ),
        "terminalgen-plan-none@1": (
            "terminalgen-plan-none",
            "none",
            "terminalgen-planner",
            256,
        ),
        "terminalgen-validate-none@1": (
            "terminalgen-validate-none",
            "none",
            "terminalgen-validator",
            2_048,
        ),
    }
    for identity, (pool, network, image_feature, pids) in expected.items():
        profile = registry.get(identity).profile
        assert profile.network_profile == network
        assert profile.required_image_features == [image_feature]
        assert profile.pids_limit == pids
        assert len(profile.execution_variants) == 1
        variant = profile.execution_variants[0]
        assert variant.variant_id == "terminalgen-cpu-x86_64"
        assert variant.pool_class == pool
        assert variant.gpu_count_exact == 0


def test_profiles_reject_network_and_memory_accounting_overrides() -> None:
    source = load_resource_profiles().get("behavior-sim-local-gateway@1").profile.model_dump()
    drift = deepcopy(source)
    drift["required_host_runtime_features"].remove("loom-secret-tmpfs-v1")
    with pytest.raises(ValidationError, match="loom-secret-tmpfs-v1"):
        ResourceProfileV1.model_validate(drift)
    drift = deepcopy(source)
    drift["execution_variants"][0]["memory_accounting_kind"] = "separate"
    with pytest.raises(ValidationError, match="unified GPU memory"):
        ResourceProfileV1.model_validate(drift)


def test_user_parameters_cannot_select_variant_pool_device_or_network() -> None:
    reject_user_resource_overrides({"task": {"seed": 7}})
    with pytest.raises(ResourceProfileRegistryError, match="execution_variant_id"):
        reject_user_resource_overrides(
            {"task": {"execution_variant_id": "gb10-shared-1gpu"}}
        )
    with pytest.raises(ResourceProfileRegistryError, match="network_profile"):
        reject_user_resource_overrides({"network_profile": "gateway"})
    with pytest.raises(ResourceProfileRegistryError, match="pids_limit"):
        reject_user_resource_overrides({"pids_limit": 65_536})
