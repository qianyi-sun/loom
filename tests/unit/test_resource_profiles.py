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
    ]
    assert INPUT_CACHE_ALLOCATABLE_BYTES_MIN == 1_649_267_441_664
    assert INPUT_CACHE_RAW_BYTES_MIN == 1_940_314_637_252
    assert all(record.profile.input_cache_capacity_bytes_min == INPUT_CACHE_ALLOCATABLE_BYTES_MIN for record in registry.list())


def test_gpu_variants_encode_split_and_shared_device_roles_exactly() -> None:
    profile = load_resource_profiles().get("behavior-sim-local-none@1").profile
    gb10, oldlab = profile.execution_variants
    assert gb10.variant_id == "gb10-shared-1gpu"
    assert (gb10.device_roles.sim_gpu_index, gb10.device_roles.vla_gpu_index) == (0, 0)  # type: ignore[union-attr]
    assert gb10.memory_accounting_kind == "unified_shared"
    assert gb10.container_memory_bytes_override == 125_829_120_000
    assert (oldlab.device_roles.sim_gpu_index, oldlab.device_roles.vla_gpu_index) == (0, 1)  # type: ignore[union-attr]
    assert oldlab.memory_accounting_kind == "separate"


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
