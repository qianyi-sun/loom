from copy import deepcopy

import pytest

from loom_control_plane.execution_placement import (
    PlacementUnavailableError,
    cold_sample,
    plan_placement,
    quota_identity,
)
from loom_execution_capacity_collector.contracts import CapacityPlacement, ResourceTotals
from tests.execution_placement_fixtures import placement_fixture


def _resources(cpu: int = 2_000, memory: int = 1_024, storage: int = 1_024):
    return ResourceTotals(cpu_millis=cpu, memory_mib=memory, storage_mib=storage)


def test_fragmented_nodes_cannot_combine_cpu_and_memory_holes():
    data = placement_fixture(target_id="a", nodes=2, node_cpu=4_000, node_memory=4_096)
    data["nodes"][0]["requested"].update(cpu_millis=3_500)
    data["nodes"][1]["requested"].update(memory_mib=3_500)
    placement = CapacityPlacement.model_validate(data)
    result = plan_placement(
        placement, [("new:1", _resources(memory=2_048))], sample=cold_sample(placement)
    )
    assert result.additional_nodes == 1


def test_actual_allocatable_daemonsets_and_slots_bound_cold_packing():
    data = placement_fixture(
        target_id="a",
        nodes=0,
        used_nodes=0,
        node_cpu=15_900,
        node_memory=63_688,
        node_storage=69_431,
        raw_storage=81_920,
    )
    sample = data["template_samples"][0]
    sample["daemonset_requests"].update(cpu_millis=220, memory_mib=408)
    sample["daemonset_slots"] = 5
    placement = CapacityPlacement.model_validate(data)
    assert (
        plan_placement(
            placement, [(f"{i}:1", _resources()) for i in range(7)], sample=cold_sample(placement)
        ).additional_nodes
        == 1
    )
    assert (
        plan_placement(
            placement, [(f"{i}:1", _resources()) for i in range(8)], sample=cold_sample(placement)
        ).additional_nodes
        == 2
    )
    sample["pod_slots"] = 6
    placement = CapacityPlacement.model_validate(data)
    assert (
        plan_placement(
            placement,
            [(f"{i}:1", _resources(100)) for i in range(2)],
            sample=cold_sample(placement),
        ).additional_nodes
        == 2
    )


def test_pod_identity_absorbs_authorization_once_and_stale_generation_does_not():
    data = placement_fixture(target_id="a", node_cpu=2_000, requested_cpu=2_000)
    data["nodes"][0]["managed_pods"] = [
        {
            "uid": "pod",
            "lease_id": "lease",
            "generation": 1,
            "requests": _resources().model_dump(),
        }
    ]
    placement = CapacityPlacement.model_validate(data)
    assert (
        plan_placement(
            placement, [("lease:1", _resources())], sample=cold_sample(placement)
        ).additional_nodes
        == 0
    )
    assert (
        plan_placement(
            placement, [("lease:2", _resources())], sample=cold_sample(placement)
        ).additional_nodes
        == 1
    )


def test_pending_pod_and_unobserved_ledger_are_one_demand():
    data = placement_fixture(target_id="a", nodes=0, node_cpu=2_000)
    data["pending_pods"] = [
        {"uid": "pod", "lease_id": "lease", "generation": 1, "requests": _resources().model_dump()}
    ]
    placement = CapacityPlacement.model_validate(data)
    assert (
        plan_placement(
            placement, [("lease:1", _resources())], sample=cold_sample(placement)
        ).additional_nodes
        == 1
    )


def test_zero_nodes_can_reuse_matching_history_but_changed_template_cannot():
    data = placement_fixture(target_id="a")
    old = CapacityPlacement.model_validate(data)
    data["nodes"] = []
    data["node_group"]["node_count"] = 0
    data["template_samples"] = []
    current = CapacityPlacement.model_validate(data)
    assert cold_sample(current, [old]) is not None
    data["node_group"]["template"]["kubernetes_version"] = "1.34"
    current = CapacityPlacement.model_validate(data)
    assert cold_sample(current, [old]) is None
    with pytest.raises(PlacementUnavailableError, match="node_allocatable_unknown"):
        plan_placement(current, [("new:1", _resources())], sample=None)


def test_oversized_task_and_full_hot_pod_slots_need_real_cold_fit():
    data = placement_fixture(target_id="a", node_cpu=2_000)
    data["nodes"][0]["used_pod_slots"] = 64
    placement = CapacityPlacement.model_validate(data)
    assert (
        plan_placement(
            placement, [("new:1", _resources())], sample=cold_sample(placement)
        ).additional_nodes
        == 1
    )
    with pytest.raises(PlacementUnavailableError, match="exceeds_node_allocatable"):
        plan_placement(placement, [("new:1", _resources(2_001))], sample=cold_sample(placement))


def test_provider_charge_is_distinct_from_allocatable_and_domain_is_native():
    data = placement_fixture(target_id="a", node_storage=65_536, raw_storage=81_920)
    placement = CapacityPlacement.model_validate(data)
    assert placement.node_group.raw_node.storage_mib == 81_920
    assert placement.nodes[0].allocatable.storage_mib == 65_536
    quota = placement.quota_resources["storage"]
    newer = quota.model_copy(update={"used": 0, "limit": 1_000_000})
    assert quota_identity(quota) == quota_identity(newer)
    assert quota_identity(quota) != quota_identity(quota.model_copy(update={"region": "other"}))


def test_daemonset_change_invalidates_historical_sample():
    data = placement_fixture(target_id="a")
    data["daemonsets"] = [
        {"uid": "ds", "generation": 1, "requests": _resources(100).model_dump(), "scheduling": {}}
    ]
    old = CapacityPlacement.model_validate(data)
    changed = deepcopy(data)
    changed["template_samples"] = []
    changed["daemonsets"][0]["generation"] = 2
    assert cold_sample(CapacityPlacement.model_validate(changed), [old]) is None


def test_registered_not_ready_node_is_already_charged_to_provider():
    data = placement_fixture(target_id="a")
    data["nodes"][0]["ready"] = False
    placement = CapacityPlacement.model_validate(data)
    result = plan_placement(placement, [("new:1", _resources())], sample=cold_sample(placement))
    assert result.cold_nodes == 1 and result.additional_nodes == 0


def test_duplicate_pending_pod_authority_is_visible_instead_of_silently_deduplicated():
    data = placement_fixture(target_id="a", nodes=0)
    data["pending_pods"] = [
        {"uid": uid, "lease_id": "lease", "generation": 1, "requests": _resources().model_dump()}
        for uid in ("pod-a", "pod-b")
    ]
    placement = CapacityPlacement.model_validate(data)
    with pytest.raises(PlacementUnavailableError, match="duplicate_pod_identity"):
        plan_placement(placement, [("lease:1", _resources())], sample=cold_sample(placement))
