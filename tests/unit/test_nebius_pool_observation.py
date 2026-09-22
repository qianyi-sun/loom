"""Pool accounting must not trust child labels or sum per-owner capacity."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

import pytest
from kubernetes import client as k8s

from loom_control_plane.execution_placement import plan_placement
from loom_execution_capacity_collector.contracts import (
    CapacityPlacement,
    NodeGroupPlacement,
    ResourceTotals,
)
from loom_execution_capacity_collector.kubernetes import (
    InClusterKubernetesCapacityReader,
    KubernetesObservationError,
)


def _scope():
    # Import inside the helper: RED names the missing pool interface, without
    # preventing the independent legacy collector regression lane from loading.
    from loom_execution_capacity_collector.pool import PoolObservationScope

    return PoolObservationScope.model_validate({
        "node_selector": {"loom.nebius/role": "execution"},
        "environments": [
            {"environment_id": UUID(int=n), "incarnation": UUID(int=n + 10),
             "execution_namespace": f"run-{n}", "build_namespace": f"run-{n}-build",
             "target_id": f"target-{n}"}
            for n in (1, 2)
        ],
        "jobs": [
            {"reservation_id": UUID(int=n + 20), "environment_id": UUID(int=n),
             "incarnation": UUID(int=n + 10), "namespace": f"run-{n}",
             "job_name": f"job-{n}", "job_uid": f"job-uid-{n}",
             "workload_kind": "trial", "lease_id": "same-local-claim", "generation": 1}
            for n in (1, 2)
        ],
    })


def _node(name="node-1"):
    return k8s.V1Node(
        metadata=k8s.V1ObjectMeta(name=name, uid=f"uid-{name}", labels={"loom.nebius/role": "execution"}),
        spec=k8s.V1NodeSpec(provider_id=f"nebius://{name}"),
        status=k8s.V1NodeStatus(
            capacity={"cpu": "4", "memory": "8Gi", "ephemeral-storage": "100Gi"},
            allocatable={"cpu": "3500m", "memory": "7Gi", "ephemeral-storage": "90Gi", "pods": "64"},
            conditions=[k8s.V1NodeCondition(type="Ready", status="True")],
        ),
    )


def _pod(owner=1, *, name=None, pending=False):
    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name=name or f"pod-{owner}", uid=f"uid-{name or owner}", namespace=f"run-{owner}",
            labels={"app.kubernetes.io/managed-by": "loom-execution-actuator",
                    "loom.openai.com/lease-id": "same-local-claim", "loom.openai.com/generation": "1"},
            annotations={"loom.openai.com/target-id": f"target-{owner}"},
            owner_references=[k8s.V1OwnerReference(
                api_version="batch/v1", kind="Job", name=f"job-{owner}", uid=f"job-uid-{owner}", controller=True,
            )],
        ),
        spec=k8s.V1PodSpec(
            containers=[k8s.V1Container(name="work", resources=k8s.V1ResourceRequirements(
                requests={"cpu": "1", "memory": "1Gi", "ephemeral-storage": "2Gi"},
            ))],
            node_name=None if pending else "node-1",
        ),
        status=k8s.V1PodStatus(phase="Pending" if pending else "Running"),
    )


async def _capture(nodes, pods, scope=None):
    calls = []

    def listing(kind, items, **kwargs):
        calls.append(kind)
        if kind == "nodes":
            assert kwargs["label_selector"] == "loom.nebius/role=execution"
        return SimpleNamespace(items=items, metadata=SimpleNamespace(resource_version=kind + "-1"))

    reader = InClusterKubernetesCapacityReader(
        core_api=SimpleNamespace(
            list_node=lambda **kw: listing("nodes", nodes, **kw),
            list_pod_for_all_namespaces=lambda **kw: listing("pods", pods, **kw),
        ),
        apps_api=SimpleNamespace(list_daemon_set_for_all_namespaces=lambda **kw: listing("daemons", [], **kw)),
    )
    snapshot = await reader.capture_pool(scope=scope or _scope())
    assert calls == ["nodes", "pods", "daemons"]
    return snapshot


def _placement(snapshot):
    return CapacityPlacement(
        node_group=NodeGroupPlacement(id="pool", max_nodes=1, node_count=1, template={},
                                     raw_node=ResourceTotals(cpu_millis=4000, memory_mib=8192, storage_mib=102400)),
        nodes=snapshot.nodes, pending_pods=snapshot.pending_pods, quota_resources={},
        daemonsets=snapshot.daemonsets, template_samples=snapshot.template_samples,
    )


@pytest.mark.asyncio
async def test_two_environments_observe_one_pool_and_distinct_global_grants():
    snapshot = await _capture([_node()], [_pod(1), _pod(2)])
    assert snapshot.active_nodes == 1
    assert snapshot.allocatable.cpu_millis == 3500
    assert snapshot.requested.cpu_millis == 2000
    managed = snapshot.nodes[0].managed_pods
    assert {p.lease_id for p in managed} == {
        "reservation:00000000-0000-0000-0000-000000000015",
        "reservation:00000000-0000-0000-0000-000000000016",
    }
    # The physical node has 1500m left. Discounting the two proven observed
    # grants permits exactly one additional 1000m grant; summing child nodes or
    # recharging observed grants breaks this hand-derived packing result.
    demand = ResourceTotals(cpu_millis=1000, memory_mib=1024, storage_mib=2048)
    result = plan_placement(_placement(snapshot), [
        (p.lease_id + ":1", demand) for p in managed
    ] + [("unobserved:1", demand)], sample=None)
    assert result.additional_nodes == 0
    assert len(result.observed_lease_ids) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["uid", "namespace", "name", "controller", "target", "lease", "generation", "no_owner"])
async def test_forged_labels_cannot_discount_another_environment_grant(change):
    pod = _pod(1)
    if change == "namespace":
        pod.metadata.namespace = "run-2"
    elif change == "target":
        pod.metadata.annotations["loom.openai.com/target-id"] = "target-2"
    elif change == "lease":
        pod.metadata.labels["loom.openai.com/lease-id"] = "different"
    elif change == "generation":
        pod.metadata.labels["loom.openai.com/generation"] = "2"
    elif change == "no_owner":
        pod.metadata.owner_references = []
    else:
        setattr(pod.metadata.owner_references[0], change, False if change == "controller" else "foreign")
    snapshot = await _capture([_node()], [pod])
    assert snapshot.nodes[0].managed_pods == []
    assert snapshot.nodes[0].requested.cpu_millis == 1000
    assert snapshot.nodes[0].used_pod_slots == 1


@pytest.mark.asyncio
async def test_unproven_pending_pods_keep_their_own_charge_even_outside_registered_namespaces():
    pod = _pod(1, pending=True)
    pod.metadata.namespace = "foreign"
    snapshot = await _capture([], [pod])
    assert snapshot.active_nodes == 0
    assert snapshot.requested.cpu_millis == 1000
    assert snapshot.pending_pods[0].lease_id == "foreign-pod:uid-1"
    assert snapshot.template_samples == []  # No invented cold-node capacity.


@pytest.mark.asyncio
async def test_proven_incompatible_pending_pods_do_not_charge_this_pool():
    pod = _pod(1, pending=True)
    pod.metadata.namespace = "foreign"
    pod.spec.node_selector = {"loom.nebius/role": "different-pool"}
    snapshot = await _capture([], [pod])
    assert snapshot.pending_pods == []
    assert snapshot.requested.cpu_millis == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", ["node_uid", "provider_id", "pod_uid", "reservation"])
async def test_ambiguous_physical_or_grant_identity_fails_closed(identity):
    nodes, pods = [_node()], [_pod()]
    if identity in {"node_uid", "provider_id"}:
        second = _node("node-2")
        if identity == "node_uid":
            second.metadata.uid = nodes[0].metadata.uid
        else:
            second.spec.provider_id = nodes[0].spec.provider_id
        nodes.append(second)
    else:
        second = _pod(name="second")
        if identity == "pod_uid":
            second.metadata.uid = pods[0].metadata.uid
        pods.append(second)
    with pytest.raises(KubernetesObservationError):
        await _capture(nodes, pods)


@pytest.mark.asyncio
async def test_registered_work_scheduled_outside_pool_is_not_invented_free_capacity():
    pod = _pod()
    pod.spec.node_name = "different-pool"
    with pytest.raises(KubernetesObservationError, match="outside"):
        await _capture([_node()], [pod])
