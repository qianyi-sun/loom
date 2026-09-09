"""Synthetic native capacity observations for disposable execution tests."""

from typing import Any


def placement_fixture(
    *,
    target_id: str,
    node_cpu: int = 64_000,
    node_memory: int = 262_144,
    node_storage: int = 1_048_576,
    raw_storage: int | None = None,
    nodes: int = 1,
    requested_cpu: int = 0,
    requested_memory: int = 0,
    requested_storage: int = 0,
    quota_nodes: int = 20,
    used_nodes: int = 1,
    parent_id: str | None = None,
    region: str = "eu-north1",
) -> dict[str, Any]:
    raw_storage = node_storage if raw_storage is None else raw_storage
    shape = {"cpu_millis": node_cpu, "memory_mib": node_memory, "storage_mib": node_storage}
    quota_resources = {
        label: {
            "parent_id": parent_id or target_id,
            "region": region,
            "service": "compute",
            "name": label,
            "unit": unit,
            "limit": quota_nodes * amount,
            "used": used_nodes * amount,
        }
        for label, amount, unit in (
            ("nodes", 1, "count"),
            ("vcpu", node_cpu, "milli-vcpu"),
            ("memory", node_memory, "MiB"),
            ("storage", raw_storage, "MiB"),
        )
    }
    return {
        "quota_resources": quota_resources,
        "node_group": {
            "id": target_id,
            "max_nodes": 100,
            "node_count": nodes,
            "template": {"preset": "test-node-v1", "kubernetes_version": "1.33"},
            "raw_node": {**shape, "storage_mib": raw_storage},
        },
        "nodes": [
            {
                "uid": f"{target_id}-node-{i}",
                "provider_id": f"test://{target_id}/{i}",
                "ready": True,
                "unschedulable": False,
                "deleting": False,
                "allocatable": shape.copy(),
                "requested": {
                    "cpu_millis": requested_cpu,
                    "memory_mib": requested_memory,
                    "storage_mib": requested_storage,
                },
                "pod_slots": 64,
                "used_pod_slots": 0,
                "managed_pods": [],
            }
            for i in range(nodes)
        ],
        "pending_pods": [],
        "daemonsets": [],
        "template_samples": [
            {
                "node_uid": f"{target_id}-sample",
                "allocatable": shape.copy(),
                "pod_slots": 64,
                "daemonset_slots": 0,
                "daemonset_requests": {"cpu_millis": 0, "memory_mib": 0, "storage_mib": 0},
                "kubelet_version": "v1.33.0",
                "daemonsets": {},
            }
        ],
    }
