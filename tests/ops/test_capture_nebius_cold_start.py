from __future__ import annotations

import json

from scripts.ops.capture_nebius_cold_start import capture


def test_capture_preserves_startup_evidence_without_workload_credentials() -> None:
    def kube(args: list[str]) -> dict:
        assert args[0] == "get"
        if args[1] == "nodes":
            return {
                "items": [
                    {
                        "metadata": {"name": "node-a", "creationTimestamp": "start"},
                        "spec": {
                            "taints": [
                                {"key": "node.cilium.io/agent-not-ready", "effect": "NoSchedule"}
                            ]
                        },
                        "status": {
                            "conditions": [
                                {"type": "Ready", "status": "True", "lastTransitionTime": "ready"}
                            ]
                        },
                    }
                ]
            }
        if args[1] == "pods":
            return {
                "items": [
                    {
                        "metadata": {"name": "trial", "uid": "uid"},
                        "spec": {
                            "containers": [
                                {
                                    "name": "main",
                                    "env": [{"value": "secret"}],
                                    "resources": {"requests": {"cpu": "1"}},
                                }
                            ],
                            "initContainers": [
                                {
                                    "name": "sandbox",
                                    "restartPolicy": "Always",
                                    "resources": {"requests": {"memory": "1Gi"}},
                                }
                            ],
                        },
                        "status": {
                            "phase": "Pending",
                            "conditions": [
                                {
                                    "type": "PodScheduled",
                                    "status": "False",
                                    "message": "untolerated taint secret",
                                }
                            ],
                        },
                    }
                ]
            }
        if args[1] == "configmap":
            return {
                "data": {
                    "status": "autoscalerStatus: Running\nnodeGroups:\n- name: group\n  health:\n    cloudProviderTarget: 5\n    nodeCounts:\n      unregistered: 4\n- name: other\n"
                }
            }
        return {
            "items": [
                {"reason": "TriggeredScaleUp", "message": "pod triggered scale-up: group 5->9"},
                {"reason": "FailedScheduling", "message": "Insufficient ephemeral-storage secret"},
                {"reason": "Failed", "message": "secret"},
            ]
        }

    result = capture(
        kube,
        lambda: {
            "metadata": {"id": "group"},
            "spec": {"autoscaling": {"max_node_count": 100}, "cloud_init": "secret"},
            "status": {"node_count": 5, "target_node_count": 9},
        },
        node_group_id="group",
        node_selector="pool=execution",
        namespace="execution",
    )
    assert "secret" not in json.dumps(result)
    assert result["nodes"]["data"][0]["taints"][0]["key"] == "node.cilium.io/agent-not-ready"
    assert result["autoscaler"]["data"]["nodeGroups"] == [
        {"name": "group", "health": {"cloudProviderTarget": 5, "nodeCounts": {"unregistered": 4}}}
    ]
    assert result["pods"]["data"][0]["containers"][1]["restart_policy"] == "Always"
    assert result["events"]["data"][0]["scale_up"] == "pod triggered scale-up: group 5->9"
    assert result["events"]["data"][1]["categories"] == ["Insufficient ephemeral-storage"]


def test_missing_managed_status_is_unknown_not_empty_healthy_capacity() -> None:
    def kube(args: list[str]) -> dict:
        if args[1] == "configmap":
            raise PermissionError("private endpoint and credential must not be serialized")
        return {"items": []}

    result = capture(
        kube,
        lambda: {"metadata": {"id": "other"}},
        node_group_id="group",
        node_selector="pool=execution",
        namespace="execution",
    )
    assert result["autoscaler"]["unavailable"] == "PermissionError"
    assert result["provider"]["unavailable"] == "ValueError"
    assert "data" not in result["provider"]
    assert result["nodes"]["data"] == []
    assert "credential" not in json.dumps(result)


def test_native_proto_zero_counts_are_normalized() -> None:
    result = capture(
        lambda _args: {"items": []},
        lambda: {
            "metadata": {"id": "group"},
            "status": {"state": "RUNNING"},
            "spec": {"autoscaling": {"max_node_count": "100"}},
        },
        node_group_id="group",
        node_selector="pool=execution",
        namespace="execution",
    )
    assert result["provider"]["data"] == {
        "status": {"node_count": 0, "target_node_count": 0, "state": "RUNNING"},
        "autoscaling": {"min_node_count": 0, "max_node_count": 100},
    }
