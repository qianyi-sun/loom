#!/usr/bin/env python3
"""Read-only, payload-free inventory before qualifying managed Nebius environments."""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from scripts.ops.deploy_nebius_platform import (  # noqa: E402
    DeploymentError,
    Kubectl,
    verify_cluster_identity,
)

RESOURCE_KEYS = ("cpu", "memory", "ephemeral-storage", "pods")


def _fields(value: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {key: value[key] for key in keys if key in value}


def _identity(item: dict[str, Any]) -> dict[str, Any]:
    return _fields(item["metadata"], ("name", "namespace", "uid"))


def _resources(value: dict[str, Any]) -> dict[str, Any]:
    return _fields(value, RESOURCE_KEYS)


def _list(kube: Kubectl, kind: str, *, namespaced: bool = False) -> list[dict[str, Any]]:
    value = json.loads(kube.run("get", kind, *(["--all-namespaces"] if namespaced else []), "-o", "json"))
    # Missing/failed inventory is not an empty list. Never expose API diagnostics.
    items = value.get("items")
    if not isinstance(items, list) or any(not isinstance(item, dict) or "metadata" not in item for item in items):
        raise DeploymentError("incomplete resource inventory")
    return items


def _containers(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"name": item["name"], "requests": _resources(item.get("resources", {}).get("requests", {})),
             **_fields(item, ("restartPolicy",))} for item in items]


def inspect(kube: Kubectl, *, namespace: str, expected_cluster_id: str) -> dict[str, Any]:
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", namespace):
        raise DeploymentError("invalid namespace")
    data = kube.get("configmap", "loom-platform-config", namespace)["data"]
    config, profile = json.loads(data["environment.json"]), json.loads(data["profile.json"])
    if config["namespace"] != namespace:
        raise DeploymentError("configured namespace does not match selected target")
    verify_cluster_identity(kube, config, expected_cluster_id)
    candidate = profile["candidate_sha"]
    if not isinstance(candidate, str) or not re.fullmatch(r"[0-9a-f]{40}", candidate):
        raise DeploymentError("invalid installed candidate identity")

    nodes = _list(kube, "nodes")
    namespaces = _list(kube, "namespaces")
    pods = _list(kube, "pods", namespaced=True)
    services = _list(kube, "services", namespaced=True)
    ingresses = _list(kube, "ingresses", namespaced=True)
    ingress_classes = _list(kube, "ingressclasses")
    volumes = _list(kube, "persistentvolumeclaims", namespaced=True)
    storage_classes = _list(kube, "storageclasses")
    # All cluster Pods count for platform sizing, including system/foreign Pods.
    # Preserve requests as declared, including init/sidecar/overhead, without
    # claiming an available budget from a naive sum or instantaneous usage.
    return {
        "schema_version": "loom.nebius-management-preflight.v1",
        "status": "observed", "observed_at": datetime.now(UTC).isoformat(),
        "cluster_id": expected_cluster_id, "namespace": namespace,
        "execution_namespace": config["execution_namespace"], "candidate_sha": candidate,
        "public_host": config["public_host"],
        "configured_execution_node_group_id": config["execution_node_group_id"],
        "nodes": [{**_identity(item), "role": item["metadata"].get("labels", {}).get("loom.nebius/node-role"),
            "provider_id": item.get("spec", {}).get("providerID"),
            "unschedulable": item.get("spec", {}).get("unschedulable", False),
            "allocatable": _resources(item.get("status", {}).get("allocatable", {})),
            "ready": any(condition.get("type") == "Ready" and condition.get("status") == "True"
                         for condition in item.get("status", {}).get("conditions", [])),
            "taints": [_fields(taint, ("key", "value", "effect")) for taint in item.get("spec", {}).get("taints", [])],
        } for item in nodes],
        "namespaces": [_identity(item) for item in namespaces],
        "pods": [{**_identity(item), "node": item["spec"].get("nodeName"),
            "phase": item.get("status", {}).get("phase"),
            "containers": _containers(item["spec"].get("containers", [])),
            "init_containers": _containers(item["spec"].get("initContainers", [])),
            "overhead": _resources(item["spec"].get("overhead", {})),
            "pod_requests": _resources(item["spec"].get("resources", {}).get("requests", {})),
        } for item in pods],
        "services": [{**_identity(item), "type": item["spec"].get("type"),
            "load_balancer": [_fields(address, ("ip", "hostname")) for address in
                              item.get("status", {}).get("loadBalancer", {}).get("ingress", [])],
        } for item in services],
        "ingresses": [{**_identity(item), "class": item["spec"].get("ingressClassName"),
            "hosts": [rule.get("host") for rule in item["spec"].get("rules", [])],
            "tls_hosts": [host for tls in item["spec"].get("tls", []) for host in tls.get("hosts", [])],
        } for item in ingresses],
        "ingress_classes": [{**_identity(item), "controller": item["spec"]["controller"]} for item in ingress_classes],
        "volumes": [{**_identity(item), "storage_class": item["spec"].get("storageClassName"),
            "requested_storage": item["spec"].get("resources", {}).get("requests", {}).get("storage"),
            "phase": item.get("status", {}).get("phase"),
        } for item in volumes],
        "storage_classes": [{**_identity(item), **_fields(item, ("provisioner", "reclaimPolicy", "volumeBindingMode"))}
                            for item in storage_classes],
        "unverified": ["provider_iam", "wildcard_dns_tls", "management_installation", "platform_child_allowance",
                       "live_nebius_pool_limits_and_quota", "concurrent_owner_acceptance"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--expected-cluster-id", required=True)
    parser.add_argument("--namespace", default="loom-nebius-platform")
    parser.add_argument("--evidence-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = inspect(Kubectl(args.kubeconfig), namespace=args.namespace, expected_cluster_id=args.expected_cluster_id)
    except Exception as exc:
        # Never print raw config, kubeconfig, API errors or exception messages.
        result = {"schema_version": "loom.nebius-management-preflight.v1", "status": "blocked",
                  "error_type": type(exc).__name__}
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    (args.evidence_dir / "management-preflight.json").write_text(json.dumps(result, sort_keys=True) + "\n")
    print(json.dumps({"status": result["status"]}))
    return 0 if result["status"] == "observed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
