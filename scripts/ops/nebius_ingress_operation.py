#!/usr/bin/env python3
"""Connected private ingress operation: live capacity before staged installation."""
from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

from scripts.ops.nebius_ingress_cutover import KubectlCutoverAPI
from scripts.ops.nebius_ingress_cutover import _identity as resource_identity
from scripts.ops.nebius_ingress_gateway import TLSBinding
from scripts.ops.nebius_ingress_image import DIGEST

from loom.nebius_environment_contract import FoundationBinding
from loom.nebius_shared_ingress import SharedIngressInstallation
from loom_execution_capacity_collector import kubernetes as accounting


class OperationError(RuntimeError):
    """Payload-free installation failure; never expose inventory or credentials."""


class LiveIngressAPI(KubectlCutoverAPI):
    def __init__(self, kubeconfig: Path, *, binding: TLSBinding, executable: Path, candidate: str,
                 cluster_id: str, api_server: str, ingress_class: str, image: str):
        super().__init__(kubeconfig, binding=binding, executable=executable, candidate=candidate)
        self.cluster_id, self.api_server = cluster_id, api_server
        self.ingress_class, self.image = ingress_class, image

    def foundation(self) -> FoundationBinding:
        """Read and validate current deployment configuration before rendering."""
        try:
            self.verify_identity(self.binding)
            view = json.loads(self._run(["config", "view", "--minify", "-o", "json"]))
            clusters = view["clusters"]
            if (len(clusters) != 1 or clusters[0]["cluster"]["server"] != self.api_server
                    or not clusters[0]["name"].endswith(self.cluster_id.removeprefix("mk8s"))
                    or clusters[0]["cluster"].get("insecure-skip-tls-verify")
                    or not (clusters[0]["cluster"].get("certificate-authority")
                            or clusters[0]["cluster"].get("certificate-authority-data"))):
                raise OperationError("Kubernetes context does not match trusted ingress binding")
            row = self._get(["get", "configmap", "loom-platform-config", "-n", self.binding.namespace])
            if row is None:
                raise OperationError("live platform configuration unavailable")
            resource_identity(row, kind="ConfigMap", name="loom-platform-config", namespace=self.binding.namespace)
            data = row["data"]
            config, profile = json.loads(data["environment.json"]), json.loads(data["profile.json"])
            if (config["namespace"] != self.binding.namespace or config["cluster_id"] != self.cluster_id
                    or config["kubernetes_api_server"] != self.api_server or profile["candidate_sha"] != self.candidate
                    or not re.fullmatch(r"cr\." + re.escape(config["region"]) + r"\.nebius\.cloud/[A-Za-z0-9_-]+/loom-shared-ingress@"
                                        + re.escape(DIGEST), self.image)):
                raise OperationError("live platform differs from installed ingress authority")
            # Validate before normalization so an invalid flag is not hidden.
            foundation = FoundationBinding(
                platform_config_json=json.dumps(config, sort_keys=True), public_dns_zone=self.binding.child_domain,
                ingress_class_name=self.ingress_class, ingress_namespace=self.binding.namespace,
                ingress_controller_label="loom-shared-ingress",
            )
            # The flag controls the existing public Service, not these staged
            # resources. Canonicalize this one operation-owned field so replay
            # after cutover keeps the initial staging render and journal intact.
            foundation = foundation.model_copy(update={
                "platform_config_json": json.dumps({**config, "shared_ingress_enabled": False}, sort_keys=True),
            })
            SharedIngressInstallation(installation_id=UUID(self.binding.installation_id), foundation=foundation,
                                      image=self.image, tls_secret_name="loom-ingress-pending")
            self.verify_identity(self.binding)
            return foundation
        except OperationError:
            raise
        except Exception:
            raise OperationError("trusted live ingress foundation is unavailable") from None


def qualify_capacity(*, nodes: list[dict[str, Any]], pods: list[dict[str, Any]]) -> dict[str, Any]:
    """Prove room for two additional ingress Pods without resizing anything.

    Count all live bound workloads, including terminating/foreign Pods, and all
    pending competitors that could use the same node. Deliberately conservative:
    no capacity from deleting nodes, resize uncertainty or future autoscaling.
    Existing ingress Pods are not subtracted without exact ownership proof.
    """
    from kubernetes import client

    try:
        for rows in (nodes, pods):
            uids = [row["metadata"]["uid"] for row in rows]
            names = [(row["metadata"].get("namespace", ""), row["metadata"]["name"]) for row in rows]
            if len(set(uids)) != len(uids) or len(set(names)) != len(names):
                raise OperationError("duplicate Kubernetes inventory")
            if any(str(UUID(uid)) != uid or UUID(uid).int == 0 for uid in uids):
                raise OperationError("Kubernetes inventory identity unavailable")
        with client.ApiClient() as decoder:
            decoded_nodes = decoder.deserialize(SimpleNamespace(data=json.dumps({
                "apiVersion": "v1", "kind": "NodeList", "items": nodes,
            }).encode()), "V1NodeList").items
        decoded_pods = accounting._decode_pool_pods(SimpleNamespace(data=json.dumps({
            "apiVersion": "v1", "kind": "PodList", "items": pods,
        }).encode())).items
        ingress = SimpleNamespace(spec=SimpleNamespace(template=SimpleNamespace(spec=SimpleNamespace(
            node_selector={"loom.nebius/node-role": "system", "loom.nebius/platform": "integration"},
            tolerations=[{"key": "loom.nebius/platform", "operator": "Equal", "value": "integration", "effect": "NoSchedule"}],
        ))))
        for node in decoded_nodes:
            if (not accounting._node_ready(node) or accounting._node_draining(node)
                    or not accounting._daemonset_matches_node(ingress, node)
                    or any(c.type in {"DiskPressure", "MemoryPressure", "PIDPressure", "NetworkUnavailable"}
                           and c.status != "False" for c in node.status.conditions or [])):
                continue
            if (not re.fullmatch(r"nebius://computeinstance-[a-z0-9]+", node.spec.provider_id or "")
                    or node.metadata.name != node.spec.provider_id.removeprefix("nebius://")):
                raise OperationError("eligible system node provider identity differs")
            allocatable = accounting._required_node_resources(node.status.allocatable, name="system")
            slots = accounting._positive_int(node.status.allocatable.get("pods"), name="system Pod slots")
            requests = []
            for pod in decoded_pods:
                if pod.status.phase in {"Succeeded", "Failed"}:
                    continue
                if pod.spec.node_name:
                    if pod.spec.node_name != node.metadata.name:
                        continue
                elif not accounting._daemonset_matches_node(
                    SimpleNamespace(spec=SimpleNamespace(template=SimpleNamespace(spec=pod.spec))), node,
                ):
                    continue
                requests.append(accounting._pod_request(pod))
            used = accounting._add(*requests)
            if (slots - len(requests) >= 2
                    and allocatable.cpu_millis - used.cpu_millis >= 200
                    and allocatable.memory_mib - used.memory_mib >= 256
                    and allocatable.storage_mib - used.storage_mib >= 128):
                return {"node_uid": node.metadata.uid, "reserved_pods": 2,
                        "cpu_millis": 200, "memory_mib": 256, "storage_mib": 128}
        raise OperationError("eligible system capacity cannot fit the ingress envelope")
    except OperationError:
        raise
    except Exception:
        raise OperationError("full live ingress capacity inventory is unqualified") from None
