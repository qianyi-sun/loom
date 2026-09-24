"""Read-only projection of the ingress installer's pre-write cluster checks.

Runs inside protected inspection, not the private gateway. It cannot establish
gateway-local execution or certificate validity and never grants a write retry.
"""
from __future__ import annotations

import json
from typing import Any

from scripts.ops.deploy_nebius_platform import Kubectl
from scripts.ops.nebius_ingress_bootstrap import validate_config
from scripts.ops.nebius_ingress_gateway import MAX_KUBECTL_OUTPUT, IngressError, TLSBinding
from scripts.ops.nebius_ingress_operation import LIVE_POD_FIELD_SELECTOR, LiveIngressAPI


class ReadOnlyIngressAPI(LiveIngressAPI):
    """Reuse live validators with a strictly read-only protected transport.

    Deliberately does not construct the gateway's local-file/mutation adapter.
    Even inherited mutation methods cannot issue writes through this _run.
    """

    def __init__(self, kube: Kubectl, config: dict[str, Any]):
        self.kube = kube
        self.binding = TLSBinding(**config["binding"])
        self.candidate, self.cluster_id = config["candidate"], config["cluster_id"]
        self.api_server = config["api_server"]
        self.ingress_class, self.image = config["ingress_class"], config["image"]
        self.reads: list[dict[str, Any]] = []
        self.failure: str | None = None
        suffix = ("--ignore-not-found", "-o", "json")
        self.allowed = {
            ("config", "view", "--minify", "-o", "json"): "context",
            ("get", "namespace", "kube-system", *suffix): "kube_system",
            ("get", "namespace", self.binding.namespace, *suffix): "namespace",
            ("get", "configmap", "loom-platform-config", "-n", self.binding.namespace, *suffix): "platform_configuration",
            ("get", "nodes", "-o", "json"): "nodes",
            ("get", "pods", "--all-namespaces", "--field-selector", LIVE_POD_FIELD_SELECTOR, "-o", "json"): "pods",
        }

    def _run(self, arguments: list[str], *, payload: bytes | None = None) -> bytes:
        resource = self.allowed.get(tuple(arguments))
        if payload is not None or resource is None:
            self.failure = "request_denied"
            raise IngressError("diagnostic request outside fixed reads")
        observation: dict[str, Any] = {"resource": resource}
        self.reads.append(observation)
        try:
            raw = self.kube.run(*arguments, timeout=40, preserve_output=True).encode()
        except Exception:
            self.failure = observation["status"] = "read_failed"
            raise IngressError("diagnostic read unavailable") from None
        observation["bytes"] = len(raw)
        if len(raw) > MAX_KUBECTL_OUTPUT:
            self.failure = observation["status"] = "response_too_large"
            raise IngressError("diagnostic read exceeds gateway response bound")
        observation["status"] = "read"
        return raw


def inspect_ingress(kube: Kubectl, raw_config: str, *, namespace: str,
                    expected_cluster_id: str) -> dict[str, Any]:
    """Fixed codes only; raw objects/exceptions must not escape this projection."""
    if not raw_config:
        return {"status": "not_configured"}
    result: dict[str, Any] = {"status": "blocked", "phase": "binding", "checks": {}}
    try:
        if len(raw_config.encode()) > 16_384:
            raise ValueError()
        config = json.loads(raw_config)
        validate_config(config)
        if (config["binding"]["namespace"] != namespace or config["cluster_id"] != expected_cluster_id
                or config["kubeconfig"] != str(kube.kubeconfig)):
            raise ValueError()
        api = ReadOnlyIngressAPI(kube, config)
    except Exception:
        return {**result, "reason": "validation_failed"}
    result.update(bound_source_sha=config["source_sha"], candidate=config["candidate"], reads=api.reads,
                  scope="current_cluster_not_historical_gateway", failures={},
                  unverified=["gateway_source_correspondence", "gateway_local_execution",
                              "certificate_delivery", "staging", "cutover"])
    result.pop("phase")
    for phase in ("foundation", "capacity"):
        api.failure = None
        try:
            getattr(api, phase)()
            result["checks"][phase] = "passed"
        except Exception as exc:
            # Both checks revalidate namespace UIDs and issue only fixed reads.
            # A stale candidate must not hide independent current capacity data.
            reason = api.failure or {
                "eligible system capacity cannot fit the ingress envelope": "insufficient_capacity",
                "complete live capacity inventory required": "inventory_unqualified",
                "duplicate Kubernetes inventory": "inventory_unqualified",
                "Kubernetes inventory identity unavailable": "inventory_unqualified",
                "full live ingress capacity inventory is unqualified": "accounting_unqualified",
                "eligible system node provider identity differs": "node_identity_mismatch",
            }.get(str(exc), "validation_failed")
            result["checks"][phase] = "blocked"
            result["failures"][phase] = reason
            result.setdefault("phase", phase)
            result.setdefault("reason", reason)
    if not result["failures"]:
        result["status"] = "passed"
    return result
