"""Pure isolated browser Job and report authority for Tier 3 rehearsal."""

from __future__ import annotations

import hashlib
import ipaddress
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.browser_report_contract import (
    BROWSER_ACCEPTANCE_USERNAME,
    BROWSER_REPORT_CHECK_IDS,
    RehearsalBrowserReportAuthority,
    browser_report_ready,
)
from loom_cli.rollout.image_readiness import BROWSER_IMAGE
from loom_cli.rollout.rehearsal_action_source import (
    RehearsalPlan,
    rehearsal_image_pull_policy,
    rehearsal_image_reference,
)

BROWSER_JOB_NAME = "loom-rehearsal-browser"
BROWSER_INGRESS_NAME = "loom-rehearsal-browser"
BROWSER_INGRESS_NETWORK_POLICY_NAME = "loom-rehearsal-browser-ingress"
BROWSER_NETWORK_POLICY_NAME = "loom-rehearsal-browser"
INGRESS_CONTROLLER_NAMESPACE = "ingress-nginx"
INGRESS_CONTROLLER_SERVICE = "ingress-nginx-controller"
_REPORT_PATH = "/evidence/rehearsal-browser-report.json"


@dataclass(frozen=True, slots=True)
class RehearsalBrowserArtifact:
    """Non-sensitive exact manifests for one isolated browser acceptance."""

    payload: bytes
    artifact_sha256: str
    ingress_ip: str
    ingress_source_ips: tuple[str, ...]
    browser_image_digest: str
    resource_count: int

    def __post_init__(self) -> None:
        try:
            address = ipaddress.ip_address(self.ingress_ip)
        except ValueError as exc:
            raise ValueError("rehearsal browser ingress identity is invalid") from exc
        try:
            source_ips = _canonical_ingress_source_ips(self.ingress_source_ips)
        except ValueError as exc:
            raise ValueError("rehearsal browser ingress source identity is invalid") from exc
        if (
            not self.payload
            or address.version != 4
            or not self.browser_image_digest.startswith("sha256:")
            or len(self.browser_image_digest) != 71
            or hashlib.sha256(self.payload).hexdigest() != self.artifact_sha256
            or source_ips != self.ingress_source_ips
            or self.resource_count != 4
        ):
            raise ValueError("rehearsal browser artifact identity is invalid")


def build_rehearsal_browser_artifact(
    plan: RehearsalPlan,
    *,
    ingress_ip: str,
    ingress_source_ips: Sequence[str],
) -> RehearsalBrowserArtifact:
    """Build one route, one browser Job and its exact network authority."""
    plan.resources.require_isolated()
    # plan.resources.route is derived from the trusted staging route
    # ({route_origin}/rehearsal/{suffix}) and validated by RehearsalResources;
    # bind it to this namespace suffix without hardcoding the env route path.
    expected_suffix = plan.resources.namespace.removeprefix("loom-rehearsal-")
    if not plan.resources.route.startswith("https://") or not plan.resources.route.endswith(
        f"/rehearsal/{expected_suffix}"
    ):
        raise ValueError("rehearsal browser route authority is invalid")
    try:
        address = ipaddress.ip_address(ingress_ip)
    except ValueError as exc:
        raise ValueError("rehearsal browser ingress identity is invalid") from exc
    image_digest = plan.image_digests.get(BROWSER_IMAGE)
    if address.version != 4:
        raise ValueError("rehearsal browser ingress identity is invalid")
    if image_digest is None:
        raise ValueError("rehearsal browser authority is incomplete")
    try:
        canonical_source_ips = _canonical_ingress_source_ips(ingress_source_ips)
    except ValueError as exc:
        raise ValueError("rehearsal browser ingress source identity is invalid") from exc
    resources = (
        _ingress(plan),
        _browser_job(plan, ingress_ip=ingress_ip),
        _browser_network_policy(plan),
        _browser_ingress_network_policy(plan, ingress_source_ips=canonical_source_ips),
    )
    payload = yaml.safe_dump_all(resources, sort_keys=True).encode()
    return RehearsalBrowserArtifact(
        payload=payload,
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
        ingress_ip=ingress_ip,
        ingress_source_ips=canonical_source_ips,
        browser_image_digest=image_digest,
        resource_count=len(resources),
    )


def ingress_controller_ip(observed: Mapping[str, object]) -> str | None:
    """Return the exact IPv4 ClusterIP of the fixed ingress controller Service."""
    metadata = observed.get("metadata")
    spec = observed.get("spec")
    if not isinstance(metadata, Mapping) or not isinstance(spec, Mapping):
        return None
    if (
        observed.get("apiVersion") != "v1"
        or observed.get("kind") != "Service"
        or metadata.get("name") != INGRESS_CONTROLLER_SERVICE
        or metadata.get("namespace") != INGRESS_CONTROLLER_NAMESPACE
        or spec.get("type") not in {"ClusterIP", "NodePort"}
    ):
        return None
    value = spec.get("clusterIP")
    try:
        address = ipaddress.ip_address(value) if isinstance(value, str) else None
    except ValueError:
        return None
    return str(address) if address is not None and address.version == 4 else None


def rehearsal_backend_endpoints(
    observed: Mapping[str, object],
    *,
    namespace: str,
    name: str,
    port: int,
) -> tuple[tuple[str, str], ...] | None:
    """Return exact ready pod/node identities for one rehearsal ingress backend."""
    metadata = observed.get("metadata")
    subsets = observed.get("subsets")
    if (
        not isinstance(metadata, Mapping)
        or observed.get("apiVersion") != "v1"
        or observed.get("kind") != "Endpoints"
        or (name, port) not in {("loom-service", 8090), ("loom-web", 8080)}
        or not namespace.startswith("loom-rehearsal-")
        or metadata.get("name") != name
        or metadata.get("namespace") != namespace
        or not isinstance(subsets, list)
        or not 1 <= len(subsets) <= 32
    ):
        return None
    values: list[tuple[str, str]] = []
    for subset in subsets:
        if not isinstance(subset, Mapping):
            return None
        addresses = subset.get("addresses")
        ports = subset.get("ports")
        if (
            not isinstance(addresses, list)
            or not addresses
            or not isinstance(ports, list)
            or not any(
                isinstance(item, Mapping)
                and item.get("port") == port
                and item.get("protocol") in (None, "TCP")
                for item in ports
            )
        ):
            return None
        for address in addresses:
            value = address.get("ip") if isinstance(address, Mapping) else None
            node_name = address.get("nodeName") if isinstance(address, Mapping) else None
            try:
                parsed = ipaddress.ip_address(value) if isinstance(value, str) else None
            except ValueError:
                return None
            if (
                not isinstance(parsed, ipaddress.IPv4Address)
                or str(parsed) != value
                or not isinstance(node_name, str)
                or not node_name
                or len(node_name) > 253
            ):
                return None
            values.append((node_name, value))
    if not 1 <= len(values) <= 32 or len(set(values)) != len(values):
        return None
    return tuple(sorted(values))


def rehearsal_backend_node_gateway_source_ip(
    observed: Mapping[str, object],
    *,
    expected_name: str,
    expected_pod_ips: Sequence[str],
) -> str | None:
    """Derive the backend node CNI gateway that host-network traffic is SNATed through."""
    metadata = observed.get("metadata")
    spec = observed.get("spec")
    if (
        not isinstance(metadata, Mapping)
        or not isinstance(spec, Mapping)
        or observed.get("apiVersion") != "v1"
        or observed.get("kind") != "Node"
        or metadata.get("name") != expected_name
        or not 1 <= len(expected_pod_ips) <= 32
    ):
        return None
    pod_cidr = spec.get("podCIDR")
    if not isinstance(pod_cidr, str) or spec.get("podCIDRs") != [pod_cidr]:
        return None
    try:
        network = ipaddress.ip_network(pod_cidr, strict=True)
        pod_ips = tuple(ipaddress.ip_address(value) for value in expected_pod_ips)
    except ValueError:
        return None
    if (
        not isinstance(network, ipaddress.IPv4Network)
        or network.prefixlen > 30
        or any(
            not isinstance(address, ipaddress.IPv4Address)
            or str(address) != value
            or address not in network
            or address in {network.network_address, network.broadcast_address}
            for value, address in zip(expected_pod_ips, pod_ips, strict=True)
        )
    ):
        return None
    return str(network.network_address + 1)


def _canonical_ingress_source_ips(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not 1 <= len(values) <= 32:
        raise ValueError("invalid ingress source count")
    addresses: list[ipaddress.IPv4Address] = []
    for value in values:
        try:
            address = ipaddress.ip_address(value) if isinstance(value, str) else None
        except ValueError as exc:
            raise ValueError("invalid ingress source address") from exc
        if (
            not isinstance(address, ipaddress.IPv4Address)
            or str(address) != value
            or address.is_unspecified
            or address.is_loopback
            or address.is_link_local
            or address.is_multicast
        ):
            raise ValueError("invalid ingress source address")
        addresses.append(address)
    if len(set(addresses)) != len(addresses):
        raise ValueError("duplicate ingress source address")
    return tuple(str(address) for address in sorted(addresses))


def rehearsal_browser_resource_ready(
    observed: Mapping[str, object],
    *,
    artifact: RehearsalBrowserArtifact,
    plan: RehearsalPlan,
    kind: str,
    name: str,
) -> bool:
    expected = _artifact_resource(artifact, kind=kind, name=name)
    return _contains_expected(observed, expected) and _plan_digest(observed) == plan.plan_digest


def rehearsal_browser_job_complete(
    observed: Mapping[str, object],
    *,
    artifact: RehearsalBrowserArtifact,
    plan: RehearsalPlan,
) -> bool:
    if not rehearsal_browser_resource_ready(
        observed,
        artifact=artifact,
        plan=plan,
        kind="Job",
        name=BROWSER_JOB_NAME,
    ):
        return False
    status = observed.get("status")
    conditions = status.get("conditions") if isinstance(status, Mapping) else None
    return bool(
        isinstance(status, Mapping)
        and status.get("succeeded") == 1
        and status.get("failed") in (None, 0)
        and isinstance(conditions, list)
        and any(
            isinstance(item, Mapping)
            and item.get("type") == "Complete"
            and item.get("status") == "True"
            for item in conditions
        )
    )


def rehearsal_browser_pod_complete(
    observed: Mapping[str, object],
    *,
    artifact: RehearsalBrowserArtifact,
    plan: RehearsalPlan,
    runtime_image_digests: Sequence[str] | None = None,
) -> bool:
    items = observed.get("items")
    if not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], Mapping):
        return False
    pod = items[0]
    metadata = pod.get("metadata")
    status = pod.get("status")
    if not isinstance(metadata, Mapping) or not isinstance(status, Mapping):
        return False
    annotations = metadata.get("annotations")
    labels = metadata.get("labels")
    containers = status.get("containerStatuses")
    init_containers = status.get("initContainerStatuses")
    if (
        not isinstance(labels, Mapping)
        or not isinstance(annotations, Mapping)
        or annotations.get("loom.openai.dev/plan-sha256") != plan.plan_digest
        or labels.get("job-name") != BROWSER_JOB_NAME
        or status.get("phase") != "Succeeded"
        or not isinstance(containers, list)
        or len(containers) != 1
        or not isinstance(containers[0], Mapping)
        or not isinstance(init_containers, list)
        or len(init_containers) != 1
        or not isinstance(init_containers[0], Mapping)
    ):
        return False
    expected_digests = runtime_image_digests or (artifact.browser_image_digest,)
    return _terminated_success(containers[0], expected_digests) and _terminated_success(
        init_containers[0], expected_digests
    )


def rehearsal_browser_report_ready(payload: object, *, plan: RehearsalPlan) -> bool:
    """Validate the complete sanitized schema-v4 rehearsal report."""
    isolation_id = plan.resources.namespace.removeprefix("loom-rehearsal-")
    return browser_report_ready(
        payload,
        authority=RehearsalBrowserReportAuthority(
            plan_sha256=plan.plan_digest,
            isolation_id=isolation_id,
            candidate_sha=plan.candidate_sha,
            route=plan.resources.route,
        ),
    )


def _ingress(plan: RehearsalPlan) -> dict[str, object]:
    route_path = "/" + plan.resources.route.split("/", 3)[-1]
    escaped = route_path.replace(".", r"\.")
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "Ingress",
        "metadata": {
            "annotations": {
                "loom.openai.dev/plan-sha256": plan.plan_digest,
                "nginx.ingress.kubernetes.io/rewrite-target": "/$2",
                "nginx.ingress.kubernetes.io/ssl-redirect": "true",
                "nginx.ingress.kubernetes.io/use-regex": "true",
            },
            "name": BROWSER_INGRESS_NAME,
            "namespace": plan.resources.namespace,
        },
        "spec": {
            "ingressClassName": "nginx",
            "rules": [
                {
                    "host": "yylx.world",
                    "http": {
                        "paths": [
                            {
                                "backend": {
                                    "service": {"name": "loom-service", "port": {"number": 8090}}
                                },
                                "path": f"{escaped}(/|$)(api(?:/|$).*)",
                                "pathType": "ImplementationSpecific",
                            },
                            {
                                "backend": {
                                    "service": {"name": "loom-web", "port": {"number": 80}}
                                },
                                "path": f"{escaped}(/|$)(.*)",
                                "pathType": "ImplementationSpecific",
                            },
                        ]
                    },
                }
            ],
            "tls": [{"hosts": ["yylx.world"], "secretName": "loom-staging-tls"}],
        },
    }


def _browser_job(plan: RehearsalPlan, *, ingress_ip: str) -> dict[str, object]:
    isolation_id = plan.resources.namespace.removeprefix("loom-rehearsal-")
    image = rehearsal_image_reference(plan, BROWSER_IMAGE)
    labels = {
        "app": BROWSER_JOB_NAME,
        "loom.openai.dev/candidate-sha": plan.candidate_sha,
    }
    locked_container = {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
        "runAsGroup": 1000,
        "runAsNonRoot": True,
        "runAsUser": 1000,
    }
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "annotations": {"loom.openai.dev/plan-sha256": plan.plan_digest},
            "labels": labels,
            "name": BROWSER_JOB_NAME,
            "namespace": plan.resources.namespace,
        },
        "spec": {
            "activeDeadlineSeconds": 900,
            # Bounded retry (#1085 phase 2): a headless-browser smoke fails
            # transiently (a flaky page load / cold-start), and with no retry a
            # single blip marked the whole restore rehearsal `job-not-complete`
            # — a transient treated as durable that repeatedly blocked deploys.
            # Each attempt self-caps at --timeout-ms=60s, so 3 attempts (~180s)
            # stay well within the 900s job deadline. A genuinely broken browser
            # still fails after the bound, so restore verification is unweakened.
            "backoffLimit": 2,
            "completions": 1,
            "parallelism": 1,
            "template": {
                "metadata": {
                    "annotations": {"loom.openai.dev/plan-sha256": plan.plan_digest},
                    "labels": labels,
                },
                "spec": {
                    "automountServiceAccountToken": False,
                    "containers": [
                        {
                            "args": [
                                "--route",
                                plan.resources.route,
                                "--expected-deployed-sha",
                                plan.candidate_sha,
                                "--admin-token-source",
                                "file:/run/secrets/admin-token",
                                "--username",
                                BROWSER_ACCEPTANCE_USERNAME,
                                "--report",
                                _REPORT_PATH,
                                "--rehearsal-plan-sha256",
                                plan.plan_digest,
                                "--rehearsal-isolation-id",
                                isolation_id,
                                "--emit-sanitized-report",
                                "--timeout-ms",
                                "60000",
                            ],
                            "env": [
                                {"name": "HOME", "value": "/tmp"},
                                {"name": "TMPDIR", "value": "/tmp"},
                            ],
                            "image": image,
                            "imagePullPolicy": rehearsal_image_pull_policy(plan),
                            "name": "browser",
                            "resources": {
                                "limits": {"cpu": "2", "memory": "2Gi"},
                                "requests": {"cpu": "250m", "memory": "512Mi"},
                            },
                            "securityContext": locked_container,
                            "volumeMounts": [
                                {"mountPath": "/dev/shm", "name": "shm"},
                                {"mountPath": "/evidence", "name": "evidence"},
                                {
                                    "mountPath": "/run/secrets",
                                    "name": "prepared-token",
                                    "readOnly": True,
                                },
                                {"mountPath": "/tmp", "name": "tmp"},
                            ],
                        }
                    ],
                    "dnsPolicy": "ClusterFirst",
                    "enableServiceLinks": False,
                    "hostAliases": [{"hostnames": ["yylx.world"], "ip": ingress_ip}],
                    "initContainers": [
                        {
                            "args": [
                                "umask 077; cp /source/admin-token /prepared/admin-token; "
                                "chmod 0600 /prepared/admin-token; "
                                'test "$(stat -c %a /prepared/admin-token)" = 600'
                            ],
                            "command": ["/bin/sh", "-ceu"],
                            "image": image,
                            "imagePullPolicy": rehearsal_image_pull_policy(plan),
                            "name": "prepare-token",
                            "resources": {
                                "limits": {"cpu": "100m", "memory": "64Mi"},
                                "requests": {"cpu": "10m", "memory": "16Mi"},
                            },
                            "securityContext": locked_container,
                            "volumeMounts": [
                                {"mountPath": "/prepared", "name": "prepared-token"},
                                {"mountPath": "/source", "name": "source-token", "readOnly": True},
                            ],
                        }
                    ],
                    "restartPolicy": "Never",
                    "securityContext": {
                        "fsGroup": 1000,
                        "fsGroupChangePolicy": "OnRootMismatch",
                        "runAsGroup": 1000,
                        "runAsNonRoot": True,
                        "runAsUser": 1000,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "serviceAccountName": "default",
                    "terminationGracePeriodSeconds": 10,
                    "volumes": [
                        {"emptyDir": {"sizeLimit": "2Mi"}, "name": "evidence"},
                        {
                            "emptyDir": {"medium": "Memory", "sizeLimit": "64Ki"},
                            "name": "prepared-token",
                        },
                        {"emptyDir": {"medium": "Memory", "sizeLimit": "512Mi"}, "name": "shm"},
                        {
                            "name": "source-token",
                            "secret": {
                                "defaultMode": 0o440,
                                "items": [{"key": "admin-token", "path": "admin-token"}],
                                "secretName": "loom-admin-secret",
                            },
                        },
                        {"emptyDir": {"sizeLimit": "1Gi"}, "name": "tmp"},
                    ],
                },
            },
            "ttlSecondsAfterFinished": 600,
        },
    }


def _browser_network_policy(plan: RehearsalPlan) -> dict[str, object]:
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "annotations": {"loom.openai.dev/plan-sha256": plan.plan_digest},
            "name": BROWSER_NETWORK_POLICY_NAME,
            "namespace": plan.resources.namespace,
        },
        "spec": {
            "egress": [
                {
                    "ports": [{"port": 443, "protocol": "TCP"}],
                    "to": [
                        {
                            "namespaceSelector": {
                                "matchLabels": {
                                    "kubernetes.io/metadata.name": INGRESS_CONTROLLER_NAMESPACE,
                                }
                            },
                            "podSelector": {
                                "matchLabels": {
                                    "app.kubernetes.io/component": "controller",
                                    "app.kubernetes.io/instance": "ingress-nginx",
                                    "app.kubernetes.io/name": "ingress-nginx",
                                }
                            },
                        }
                    ],
                }
            ],
            "podSelector": {"matchLabels": {"app": BROWSER_JOB_NAME}},
            "policyTypes": ["Egress"],
        },
    }


def _browser_ingress_network_policy(
    plan: RehearsalPlan,
    *,
    ingress_source_ips: Sequence[str],
) -> dict[str, object]:
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "annotations": {"loom.openai.dev/plan-sha256": plan.plan_digest},
            "name": BROWSER_INGRESS_NETWORK_POLICY_NAME,
            "namespace": plan.resources.namespace,
        },
        "spec": {
            "ingress": [
                {
                    "from": [
                        {"ipBlock": {"cidr": f"{address}/32"}}
                        for address in ingress_source_ips
                    ],
                    "ports": [
                        {"port": 8080, "protocol": "TCP"},
                        {"port": 8090, "protocol": "TCP"},
                    ],
                }
            ],
            "podSelector": {
                "matchExpressions": [
                    {
                        "key": "app",
                        "operator": "In",
                        "values": ["loom-service", "loom-web"],
                    }
                ]
            },
            "policyTypes": ["Ingress"],
        },
    }


def _artifact_resource(
    artifact: RehearsalBrowserArtifact,
    *,
    kind: str,
    name: str,
) -> Mapping[str, object]:
    try:
        resources = list(yaml.safe_load_all(artifact.payload))
    except yaml.YAMLError as exc:  # pragma: no cover - guarded by digest
        raise ValueError("rehearsal browser artifact is invalid") from exc
    matches = [
        item
        for item in resources
        if isinstance(item, Mapping)
        and item.get("kind") == kind
        and isinstance(item.get("metadata"), Mapping)
        and item["metadata"].get("name") == name
    ]
    if len(matches) != 1:
        raise ValueError("rehearsal browser artifact resource is invalid")
    return matches[0]


def _plan_digest(value: Mapping[str, object]) -> str | None:
    metadata = value.get("metadata")
    annotations = metadata.get("annotations") if isinstance(metadata, Mapping) else None
    digest = (
        annotations.get("loom.openai.dev/plan-sha256") if isinstance(annotations, Mapping) else None
    )
    return digest if isinstance(digest, str) else None


def _terminated_success(value: Mapping[str, object], expected_digests: Sequence[str]) -> bool:
    state = value.get("state")
    terminated = state.get("terminated") if isinstance(state, Mapping) else None
    image_id = value.get("imageID")
    return bool(
        isinstance(image_id, str)
        and any(image_id.endswith(digest) for digest in expected_digests)
        and isinstance(terminated, Mapping)
        and terminated.get("exitCode") == 0
        and terminated.get("reason") == "Completed"
    )


def _contains_expected(observed: object, expected: object) -> bool:
    if isinstance(expected, Mapping):
        return isinstance(observed, Mapping) and all(
            key in observed and _contains_expected(observed[key], item)
            for key, item in expected.items()
        )
    if isinstance(expected, list):
        return (
            isinstance(observed, list)
            and len(observed) == len(expected)
            and all(
                _contains_expected(item, expected_item)
                for item, expected_item in zip(observed, expected, strict=True)
            )
        )
    return observed == expected


__all__ = [
    "BROWSER_ACCEPTANCE_USERNAME",
    "BROWSER_INGRESS_NAME",
    "BROWSER_INGRESS_NETWORK_POLICY_NAME",
    "BROWSER_JOB_NAME",
    "BROWSER_NETWORK_POLICY_NAME",
    "BROWSER_REPORT_CHECK_IDS",
    "INGRESS_CONTROLLER_NAMESPACE",
    "INGRESS_CONTROLLER_SERVICE",
    "RehearsalBrowserArtifact",
    "build_rehearsal_browser_artifact",
    "ingress_controller_ip",
    "rehearsal_backend_endpoints",
    "rehearsal_backend_node_gateway_source_ip",
    "rehearsal_browser_job_complete",
    "rehearsal_browser_pod_complete",
    "rehearsal_browser_report_ready",
    "rehearsal_browser_resource_ready",
]
