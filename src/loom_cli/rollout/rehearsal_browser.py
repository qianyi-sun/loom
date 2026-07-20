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
from loom_cli.rollout.rehearsal_action_source import RehearsalPlan

BROWSER_JOB_NAME = "loom-rehearsal-browser"
BROWSER_INGRESS_NAME = "loom-rehearsal-browser"
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
    browser_image_digest: str
    resource_count: int

    def __post_init__(self) -> None:
        try:
            address = ipaddress.ip_address(self.ingress_ip)
        except ValueError as exc:
            raise ValueError("rehearsal browser ingress identity is invalid") from exc
        if (
            not self.payload
            or address.version != 4
            or not self.browser_image_digest.startswith("sha256:")
            or len(self.browser_image_digest) != 71
            or hashlib.sha256(self.payload).hexdigest() != self.artifact_sha256
            or self.resource_count != 3
        ):
            raise ValueError("rehearsal browser artifact identity is invalid")


def build_rehearsal_browser_artifact(
    plan: RehearsalPlan,
    *,
    ingress_ip: str,
) -> RehearsalBrowserArtifact:
    """Build one route, one browser Job and its exact egress authority."""
    plan.resources.require_isolated()
    expected_route = "https://yylx.world/dev/rehearsal/" + plan.resources.namespace.removeprefix(
        "loom-rehearsal-"
    )
    if plan.resources.route != expected_route:
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
    resources = (
        _ingress(plan),
        _browser_job(plan, ingress_ip=ingress_ip),
        _browser_network_policy(plan, ingress_ip=ingress_ip),
    )
    payload = yaml.safe_dump_all(resources, sort_keys=True).encode()
    return RehearsalBrowserArtifact(
        payload=payload,
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
        ingress_ip=ingress_ip,
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
        or spec.get("type") != "ClusterIP"
    ):
        return None
    value = spec.get("clusterIP")
    try:
        address = ipaddress.ip_address(value) if isinstance(value, str) else None
    except ValueError:
        return None
    return str(address) if address is not None and address.version == 4 else None


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
    labels = metadata.get("labels")
    containers = status.get("containerStatuses")
    init_containers = status.get("initContainerStatuses")
    if (
        not isinstance(labels, Mapping)
        or labels.get("loom.openai.dev/plan-sha256") != plan.plan_digest
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
                                "path": f"^{escaped}(/|$)(api(?:/|$).*)",
                                "pathType": "ImplementationSpecific",
                            },
                            {
                                "backend": {
                                    "service": {"name": "loom-web", "port": {"number": 80}}
                                },
                                "path": f"^{escaped}(/|$)(.*)",
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
    image = f"{BROWSER_IMAGE}:{plan.image_tag}"
    labels = {
        "app": BROWSER_JOB_NAME,
        "loom.openai.dev/candidate-sha": plan.candidate_sha,
        "loom.openai.dev/plan-sha256": plan.plan_digest,
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
            "backoffLimit": 0,
            "completions": 1,
            "parallelism": 1,
            "template": {
                "metadata": {"labels": labels},
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
                            "imagePullPolicy": "Never",
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
                            "imagePullPolicy": "Never",
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


def _browser_network_policy(plan: RehearsalPlan, *, ingress_ip: str) -> dict[str, object]:
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
                    "to": [{"ipBlock": {"cidr": f"{ingress_ip}/32"}}],
                }
            ],
            "podSelector": {"matchLabels": {"app": BROWSER_JOB_NAME}},
            "policyTypes": ["Egress"],
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
    "BROWSER_JOB_NAME",
    "BROWSER_NETWORK_POLICY_NAME",
    "BROWSER_REPORT_CHECK_IDS",
    "INGRESS_CONTROLLER_NAMESPACE",
    "INGRESS_CONTROLLER_SERVICE",
    "RehearsalBrowserArtifact",
    "build_rehearsal_browser_artifact",
    "ingress_controller_ip",
    "rehearsal_browser_job_complete",
    "rehearsal_browser_pod_complete",
    "rehearsal_browser_report_ready",
    "rehearsal_browser_resource_ready",
]
