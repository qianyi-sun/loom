"""Pure exact-artifact transformation for an isolated candidate release."""

from __future__ import annotations

import copy
import hashlib
import os
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.rehearsal_action_source import RehearsalPlan

_MAX_RENDERED_BYTES = 16 * 1024 * 1024
_DEPLOYMENT_IMAGES = {
    "loom-control-plane": "loom-control-plane",
    "loom-service": "loom-service",
    "loom-web": "loom-web",
}
_SERVICE_NAMES = ("loom-control-plane", "loom-postgres", "loom-service", "loom-web")
_EXPECTED_IDENTITIES = {
    *(("apps/v1", "Deployment", name) for name in _DEPLOYMENT_IMAGES),
    *(("v1", "Service", name) for name in _SERVICE_NAMES),
}


@dataclass(frozen=True, slots=True)
class RehearsalReleaseArtifact:
    """Non-sensitive manifest bytes and exact rollout image bindings."""

    payload: bytes
    artifact_sha256: str
    deployment_images: Mapping[str, str]
    deployment_selectors: Mapping[str, Mapping[str, str]]
    resource_count: int

    def __post_init__(self) -> None:
        images = dict(self.deployment_images)
        selectors = {name: dict(value) for name, value in self.deployment_selectors.items()}
        if (
            not self.payload
            or hashlib.sha256(self.payload).hexdigest() != self.artifact_sha256
            or set(images) != set(_DEPLOYMENT_IMAGES)
            or set(selectors) != set(_DEPLOYMENT_IMAGES)
            or any(not selector for selector in selectors.values())
            or self.resource_count != len(_EXPECTED_IDENTITIES) + 1
        ):
            raise ValueError("rehearsal release artifact identity is invalid")
        object.__setattr__(self, "deployment_images", MappingProxyType(images))
        object.__setattr__(
            self,
            "deployment_selectors",
            MappingProxyType(
                {name: MappingProxyType(selector) for name, selector in selectors.items()}
            ),
        )


def build_rehearsal_release_artifact(
    plan: RehearsalPlan,
    *,
    service_uid: int | None = None,
) -> RehearsalReleaseArtifact:
    """Read the immutable Tier-1 render and derive one isolated release set."""
    uid = os.geteuid() if service_uid is None else service_uid
    trusted = read_trusted_file(
        plan.rendered_manifest_path,
        service_uid=uid,
        private=True,
        max_bytes=_MAX_RENDERED_BYTES,
        require_nonempty=True,
    )
    if hashlib.sha256(trusted.payload).hexdigest() != plan.rendered_manifest_sha256:
        raise ValueError("rehearsal rendered manifest digest drifted")
    try:
        loaded = list(yaml.safe_load_all(trusted.payload))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ValueError("rehearsal rendered manifest is invalid") from exc
    resources: dict[tuple[str, str, str], dict[str, object]] = {}
    for item in loaded:
        if item is None:
            continue
        if not isinstance(item, dict):
            raise ValueError("rehearsal rendered resource is invalid")
        metadata = item.get("metadata")
        identity = (
            item.get("apiVersion"),
            item.get("kind"),
            metadata.get("name") if isinstance(metadata, Mapping) else None,
        )
        if identity in _EXPECTED_IDENTITIES:
            if identity in resources:
                raise ValueError("rehearsal rendered resource is duplicated")
            resources[identity] = item
    if set(resources) != _EXPECTED_IDENTITIES:
        raise ValueError("rehearsal rendered release subset is incomplete")

    output: list[dict[str, object]] = []
    deployment_images: dict[str, str] = {}
    deployment_selectors: dict[str, dict[str, str]] = {}
    for name, image_name in _DEPLOYMENT_IMAGES.items():
        deployment, selector = _isolate_deployment(
            resources[("apps/v1", "Deployment", name)],
            plan=plan,
            name=name,
            image_name=image_name,
        )
        output.append(deployment)
        deployment_images[name] = plan.image_digests[image_name]
        deployment_selectors[name] = selector
    for name in _SERVICE_NAMES:
        output.append(
            _isolate_service(
                resources[("v1", "Service", name)],
                plan=plan,
                name=name,
            )
        )
    output.append(_network_policy(plan))
    payload = yaml.safe_dump_all(output, sort_keys=True).encode()
    return RehearsalReleaseArtifact(
        payload=payload,
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
        deployment_images=deployment_images,
        deployment_selectors=deployment_selectors,
        resource_count=len(output),
    )


def _isolate_deployment(
    source: dict[str, object],
    *,
    plan: RehearsalPlan,
    name: str,
    image_name: str,
) -> tuple[dict[str, object], dict[str, str]]:
    value = copy.deepcopy(source)
    metadata = _mapping(value.get("metadata"), label="deployment metadata")
    spec = _mapping(value.get("spec"), label="deployment spec")
    selector_record = _mapping(spec.get("selector"), label="deployment selector")
    selector = _string_map(
        selector_record.get("matchLabels"),
        label="deployment selector labels",
    )
    template = _mapping(spec.get("template"), label="deployment template")
    template_metadata = _mapping(template.get("metadata"), label="pod metadata")
    template_labels = _string_map(template_metadata.get("labels"), label="pod labels")
    if any(template_labels.get(key) != item for key, item in selector.items()):
        raise ValueError("rehearsal deployment selector drifted")
    pod = _mapping(template.get("spec"), label="pod spec")
    if pod.get("initContainers") not in (None, []) or pod.get("ephemeralContainers") not in (
        None,
        [],
    ):
        raise ValueError("rehearsal deployment auxiliary containers are forbidden")
    containers = pod.get("containers")
    if (
        not isinstance(containers, list)
        or len(containers) != 1
        or not isinstance(containers[0], dict)
    ):
        raise ValueError("rehearsal deployment container set is invalid")
    container = containers[0]
    image = container.get("image")
    expected_image = f"{image_name}:{plan.image_tag}"
    if image != expected_image or plan.image_digests.get(image_name) is None:
        raise ValueError("rehearsal deployment image binding drifted")
    run_as_user = 101 if name == "loom-web" else 10001
    container["securityContext"] = {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
        "runAsGroup": run_as_user,
        "runAsNonRoot": True,
        "runAsUser": run_as_user,
    }
    environment = container.get("env")
    if environment is None:
        environment = []
        container["env"] = environment
    if not isinstance(environment, list) or any(not isinstance(item, dict) for item in environment):
        raise ValueError("rehearsal deployment environment is invalid")
    _rewrite_environment(environment, plan=plan)
    environment.extend(
        (
            {"name": "HOME", "value": "/tmp"},
            {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
            {"name": "TMPDIR", "value": "/tmp"},
        )
    )
    mounts = container.get("volumeMounts")
    if mounts is None:
        mounts = []
        container["volumeMounts"] = mounts
    if not isinstance(mounts, list) or any(
        not isinstance(item, dict) or item.get("name") != "loom-admin-secret" for item in mounts
    ):
        raise ValueError("rehearsal deployment volume authority is invalid")
    mounts.append({"mountPath": "/tmp", "name": "loom-rehearsal-tmp"})
    volumes = pod.get("volumes")
    if volumes is None:
        volumes = []
        pod["volumes"] = volumes
    if not isinstance(volumes, list):
        raise ValueError("rehearsal deployment volume authority is invalid")
    if volumes:
        if len(volumes) != 1 or not isinstance(volumes[0], dict):
            raise ValueError("rehearsal deployment volume authority is invalid")
        secret = volumes[0].get("secret")
        if (
            volumes[0].get("name") != "loom-admin-secret"
            or not isinstance(secret, dict)
            or secret.get("secretName") != "loom-admin-secret"
        ):
            raise ValueError("rehearsal deployment volume authority is invalid")
        secret["defaultMode"] = 0o440
    volumes.append({"emptyDir": {"sizeLimit": "128Mi"}, "name": "loom-rehearsal-tmp"})
    forbidden = {
        "affinity",
        "dnsConfig",
        "hostAliases",
        "hostIPC",
        "hostNetwork",
        "hostPID",
        "hostname",
        "imagePullSecrets",
        "nodeName",
        "nodeSelector",
        "overhead",
        "preemptionPolicy",
        "priorityClassName",
        "runtimeClassName",
        "schedulerName",
        "shareProcessNamespace",
        "subdomain",
        "tolerations",
        "topologySpreadConstraints",
    }
    for key in forbidden:
        pod.pop(key, None)
    pod["automountServiceAccountToken"] = False
    pod["dnsPolicy"] = "ClusterFirst"
    pod["enableServiceLinks"] = False
    pod["securityContext"] = {
        "fsGroup": run_as_user,
        "runAsGroup": run_as_user,
        "runAsNonRoot": True,
        "runAsUser": run_as_user,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    pod["serviceAccountName"] = "default"
    metadata["namespace"] = plan.resources.namespace
    _bind_metadata(metadata, plan=plan)
    _bind_metadata(template_metadata, plan=plan)
    spec["replicas"] = 1
    value["metadata"] = metadata
    value["spec"] = spec
    template["metadata"] = template_metadata
    template["spec"] = pod
    spec["template"] = template
    _reject_unsafe_fields(value)
    return value, selector


def _isolate_service(
    source: dict[str, object],
    *,
    plan: RehearsalPlan,
    name: str,
) -> dict[str, object]:
    value = copy.deepcopy(source)
    metadata = _mapping(value.get("metadata"), label="service metadata")
    spec = _mapping(value.get("spec"), label="service spec")
    metadata["namespace"] = plan.resources.namespace
    _bind_metadata(metadata, plan=plan)
    spec.pop("clusterIP", None)
    spec.pop("clusterIPs", None)
    spec.pop("ipFamilies", None)
    spec.pop("ipFamilyPolicy", None)
    spec.pop("healthCheckNodePort", None)
    forbidden = {
        "allocateLoadBalancerNodePorts",
        "externalIPs",
        "externalName",
        "loadBalancerClass",
        "loadBalancerIP",
        "loadBalancerSourceRanges",
    }
    if any(key in spec for key in forbidden):
        raise ValueError("rehearsal service contains external authority")
    ports = spec.get("ports")
    if (
        not isinstance(ports, list)
        or not ports
        or any(not isinstance(port, dict) or "nodePort" in port for port in ports)
    ):
        raise ValueError("rehearsal service port authority is invalid")
    if name == "loom-postgres":
        spec["selector"] = {"loom.openai.dev/component": "rehearsal-database"}
    else:
        _string_map(spec.get("selector"), label="service selector")
    spec = {
        "ports": ports,
        "selector": spec["selector"],
        "type": "ClusterIP",
    }
    value["metadata"] = metadata
    value["spec"] = spec
    _reject_unsafe_fields(value)
    return value


def _rewrite_environment(environment: list[dict[str, object]], *, plan: RehearsalPlan) -> None:
    route_path = "/" + plan.resources.route.split("/", 3)[-1]
    origin = plan.resources.route.split(route_path, 1)[0]
    replacements = {
        "LOOM_ENV": "staging",
        "LOOM_NAMESPACE": plan.resources.namespace,
        "LOOM_FRONTEND_API_BASE": route_path,
        "LOOM_FRONTEND_ROUTE_PATH": route_path,
        "LOOM_FRONTEND_PUBLIC_ORIGIN": origin,
        "LOOM_SVC_PUBLIC_BASE_URL": plan.resources.route,
    }
    seen: set[str] = set()
    for item in environment:
        name = item.get("name")
        if not isinstance(name, str) or not name or name in seen:
            raise ValueError("rehearsal deployment environment identity drifted")
        seen.add(name)
        if name in replacements:
            item.clear()
            item.update({"name": name, "value": replacements[name]})
    if seen & {"HOME", "PYTHONDONTWRITEBYTECODE", "TMPDIR"}:
        raise ValueError("rehearsal deployment scratch environment drifted")


def _network_policy(plan: RehearsalPlan) -> dict[str, object]:
    same_namespace = {
        "namespaceSelector": {
            "matchLabels": {"kubernetes.io/metadata.name": plan.resources.namespace}
        }
    }
    dns_namespace = {
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}},
        "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
    }
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": {
            "annotations": {"loom.openai.dev/plan-sha256": plan.plan_digest},
            "name": "loom-rehearsal-release",
            "namespace": plan.resources.namespace,
        },
        "spec": {
            "egress": [
                {"to": [same_namespace]},
                {
                    "ports": [
                        {"port": 53, "protocol": "UDP"},
                        {"port": 53, "protocol": "TCP"},
                    ],
                    "to": [dns_namespace],
                },
            ],
            "ingress": [{"from": [same_namespace]}],
            "podSelector": {},
            "policyTypes": ["Ingress", "Egress"],
        },
    }


def _bind_metadata(metadata: dict[str, object], *, plan: RehearsalPlan) -> None:
    annotations = metadata.get("annotations")
    if annotations is None:
        annotations = {}
    if not isinstance(annotations, dict):
        raise ValueError("rehearsal resource annotations are invalid")
    annotations.update(
        {
            "loom.openai.dev/candidate-sha": plan.candidate_sha,
            "loom.openai.dev/candidate-tree": plan.candidate_tree,
            "loom.openai.dev/plan-sha256": plan.plan_digest,
        }
    )
    metadata["annotations"] = annotations


def _reject_unsafe_fields(value: object) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"hostPath", "hostPort", "privileged"}:
                raise ValueError("rehearsal resource contains forbidden host authority")
            _reject_unsafe_fields(item)
    elif isinstance(value, list):
        for item in value:
            _reject_unsafe_fields(item)


def _mapping(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"rehearsal {label} is invalid")
    return value


def _string_map(value: object, *, label: str) -> dict[str, str]:
    if (
        not isinstance(value, Mapping)
        or not value
        or not all(
            isinstance(key, str) and key and isinstance(item, str) and item
            for key, item in value.items()
        )
    ):
        raise ValueError(f"rehearsal {label} is invalid")
    return dict(value)


__all__ = ["RehearsalReleaseArtifact", "build_rehearsal_release_artifact"]
