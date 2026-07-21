"""Pure exact-artifact transformation for an isolated candidate release."""

from __future__ import annotations

import copy
import hashlib
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.rehearsal_action_source import RehearsalPlan

_MAX_RENDERED_BYTES = 16 * 1024 * 1024
_LABEL_KEY_RE = re.compile(
    r"(?:(?:[a-z0-9](?:[-a-z0-9.]{0,251}[a-z0-9])?)/)?"
    r"[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?\Z"
)
_LABEL_VALUE_RE = re.compile(r"[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?\Z")
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
            or any(
                _LABEL_KEY_RE.fullmatch(key) is None or _LABEL_VALUE_RE.fullmatch(item) is None
                for selector in selectors.values()
                for key, item in selector.items()
            )
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
    _rewrite_environment(environment, plan=plan, deployment_name=name)
    environment.extend(
        (
            {"name": "HOME", "value": "/tmp"},
            {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
            {"name": "TMPDIR", "value": "/tmp"},
        )
    )
    _canonicalize_resource_quantities(container)
    mounts = container.get("volumeMounts")
    if mounts is None:
        mounts = []
        container["volumeMounts"] = mounts
    if not isinstance(mounts, list) or any(
        not isinstance(item, dict) or item.get("name") != "loom-admin-secret" for item in mounts
    ):
        raise ValueError("rehearsal deployment volume authority is invalid")
    if name == "loom-service":
        mounts.append(
            {
                "mountPath": "/var/run/loom/rehearsal-admin/secrets.toml",
                "name": "loom-admin-secret",
                "readOnly": True,
                "subPath": "secrets.toml",
            }
        )
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
    _drop_null_mapping_fields(value)
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


def _rewrite_environment(
    environment: list[dict[str, object]],
    *,
    plan: RehearsalPlan,
    deployment_name: str,
) -> None:
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
    if deployment_name == "loom-web":
        environment.append(
            {
                "name": "LOOM_FRONTEND_REHEARSAL_ID",
                "value": plan.resources.namespace.removeprefix("loom-rehearsal-"),
            }
        )


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
    ingress_controller = {
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "ingress-nginx"}},
        "podSelector": {"matchLabels": {"app.kubernetes.io/component": "controller"}},
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
            "ingress": [{"from": [same_namespace, ingress_controller]}],
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


def _canonicalize_resource_quantities(container: dict[str, object]) -> None:
    resources = container.get("resources")
    if resources is None:
        return
    resources = _mapping(resources, label="container resources")
    for boundary in ("limits", "requests"):
        quantities = resources.get(boundary)
        if quantities is None:
            continue
        quantities = _mapping(quantities, label=f"container resource {boundary}")
        for name, value in quantities.items():
            if (
                not isinstance(name, str)
                or not name
                or isinstance(value, bool)
                or not isinstance(value, (int, str))
            ):
                raise ValueError("rehearsal container resource quantity is invalid")
            quantities[name] = str(value)
        resources[boundary] = quantities
    container["resources"] = resources


def _drop_null_mapping_fields(value: object) -> None:
    if isinstance(value, dict):
        for key in tuple(value):
            item = value[key]
            if item is None:
                value.pop(key)
                continue
            _drop_null_mapping_fields(item)
    elif isinstance(value, list):
        for item in value:
            if item is None:
                raise ValueError("rehearsal resource contains a null list entry")
            _drop_null_mapping_fields(item)


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


def rehearsal_selector_argument(
    artifact: RehearsalReleaseArtifact,
    deployment_name: str,
) -> str:
    """Return the already-validated exact selector for one release workload."""
    selector = artifact.deployment_selectors.get(deployment_name)
    if selector is None:
        raise ValueError("rehearsal release selector identity is invalid")
    return ",".join(f"{key}={selector[key]}" for key in sorted(selector))


def rehearsal_deployment_ready(
    observed: Mapping[str, object],
    *,
    artifact: RehearsalReleaseArtifact,
    plan: RehearsalPlan,
    deployment_name: str,
) -> bool:
    expected = _artifact_resource(artifact, kind="Deployment", name=deployment_name)
    metadata = observed.get("metadata")
    status = observed.get("status")
    if not isinstance(metadata, Mapping) or not isinstance(status, Mapping):
        return False
    generation = metadata.get("generation")
    return bool(
        _contains_expected(observed, expected)
        and type(generation) is int
        and generation > 0
        and status.get("observedGeneration") == generation
        and status.get("availableReplicas") == 1
        and status.get("readyReplicas") == 1
        and status.get("replicas") == 1
        and status.get("updatedReplicas") == 1
        and status.get("unavailableReplicas") in (None, 0)
        and _resource_plan_digest(observed) == plan.plan_digest
    )


def rehearsal_service_ready(
    observed: Mapping[str, object],
    *,
    artifact: RehearsalReleaseArtifact,
    plan: RehearsalPlan,
    service_name: str,
) -> bool:
    expected = _artifact_resource(artifact, kind="Service", name=service_name)
    return bool(
        _contains_expected(observed, expected)
        and _resource_plan_digest(observed) == plan.plan_digest
    )


def rehearsal_network_policy_ready(
    observed: Mapping[str, object],
    *,
    artifact: RehearsalReleaseArtifact,
    plan: RehearsalPlan,
) -> bool:
    expected = _artifact_resource(
        artifact,
        kind="NetworkPolicy",
        name="loom-rehearsal-release",
    )
    return bool(
        _contains_expected(observed, expected)
        and _resource_plan_digest(observed) == plan.plan_digest
    )


def rehearsal_pods_ready(
    observed: Mapping[str, object],
    *,
    artifact: RehearsalReleaseArtifact,
    deployment_name: str,
    runtime_image_digests: Sequence[str] | None = None,
) -> bool:
    expected = _artifact_resource(artifact, kind="Deployment", name=deployment_name)
    expected_container = _deployment_container(expected)
    expected_selector = artifact.deployment_selectors.get(deployment_name)
    items = observed.get("items")
    if (
        not isinstance(expected_selector, Mapping)
        or not isinstance(items, list)
        or len(items) != 1
        or not isinstance(items[0], Mapping)
    ):
        return False
    pod = items[0]
    metadata = pod.get("metadata")
    status = pod.get("status")
    if not isinstance(metadata, Mapping) or not isinstance(status, Mapping):
        return False
    labels = metadata.get("labels")
    conditions = status.get("conditions")
    container_statuses = status.get("containerStatuses")
    if (
        not isinstance(labels, Mapping)
        or any(labels.get(key) != value for key, value in expected_selector.items())
        or status.get("phase") != "Running"
        or not isinstance(conditions, list)
        or not any(
            isinstance(condition, Mapping)
            and condition.get("type") == "Ready"
            and condition.get("status") == "True"
            for condition in conditions
        )
        or not isinstance(container_statuses, list)
        or len(container_statuses) != 1
        or not isinstance(container_statuses[0], Mapping)
    ):
        return False
    image_id = container_statuses[0].get("imageID")
    image_match = (
        re.search(r"sha256:[0-9a-f]{64}\Z", image_id) if isinstance(image_id, str) else None
    )
    image_digest = image_match.group(0) if image_match is not None else None
    expected_digests = runtime_image_digests or (artifact.deployment_images[deployment_name],)
    return bool(
        container_statuses[0].get("name") == expected_container["name"]
        and container_statuses[0].get("ready") is True
        and image_digest in expected_digests
    )


def _artifact_resource(
    artifact: RehearsalReleaseArtifact,
    *,
    kind: str,
    name: str,
) -> Mapping[str, object]:
    try:
        resources = list(yaml.safe_load_all(artifact.payload))
    except yaml.YAMLError as exc:  # guarded by artifact digest, defensive only
        raise ValueError("rehearsal release artifact is invalid") from exc
    matches = [
        resource
        for resource in resources
        if isinstance(resource, Mapping)
        and resource.get("kind") == kind
        and isinstance(resource.get("metadata"), Mapping)
        and resource["metadata"].get("name") == name
    ]
    if len(matches) != 1:
        raise ValueError("rehearsal release artifact resource is invalid")
    return matches[0]


def _deployment_container(resource: Mapping[str, object]) -> Mapping[str, object]:
    spec = resource.get("spec")
    template = spec.get("template") if isinstance(spec, Mapping) else None
    pod = template.get("spec") if isinstance(template, Mapping) else None
    containers = pod.get("containers") if isinstance(pod, Mapping) else None
    if (
        not isinstance(containers, list)
        or len(containers) != 1
        or not isinstance(containers[0], Mapping)
    ):
        raise ValueError("rehearsal release artifact container is invalid")
    return containers[0]


def _resource_plan_digest(resource: Mapping[str, object]) -> str | None:
    metadata = resource.get("metadata")
    annotations = metadata.get("annotations") if isinstance(metadata, Mapping) else None
    value = (
        annotations.get("loom.openai.dev/plan-sha256") if isinstance(annotations, Mapping) else None
    )
    return value if isinstance(value, str) else None


def _contains_expected(observed: object, expected: object) -> bool:
    if isinstance(expected, Mapping):
        return isinstance(observed, Mapping) and all(
            key in observed and _contains_expected(observed[key], value)
            for key, value in expected.items()
        )
    if isinstance(expected, list):
        return (
            isinstance(observed, list)
            and len(observed) == len(expected)
            and all(
                _contains_expected(item, value)
                for item, value in zip(observed, expected, strict=True)
            )
        )
    return observed == expected


__all__ = [
    "RehearsalReleaseArtifact",
    "build_rehearsal_release_artifact",
    "rehearsal_deployment_ready",
    "rehearsal_network_policy_ready",
    "rehearsal_pods_ready",
    "rehearsal_selector_argument",
    "rehearsal_service_ready",
]
