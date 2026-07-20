"""Render-once Kubernetes artifacts and fail-closed schema checks."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from threading import Lock
from types import MappingProxyType
from typing import Protocol

import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.image_readiness import ALL_BUILD_IMAGES, ROLLOUT_IMAGES

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_IMAGE_TAG_RE = re.compile(r"^staging-[a-z0-9][a-z0-9-]{5,63}$")
_DNS_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_MAX_RENDERED_BYTES = 16 * 1024 * 1024
_MAX_RESOURCES = 512


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...


RenderManifest = Callable[[], str]
ServerDryRun = Callable[[str], CommandResult]


@dataclass(frozen=True, slots=True)
class ManifestArtifact:
    rendered_yaml: str
    rendered_sha256: str
    resource_count: int
    resource_set_digest: str
    image_identities: Mapping[str, str]
    artifact_digest: str

    def __post_init__(self) -> None:
        identities = dict(self.image_identities)
        if (
            not self.rendered_yaml
            or _SHA256_RE.fullmatch(self.rendered_sha256) is None
            or _SHA256_RE.fullmatch(self.resource_set_digest) is None
            or _SHA256_RE.fullmatch(self.artifact_digest) is None
            or not 1 <= self.resource_count <= _MAX_RESOURCES
            or not identities
            or not set(identities) <= {name for name, _path in ROLLOUT_IMAGES}
            or any(_IMAGE_ID_RE.fullmatch(value) is None for value in identities.values())
        ):
            raise ValueError("rendered manifest artifact identity is invalid")
        object.__setattr__(self, "image_identities", MappingProxyType(identities))


class ManifestRenderSession:
    """Own one exact render for manifest and server-schema DAG checks."""

    def __init__(
        self,
        render: RenderManifest,
        server_dry_run: ServerDryRun,
        *,
        field_ownership_dry_run: ServerDryRun | None = None,
        image_tag: str,
        namespace: str,
        image_digests: Mapping[str, str],
        expected_image_names: Collection[str] | None = None,
        artifact: ManifestArtifact | None = None,
    ) -> None:
        self._render = render
        self._server_dry_run = server_dry_run
        self._field_ownership_dry_run = (
            server_dry_run if field_ownership_dry_run is None else field_ownership_dry_run
        )
        self._image_tag = image_tag
        self._namespace = namespace
        self._image_digests = dict(image_digests)
        self._expected_image_names = frozenset(
            {name for name, _path in ROLLOUT_IMAGES}
            if expected_image_names is None
            else expected_image_names
        )
        if artifact is not None and artifact != inspect_rendered_manifests(
            artifact.rendered_yaml,
            image_tag=image_tag,
            namespace=namespace,
            image_digests=image_digests,
            expected_image_names=self._expected_image_names,
        ):
            raise ValueError("seeded manifest artifact identity is invalid")
        self._artifact: ManifestArtifact | None = artifact
        self._lock = Lock()

    def render(self) -> ManifestArtifact:
        with self._lock:
            if self._artifact is None:
                self._artifact = inspect_rendered_manifests(
                    self._render(),
                    image_tag=self._image_tag,
                    namespace=self._namespace,
                    image_digests=self._image_digests,
                    expected_image_names=self._expected_image_names,
                )
            return self._artifact

    def server_validate(self) -> ManifestArtifact:
        with self._lock:
            artifact = self._artifact
            if artifact is None:
                raise ValueError("manifest was not rendered by this preflight session")
            result = self._server_dry_run(artifact.rendered_yaml)
            if result.returncode != 0:
                raise ValueError("rendered manifests failed server-side dry-run")
            return artifact

    def field_ownership_validate(self) -> ManifestArtifact:
        with self._lock:
            artifact = self._artifact
            if artifact is None:
                raise ValueError("manifest was not rendered by this preflight session")
            result = self._field_ownership_dry_run(artifact.rendered_yaml)
            if result.returncode != 0:
                raise ValueError("rendered manifests failed field-ownership dry-run")
            return artifact


def inspect_rendered_manifests(
    rendered_yaml: str,
    *,
    image_tag: str,
    namespace: str,
    image_digests: Mapping[str, str],
    expected_image_names: Collection[str] | None = None,
) -> ManifestArtifact:
    """Validate one bounded render and bind local image refs to exact IDs."""
    encoded = rendered_yaml.encode("utf-8")
    all_rollout_images = {name for name, _path in ROLLOUT_IMAGES}
    expected_images = set(
        all_rollout_images if expected_image_names is None else expected_image_names
    )
    if (
        not rendered_yaml.strip()
        or len(encoded) > _MAX_RENDERED_BYTES
        or _IMAGE_TAG_RE.fullmatch(image_tag) is None
        or _DNS_RE.fullmatch(namespace) is None
        or set(image_digests) != {name for name, _path in ALL_BUILD_IMAGES}
        or not expected_images
        or not expected_images <= all_rollout_images
        or any(_IMAGE_ID_RE.fullmatch(value) is None for value in image_digests.values())
    ):
        raise ValueError("rendered manifest binding is invalid")
    try:
        documents = list(yaml.safe_load_all(rendered_yaml))
    except yaml.YAMLError as exc:
        raise ValueError("rendered manifest YAML is invalid") from exc
    resources = [document for document in documents if document is not None]
    if not 1 <= len(resources) <= _MAX_RESOURCES or any(
        not isinstance(resource, dict) for resource in resources
    ):
        raise ValueError("rendered manifest resource set is invalid")

    identities: list[str] = []
    observed_images: set[str] = set()
    for resource in resources:
        api_version = resource.get("apiVersion")
        kind = resource.get("kind")
        metadata = resource.get("metadata")
        if (
            not isinstance(api_version, str)
            or not api_version
            or not isinstance(kind, str)
            or not kind
            or not isinstance(metadata, dict)
            or not isinstance(metadata.get("name"), str)
            or not metadata["name"]
        ):
            raise ValueError("rendered manifest resource identity is invalid")
        resource_namespace = metadata.get("namespace")
        if resource_namespace is not None and resource_namespace != namespace:
            raise ValueError("rendered manifest namespace drifted")
        identity = f"{api_version}|{kind}|{resource_namespace or namespace}|{metadata['name']}"
        if identity in identities:
            raise ValueError("rendered manifest contains duplicate resource identity")
        identities.append(identity)
        _validate_nonroot_identities(resource)
        for image in _container_images(resource):
            name = image.rsplit("/", 1)[-1].split(":", 1)[0]
            if name in all_rollout_images and name not in expected_images:
                raise ValueError(f"rendered manifest contains disabled rollout image {name}")
            if name not in expected_images:
                continue
            if image.rsplit(":", 1)[-1] != image_tag:
                raise ValueError(f"rendered manifest image tag drifted for {name}")
            observed_images.add(name)
    if observed_images != expected_images:
        raise ValueError("rendered manifest rollout image set is incomplete")
    _validate_network_policy_graph(resources, default_namespace=namespace)

    rendered_sha = hashlib.sha256(encoded).hexdigest()
    resource_set_digest = _hash_json(sorted(identities))
    bound_images = {name: image_digests[name] for name in sorted(expected_images)}
    artifact_digest = _hash_json(
        {
            "image_identities": bound_images,
            "image_tag": image_tag,
            "namespace": namespace,
            "rendered_sha256": rendered_sha,
            "resource_set_digest": resource_set_digest,
        }
    )
    return ManifestArtifact(
        rendered_yaml=rendered_yaml,
        rendered_sha256=rendered_sha,
        resource_count=len(resources),
        resource_set_digest=resource_set_digest,
        image_identities=bound_images,
        artifact_digest=artifact_digest,
    )


def _validate_nonroot_identities(resource: Mapping[str, object]) -> None:
    """Reject workloads whose non-root promise depends on ambient image metadata."""
    for pod_spec in _pod_specs(resource):
        pod_security = pod_spec.get("securityContext")
        pod_security = pod_security if isinstance(pod_security, dict) else {}
        for container_key in ("containers", "initContainers", "ephemeralContainers"):
            containers = pod_spec.get(container_key)
            if not isinstance(containers, list):
                continue
            for container in containers:
                if not isinstance(container, dict):
                    continue
                container_security = container.get("securityContext")
                container_security = (
                    container_security if isinstance(container_security, dict) else {}
                )
                run_as_non_root = container_security.get(
                    "runAsNonRoot", pod_security.get("runAsNonRoot")
                )
                if run_as_non_root is not True:
                    continue
                run_as_user = container_security.get("runAsUser", pod_security.get("runAsUser"))
                if type(run_as_user) is not int or run_as_user <= 0:
                    raise ValueError("rendered manifest non-root identity is ambiguous")


def _validate_network_policy_graph(
    resources: Collection[Mapping[str, object]], *, default_namespace: str
) -> None:
    """Reject a declared pod egress edge denied by rendered target ingress policy.

    Kubernetes unions all policies selecting a pod.  This static proof therefore
    requires one compatible target ingress rule for every explicit pod-selector
    egress edge whose destination is ingress-isolated in the same render.  Edges
    to namespaces or workloads outside the rendered graph remain runtime probes.
    """
    policies: list[tuple[str, str, Mapping[str, object]]] = []
    pod_labels = _pod_label_sets(resources, default_namespace=default_namespace)
    for resource in resources:
        if resource.get("kind") != "NetworkPolicy":
            continue
        metadata = resource.get("metadata")
        spec = resource.get("spec")
        if not isinstance(metadata, dict) or not isinstance(spec, dict):
            raise ValueError("rendered NetworkPolicy structure is invalid")
        name = metadata.get("name")
        namespace = metadata.get("namespace", default_namespace)
        if not isinstance(name, str) or not isinstance(namespace, str):
            raise ValueError("rendered NetworkPolicy identity is invalid")
        if _match_labels(spec.get("podSelector")) is None:
            raise ValueError("rendered NetworkPolicy selector is unsupported")
        policies.append((name, namespace, spec))

    for source_name, source_namespace, source_spec in policies:
        source_labels = _match_labels(source_spec.get("podSelector"))
        if source_labels is None or "Egress" not in _policy_types(source_spec):
            continue
        source_pods = [
            labels
            for namespace, labels in pod_labels
            if namespace == source_namespace and source_labels.items() <= labels.items()
        ]
        if not source_pods:
            continue
        egress = source_spec.get("egress")
        if not isinstance(egress, list):
            continue
        for rule in egress:
            if not isinstance(rule, dict):
                raise ValueError("rendered NetworkPolicy egress rule is invalid")
            peers = rule.get("to")
            if not isinstance(peers, list):
                continue
            for peer in peers:
                if not isinstance(peer, dict) or "namespaceSelector" in peer or "ipBlock" in peer:
                    continue
                target_labels = _match_labels(peer.get("podSelector"))
                if target_labels is None:
                    raise ValueError("rendered NetworkPolicy peer selector is unsupported")
                destination_pods = [
                    labels
                    for namespace, labels in pod_labels
                    if namespace == source_namespace and target_labels.items() <= labels.items()
                ]
                if not destination_pods:
                    continue
                for source_pod in source_pods:
                    for destination_pod in destination_pods:
                        isolating_targets = [
                            (target_name, target_spec)
                            for target_name, target_namespace, target_spec in policies
                            if target_namespace == source_namespace
                            and "Ingress" in _policy_types(target_spec)
                            and (
                                (selector := _match_labels(target_spec.get("podSelector")))
                                is not None
                            )
                            and selector.items() <= destination_pod.items()
                        ]
                        if not isolating_targets or any(
                            _policy_allows_ingress(
                                target_spec,
                                source_labels=source_pod,
                                source_namespace=source_namespace,
                                egress_ports=rule.get("ports"),
                            )
                            for _target_name, target_spec in isolating_targets
                        ):
                            continue
                        target_names = ",".join(sorted(name for name, _spec in isolating_targets))
                        raise ValueError(
                            "rendered manifest network policy graph denies declared egress "
                            f"from {source_name} to {target_names}"
                        )


def _match_labels(value: object) -> Mapping[str, str] | None:
    if not isinstance(value, dict) or value.get("matchExpressions"):
        return None
    labels = value.get("matchLabels", {})
    if not isinstance(labels, dict) or any(
        not isinstance(key, str) or not isinstance(item, str) for key, item in labels.items()
    ):
        return None
    return labels


def _pod_label_sets(
    resources: Collection[Mapping[str, object]], *, default_namespace: str
) -> tuple[tuple[str, Mapping[str, str]], ...]:
    found: list[tuple[str, Mapping[str, str]]] = []
    for resource in resources:
        kind = resource.get("kind")
        metadata = resource.get("metadata")
        spec = resource.get("spec")
        if not isinstance(kind, str) or not isinstance(metadata, dict):
            continue
        namespace = metadata.get("namespace", default_namespace)
        if not isinstance(namespace, str):
            continue
        pod_metadata: object
        if kind == "Pod":
            pod_metadata = metadata
        elif kind in {"Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "Job"}:
            template = spec.get("template") if isinstance(spec, dict) else None
            pod_metadata = template.get("metadata") if isinstance(template, dict) else None
        elif kind == "CronJob":
            job_template = spec.get("jobTemplate") if isinstance(spec, dict) else None
            job_spec = job_template.get("spec") if isinstance(job_template, dict) else None
            template = job_spec.get("template") if isinstance(job_spec, dict) else None
            pod_metadata = template.get("metadata") if isinstance(template, dict) else None
        else:
            continue
        labels = pod_metadata.get("labels") if isinstance(pod_metadata, dict) else None
        if not isinstance(labels, dict) or not labels:
            continue
        if any(
            not isinstance(key, str) or not isinstance(value, str) for key, value in labels.items()
        ):
            raise ValueError("rendered pod label identity is invalid")
        found.append((namespace, labels))
    return tuple(found)


def _policy_types(spec: Mapping[str, object]) -> frozenset[str]:
    value = spec.get("policyTypes")
    if value is None:
        inferred = {"Ingress"}
        if "egress" in spec:
            inferred.add("Egress")
        return frozenset(inferred)
    if not isinstance(value, list):
        return frozenset()
    return frozenset(item for item in value if isinstance(item, str))


def _policy_allows_ingress(
    spec: Mapping[str, object],
    *,
    source_labels: Mapping[str, str],
    source_namespace: str,
    egress_ports: object,
) -> bool:
    ingress = spec.get("ingress")
    if not isinstance(ingress, list):
        return False
    for rule in ingress:
        if not isinstance(rule, dict) or not _ports_overlap(egress_ports, rule.get("ports")):
            continue
        if "from" not in rule:
            return True
        peers = rule.get("from")
        if not isinstance(peers, list):
            continue
        for peer in peers:
            if not isinstance(peer, dict) or "ipBlock" in peer:
                continue
            selector = _match_labels(peer.get("podSelector"))
            if "podSelector" in peer and selector is None:
                raise ValueError("rendered NetworkPolicy peer selector is unsupported")
            namespace_selector = peer.get("namespaceSelector")
            if not _namespace_selector_allows(namespace_selector, source_namespace):
                continue
            if selector is not None and selector.items() <= source_labels.items():
                return True
            if "podSelector" not in peer and "namespaceSelector" in peer:
                return True
    return False


def _namespace_selector_allows(value: object, namespace: str) -> bool:
    if value is None:
        return True
    labels = _match_labels(value)
    if labels is None:
        raise ValueError("rendered NetworkPolicy namespace selector is unsupported")
    required = labels.get("kubernetes.io/metadata.name")
    return required is None or required == namespace


def _ports_overlap(egress_ports: object, ingress_ports: object) -> bool:
    if egress_ports is None or ingress_ports is None:
        return True
    if not isinstance(egress_ports, list) or not isinstance(ingress_ports, list):
        return False
    return bool(_normalized_ports(egress_ports) & _normalized_ports(ingress_ports))


def _normalized_ports(values: Collection[object]) -> frozenset[tuple[object, str]]:
    normalized: set[tuple[object, str]] = set()
    for value in values:
        if not isinstance(value, dict):
            continue
        port = value.get("port")
        protocol = value.get("protocol", "TCP")
        if isinstance(port, (int, str)) and isinstance(protocol, str):
            normalized.add((port, protocol))
    return frozenset(normalized)


def _pod_specs(resource: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    kind = resource.get("kind")
    spec = resource.get("spec")
    if not isinstance(kind, str) or not isinstance(spec, dict):
        return ()
    pod_spec: object
    if kind == "Pod":
        pod_spec = spec
    elif kind in {"Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "Job"}:
        template = spec.get("template")
        pod_spec = template.get("spec") if isinstance(template, dict) else None
    elif kind == "CronJob":
        job_template = spec.get("jobTemplate")
        job_spec = job_template.get("spec") if isinstance(job_template, dict) else None
        template = job_spec.get("template") if isinstance(job_spec, dict) else None
        pod_spec = template.get("spec") if isinstance(template, dict) else None
    else:
        return ()
    return (pod_spec,) if isinstance(pod_spec, dict) else ()


def _container_images(value: object) -> tuple[str, ...]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key in {"containers", "initContainers"} and isinstance(child, list):
                for container in child:
                    if isinstance(container, dict) and isinstance(container.get("image"), str):
                        found.append(container["image"])
            else:
                found.extend(_container_images(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_container_images(child))
    return tuple(found)


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = [
    "ManifestArtifact",
    "ManifestRenderSession",
    "RenderManifest",
    "ServerDryRun",
    "inspect_rendered_manifests",
]
