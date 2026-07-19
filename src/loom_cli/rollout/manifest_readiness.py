"""Render-once Kubernetes artifacts and fail-closed schema checks."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import Lock
from types import MappingProxyType
from typing import Protocol

import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.image_readiness import ROLLOUT_IMAGES

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
            or set(identities) != {name for name, _path in ROLLOUT_IMAGES}
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
        image_tag: str,
        namespace: str,
        image_digests: Mapping[str, str],
    ) -> None:
        self._render = render
        self._server_dry_run = server_dry_run
        self._image_tag = image_tag
        self._namespace = namespace
        self._image_digests = dict(image_digests)
        self._artifact: ManifestArtifact | None = None
        self._lock = Lock()

    def render(self) -> ManifestArtifact:
        with self._lock:
            if self._artifact is None:
                self._artifact = inspect_rendered_manifests(
                    self._render(),
                    image_tag=self._image_tag,
                    namespace=self._namespace,
                    image_digests=self._image_digests,
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


def inspect_rendered_manifests(
    rendered_yaml: str,
    *,
    image_tag: str,
    namespace: str,
    image_digests: Mapping[str, str],
) -> ManifestArtifact:
    """Validate one bounded render and bind local image refs to exact IDs."""
    encoded = rendered_yaml.encode("utf-8")
    expected_images = {name for name, _path in ROLLOUT_IMAGES}
    if (
        not rendered_yaml.strip()
        or len(encoded) > _MAX_RENDERED_BYTES
        or _IMAGE_TAG_RE.fullmatch(image_tag) is None
        or _DNS_RE.fullmatch(namespace) is None
        or set(image_digests) != expected_images | {"loom-staging-admin-browser-smoke"}
        or any(_IMAGE_ID_RE.fullmatch(value) is None for value in image_digests.values())
    ):
        raise ValueError("rendered manifest binding is invalid")
    try:
        resources = list(yaml.safe_load_all(rendered_yaml))
    except yaml.YAMLError as exc:
        raise ValueError("rendered manifest YAML is invalid") from exc
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
        for image in _container_images(resource):
            name = image.rsplit("/", 1)[-1].split(":", 1)[0]
            if name not in expected_images:
                continue
            if image.rsplit(":", 1)[-1] != image_tag:
                raise ValueError(f"rendered manifest image tag drifted for {name}")
            observed_images.add(name)
    if observed_images != expected_images:
        raise ValueError("rendered manifest rollout image set is incomplete")

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
