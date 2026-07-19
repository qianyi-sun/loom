"""Build-once exact-candidate rollout images and immutable contracts."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from types import MappingProxyType
from typing import Protocol

ROLLOUT_IMAGES: tuple[tuple[str, str], ...] = (
    ("loom-control-plane", "deploy/Dockerfile.control-plane"),
    ("loom-family-orchestrator", "deploy/Dockerfile.family-orchestrator"),
    ("loom-llm-gateway", "deploy/Dockerfile.gateway"),
    ("loom-service", "deploy/Dockerfile.service"),
    ("loom-web", "deploy/Dockerfile.web"),
    ("loom-worker", "deploy/Dockerfile.worker"),
    ("loom-egress-xds", "deploy/Dockerfile.egress-xds"),
)
AUXILIARY_ROLLOUT_IMAGES: tuple[tuple[str, str], ...] = (
    ("loom-staging-admin-browser-smoke", "deploy/Dockerfile.staging-admin-browser-smoke"),
)
ALL_BUILD_IMAGES = ROLLOUT_IMAGES + AUXILIARY_ROLLOUT_IMAGES
REVISION_LABEL = "org.opencontainers.image.revision"
BROWSER_IMAGE = "loom-staging-admin-browser-smoke"
BROWSER_ENTRYPOINT = ("node", "/opt/loom/web/scripts/staging-admin-browser-smoke.mjs")
_IMAGE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_HEX_SHA_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_IMAGE_TAG_RE = re.compile(r"^staging-[a-z0-9][a-z0-9-]{5,63}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...


DockerRunner = Callable[[Sequence[str], Path | None], CommandResult]


def _string(value: object) -> str:
    return value if isinstance(value, str) else ""


@dataclass(frozen=True, slots=True)
class ImageDescriptor:
    image_id: str
    revision: str
    os: str
    architecture: str
    entrypoint: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            _IMAGE_ID_RE.fullmatch(self.image_id) is None
            or _HEX_SHA_RE.fullmatch(self.revision) is None
            or self.os != "linux"
            or self.architecture != "amd64"
            or any(not value or len(value) > 512 for value in self.entrypoint)
        ):
            raise ValueError("rollout image descriptor is invalid")


@dataclass(frozen=True, slots=True)
class ImageArtifactSet:
    descriptors: Mapping[str, ImageDescriptor]
    plan_digest: str
    artifact_digest: str

    def __post_init__(self) -> None:
        descriptors = dict(self.descriptors)
        if set(descriptors) != {name for name, _path in ALL_BUILD_IMAGES}:
            raise ValueError("rollout image artifact set is incomplete")
        if (
            _SHA256_RE.fullmatch(self.plan_digest) is None
            or _SHA256_RE.fullmatch(self.artifact_digest) is None
            or any(not isinstance(value, ImageDescriptor) for value in descriptors.values())
        ):
            raise ValueError("rollout image artifact identity is invalid")
        object.__setattr__(self, "descriptors", MappingProxyType(descriptors))

    @property
    def image_digests(self) -> Mapping[str, str]:
        return MappingProxyType(
            {name: descriptor.image_id for name, descriptor in self.descriptors.items()}
        )


class ImageBuildSession:
    """Own one exact build output for dependent preflight checks."""

    def __init__(
        self,
        run: DockerRunner,
        *,
        candidate_root: Path,
        image_tag: str,
        resolved_sha: str,
    ) -> None:
        self._run = run
        self._candidate_root = candidate_root
        self._image_tag = image_tag
        self._resolved_sha = resolved_sha
        self._artifact: ImageArtifactSet | None = None
        self._lock = Lock()

    def build(self) -> ImageArtifactSet:
        with self._lock:
            if self._artifact is None:
                self._artifact = build_exact_images(
                    self._run,
                    candidate_root=self._candidate_root,
                    image_tag=self._image_tag,
                    resolved_sha=self._resolved_sha,
                )
            return self._artifact

    def verify(self) -> ImageArtifactSet:
        with self._lock:
            artifact = self._artifact
            if artifact is None:
                raise ValueError("rollout images were not built by this preflight session")
            return verify_image_contract(
                self._run,
                image_tag=self._image_tag,
                resolved_sha=self._resolved_sha,
                expected_digests=artifact.image_digests,
            )


def image_plan_digest() -> str:
    payload = {
        "browser_entrypoint": BROWSER_ENTRYPOINT,
        "images": ALL_BUILD_IMAGES,
        "revision_label": REVISION_LABEL,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _inspect_image(run: DockerRunner, tag: str) -> ImageDescriptor | None:
    try:
        result = run(("docker", "image", "inspect", tag), None)
    except Exception:
        return None
    if result.returncode != 0 or not isinstance(result.stdout, str) or len(result.stdout) > 1024**2:
        return None
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
        return None
    item = payload[0]
    config = item.get("Config")
    labels = config.get("Labels") if isinstance(config, dict) else None
    entrypoint = config.get("Entrypoint") if isinstance(config, dict) else None
    try:
        return ImageDescriptor(
            image_id=_string(item.get("Id")),
            revision=(_string(labels.get(REVISION_LABEL)) if isinstance(labels, dict) else ""),
            os=_string(item.get("Os")),
            architecture=_string(item.get("Architecture")),
            entrypoint=(
                tuple(entrypoint)
                if isinstance(entrypoint, list)
                and all(isinstance(value, str) for value in entrypoint)
                else ()
            ),
        )
    except ValueError:
        return None


def _contract_matches(name: str, descriptor: ImageDescriptor, resolved_sha: str) -> bool:
    return bool(
        descriptor.revision == resolved_sha
        and descriptor.os == "linux"
        and descriptor.architecture == "amd64"
        and (name != BROWSER_IMAGE or descriptor.entrypoint == BROWSER_ENTRYPOINT)
    )


def inspect_exact_images(
    run: DockerRunner,
    *,
    image_tag: str,
    resolved_sha: str,
) -> ImageArtifactSet:
    """Read every image contract without building or modifying Docker state."""
    if _IMAGE_TAG_RE.fullmatch(image_tag) is None or _HEX_SHA_RE.fullmatch(resolved_sha) is None:
        raise ValueError("rollout image inspection binding is invalid")
    descriptors: dict[str, ImageDescriptor] = {}
    for name, _dockerfile in ALL_BUILD_IMAGES:
        descriptor = _inspect_image(run, f"{name}:{image_tag}")
        if descriptor is None or not _contract_matches(name, descriptor, resolved_sha):
            raise ValueError(f"rollout image contract failed for {name}")
        descriptors[name] = descriptor
    return _artifact_set(descriptors)


def build_exact_images(
    run: DockerRunner,
    *,
    candidate_root: Path,
    image_tag: str,
    resolved_sha: str,
) -> ImageArtifactSet:
    """Build each missing/drifted tag once, then bind its immutable local ID."""
    if (
        not candidate_root.is_absolute()
        or not candidate_root.is_dir()
        or _IMAGE_TAG_RE.fullmatch(image_tag) is None
        or _HEX_SHA_RE.fullmatch(resolved_sha) is None
    ):
        raise ValueError("rollout image build binding is invalid")
    descriptors: dict[str, ImageDescriptor] = {}
    for name, dockerfile in ALL_BUILD_IMAGES:
        tag = f"{name}:{image_tag}"
        descriptor = _inspect_image(run, tag)
        if descriptor is None or not _contract_matches(name, descriptor, resolved_sha):
            command = (
                "docker",
                "build",
                "--label",
                f"{REVISION_LABEL}={resolved_sha}",
                "--build-arg",
                f"LOOM_BUILD_SHA={resolved_sha}",
                "-f",
                dockerfile,
                "-t",
                tag,
                ".",
            )
            result = run(command, candidate_root)
            if result.returncode != 0:
                raise ValueError(f"rollout image build failed for {name}")
            descriptor = _inspect_image(run, tag)
        if descriptor is None or not _contract_matches(name, descriptor, resolved_sha):
            raise ValueError(f"rollout image contract failed for {name}")
        descriptors[name] = descriptor
    return _artifact_set(descriptors)


def verify_image_contract(
    run: DockerRunner,
    *,
    image_tag: str,
    resolved_sha: str,
    expected_digests: Mapping[str, str],
) -> ImageArtifactSet:
    """Re-inspect preflight-built images without rebuilding them."""
    if (
        _IMAGE_TAG_RE.fullmatch(image_tag) is None
        or _HEX_SHA_RE.fullmatch(resolved_sha) is None
        or set(expected_digests) != {name for name, _path in ALL_BUILD_IMAGES}
        or any(_IMAGE_ID_RE.fullmatch(value) is None for value in expected_digests.values())
    ):
        raise ValueError("rollout image digest set is incomplete")
    descriptors: dict[str, ImageDescriptor] = {}
    for name, _dockerfile in ALL_BUILD_IMAGES:
        descriptor = _inspect_image(run, f"{name}:{image_tag}")
        if (
            descriptor is None
            or descriptor.image_id != expected_digests[name]
            or not _contract_matches(name, descriptor, resolved_sha)
        ):
            raise ValueError(f"rollout image contract drifted for {name}")
        descriptors[name] = descriptor
    return _artifact_set(descriptors)


def _artifact_set(descriptors: Mapping[str, ImageDescriptor]) -> ImageArtifactSet:
    payload = {
        "images": {
            name: {
                "architecture": descriptor.architecture,
                "entrypoint": descriptor.entrypoint,
                "id": descriptor.image_id,
                "os": descriptor.os,
                "revision": descriptor.revision,
            }
            for name, descriptor in sorted(descriptors.items())
        },
        "plan_digest": image_plan_digest(),
    }
    return ImageArtifactSet(
        descriptors=descriptors,
        plan_digest=image_plan_digest(),
        artifact_digest=hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    )


__all__ = [
    "ALL_BUILD_IMAGES",
    "AUXILIARY_ROLLOUT_IMAGES",
    "BROWSER_ENTRYPOINT",
    "BROWSER_IMAGE",
    "REVISION_LABEL",
    "ROLLOUT_IMAGES",
    "DockerRunner",
    "ImageArtifactSet",
    "ImageBuildSession",
    "ImageDescriptor",
    "build_exact_images",
    "image_plan_digest",
    "inspect_exact_images",
    "verify_image_contract",
]
