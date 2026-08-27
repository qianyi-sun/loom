"""Build-once exact-candidate rollout images and immutable contracts."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from types import MappingProxyType
from typing import Protocol

from loom_cli.cluster_config import validate_container_registry_prefixes

ROLLOUT_IMAGES: tuple[tuple[str, str], ...] = (
    ("loom-control-plane", "deploy/Dockerfile.control-plane"),
    ("loom-execution-actuator", "deploy/Dockerfile.execution-actuator"),
    ("loom-execution-runtime", "deploy/Dockerfile.execution-runtime"),
    ("loom-family-orchestrator", "deploy/Dockerfile.family-orchestrator"),
    ("loom-pipeline-orchestrator", "deploy/Dockerfile.pipeline-orchestrator"),
    ("loom-llm-gateway", "deploy/Dockerfile.gateway"),
    ("loom-service", "deploy/Dockerfile.service"),
    ("loom-web", "deploy/Dockerfile.web"),
    ("loom-worker", "deploy/Dockerfile.worker"),
    ("loom-egress-xds", "deploy/Dockerfile.egress-xds"),
)
AUXILIARY_ROLLOUT_IMAGES: tuple[tuple[str, str], ...] = (
    ("loom-staging-admin-browser-smoke", "deploy/Dockerfile.staging-admin-browser-smoke"),
    ("loom-rehearsal-postgres", "deploy/Dockerfile.rehearsal-postgres"),
)
ALL_BUILD_IMAGES = ROLLOUT_IMAGES + AUXILIARY_ROLLOUT_IMAGES
RolloutImage = tuple[str, str, str]
RolloutImagePlan = tuple[RolloutImage, ...]
DEFAULT_ROLLOUT_IMAGE_PLAN: RolloutImagePlan = tuple(
    (name, dockerfile, ".") for name, dockerfile in ALL_BUILD_IMAGES
)
REVISION_LABEL = "org.opencontainers.image.revision"
BROWSER_IMAGE = "loom-staging-admin-browser-smoke"
BROWSER_ENTRYPOINT = ("node", "/opt/loom/web/scripts/staging-admin-browser-smoke.mjs")
REHEARSAL_POSTGRES_IMAGE = "loom-rehearsal-postgres"
REHEARSAL_POSTGRES_ENTRYPOINT = ("docker-entrypoint.sh",)
NONROOT_RUNTIME_PROBES: Mapping[str, tuple[int, tuple[str, ...]]] = MappingProxyType(
    {
        "loom-control-plane": (
            65532,
            ("python", "-I", "-B", "-c", "import loom.data_lifecycle_maintenance"),
        ),
    }
)
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
    registry_digests: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        descriptors = dict(self.descriptors)
        registry_digests = dict(self.registry_digests)
        if set(descriptors) != {name for name, _path in ALL_BUILD_IMAGES}:
            raise ValueError("rollout image artifact set is incomplete")
        if (
            _SHA256_RE.fullmatch(self.plan_digest) is None
            or _SHA256_RE.fullmatch(self.artifact_digest) is None
            or any(not isinstance(value, ImageDescriptor) for value in descriptors.values())
            or (
                bool(registry_digests)
                and set(registry_digests) != {name for name, _path in ALL_BUILD_IMAGES}
            )
            or any(_IMAGE_ID_RE.fullmatch(value) is None for value in registry_digests.values())
        ):
            raise ValueError("rollout image artifact identity is invalid")
        object.__setattr__(self, "descriptors", MappingProxyType(descriptors))
        object.__setattr__(self, "registry_digests", MappingProxyType(registry_digests))

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
        artifact: ImageArtifactSet | None = None,
        container_registry: str = "",
        container_registry_push: str = "",
    ) -> None:
        self._run = run
        self._candidate_root = candidate_root
        self._image_tag = image_tag
        self._resolved_sha = resolved_sha
        self._registry_publication = validate_container_registry_prefixes(
            container_registry,
            container_registry_push,
        )
        if artifact is not None and (
            artifact.plan_digest != image_plan_digest()
            or any(
                descriptor.revision != resolved_sha for descriptor in artifact.descriptors.values()
            )
            or bool(artifact.registry_digests) != bool(self._registry_publication)
        ):
            raise ValueError("seeded rollout image artifact identity is invalid")
        self._artifact: ImageArtifactSet | None = artifact
        self._lock = Lock()

    def build(self) -> ImageArtifactSet:
        with self._lock:
            if self._artifact is None:
                artifact = build_exact_images(
                    self._run,
                    candidate_root=self._candidate_root,
                    image_tag=self._image_tag,
                    resolved_sha=self._resolved_sha,
                )
                self._artifact = (
                    publish_exact_images(
                        self._run,
                        artifact=artifact,
                        image_tag=self._image_tag,
                        container_registry_push=self._registry_publication[1],
                    )
                    if self._registry_publication is not None
                    else artifact
                )
            return self._artifact

    def verify(self) -> ImageArtifactSet:
        with self._lock:
            artifact = self._artifact
            if artifact is None:
                raise ValueError("rollout images were not built by this preflight session")
            verified = verify_image_contract(
                self._run,
                image_tag=self._image_tag,
                resolved_sha=self._resolved_sha,
                expected_digests=artifact.image_digests,
            )
            if self._registry_publication is None:
                return verified
            return verify_published_images(
                self._run,
                artifact=verified,
                image_tag=self._image_tag,
                container_registry_push=self._registry_publication[1],
                expected_registry_digests=artifact.registry_digests,
            )


def _validate_image_plan(plan: RolloutImagePlan) -> None:
    if len(plan) != len(DEFAULT_ROLLOUT_IMAGE_PLAN) or set(plan) != set(DEFAULT_ROLLOUT_IMAGE_PLAN):
        raise ValueError("rollout image plan differs from the exact ten-image contract")


def image_plan_digest(
    plan: RolloutImagePlan = DEFAULT_ROLLOUT_IMAGE_PLAN,
) -> str:
    _validate_image_plan(plan)
    payload = {
        "browser_entrypoint": BROWSER_ENTRYPOINT,
        "images": plan,
        "nonroot_runtime_probes": dict(NONROOT_RUNTIME_PROBES),
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
        and (
            name != REHEARSAL_POSTGRES_IMAGE
            or descriptor.entrypoint == REHEARSAL_POSTGRES_ENTRYPOINT
        )
    )


def inspect_exact_images(
    run: DockerRunner,
    *,
    plan: RolloutImagePlan = DEFAULT_ROLLOUT_IMAGE_PLAN,
    image_tag: str,
    resolved_sha: str,
) -> ImageArtifactSet:
    """Read every image contract without building or modifying Docker state."""
    _validate_image_plan(plan)
    if _IMAGE_TAG_RE.fullmatch(image_tag) is None or _HEX_SHA_RE.fullmatch(resolved_sha) is None:
        raise ValueError("rollout image inspection binding is invalid")
    descriptors: dict[str, ImageDescriptor] = {}
    for name, _dockerfile, _context in plan:
        descriptor = _inspect_image(run, f"{name}:{image_tag}")
        if descriptor is None or not _contract_matches(name, descriptor, resolved_sha):
            raise ValueError(f"rollout image contract failed for {name}")
        descriptors[name] = descriptor
    return _artifact_set(descriptors, plan=plan)


def build_exact_images(
    run: DockerRunner,
    *,
    plan: RolloutImagePlan = DEFAULT_ROLLOUT_IMAGE_PLAN,
    candidate_root: Path,
    image_tag: str,
    resolved_sha: str,
) -> ImageArtifactSet:
    """Build each missing/drifted tag once, then bind its immutable local ID."""
    _validate_image_plan(plan)
    if (
        not candidate_root.is_absolute()
        or not candidate_root.is_dir()
        or _IMAGE_TAG_RE.fullmatch(image_tag) is None
        or _HEX_SHA_RE.fullmatch(resolved_sha) is None
    ):
        raise ValueError("rollout image build binding is invalid")
    descriptors: dict[str, ImageDescriptor] = {}
    for name, dockerfile, build_context in plan:
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
                build_context,
            )
            result = run(command, candidate_root)
            if result.returncode != 0:
                raise ValueError(f"rollout image build failed for {name}")
            descriptor = _inspect_image(run, tag)
        if descriptor is None or not _contract_matches(name, descriptor, resolved_sha):
            raise ValueError(f"rollout image contract failed for {name}")
        descriptors[name] = descriptor
    return _artifact_set(descriptors, plan=plan)


def verify_image_contract(
    run: DockerRunner,
    *,
    plan: RolloutImagePlan = DEFAULT_ROLLOUT_IMAGE_PLAN,
    image_tag: str,
    resolved_sha: str,
    expected_digests: Mapping[str, str],
) -> ImageArtifactSet:
    """Re-inspect preflight-built images without rebuilding them."""
    _validate_image_plan(plan)
    if (
        _IMAGE_TAG_RE.fullmatch(image_tag) is None
        or _HEX_SHA_RE.fullmatch(resolved_sha) is None
        or set(expected_digests) != {name for name, _dockerfile, _context in plan}
        or any(_IMAGE_ID_RE.fullmatch(value) is None for value in expected_digests.values())
    ):
        raise ValueError("rollout image digest set is incomplete")
    descriptors: dict[str, ImageDescriptor] = {}
    for name, _dockerfile, _context in plan:
        descriptor = _inspect_image(run, f"{name}:{image_tag}")
        if (
            descriptor is None
            or descriptor.image_id != expected_digests[name]
            or not _contract_matches(name, descriptor, resolved_sha)
        ):
            raise ValueError(f"rollout image contract drifted for {name}")
        descriptors[name] = descriptor
    _verify_nonroot_runtime_contract(run, image_tag=image_tag)
    return _artifact_set(descriptors, plan=plan)


def _inspect_registry_manifest(
    run: DockerRunner,
    reference: str,
    *,
    expected_image_id: str,
) -> str | None:
    try:
        result = run(
            ("docker", "manifest", "inspect", "--insecure", "--verbose", reference),
            None,
        )
    except Exception:
        return None
    if result.returncode != 0 or not isinstance(result.stdout, str) or len(result.stdout) > 1024**2:
        return None
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    if isinstance(payload, dict):
        manifest = payload.get("SchemaV2Manifest", payload)
        descriptor = payload.get("Descriptor")
        config = manifest.get("config") if isinstance(manifest, dict) else None
        config_digest = config.get("digest") if isinstance(config, dict) else None
        manifest_digest = descriptor.get("digest") if isinstance(descriptor, dict) else None
        if config_digest != expected_image_id or not isinstance(manifest_digest, str):
            return None
        return manifest_digest if _IMAGE_ID_RE.fullmatch(manifest_digest) is not None else None
    if not isinstance(payload, list) or not payload:
        return None

    # Docker's containerd image store publishes a provenance-bearing OCI index.
    # ``docker manifest inspect --verbose`` expands that index into a JSON list
    # and omits the index digest, so bind it with buildx's descriptor view and
    # cross-check the complete child set from both independent views.
    try:
        index_result = run(
            (
                "docker",
                "buildx",
                "imagetools",
                "inspect",
                reference,
                "--format",
                "{{json .Manifest}}",
            ),
            None,
        )
    except Exception:
        return None
    if (
        index_result.returncode != 0
        or not isinstance(index_result.stdout, str)
        or len(index_result.stdout) > 1024**2
    ):
        return None
    try:
        index = json.loads(index_result.stdout)
    except json.JSONDecodeError:
        return None
    if (
        not isinstance(index, dict)
        or index.get("schemaVersion") != 2
        or index.get("mediaType")
        not in {
            "application/vnd.docker.distribution.manifest.list.v2+json",
            "application/vnd.oci.image.index.v1+json",
        }
        or index.get("digest") != expected_image_id
        or _IMAGE_ID_RE.fullmatch(expected_image_id) is None
    ):
        return None
    manifests = index.get("manifests")
    if not isinstance(manifests, list) or not manifests:
        return None

    def descriptor_identity(value: object) -> tuple[str, str, str] | None:
        if not isinstance(value, dict):
            return None
        digest = value.get("digest")
        platform = value.get("platform")
        if (
            not isinstance(digest, str)
            or _IMAGE_ID_RE.fullmatch(digest) is None
            or not isinstance(platform, dict)
            or not isinstance(platform.get("os"), str)
            or not isinstance(platform.get("architecture"), str)
        ):
            return None
        return digest, platform["os"], platform["architecture"]

    verbose_identities: list[tuple[str, str, str]] = []
    for item in payload:
        descriptor = item.get("Descriptor") if isinstance(item, dict) else None
        identity = descriptor_identity(descriptor)
        if identity is None:
            return None
        verbose_identities.append(identity)
    index_identities = [descriptor_identity(item) for item in manifests]
    if (
        any(identity is None for identity in index_identities)
        or len(set(verbose_identities)) != len(verbose_identities)
        or len(set(index_identities)) != len(index_identities)
        or set(verbose_identities) != set(index_identities)
    ):
        return None

    workload: list[dict[str, object]] = []
    attestations: list[dict[str, object]] = []
    for item in manifests:
        if not isinstance(item, dict):  # narrowed by descriptor_identity above
            return None
        annotations = item.get("annotations")
        if (
            isinstance(annotations, dict)
            and annotations.get("vnd.docker.reference.type") == "attestation-manifest"
        ):
            attestations.append(item)
        else:
            workload.append(item)
    if len(workload) != 1:
        return None
    workload_identity = descriptor_identity(workload[0])
    if workload_identity is None or workload_identity[1:] != ("linux", "amd64"):
        return None
    for attestation in attestations:
        identity = descriptor_identity(attestation)
        annotations = attestation.get("annotations")
        if (
            identity is None
            or identity[1:] != ("unknown", "unknown")
            or not isinstance(annotations, dict)
            or annotations.get("vnd.docker.reference.digest") != workload_identity[0]
        ):
            return None
    return expected_image_id


def publish_exact_images(
    run: DockerRunner,
    *,
    artifact: ImageArtifactSet,
    image_tag: str,
    container_registry_push: str,
) -> ImageArtifactSet:
    """Publish every exact local image and bind its immutable registry manifest."""
    registry_digests: dict[str, str] = {}
    for name, _dockerfile, _context in DEFAULT_ROLLOUT_IMAGE_PLAN:
        source = f"{name}:{image_tag}"
        target = f"{container_registry_push}/{source}"
        if run(("docker", "tag", source, target), None).returncode != 0:
            raise ValueError(f"rollout image registry tag failed for {name}")
        if run(("docker", "push", target), None).returncode != 0:
            raise ValueError(f"rollout image registry publication failed for {name}")
        digest = _inspect_registry_manifest(
            run,
            target,
            expected_image_id=artifact.image_digests[name],
        )
        if digest is None:
            raise ValueError(f"rollout image registry contract failed for {name}")
        registry_digests[name] = digest
    return _artifact_set(artifact.descriptors, registry_digests=registry_digests)


def verify_published_images(
    run: DockerRunner,
    *,
    artifact: ImageArtifactSet,
    image_tag: str,
    container_registry_push: str,
    expected_registry_digests: Mapping[str, str],
) -> ImageArtifactSet:
    """Re-read every published tag and require the attested manifest digest."""
    if set(expected_registry_digests) != {name for name, _path in ALL_BUILD_IMAGES}:
        raise ValueError("rollout registry image digest set is incomplete")
    observed: dict[str, str] = {}
    for name, _dockerfile, _context in DEFAULT_ROLLOUT_IMAGE_PLAN:
        target = f"{container_registry_push}/{name}:{image_tag}"
        digest = _inspect_registry_manifest(
            run,
            target,
            expected_image_id=artifact.image_digests[name],
        )
        if digest != expected_registry_digests[name]:
            raise ValueError(f"rollout registry image contract drifted for {name}")
        observed[name] = digest
    return _artifact_set(artifact.descriptors, registry_digests=observed)


def _verify_nonroot_runtime_contract(run: DockerRunner, *, image_tag: str) -> None:
    """Launch bounded no-network imports under every declared non-root identity."""
    for name, (uid, command) in NONROOT_RUNTIME_PROBES.items():
        entrypoint, *arguments = command
        result = run(
            (
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--read-only",
                "--user",
                f"{uid}:{uid}",
                "--entrypoint",
                entrypoint,
                f"{name}:{image_tag}",
                *arguments,
            ),
            None,
        )
        if result.returncode != 0:
            raise ValueError(f"rollout image non-root runtime failed for {name}")


def _artifact_set(
    descriptors: Mapping[str, ImageDescriptor],
    *,
    plan: RolloutImagePlan = DEFAULT_ROLLOUT_IMAGE_PLAN,
    registry_digests: Mapping[str, str] | None = None,
) -> ImageArtifactSet:
    exact_registry_digests = dict(registry_digests or {})
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
        "plan_digest": image_plan_digest(plan),
    }
    if exact_registry_digests:
        payload["registry_digests"] = exact_registry_digests
    return ImageArtifactSet(
        descriptors=descriptors,
        plan_digest=image_plan_digest(plan),
        artifact_digest=hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        registry_digests=exact_registry_digests,
    )


__all__ = [
    "ALL_BUILD_IMAGES",
    "AUXILIARY_ROLLOUT_IMAGES",
    "BROWSER_ENTRYPOINT",
    "BROWSER_IMAGE",
    "DEFAULT_ROLLOUT_IMAGE_PLAN",
    "NONROOT_RUNTIME_PROBES",
    "REHEARSAL_POSTGRES_ENTRYPOINT",
    "REHEARSAL_POSTGRES_IMAGE",
    "REVISION_LABEL",
    "ROLLOUT_IMAGES",
    "DockerRunner",
    "ImageArtifactSet",
    "ImageBuildSession",
    "ImageDescriptor",
    "RolloutImage",
    "RolloutImagePlan",
    "build_exact_images",
    "image_plan_digest",
    "inspect_exact_images",
    "publish_exact_images",
    "verify_image_contract",
    "verify_published_images",
]
