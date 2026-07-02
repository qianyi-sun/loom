"""Load local docker images into a kind cluster's node runtime (#96).

Public-beta rollouts on kind clusters build images with host docker but the
kind node's containerd doesn't see host-local tags without an explicit
`kind load docker-image` step. Missing that step causes ErrImagePull /
ImagePullBackOff — the current recovery is a manual `kind load` sequence
per image, per rollout, per human runbook execution.

This module gives two supported operations:

* Load: shell out to `kind load docker-image --name CLUSTER TAG` per image.
  Idempotent (kind detects an already-loaded image and no-ops the transfer).

* Check-only: query the kind control-plane node's containerd (`crictl
  images` via `docker exec`) and report which requested tags are missing.
  Non-zero exit means the rollout would ErrImagePull; the error message
  names the missing images and the fix command.

Image resolution combines an explicit `--image TAG` list with parsed
manifests (`--from-manifest PATH`). Manifest parsing extracts pod-spec
container/initContainer `image:` fields, skips registry-qualified images
(anything containing `.` or `/` in the pre-`:` segment) since those can be
pulled by the cluster on its own, and dedupes.

The design mirrors `bootstrap-evidence-paths` (#174): operator-visible
subcommand + pure functions the driver (#340) can reuse.
"""

from __future__ import annotations

import enum
import subprocess
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import yaml  # type: ignore[import-untyped]

# Public: crictl invocation format. Kept as module-level constants for tests.
_DOCKER_EXEC_CRICTL: tuple[str, ...] = (
    "docker", "exec", "{node}", "crictl", "images",
)


class ImageStatus(enum.StrEnum):
    PRESENT = "present"
    MISSING = "missing"
    UNKNOWN = "unknown"  # `docker exec` or `crictl` itself failed.


@dataclass(frozen=True, slots=True)
class LoadResult:
    """Aggregate result of :func:`load_images_into_kind`.

    ``loaded``: images kind reported success on.
    ``missing``: in check-only mode, images not present in the node cache.
    ``failed``: images where `kind load` returned non-zero; ``stderr`` map
    exposes the diagnostic per image.
    """

    loaded: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    stderr: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class KindLoadResult:
    """Single-image result of :func:`run_kind_load`."""

    image: str
    returncode: int
    stdout: str
    stderr: str


def parse_images_from_manifest_text(yaml_text: str) -> list[str]:
    """Extract candidate local images from Kubernetes manifest YAML.

    Skips images that look registry-qualified (dots or slashes in the
    ``repository`` segment before the tag), because those can be resolved
    by the cluster's normal image pull path and don't need kind-load.
    Dedupes preserving insertion order.
    """
    if not yaml_text.strip():
        return []
    seen: dict[str, None] = {}
    for doc in yaml.safe_load_all(yaml_text):
        if not isinstance(doc, dict):
            continue
        for image in _walk_pod_spec_images(doc):
            if _looks_registry_qualified(image):
                continue
            seen.setdefault(image, None)
    return list(seen.keys())


def _walk_pod_spec_images(node: object) -> Iterable[str]:
    """Recursively yield `image:` strings from a manifest tree.

    Look inside `spec.template.spec.containers[].image` and
    `spec.template.spec.initContainers[].image` primarily, but also any
    other `containers`/`initContainers` list found in the tree — some
    resources (Jobs, CronJobs, StatefulSets) nest differently, and being
    liberal about what we accept is cheaper than encoding the k8s type
    system here.
    """
    if isinstance(node, dict):
        for key in ("containers", "initContainers"):
            value = node.get(key)
            if isinstance(value, list):
                for entry in value:
                    if isinstance(entry, dict):
                        image = entry.get("image")
                        if isinstance(image, str):
                            yield image
        for value in node.values():
            yield from _walk_pod_spec_images(value)
    elif isinstance(node, list):
        for entry in node:
            yield from _walk_pod_spec_images(entry)


def _looks_registry_qualified(image: str) -> bool:
    """Heuristic: does this image reference include an explicit registry?

    Kubernetes treats an image ref with a ``/`` in the pre-``:`` segment
    OR a ``.`` in the first path component as a registry-qualified image.
    We're conservative: prefer to LOAD anything ambiguous into kind (cheap,
    idempotent) rather than skip and cause an ErrImagePull later. So the
    rule is: skip only when the registry qualification is unambiguous.
    """
    repo = image.split(":", 1)[0]
    if "/" not in repo:
        # No slashes → definitely not registry-qualified (`loom-worker`).
        return False
    first_segment = repo.split("/", 1)[0]
    # A first segment containing a `.` (like `docker.io`, `gcr.io`,
    # `registry.k8s.io`) or being exactly `localhost` is a registry.
    return "." in first_segment or first_segment == "localhost"


def resolve_images(
    *,
    explicit: Sequence[str],
    manifest_paths: Sequence[Path],
) -> list[str]:
    """Merge explicit image list + parsed manifests, dedupe.

    Raises FileNotFoundError if a manifest path doesn't exist. Errors
    out early on missing files rather than silently producing an
    incomplete image list that could cause a mid-rollout stall.
    """
    seen: dict[str, None] = {}
    for tag in explicit:
        if tag:
            seen.setdefault(tag, None)
    for path in manifest_paths:
        text = path.read_text()  # FileNotFoundError propagates.
        for tag in parse_images_from_manifest_text(text):
            seen.setdefault(tag, None)
    return list(seen.keys())


def run_kind_load(
    *,
    cluster_name: str,
    image: str,
    kind_bin: str = "kind",
) -> KindLoadResult:
    """Invoke ``kind load docker-image --name CLUSTER IMAGE``.

    Returns the raw subprocess result. Callers decide how to render
    successes and failures; we don't raise on non-zero because the
    orchestrator wants to keep going and report a full failure map at the
    end rather than aborting on the first miss.
    """
    proc = subprocess.run(
        [
            kind_bin, "load", "docker-image",
            "--name", cluster_name,
            image,
        ],
        capture_output=True,
        check=False,
        text=True,
    )
    return KindLoadResult(
        image=image,
        returncode=proc.returncode,
        stdout=proc.stdout,
        stderr=proc.stderr,
    )


def check_kind_image_loaded(
    *,
    cluster_name: str,
    image: str,
    docker_bin: str = "docker",
) -> ImageStatus:
    """Query the kind control-plane node's crictl for ``image``.

    Kind names its control-plane container ``<cluster>-control-plane``.
    We exec `crictl images` and look for the tag in stdout. Returns
    ``UNKNOWN`` on any subprocess failure so the CLI layer can decide
    whether to treat that as pass or fail.
    """
    node = f"{cluster_name}-control-plane"
    proc = subprocess.run(
        [docker_bin, "exec", node, "crictl", "images"],
        capture_output=True,
        check=False,
        text=True,
    )
    if proc.returncode != 0:
        return ImageStatus.UNKNOWN
    # crictl images output shape:
    #   IMAGE                                     TAG            IMAGE ID
    #   docker.io/library/loom-worker             public-beta-a  abc
    # Kind normalizes local `loom-worker:tag` to `docker.io/library/loom-worker:tag`
    # in some versions, and leaves it bare in others. Do a substring
    # match on the tag portion after prefix normalisation.
    repo, _, tag = image.partition(":")
    tag = tag or "latest"
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line or line.startswith("IMAGE"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        line_repo, line_tag = parts[0], parts[1]
        if _repos_match(line_repo, repo) and line_tag == tag:
            return ImageStatus.PRESENT
    return ImageStatus.MISSING


def _repos_match(observed_repo: str, requested_repo: str) -> bool:
    """Handle kind's docker.io/library/ normalization."""
    if observed_repo == requested_repo:
        return True
    if observed_repo == f"docker.io/library/{requested_repo}":
        return True
    if observed_repo.endswith(f"/{requested_repo}"):
        return True
    return False


def load_images_into_kind(
    *,
    cluster_name: str,
    images: Sequence[str],
    check_only: bool = False,
    kind_bin: str = "kind",
    docker_bin: str = "docker",
) -> LoadResult:
    """Load images (or verify their presence) in the kind cluster."""
    if check_only:
        missing: list[str] = []
        for image in images:
            status = check_kind_image_loaded(
                cluster_name=cluster_name,
                image=image,
                docker_bin=docker_bin,
            )
            if status != ImageStatus.PRESENT:
                missing.append(image)
        return LoadResult(missing=missing)

    loaded: list[str] = []
    failed: list[str] = []
    stderr_map: dict[str, str] = {}
    for image in images:
        result = run_kind_load(
            cluster_name=cluster_name,
            image=image,
            kind_bin=kind_bin,
        )
        if result.returncode == 0:
            loaded.append(image)
        else:
            failed.append(image)
            stderr_map[image] = result.stderr
    return LoadResult(loaded=loaded, failed=failed, stderr=stderr_map)
