"""Render-once exact migration Job artifact for rehearsal and final apply."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

import yaml  # type: ignore[import-untyped]

from loom_cli.cluster_config import validate_container_registry_prefix
from loom_cli.cluster_migration import render_migration_manifest

from .manifest_readiness import ServerDryRun

_SHA_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_IMAGE_TAG_RE = re.compile(r"^staging-[a-z0-9][a-z0-9-]{5,63}$")
_REVISION_RE = re.compile(r"^[0-9]{4}(?:_[a-z0-9_]+)?$")
_DNS_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_MAX_MANIFEST_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class MigrationManifestArtifact:
    """One exact Job render, bound to the image and migration graph."""

    rendered_yaml: str
    rendered_sha256: str
    job_name: str
    candidate_sha: str
    candidate_tree: str
    image_tag: str
    image_id: str
    namespace: str
    migration_plan_sha256: str
    migration_target_revision: str
    artifact_digest: str
    container_registry: str = ""
    registry_digest: str = ""

    def __post_init__(self) -> None:
        if (
            not self.rendered_yaml
            or len(self.rendered_yaml.encode()) > _MAX_MANIFEST_BYTES
            or _SHA256_RE.fullmatch(self.rendered_sha256) is None
            or _SHA_RE.fullmatch(self.candidate_sha) is None
            or _SHA_RE.fullmatch(self.candidate_tree) is None
            or _IMAGE_TAG_RE.fullmatch(self.image_tag) is None
            or _IMAGE_ID_RE.fullmatch(self.image_id) is None
            or _DNS_RE.fullmatch(self.namespace) is None
            or _DNS_RE.fullmatch(self.job_name) is None
            or _SHA256_RE.fullmatch(self.migration_plan_sha256) is None
            or _REVISION_RE.fullmatch(self.migration_target_revision) is None
            or _SHA256_RE.fullmatch(self.artifact_digest) is None
        ):
            raise ValueError("migration manifest artifact identity is invalid")
        if self.container_registry:
            validate_container_registry_prefix(
                self.container_registry,
                name="container_registry",
            )
        if self.registry_digest and _IMAGE_ID_RE.fullmatch(self.registry_digest) is None:
            raise ValueError("migration registry digest is invalid")


def build_migration_manifest_artifact(
    server_dry_run: ServerDryRun,
    *,
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
    image_id: str,
    namespace: str,
    migration_plan_sha256: str,
    migration_target_revision: str,
    container_registry: str = "",
    registry_digest: str = "",
) -> MigrationManifestArtifact:
    """Render and server-validate the only migration Job used downstream."""
    binding = {
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "image_id": image_id,
        "image_tag": image_tag,
        "migration_plan_sha256": migration_plan_sha256,
        "migration_target_revision": migration_target_revision,
        "namespace": namespace,
    }
    if container_registry:
        binding["container_registry"] = container_registry
    if registry_digest:
        binding["registry_digest"] = registry_digest
    _validate_binding(binding)
    suffix = hashlib.sha256(_json_bytes(binding)).hexdigest()[:12]
    rendered = render_migration_manifest(
        image_tag=image_tag,
        namespace=namespace,
        job_suffix=f"pf-{suffix}",
        container_registry=container_registry,
        registry_digest=registry_digest,
    )
    job_name = _validate_rendered_job(
        rendered,
        image_tag=image_tag,
        namespace=namespace,
        suffix=suffix,
        container_registry=container_registry,
        registry_digest=registry_digest,
    )
    result = server_dry_run(rendered)
    if result.returncode != 0:
        raise ValueError("migration manifest failed server-side dry-run")
    rendered_sha256 = hashlib.sha256(rendered.encode()).hexdigest()
    artifact_payload = {
        **binding,
        "job_name": job_name,
        "rendered_sha256": rendered_sha256,
    }
    return MigrationManifestArtifact(
        rendered_yaml=rendered,
        rendered_sha256=rendered_sha256,
        job_name=job_name,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        image_tag=image_tag,
        image_id=image_id,
        namespace=namespace,
        migration_plan_sha256=migration_plan_sha256,
        migration_target_revision=migration_target_revision,
        artifact_digest=hashlib.sha256(_json_bytes(artifact_payload)).hexdigest(),
        container_registry=container_registry,
        registry_digest=registry_digest,
    )


def inspect_migration_manifest_artifact(
    rendered: str,
    *,
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
    image_id: str,
    namespace: str,
    migration_plan_sha256: str,
    migration_target_revision: str,
    container_registry: str = "",
    registry_digest: str = "",
) -> MigrationManifestArtifact:
    """Reconstruct one publication without rerendering or server mutation."""
    binding = {
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "image_id": image_id,
        "image_tag": image_tag,
        "migration_plan_sha256": migration_plan_sha256,
        "migration_target_revision": migration_target_revision,
        "namespace": namespace,
    }
    if container_registry:
        binding["container_registry"] = container_registry
    if registry_digest:
        binding["registry_digest"] = registry_digest
    _validate_binding(binding)
    suffix = hashlib.sha256(_json_bytes(binding)).hexdigest()[:12]
    job_name = _validate_rendered_job(
        rendered,
        image_tag=image_tag,
        namespace=namespace,
        suffix=suffix,
        container_registry=container_registry,
        registry_digest=registry_digest,
    )
    rendered_sha256 = hashlib.sha256(rendered.encode()).hexdigest()
    artifact_payload = {
        **binding,
        "job_name": job_name,
        "rendered_sha256": rendered_sha256,
    }
    return MigrationManifestArtifact(
        rendered_yaml=rendered,
        rendered_sha256=rendered_sha256,
        job_name=job_name,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        image_tag=image_tag,
        image_id=image_id,
        namespace=namespace,
        migration_plan_sha256=migration_plan_sha256,
        migration_target_revision=migration_target_revision,
        artifact_digest=hashlib.sha256(_json_bytes(artifact_payload)).hexdigest(),
        container_registry=container_registry,
        registry_digest=registry_digest,
    )


def _validate_binding(binding: dict[str, str]) -> None:
    if (
        _SHA_RE.fullmatch(binding["candidate_sha"]) is None
        or _SHA_RE.fullmatch(binding["candidate_tree"]) is None
        or _IMAGE_ID_RE.fullmatch(binding["image_id"]) is None
        or _IMAGE_TAG_RE.fullmatch(binding["image_tag"]) is None
        or _SHA256_RE.fullmatch(binding["migration_plan_sha256"]) is None
        or _REVISION_RE.fullmatch(binding["migration_target_revision"]) is None
        or _DNS_RE.fullmatch(binding["namespace"]) is None
    ):
        raise ValueError("migration manifest binding is invalid")
    if binding.get("container_registry"):
        validate_container_registry_prefix(
            binding["container_registry"],
            name="container_registry",
        )
    if binding.get("registry_digest") and _IMAGE_ID_RE.fullmatch(binding["registry_digest"]) is None:
        raise ValueError("migration manifest registry digest is invalid")


def _validate_rendered_job(
    rendered: str,
    *,
    image_tag: str,
    namespace: str,
    suffix: str,
    container_registry: str,
    registry_digest: str,
) -> str:
    if not rendered or len(rendered.encode()) > _MAX_MANIFEST_BYTES:
        raise ValueError("migration manifest is empty or unbounded")
    try:
        documents = [value for value in yaml.safe_load_all(rendered) if value is not None]
    except yaml.YAMLError as exc:
        raise ValueError("migration manifest YAML is invalid") from exc
    if len(documents) != 1 or not isinstance(documents[0], dict):
        raise ValueError("migration manifest must contain exactly one resource")
    job = documents[0]
    metadata = job.get("metadata")
    spec = job.get("spec")
    template = spec.get("template") if isinstance(spec, dict) else None
    pod_spec = template.get("spec") if isinstance(template, dict) else None
    containers = pod_spec.get("containers") if isinstance(pod_spec, dict) else None
    if (
        job.get("apiVersion") != "batch/v1"
        or job.get("kind") != "Job"
        or not isinstance(metadata, dict)
        or metadata.get("namespace") != namespace
        or not isinstance(metadata.get("name"), str)
        or not str(metadata["name"]).endswith(f"-pf-{suffix}")
        or metadata.get("labels")
        != {"app": "loom-migration", "loom.image-tag": image_tag}
        or not isinstance(spec, dict)
        or spec.get("backoffLimit") != 1
        or spec.get("activeDeadlineSeconds") != 600
        or not isinstance(containers, list)
        or len(containers) != 1
        or not isinstance(containers[0], dict)
        or containers[0].get("name") != "migrate"
        or containers[0].get("image")
        != (
            f"{container_registry}/loom-control-plane@{registry_digest}"
            if container_registry and registry_digest
            else (
                f"{container_registry}/loom-control-plane:{image_tag}"
                if container_registry
                else f"loom-control-plane:{image_tag}"
            )
        )
        or containers[0].get("command")
        != ["alembic", "-c", "migrations/alembic.ini", "upgrade", "head"]
    ):
        raise ValueError("migration manifest contract drifted")
    return str(metadata["name"])


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


__all__ = [
    "MigrationManifestArtifact",
    "build_migration_manifest_artifact",
    "inspect_migration_manifest_artifact",
]
