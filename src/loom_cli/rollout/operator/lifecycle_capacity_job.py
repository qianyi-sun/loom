"""Exact-candidate one-shot staging lifecycle capacity Job contract."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, cast

import yaml  # type: ignore[import-untyped]

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_ID_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_IMAGE_TAG_RE = re.compile(r"^staging-[a-z0-9][a-z0-9-]{5,63}$")
_DNS_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_NAMESPACE = "loom-staging"
_CRONJOB = "loom-staging-data-lifecycle"
_APP_LABEL = "loom-staging-data-lifecycle"
_COMMAND = ("python", "-I", "-B", "-m", "loom.data_lifecycle_maintenance")
_ENV_NAMES = frozenset(
    {
        "LOOM_LIFECYCLE_DB_URL",
        "LOOM_LIFECYCLE_MINIO_ENDPOINT",
        "LOOM_LIFECYCLE_MINIO_ACCESS_KEY",
        "LOOM_LIFECYCLE_MINIO_SECRET_KEY",
        "LOOM_LIFECYCLE_MINIO_REGION",
        "LOOM_LIFECYCLE_STORAGE_AUTH_KIND",
    }
)


class LifecycleCapacityJobError(ValueError):
    """Raised when the immutable one-shot Job authority is incomplete."""


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise LifecycleCapacityJobError(f"{label} is invalid")
    return cast(dict[str, Any], value)


def _sequence(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise LifecycleCapacityJobError(f"{label} is invalid")
    return value


def _require_job_template(
    rendered_yaml: str,
    *,
    image_tag: str,
    container_registry: str,
    registry_digest: str,
    expected_buckets: tuple[str, ...],
    capacity_source: str,
    expected_filesystem_paths: tuple[str, ...],
) -> dict[str, Any]:
    if not rendered_yaml or len(rendered_yaml.encode()) > 16 * 1024 * 1024:
        raise LifecycleCapacityJobError("rendered manifest authority is invalid")
    matches: list[dict[str, Any]] = []
    try:
        documents = yaml.safe_load_all(rendered_yaml)
        for raw in documents:
            if not isinstance(raw, dict):
                continue
            metadata = raw.get("metadata")
            if (
                raw.get("apiVersion") == "batch/v1"
                and raw.get("kind") == "CronJob"
                and isinstance(metadata, dict)
                and metadata.get("name") == _CRONJOB
            ):
                matches.append(cast(dict[str, Any], raw))
    except yaml.YAMLError as exc:
        raise LifecycleCapacityJobError("rendered manifest authority is invalid") from exc
    if len(matches) != 1:
        raise LifecycleCapacityJobError("lifecycle CronJob authority is ambiguous")
    cronjob = matches[0]
    metadata = _mapping(cronjob["metadata"], "lifecycle CronJob metadata")
    if metadata.get("namespace") != _NAMESPACE or metadata.get("labels") != {"app": _APP_LABEL}:
        raise LifecycleCapacityJobError("lifecycle CronJob identity drifted")
    spec = _mapping(cronjob.get("spec"), "lifecycle CronJob spec")
    if (
        spec.get("concurrencyPolicy") != "Forbid"
        or spec.get("suspend") is not False
        or type(spec.get("startingDeadlineSeconds")) is not int
    ):
        raise LifecycleCapacityJobError("lifecycle CronJob execution policy drifted")
    job_template = _mapping(spec.get("jobTemplate"), "lifecycle Job template")
    job_spec = _mapping(job_template.get("spec"), "lifecycle Job spec")
    if (
        job_spec.get("backoffLimit") != 0
        or type(job_spec.get("activeDeadlineSeconds")) is not int
        or type(job_spec.get("ttlSecondsAfterFinished")) is not int
    ):
        raise LifecycleCapacityJobError("lifecycle Job bounds drifted")
    template = _mapping(job_spec.get("template"), "lifecycle Pod template")
    pod_metadata = _mapping(template.get("metadata"), "lifecycle Pod metadata")
    pod_spec = _mapping(template.get("spec"), "lifecycle Pod spec")
    if pod_metadata.get("labels") != {"app": _APP_LABEL}:
        raise LifecycleCapacityJobError("lifecycle Pod identity drifted")
    if (
        pod_spec.get("automountServiceAccountToken") is not False
        or pod_spec.get("restartPolicy") != "Never"
        or pod_spec.get("securityContext")
        != {
            "runAsNonRoot": True,
            "runAsUser": 65532,
            "runAsGroup": 65532,
            "seccompProfile": {"type": "RuntimeDefault"},
        }
    ):
        raise LifecycleCapacityJobError("lifecycle Pod authority drifted")
    containers = _sequence(pod_spec.get("containers"), "lifecycle containers")
    if len(containers) != 1:
        raise LifecycleCapacityJobError("lifecycle container authority is ambiguous")
    container = _mapping(containers[0], "lifecycle container")
    expected_image = (
        f"{container_registry}/loom-control-plane@{registry_digest}"
        if container_registry and registry_digest
        else f"loom-control-plane:{image_tag}"
    )
    if (
        container.get("name") != "lifecycle"
        or container.get("image") != expected_image
        or tuple(container.get("command", ())) != _COMMAND
        or container.get("securityContext")
        != {
            "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"]},
            "readOnlyRootFilesystem": True,
        }
    ):
        raise LifecycleCapacityJobError("lifecycle container authority drifted")
    expected_args = ["--action", "auto", "--namespace", _NAMESPACE]
    for bucket in expected_buckets:
        expected_args.extend(("--bucket", bucket))
    expected_args.extend(("--capacity-source", capacity_source))
    if capacity_source == "filesystem":
        for filesystem_path in expected_filesystem_paths:
            expected_args.extend(("--filesystem-path", filesystem_path))
    args = _sequence(container.get("args"), "lifecycle arguments")
    if args != expected_args:
        raise LifecycleCapacityJobError("lifecycle capacity input authority drifted")
    environment = _sequence(container.get("env"), "lifecycle environment")
    observed_environment = {
        item.get("name"): {key: value for key, value in item.items() if key != "name"}
        for raw in environment
        if isinstance((item := raw), dict)
    }
    expected_environment = {
        "LOOM_LIFECYCLE_DB_URL": {
            "valueFrom": {"secretKeyRef": {"key": "cp-db-url", "name": "loom-secrets"}}
        },
        "LOOM_LIFECYCLE_MINIO_ENDPOINT": {"value": "http://loom-minio:9000"},
        "LOOM_LIFECYCLE_MINIO_ACCESS_KEY": {
            "valueFrom": {"secretKeyRef": {"key": "minio-access-key", "name": "loom-secrets"}}
        },
        "LOOM_LIFECYCLE_MINIO_SECRET_KEY": {
            "valueFrom": {"secretKeyRef": {"key": "minio-secret-key", "name": "loom-secrets"}}
        },
        "LOOM_LIFECYCLE_MINIO_REGION": {"value": "us-east-1"},
        "LOOM_LIFECYCLE_STORAGE_AUTH_KIND": {"value": "static_keys"},
    }
    if (
        set(observed_environment) != _ENV_NAMES
        or len(environment) != len(_ENV_NAMES)
        or observed_environment != expected_environment
    ):
        raise LifecycleCapacityJobError("lifecycle environment authority drifted")
    volume_mounts = _sequence(container.get("volumeMounts", []), "lifecycle volume mounts")
    volumes = _sequence(pod_spec.get("volumes", []), "lifecycle volumes")
    expected_volume_mounts = (
        [
            {
                "mountPath": filesystem_path,
                "name": f"minio-capacity-{index}",
                "readOnly": True,
            }
            for index, filesystem_path in enumerate(expected_filesystem_paths)
        ]
        if capacity_source == "filesystem"
        else []
    )
    expected_volumes = (
        [
            {
                "name": f"minio-capacity-{index}",
                "persistentVolumeClaim": {
                    "claimName": f"data-loom-minio-{index}",
                    "readOnly": True,
                },
            }
            for index in range(len(expected_filesystem_paths))
        ]
        if capacity_source == "filesystem"
        else []
    )
    if volume_mounts != expected_volume_mounts or volumes != expected_volumes:
        raise LifecycleCapacityJobError("lifecycle capacity volume authority drifted")
    return copy.deepcopy(job_spec)


@dataclass(frozen=True, slots=True)
class LifecycleCapacityJobPlan:
    candidate_sha: str
    candidate_tree: str
    mutation_epoch: int
    artifact_bundle_sha256: str
    rendered_manifest_sha256: str
    control_plane_image_id: str
    image_tag: str
    namespace: str
    job_name: str
    job_manifest: str
    job_manifest_sha256: str
    plan_digest: str
    control_plane_registry_digest: str = ""
    schema_version: int = 1

    def __post_init__(self) -> None:
        record = self._record(include_digest=False)
        if (
            _SHA_RE.fullmatch(self.candidate_sha) is None
            or _SHA_RE.fullmatch(self.candidate_tree) is None
            or self.mutation_epoch < 0
            or _SHA256_RE.fullmatch(self.artifact_bundle_sha256) is None
            or _SHA256_RE.fullmatch(self.rendered_manifest_sha256) is None
            or _IMAGE_ID_RE.fullmatch(self.control_plane_image_id) is None
            or (
                self.control_plane_registry_digest
                and _IMAGE_ID_RE.fullmatch(self.control_plane_registry_digest) is None
            )
            or _IMAGE_TAG_RE.fullmatch(self.image_tag) is None
            or self.namespace != _NAMESPACE
            or _DNS_RE.fullmatch(self.job_name) is None
            or not self.job_manifest.endswith("\n")
            or hashlib.sha256(self.job_manifest.encode()).hexdigest() != self.job_manifest_sha256
            or _digest(record) != self.plan_digest
            or self.schema_version != 1
        ):
            raise LifecycleCapacityJobError("lifecycle capacity Job plan identity is invalid")

    def _record(self, *, include_digest: bool) -> dict[str, object]:
        value: dict[str, object] = {
            "artifact_bundle_sha256": self.artifact_bundle_sha256,
            "candidate_sha": self.candidate_sha,
            "candidate_tree": self.candidate_tree,
            "control_plane_image_id": self.control_plane_image_id,
            "image_tag": self.image_tag,
            "job_manifest": self.job_manifest,
            "job_manifest_sha256": self.job_manifest_sha256,
            "job_name": self.job_name,
            "mutation_epoch": self.mutation_epoch,
            "namespace": self.namespace,
            "rendered_manifest_sha256": self.rendered_manifest_sha256,
            "schema_version": self.schema_version,
        }
        if include_digest:
            value["plan_digest"] = self.plan_digest
        if self.control_plane_registry_digest:
            value["control_plane_registry_digest"] = self.control_plane_registry_digest
        return value

    def to_dict(self) -> dict[str, object]:
        return self._record(include_digest=True)

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> LifecycleCapacityJobPlan:
        expected = {
            "artifact_bundle_sha256",
            "candidate_sha",
            "candidate_tree",
            "control_plane_image_id",
            "image_tag",
            "job_manifest",
            "job_manifest_sha256",
            "job_name",
            "mutation_epoch",
            "namespace",
            "plan_digest",
            "rendered_manifest_sha256",
            "schema_version",
        }
        if set(value) not in {frozenset(expected), frozenset((*expected, "control_plane_registry_digest"))}:
            raise LifecycleCapacityJobError("lifecycle capacity Job plan keys are invalid")
        try:
            return cls(**cast(Any, value))
        except TypeError as exc:
            raise LifecycleCapacityJobError("lifecycle capacity Job plan is invalid") from exc


def build_lifecycle_capacity_job_plan(
    *,
    candidate_sha: str,
    candidate_tree: str,
    mutation_epoch: int,
    artifact_bundle_sha256: str,
    rendered_manifest_sha256: str,
    control_plane_image_id: str,
    image_tag: str,
    rendered_yaml: str,
    expected_buckets: tuple[str, ...],
    expected_filesystem_paths: tuple[str, ...],
    capacity_source: str = "filesystem",
    container_registry: str = "",
    registry_digest: str = "",
) -> LifecycleCapacityJobPlan:
    if (
        len(expected_buckets) != 2
        or len(set(expected_buckets)) != 2
        or capacity_source not in {"filesystem", "minio-admin"}
        or (capacity_source == "filesystem" and not expected_filesystem_paths)
        or (capacity_source == "minio-admin" and bool(expected_filesystem_paths))
        or len(set(expected_filesystem_paths)) != len(expected_filesystem_paths)
        or bool(container_registry) != bool(registry_digest)
        or (registry_digest and _IMAGE_ID_RE.fullmatch(registry_digest) is None)
        or any(
            not value or "\x00" in value
            for value in (*expected_buckets, *expected_filesystem_paths)
        )
    ):
        raise LifecycleCapacityJobError("lifecycle capacity expected inputs are invalid")
    job_spec = _require_job_template(
        rendered_yaml,
        image_tag=image_tag,
        container_registry=container_registry,
        registry_digest=registry_digest,
        expected_buckets=expected_buckets,
        capacity_source=capacity_source,
        expected_filesystem_paths=expected_filesystem_paths,
    )
    job_name = f"loom-staging-capacity-{candidate_sha[:8]}-{artifact_bundle_sha256[:8]}"
    annotations = {
        "loom.carin.dev/candidate-sha": candidate_sha,
        "loom.carin.dev/candidate-tree": candidate_tree,
        "loom.carin.dev/control-plane-image-id": control_plane_image_id,
        "loom.carin.dev/preflight-artifact": artifact_bundle_sha256,
        "loom.carin.dev/rendered-manifest": rendered_manifest_sha256,
    }
    template = _mapping(job_spec["template"], "lifecycle Pod template")
    metadata = _mapping(template["metadata"], "lifecycle Pod metadata")
    metadata["annotations"] = annotations
    container = _mapping(
        _sequence(_mapping(template["spec"], "lifecycle Pod spec")["containers"], "containers")[0],
        "lifecycle container",
    )
    args = _sequence(container["args"], "lifecycle arguments")
    args[1] = "capacity"
    document = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "annotations": annotations,
            "labels": {"app": _APP_LABEL},
            "name": job_name,
            "namespace": _NAMESPACE,
        },
        "spec": job_spec,
    }
    job_manifest = yaml.safe_dump(document, sort_keys=True)
    job_manifest_sha256 = hashlib.sha256(job_manifest.encode()).hexdigest()
    raw = {
        "artifact_bundle_sha256": artifact_bundle_sha256,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "control_plane_image_id": control_plane_image_id,
        "image_tag": image_tag,
        "job_manifest": job_manifest,
        "job_manifest_sha256": job_manifest_sha256,
        "job_name": job_name,
        "mutation_epoch": mutation_epoch,
        "namespace": _NAMESPACE,
        "rendered_manifest_sha256": rendered_manifest_sha256,
        "schema_version": 1,
    }
    if registry_digest:
        raw["control_plane_registry_digest"] = registry_digest
    return LifecycleCapacityJobPlan(
        **raw,
        plan_digest=_digest(raw),
    )


__all__ = [
    "LifecycleCapacityJobError",
    "LifecycleCapacityJobPlan",
    "build_lifecycle_capacity_job_plan",
]
