"""Canonical Trial bundle identity, inventory parsing and lookup.

Shared by Run Library, Trial downloads and delivery exports. This module reads
retained Artifact records without constructing archives or publishing objects;
callers retain authorization and transaction ownership.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import UUID

from sqlalchemy import select

from loom.db.schema import Artifact, Trial
from loom_service.delivery_export_errors import InvalidDeliveryBatchFamilyError


@dataclass(frozen=True)
class ObjectRef:
    kind: str
    trial_id: UUID
    bucket: str
    key: str

    def as_dict(self) -> dict[str, str]:
        return {
            "kind": self.kind,
            "trial_id": str(self.trial_id),
            "bucket": self.bucket,
            "key": self.key,
        }


@dataclass(frozen=True)
class CanonicalTrialBundleFile:
    relative_path: str
    ref: ObjectRef
    size_bytes: int
    sha256: str
    media_type: str


@dataclass(frozen=True)
class CanonicalTrialBundle:
    artifact_id: UUID
    trial_id: UUID
    task_id: str
    attempt: int
    manifest_sha256: str
    content_sha256: str
    files: tuple[CanonicalTrialBundleFile, ...]


class CanonicalTrialIdentity(Protocol):
    @property
    def id(self) -> UUID: ...

    @property
    def task_id(self) -> str: ...

    @property
    def attempt_count(self) -> int: ...


def has_archive_path_traversal(rel: str) -> bool:
    parts = Path(rel.replace("\\", "/")).parts
    if not parts:
        return False
    if parts[0] in ("/", "\\") or (len(parts[0]) == 2 and parts[0][1] == ":"):
        return True
    return ".." in parts


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and value.startswith("sha256:")
        and len(value) == 71
        and all(character in "0123456789abcdef" for character in value[7:])
    )


def canonical_bundle_from_artifact(
    artifact: Artifact,
    *,
    trial: CanonicalTrialIdentity,
) -> CanonicalTrialBundle | None:
    if artifact.trial_id != trial.id:
        return None
    storage = artifact.storage if isinstance(artifact.storage, dict) else {}
    if storage.get("schema_version") != "loom.canonical-trial-bundle-storage.v1":
        return None
    attempt = storage.get("attempt")
    if attempt != trial.attempt_count:
        return None
    raw_files = storage.get("files")
    raw_source_evidence = storage.get("source_evidence")
    manifest_sha256 = artifact.manifest_sha256
    if not _is_sha256(manifest_sha256) and isinstance(raw_source_evidence, list):
        source_manifest = next(
            (
                raw.get("sha256")
                for raw in raw_source_evidence
                if isinstance(raw, dict)
                and raw.get("relative_path") == "source/_manifest.json"
            ),
            None,
        )
        manifest_sha256 = source_manifest if isinstance(source_manifest, str) else None
    if not _is_sha256(manifest_sha256) or not _is_sha256(artifact.content_hash):
        raise InvalidDeliveryBatchFamilyError(
            {"message": "canonical Trial bundle digest identity is invalid"}
        )
    if not isinstance(raw_files, list) or not isinstance(raw_source_evidence, list):
        raise InvalidDeliveryBatchFamilyError(
            {"message": "canonical Trial bundle inventory is incomplete"}
        )
    files: list[CanonicalTrialBundleFile] = []
    archive_paths: set[str] = set()
    for raw, prefix in (
        *((record, "files/") for record in raw_files),
        *((record, "") for record in raw_source_evidence),
    ):
        if not isinstance(raw, dict):
            raise InvalidDeliveryBatchFamilyError(
                {"message": "canonical Trial bundle file is invalid"}
            )
        source_path = raw.get("relative_path")
        relative_path = prefix + source_path if isinstance(source_path, str) else ""
        bucket = raw.get("bucket")
        key = raw.get("key")
        size_bytes = raw.get("size_bytes")
        sha256 = raw.get("sha256")
        media_type = raw.get("media_type")
        if (
            not relative_path
            or has_archive_path_traversal(relative_path)
            or relative_path in archive_paths
            or not isinstance(bucket, str)
            or not bucket
            or not isinstance(key, str)
            or not key
            or isinstance(size_bytes, bool)
            or not isinstance(size_bytes, int)
            or size_bytes < 0
            or not _is_sha256(sha256)
            or not isinstance(media_type, str)
            or not media_type
        ):
            raise InvalidDeliveryBatchFamilyError(
                {"message": "canonical Trial bundle file identity is invalid"}
            )
        archive_paths.add(relative_path)
        files.append(
            CanonicalTrialBundleFile(
                relative_path=relative_path,
                ref=ObjectRef(
                    kind="trial_bundle",
                    trial_id=trial.id,
                    bucket=bucket,
                    key=key,
                ),
                size_bytes=size_bytes,
                sha256=cast(str, sha256),
                media_type=media_type,
            )
        )
    return CanonicalTrialBundle(
        artifact_id=artifact.id,
        trial_id=trial.id,
        task_id=trial.task_id,
        attempt=attempt,
        manifest_sha256=cast(str, manifest_sha256),
        content_sha256=str(artifact.content_hash or ""),
        files=tuple(files),
    )


async def canonical_bundle_for_trial(
    session: Any,
    *,
    trial: Trial,
) -> CanonicalTrialBundle | None:
    artifacts = list(
        (
            await session.execute(
                select(Artifact)
                .where(
                    Artifact.trial_id == trial.id,
                    Artifact.control_producer_kind == "service_execution",
                )
                .order_by(Artifact.created_at.asc(), Artifact.id.asc()),
            )
        )
        .scalars()
        .all()
    )
    bundles = [
        bundle
        for artifact in artifacts
        if (bundle := canonical_bundle_from_artifact(artifact, trial=trial)) is not None
    ]
    if len(bundles) > 1:
        raise InvalidDeliveryBatchFamilyError(
            {"message": "multiple canonical Trial bundles match one selected attempt"}
        )
    return bundles[0] if bundles else None


def canonical_trial_bundle_manifest(bundle: CanonicalTrialBundle) -> dict[str, Any]:
    return {
        "schema_version": "loom.canonical-trial-bundle-export.v1",
        "artifact_id": str(bundle.artifact_id),
        "trial_id": str(bundle.trial_id),
        "task_id": bundle.task_id,
        "attempt": bundle.attempt,
        "manifest_sha256": bundle.manifest_sha256,
        "content_sha256": bundle.content_sha256,
        "files": [
            {
                "relative_path": file.relative_path,
                "size_bytes": file.size_bytes,
                "sha256": file.sha256,
                "media_type": file.media_type,
            }
            for file in bundle.files
        ],
    }
