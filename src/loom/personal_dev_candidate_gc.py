"""Lease-fenced, repeatable garbage collection for personal-dev artifacts."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from loom.personal_dev_candidate import (
    PERSONAL_DEV_COMPONENTS,
    PersonalDevCandidateBuildAttemptRecord,
    PersonalDevCandidateRecord,
)

_REGISTRY_PREFIX_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,511}")
_MAX_DELETE_BATCH = 1000
_MAX_GC_ATTEMPTS = 1024
_MAX_GC_REGISTRY_TAGS = 10_000
_MAX_GC_MANIFEST_BYTES = 4 * 1024 * 1024
_MAX_REGISTRY_REPOSITORY_LENGTH = 512
_MAX_REGISTRY_PREFIX_LENGTH = _MAX_REGISTRY_REPOSITORY_LENGTH - (
    5  # separators before team, owner, candidate, attempt, and component
    + 36 * 3  # team, owner, and attempt UUIDs
    + 64  # candidate digest
    + len("loom-")
    + max(len(component) for component in PERSONAL_DEV_COMPONENTS)
)
_MANIFEST_KEYS = frozenset(
    {
        "candidate_id",
        "candidate_sha",
        "object_bucket",
        "object_prefixes",
        "owner_team_id",
        "owner_user_id",
        "registry_tags",
        "schema_version",
        "source_generation_id",
        "source_object_key",
    }
)


class PersonalDevArtifactGcAuthorityUnavailableError(ValueError):
    """Legacy candidate artifacts lack a safe registry deletion authority."""


def validate_personal_dev_registry_prefix(value: str) -> str:
    """Validate the one configured registry authority used for personal dev."""

    if (
        _REGISTRY_PREFIX_RE.fullmatch(value) is None
        or len(value) > _MAX_REGISTRY_PREFIX_LENGTH
        or value.endswith(("/", ":"))
        or "://" in value
        or "@" in value
    ):
        raise ValueError("personal-dev registry prefix is invalid")
    return value


def personal_dev_source_object_keys(
    candidate: PersonalDevCandidateRecord,
) -> frozenset[str]:
    """Return the generated key plus the migration-only legacy binding."""

    root = (
        f"personal-dev/sources/{candidate.owner_team_id}/{candidate.owner_user_id}/"
        f"{candidate.candidate_sha}/"
    )
    generated = (
        f"{root}{candidate.source_generation_id}/{candidate.archive_sha256}.tar"
    )
    if candidate.source_generation_id != candidate.id:
        return frozenset({generated})
    return frozenset({generated, f"{root}{candidate.archive_sha256}.tar"})


def personal_dev_registry_repository(
    candidate: PersonalDevCandidateRecord,
    attempt: PersonalDevCandidateBuildAttemptRecord,
    *,
    component: str,
) -> str:
    """Return the attempt-isolated repository used by trusted publication."""

    prefix = candidate.registry_prefix
    if (
        prefix is None
        or component not in PERSONAL_DEV_COMPONENTS
        or attempt.candidate_id != candidate.id
    ):
        raise ValueError("personal-dev registry binding is invalid")
    try:
        validate_personal_dev_registry_prefix(prefix)
    except ValueError as exc:
        raise ValueError("personal-dev registry binding is invalid") from exc
    repository = (
        f"{prefix}/{candidate.owner_team_id}/{candidate.owner_user_id}/"
        f"{candidate.candidate_sha}/{attempt.id}/loom-{component}"
    )
    if len(repository) > _MAX_REGISTRY_REPOSITORY_LENGTH:  # pragma: no cover - prefix bound
        raise ValueError("personal-dev registry binding is invalid")
    return repository


def personal_dev_registry_tag(
    candidate: PersonalDevCandidateRecord,
    attempt: PersonalDevCandidateBuildAttemptRecord,
    *,
    lease_epoch: int,
    suffix: str,
) -> str:
    if type(lease_epoch) is not int or lease_epoch <= 0:
        raise ValueError("personal-dev registry lease epoch is invalid")
    if suffix not in {"amd64", "arm64", "index"}:
        raise ValueError("personal-dev registry tag suffix is invalid")
    return (
        f"pdc-{candidate.candidate_sha[:12]}-{attempt.id.hex}-"
        f"l{lease_epoch:016x}-{suffix}"
    )


@dataclass(frozen=True, slots=True)
class PersonalDevArtifactGcManifest:
    candidate_id: UUID
    owner_user_id: UUID
    owner_team_id: UUID
    candidate_sha: str
    object_bucket: str
    source_generation_id: UUID
    source_object_key: str
    object_prefixes: tuple[str, ...]
    registry_tags: tuple[str, ...]
    manifest_sha256: str
    schema_version: int = 1

    def payload(self) -> dict[str, object]:
        return {
            "candidate_id": str(self.candidate_id),
            "candidate_sha": self.candidate_sha,
            "object_bucket": self.object_bucket,
            "object_prefixes": list(self.object_prefixes),
            "owner_team_id": str(self.owner_team_id),
            "owner_user_id": str(self.owner_user_id),
            "registry_tags": list(self.registry_tags),
            "schema_version": self.schema_version,
            "source_generation_id": str(self.source_generation_id),
            "source_object_key": self.source_object_key,
        }

    def validate(self) -> None:
        _validate_manifest(self)

    @classmethod
    def from_json(cls, value: Mapping[str, object], digest: str) -> PersonalDevArtifactGcManifest:
        if set(value) != _MANIFEST_KEYS:
            raise ValueError("personal-dev artifact GC manifest is invalid")
        try:
            candidate_id_value = value["candidate_id"]
            owner_user_id_value = value["owner_user_id"]
            owner_team_id_value = value["owner_team_id"]
            candidate_sha = value["candidate_sha"]
            object_bucket = value["object_bucket"]
            source_generation_id_value = value["source_generation_id"]
            source_object_key = value["source_object_key"]
            object_prefix_values = value["object_prefixes"]
            registry_tag_values = value["registry_tags"]
            schema_version_value = value["schema_version"]
        except KeyError as exc:  # pragma: no cover - exact keys checked above
            raise ValueError("personal-dev artifact GC manifest is invalid") from exc
        if (
            not isinstance(candidate_id_value, str)
            or not isinstance(owner_user_id_value, str)
            or not isinstance(owner_team_id_value, str)
            or not isinstance(source_generation_id_value, str)
            or not isinstance(candidate_sha, str)
            or not isinstance(object_bucket, str)
            or not isinstance(source_object_key, str)
            or not isinstance(object_prefix_values, list)
            or any(not isinstance(item, str) for item in object_prefix_values)
            or not isinstance(registry_tag_values, list)
            or any(not isinstance(item, str) for item in registry_tag_values)
            or type(schema_version_value) is not int
        ):
            raise ValueError("personal-dev artifact GC manifest is invalid")
        try:
            candidate_id = UUID(candidate_id_value)
            owner_user_id = UUID(owner_user_id_value)
            owner_team_id = UUID(owner_team_id_value)
            source_generation_id = UUID(source_generation_id_value)
        except ValueError as exc:
            raise ValueError("personal-dev artifact GC manifest is invalid") from exc
        object_prefixes = tuple(object_prefix_values)
        registry_tags = tuple(registry_tag_values)
        schema_version = schema_version_value
        manifest = cls(
            candidate_id=candidate_id,
            owner_user_id=owner_user_id,
            owner_team_id=owner_team_id,
            candidate_sha=candidate_sha,
            object_bucket=object_bucket,
            source_generation_id=source_generation_id,
            source_object_key=source_object_key,
            object_prefixes=object_prefixes,
            registry_tags=registry_tags,
            manifest_sha256=digest,
            schema_version=schema_version,
        )
        if _manifest_digest(manifest.payload()) != digest:
            raise ValueError("personal-dev artifact GC manifest digest is invalid")
        _validate_manifest(manifest)
        return manifest


def _manifest_digest(payload: Mapping[str, object]) -> str:
    try:
        encoded = json.dumps(
            dict(payload),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ValueError("personal-dev artifact GC manifest is invalid") from exc
    if len(encoded) > _MAX_GC_MANIFEST_BYTES:
        raise ValueError("personal-dev artifact GC manifest is too large")
    return hashlib.sha256(encoded).hexdigest()


def _validate_manifest(manifest: PersonalDevArtifactGcManifest) -> None:
    if (
        manifest.schema_version != 1
        or re.fullmatch(r"[0-9a-f]{64}", manifest.candidate_sha) is None
        or re.fullmatch(r"[0-9a-f]{64}", manifest.manifest_sha256) is None
        or not manifest.object_bucket
        or manifest.object_bucket.strip() != manifest.object_bucket
        or "/" in manifest.object_bucket
        or tuple(sorted(set(manifest.object_prefixes))) != manifest.object_prefixes
        or tuple(sorted(set(manifest.registry_tags))) != manifest.registry_tags
        or len(manifest.object_prefixes) > _MAX_GC_ATTEMPTS * 2
        or len(manifest.registry_tags) > _MAX_GC_REGISTRY_TAGS
        or _manifest_digest(manifest.payload()) != manifest.manifest_sha256
    ):
        raise ValueError("personal-dev artifact GC manifest is invalid")
    team = re.escape(str(manifest.owner_team_id))
    owner = re.escape(str(manifest.owner_user_id))
    candidate = re.escape(manifest.candidate_sha)
    legacy_source_pattern = re.compile(
        rf"personal-dev/sources/{team}/{owner}/{candidate}/[0-9a-f]{{64}}\.tar"
    )
    generated_source_pattern = re.compile(
        rf"personal-dev/sources/{team}/{owner}/{candidate}/"
        rf"{re.escape(str(manifest.source_generation_id))}/[0-9a-f]{{64}}\.tar"
    )
    build_pattern = re.compile(
        rf"personal-dev/builds/{team}/{owner}/{candidate}/"
        r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/"
    )
    evidence_pattern = re.compile(
        rf"personal-dev/evidence/{candidate}/"
        r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/"
    )
    if (
        generated_source_pattern.fullmatch(manifest.source_object_key) is None
        and (
            manifest.source_generation_id != manifest.candidate_id
            or legacy_source_pattern.fullmatch(manifest.source_object_key) is None
        )
    ):
        raise ValueError("personal-dev artifact GC source scope is invalid")
    build_attempts = {
        match.group(1)
        for prefix in manifest.object_prefixes
        if (match := build_pattern.fullmatch(prefix)) is not None
    }
    evidence_attempts = {
        match.group(1)
        for prefix in manifest.object_prefixes
        if (match := evidence_pattern.fullmatch(prefix)) is not None
    }
    if (
        len(manifest.object_prefixes) != len(build_attempts) + len(evidence_attempts)
        or build_attempts != evidence_attempts
    ):
        raise ValueError("personal-dev artifact GC object scope is invalid")
    component = "(?:" + "|".join(re.escape(item) for item in PERSONAL_DEV_COMPONENTS) + ")"
    registry_pattern = re.compile(
        rf"[A-Za-z0-9][A-Za-z0-9._:/-]*/{team}/{owner}/{candidate}/"
        rf"(?P<attempt>[0-9a-f-]{{36}})/loom-{component}:"
        rf"pdc-{re.escape(manifest.candidate_sha[:12])}-"
        r"(?P<attempt_hex>[0-9a-f]{32})-l[0-9a-f]{16}-(?:amd64|arm64|index)"
    )
    for reference in manifest.registry_tags:
        match = registry_pattern.fullmatch(reference)
        if (
            match is None
            or match.group("attempt") not in build_attempts
            or UUID(match.group("attempt")).hex != match.group("attempt_hex")
        ):
            raise ValueError("personal-dev artifact GC registry scope is invalid")


def build_personal_dev_artifact_gc_manifest(
    candidate: PersonalDevCandidateRecord,
    attempts: Sequence[PersonalDevCandidateBuildAttemptRecord],
) -> PersonalDevArtifactGcManifest:
    if candidate.object_key not in personal_dev_source_object_keys(candidate):
        raise ValueError("personal-dev source object key is outside its owner scope")
    ordered_attempts = sorted(attempts, key=lambda item: (item.id.hex, item.attempt_sequence))
    if len(ordered_attempts) > _MAX_GC_ATTEMPTS:
        raise ValueError("personal-dev artifact GC attempt history is too large")
    if any(attempt.candidate_id != candidate.id for attempt in ordered_attempts):
        raise ValueError("personal-dev artifact GC attempt binding is invalid")
    object_prefixes = tuple(
        sorted(
            prefix
            for attempt in ordered_attempts
            for prefix in (
                f"personal-dev/builds/{candidate.owner_team_id}/{candidate.owner_user_id}/"
                f"{candidate.candidate_sha}/{attempt.id}/",
                f"personal-dev/evidence/{candidate.candidate_sha}/{attempt.id}/",
            )
        )
    )
    registry_tags: tuple[str, ...] = ()
    if candidate.registry_prefix is None and any(
        attempt.lease_epoch > 0 for attempt in ordered_attempts
    ):
        raise PersonalDevArtifactGcAuthorityUnavailableError(
            "personal-dev artifact GC registry authority is unavailable"
        )
    if candidate.registry_prefix is not None:
        expected_tags = sum(
            attempt.lease_epoch * len(PERSONAL_DEV_COMPONENTS) * 3
            for attempt in ordered_attempts
            if attempt.lease_epoch > 0
        )
        if expected_tags > _MAX_GC_REGISTRY_TAGS:
            raise ValueError("personal-dev artifact GC registry history is too large")
        tags: list[str] = []
        for attempt in ordered_attempts:
            if attempt.lease_epoch < 0:
                raise ValueError("personal-dev artifact GC lease history is invalid")
            for component in PERSONAL_DEV_COMPONENTS:
                repository = personal_dev_registry_repository(
                    candidate,
                    attempt,
                    component=component,
                )
                for lease_epoch in range(1, attempt.lease_epoch + 1):
                    for suffix in ("amd64", "arm64", "index"):
                        tag = personal_dev_registry_tag(
                            candidate,
                            attempt,
                            lease_epoch=lease_epoch,
                            suffix=suffix,
                        )
                        tags.append(f"{repository}:{tag}")
        registry_tags = tuple(sorted(set(tags)))
    payload: dict[str, object] = {
        "candidate_id": str(candidate.id),
        "candidate_sha": candidate.candidate_sha,
        "object_bucket": candidate.object_bucket,
        "object_prefixes": list(object_prefixes),
        "owner_team_id": str(candidate.owner_team_id),
        "owner_user_id": str(candidate.owner_user_id),
        "registry_tags": list(registry_tags),
        "schema_version": 1,
        "source_generation_id": str(candidate.source_generation_id),
        "source_object_key": candidate.object_key,
    }
    manifest = PersonalDevArtifactGcManifest(
        candidate_id=candidate.id,
        owner_user_id=candidate.owner_user_id,
        owner_team_id=candidate.owner_team_id,
        candidate_sha=candidate.candidate_sha,
        object_bucket=candidate.object_bucket,
        source_generation_id=candidate.source_generation_id,
        source_object_key=candidate.object_key,
        object_prefixes=object_prefixes,
        registry_tags=registry_tags,
        manifest_sha256=_manifest_digest(payload),
    )
    _validate_manifest(manifest)
    return manifest


@dataclass(frozen=True, slots=True)
class PersonalDevArtifactGcClaim:
    candidate_id: UUID
    collector_id: str
    lease_epoch: int
    lease_expires_at: datetime
    manifest: PersonalDevArtifactGcManifest


class PersonalDevArtifactGcAuthority(Protocol):
    async def claim_next_artifact_gc(self, **kwargs: object) -> PersonalDevArtifactGcClaim | None: ...

    async def mark_next_artifact_gc(self, **kwargs: object) -> bool: ...

    async def heartbeat_artifact_gc(self, **kwargs: object) -> None: ...

    async def finish_artifact_gc(self, **kwargs: object) -> None: ...


class PersonalDevArtifactCollector(Protocol):
    async def collect(self, manifest: PersonalDevArtifactGcManifest) -> None: ...


@dataclass(slots=True)
class PersonalDevArtifactGcCoordinator:
    authority: PersonalDevArtifactGcAuthority
    collector: PersonalDevArtifactCollector
    collector_id: str
    retention_seconds: int
    lease_seconds: int
    heartbeat_interval_seconds: float | None = None

    def __post_init__(self) -> None:
        if (
            not self.collector_id
            or self.collector_id.strip() != self.collector_id
            or len(self.collector_id) > 128
        ):
            raise ValueError("personal-dev artifact collector identifier is invalid")
        if type(self.retention_seconds) is not int or self.retention_seconds < 0:
            raise ValueError("personal-dev artifact retention must be a non-negative integer")
        if type(self.lease_seconds) is not int or self.lease_seconds <= 0:
            raise ValueError("personal-dev artifact GC lease must be a positive integer")
        if self.heartbeat_interval_seconds is not None and (
            self.heartbeat_interval_seconds <= 0
            or self.heartbeat_interval_seconds >= self.lease_seconds
        ):
            raise ValueError("personal-dev artifact GC heartbeat interval is invalid")

    async def collect_once(self, *, now: datetime) -> bool:
        if now.tzinfo is None:
            raise ValueError("personal-dev artifact GC time must include a timezone")
        claim = await self.authority.claim_next_artifact_gc(
            collector_id=self.collector_id,
            now=now,
            retention_seconds=self.retention_seconds,
            lease_seconds=self.lease_seconds,
        )
        if claim is None:
            return await self.authority.mark_next_artifact_gc(now=now)
        if (
            claim.collector_id != self.collector_id
            or claim.lease_epoch <= 0
            or claim.lease_expires_at <= now
            or claim.candidate_id != claim.manifest.candidate_id
        ):
            raise RuntimeError("personal-dev artifact GC claim is inconsistent")
        loop = asyncio.get_running_loop()
        started = loop.time()
        heartbeat_interval = self.heartbeat_interval_seconds or max(
            0.1,
            self.lease_seconds / 3,
        )
        collected = asyncio.create_task(
            self.collector.collect(claim.manifest),
            name=f"loom-personal-dev-artifact-gc-{claim.candidate_id}",
        )
        authority_now = now
        try:
            while not collected.done():
                done, _pending = await asyncio.wait(
                    {collected},
                    timeout=heartbeat_interval,
                )
                if done:
                    break
                authority_now = now + timedelta(seconds=loop.time() - started)
                await self.authority.heartbeat_artifact_gc(
                    candidate_id=claim.candidate_id,
                    collector_id=self.collector_id,
                    lease_epoch=claim.lease_epoch,
                    manifest_sha256=claim.manifest.manifest_sha256,
                    now=authority_now,
                    lease_seconds=self.lease_seconds,
                )
            await collected
        except BaseException:
            collected.cancel()
            await asyncio.gather(collected, return_exceptions=True)
            raise
        authority_now = now + timedelta(seconds=loop.time() - started)
        await self.authority.finish_artifact_gc(
            candidate_id=claim.candidate_id,
            collector_id=self.collector_id,
            lease_epoch=claim.lease_epoch,
            manifest_sha256=claim.manifest.manifest_sha256,
            now=authority_now,
        )
        return True


@dataclass(slots=True)
class S3PersonalDevArtifactCollector:
    client: Any
    expected_bucket: str

    def __post_init__(self) -> None:
        if not self.expected_bucket or "/" in self.expected_bucket:
            raise ValueError("personal-dev artifact bucket is invalid")

    @staticmethod
    def _page_items(page: object, field: str) -> list[Mapping[str, object]]:
        if not isinstance(page, Mapping):
            raise RuntimeError("personal-dev object cleanup listing is invalid")
        items = page.get(field, [])
        if not isinstance(items, list) or any(not isinstance(item, Mapping) for item in items):
            raise RuntimeError("personal-dev object cleanup listing is invalid")
        return items

    def _delete(self, bucket: str, objects: list[dict[str, str]]) -> None:
        for offset in range(0, len(objects), _MAX_DELETE_BATCH):
            batch = objects[offset : offset + _MAX_DELETE_BATCH]
            if not batch:
                continue
            result = self.client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": batch, "Quiet": False},
            )
            if not isinstance(result, Mapping) or result.get("Errors"):
                raise RuntimeError("personal-dev object cleanup batch failed")
            deleted = result.get("Deleted")
            if not isinstance(deleted, list):
                raise RuntimeError("personal-dev object cleanup returned no deletion evidence")
            expected = {
                (item["Key"], item.get("VersionId", ""))
                for item in batch
            }
            observed = {
                (str(item.get("Key", "")), str(item.get("VersionId", "")))
                for item in deleted
                if isinstance(item, Mapping)
            }
            if observed != expected:
                raise RuntimeError("personal-dev object cleanup identity drifted")

    def _collect_target(self, bucket: str, target: str, *, exact: bool) -> None:
        for page in self.client.get_paginator("list_multipart_uploads").paginate(
            Bucket=bucket,
            Prefix=target,
        ):
            for upload in self._page_items(page, "Uploads"):
                key = upload.get("Key")
                upload_id = upload.get("UploadId")
                if not isinstance(key, str) or not isinstance(upload_id, str):
                    raise RuntimeError("personal-dev multipart cleanup identity is invalid")
                if exact and key != target:
                    continue
                self.client.abort_multipart_upload(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                )
        for page in self.client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket,
            Prefix=target,
        ):
            objects = []
            for item in self._page_items(page, "Contents"):
                key = item.get("Key")
                if not isinstance(key, str):
                    raise RuntimeError("personal-dev object cleanup identity is invalid")
                if not exact or key == target:
                    objects.append({"Key": key})
            self._delete(bucket, objects)
        for page in self.client.get_paginator("list_object_versions").paginate(
            Bucket=bucket,
            Prefix=target,
        ):
            versions = []
            for field in ("Versions", "DeleteMarkers"):
                for item in self._page_items(page, field):
                    key = item.get("Key")
                    version = item.get("VersionId")
                    if not isinstance(key, str) or not isinstance(version, str):
                        raise RuntimeError("personal-dev object version identity is invalid")
                    if not exact or key == target:
                        versions.append({"Key": key, "VersionId": version})
            self._delete(bucket, versions)
        self._assert_target_empty(bucket, target, exact=exact)

    def _assert_target_empty(self, bucket: str, target: str, *, exact: bool) -> None:
        listing_fields = (
            ("list_multipart_uploads", ("Uploads",)),
            ("list_objects_v2", ("Contents",)),
            ("list_object_versions", ("Versions", "DeleteMarkers")),
        )
        for paginator_name, fields in listing_fields:
            for page in self.client.get_paginator(paginator_name).paginate(
                Bucket=bucket,
                Prefix=target,
            ):
                for field in fields:
                    for item in self._page_items(page, field):
                        key = item.get("Key")
                        if not isinstance(key, str):
                            raise RuntimeError(
                                "personal-dev object cleanup verification is invalid"
                            )
                        if not exact or key == target:
                            raise RuntimeError(
                                "personal-dev object cleanup verification failed"
                            )

    def _collect(self, manifest: PersonalDevArtifactGcManifest) -> None:
        manifest.validate()
        if manifest.object_bucket != self.expected_bucket:
            raise RuntimeError("personal-dev artifact GC bucket binding changed")
        for prefix in manifest.object_prefixes:
            self._collect_target(manifest.object_bucket, prefix, exact=False)
        self._collect_target(
            manifest.object_bucket,
            manifest.source_object_key,
            exact=True,
        )

    async def collect(self, manifest: PersonalDevArtifactGcManifest) -> None:
        operation = asyncio.create_task(
            asyncio.to_thread(self._collect, manifest),
            name=f"loom-personal-dev-s3-artifact-gc-{manifest.candidate_id}",
        )
        try:
            await asyncio.shield(operation)
        except asyncio.CancelledError:
            # Python cannot stop a running worker thread. Wait for the exact,
            # idempotent deletion manifest to finish before allowing service
            # shutdown to abandon its lease and a re-upload to race the thread.
            await operation
            raise


__all__ = [
    "PersonalDevArtifactGcAuthorityUnavailableError",
    "PersonalDevArtifactGcClaim",
    "PersonalDevArtifactGcCoordinator",
    "PersonalDevArtifactGcManifest",
    "S3PersonalDevArtifactCollector",
    "build_personal_dev_artifact_gc_manifest",
    "personal_dev_registry_repository",
    "personal_dev_registry_tag",
    "personal_dev_source_object_keys",
    "validate_personal_dev_registry_prefix",
]
