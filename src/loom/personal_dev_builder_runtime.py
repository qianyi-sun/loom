"""Kubernetes execution boundary for native personal-candidate build sandboxes."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol
from urllib.parse import urlsplit

import yaml  # type: ignore[import-untyped]

from loom.personal_dev_builder_manifest import (
    PersonalDevBuilderManifestConfig,
    personal_dev_builder_manifest_documents,
)
from loom.personal_dev_candidate import (
    PERSONAL_DEV_PLATFORMS,
    CandidateRegistration,
    PersonalDevPlatform,
)


@dataclass(frozen=True, slots=True)
class PersonalDevBuildCapability:
    """Short-lived, attempt-scoped source-read and artifact-write authority."""

    source_get_url: str
    artifact_upload_url: str
    artifact_upload_fields: Mapping[str, str]
    artifact_max_bytes: int
    expires_at: datetime

    def __post_init__(self) -> None:
        for label, value in (
            ("source", self.source_get_url),
            ("artifact", self.artifact_upload_url),
        ):
            parsed = urlsplit(value)
            if (
                len(value) > 4096
                or parsed.scheme not in {"http", "https"}
                or not parsed.netloc
                or parsed.username is not None
                or parsed.password is not None
                or parsed.fragment
                or any(character in value for character in "\r\n\0")
            ):
                raise ValueError(f"personal-dev builder {label} capability URL is invalid")
        if self.expires_at.tzinfo is None:
            raise ValueError("personal-dev builder capability expiry must include a timezone")
        fields = dict(self.artifact_upload_fields)
        if (
            not 1 <= len(fields) <= 32
            or any(
                not isinstance(key, str)
                or not isinstance(value, str)
                or not key
                or len(key) > 256
                or len(value) > 8192
                or any(character in key + value for character in "\r\n\0")
                for key, value in fields.items()
            )
        ):
            raise ValueError("personal-dev builder upload capability fields are invalid")
        if type(self.artifact_max_bytes) is not int or self.artifact_max_bytes <= 0:
            raise ValueError("personal-dev builder upload capability limit is invalid")
        object.__setattr__(self, "artifact_upload_fields", MappingProxyType(fields))


class PersonalDevBuildCapabilityProvider(Protocol):
    async def issue(
        self,
        registration: CandidateRegistration,
        *,
        platform: str,
    ) -> PersonalDevBuildCapability: ...


class PersonalDevBuildPublicationExporter(Protocol):
    async def publish(
        self,
        registration: CandidateRegistration,
    ) -> Mapping[str, object]: ...


class PresigningObjectStore(Protocol):
    def generate_presigned_url(
        self,
        operation_name: str,
        *,
        Params: Mapping[str, Any],  # noqa: N803 - boto3 API contract
        ExpiresIn: int,  # noqa: N803 - boto3 API contract
        HttpMethod: str | None = None,  # noqa: N803 - boto3 API contract
    ) -> str: ...

    def generate_presigned_post(
        self,
        *,
        Bucket: str,  # noqa: N803 - boto3 API contract
        Key: str,  # noqa: N803 - boto3 API contract
        Fields: Mapping[str, str],  # noqa: N803 - boto3 API contract
        Conditions: list[object],  # noqa: N803 - boto3 API contract
        ExpiresIn: int,  # noqa: N803 - boto3 API contract
    ) -> Mapping[str, object]: ...


class PersonalDevBuilderCluster(Protocol):
    async def apply(self, manifest: str, *, timeout_seconds: float = 120.0) -> None: ...

    async def wait_job(self, namespace: str, name: str) -> None: ...

    async def delete_namespace(self, namespace: str) -> None: ...


def _namespace(registration: CandidateRegistration) -> str:
    attempt = registration.build_attempt
    if attempt is None or attempt.lease_epoch <= 0:
        raise ValueError("personal-dev build attempt is unavailable")
    return f"loom-build-{attempt.id.hex}-l{attempt.lease_epoch:016x}"


def personal_dev_build_artifact_key(
    registration: CandidateRegistration,
    *,
    platform: PersonalDevPlatform,
) -> str:
    attempt = registration.build_attempt
    candidate = registration.candidate
    if (
        attempt is None
        or attempt.candidate_id != candidate.id
        or attempt.lease_epoch <= 0
        or platform not in PERSONAL_DEV_PLATFORMS
    ):
        raise ValueError("personal-dev build artifact binding is invalid")
    architecture = platform.rsplit("/", 1)[1]
    return (
        f"personal-dev/builds/{candidate.owner_team_id}/{candidate.owner_user_id}/"
        f"{candidate.candidate_sha}/{attempt.id}/l{attempt.lease_epoch:016x}/"
        f"{architecture}/artifacts.tar"
    )


@dataclass(slots=True)
class S3PersonalDevBuildCapabilityProvider:
    """Issue URL capabilities scoped to one input and one native output key."""

    object_store: PresigningObjectStore
    expected_bucket: str
    expiry_seconds: int = 4200
    max_artifact_bytes: int = 8 * 1024 * 1024 * 1024

    def __post_init__(self) -> None:
        if (
            not self.expected_bucket
            or self.expected_bucket.strip() != self.expected_bucket
            or "/" in self.expected_bucket
        ):
            raise ValueError("personal-dev builder capability bucket is invalid")
        if type(self.expiry_seconds) is not int or not 300 <= self.expiry_seconds <= 7200:
            raise ValueError("personal-dev builder capability expiry is invalid")
        if type(self.max_artifact_bytes) is not int or self.max_artifact_bytes <= 0:
            raise ValueError("personal-dev builder artifact limit is invalid")

    def issue_sync(
        self,
        registration: CandidateRegistration,
        *,
        platform: PersonalDevPlatform,
        now: datetime,
    ) -> PersonalDevBuildCapability:
        candidate = registration.candidate
        expected_source_key = (
            f"personal-dev/sources/{candidate.owner_team_id}/{candidate.owner_user_id}/"
            f"{candidate.candidate_sha}/{candidate.archive_sha256}.tar"
        )
        if (
            candidate.object_bucket != self.expected_bucket
            or candidate.object_key != expected_source_key
            or now.tzinfo is None
        ):
            raise ValueError("personal-dev builder source capability binding is invalid")
        output_key = personal_dev_build_artifact_key(registration, platform=platform)
        attempt = registration.build_attempt
        assert attempt is not None  # validated by artifact-key construction
        source_url = self.object_store.generate_presigned_url(
            "get_object",
            Params={"Bucket": candidate.object_bucket, "Key": candidate.object_key},
            ExpiresIn=self.expiry_seconds,
        )
        fields = {
            "Content-Type": "application/vnd.loom.personal-dev-build.v1+tar",
            "x-amz-meta-attestation-scope": "personal-dev-only",
            "x-amz-meta-build-attempt-id": str(attempt.id),
            "x-amz-meta-build-lease-epoch": str(attempt.lease_epoch),
            "x-amz-meta-candidate-sha256": candidate.candidate_sha,
            "x-amz-meta-platform": platform,
        }
        post = self.object_store.generate_presigned_post(
            Bucket=candidate.object_bucket,
            Key=output_key,
            Fields=fields,
            Conditions=[
                {key: value} for key, value in fields.items()
            ]
            + [["content-length-range", 1, self.max_artifact_bytes]],
            ExpiresIn=self.expiry_seconds,
        )
        artifact_url = post.get("url")
        artifact_fields = post.get("fields")
        if not isinstance(artifact_url, str) or not isinstance(artifact_fields, Mapping):
            raise RuntimeError("personal-dev builder upload capability is invalid")
        normalized_fields = {
            str(key): str(value) for key, value in artifact_fields.items()
        }
        if any(normalized_fields.get(key) != value for key, value in fields.items()):
            raise RuntimeError("personal-dev builder upload capability lost its binding")
        return PersonalDevBuildCapability(
            source_get_url=source_url,
            artifact_upload_url=artifact_url,
            artifact_upload_fields=normalized_fields,
            artifact_max_bytes=self.max_artifact_bytes,
            expires_at=now + timedelta(seconds=self.expiry_seconds),
        )

    async def issue(
        self,
        registration: CandidateRegistration,
        *,
        platform: str,
    ) -> PersonalDevBuildCapability:
        if platform not in PERSONAL_DEV_PLATFORMS:
            raise ValueError("personal-dev builder platform is unsupported")
        return await asyncio.to_thread(
            self.issue_sync,
            registration,
            platform=platform,
            now=datetime.now(UTC),
        )


def _verify_staged_source(
    registration: CandidateRegistration,
    source_archive: Path,
) -> None:
    candidate = registration.candidate
    try:
        descriptor = os.open(
            source_archive,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
        )
    except OSError:
        raise RuntimeError("personal-dev staged source is unavailable") from None
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_size != candidate.archive_size_bytes
        ):
            raise RuntimeError("personal-dev staged source authority is invalid")
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        digest.hexdigest() != candidate.archive_sha256
        or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    ):
        raise RuntimeError("personal-dev staged source binding changed")


@dataclass(slots=True)
class KubectlPersonalDevBuildExecutor:
    """Run native sandboxes; retain registry/export authority outside them."""

    cluster: PersonalDevBuilderCluster
    capabilities: PersonalDevBuildCapabilityProvider
    exporter: PersonalDevBuildPublicationExporter
    manifest_config: PersonalDevBuilderManifestConfig

    async def build(
        self,
        registration: CandidateRegistration,
        *,
        source_archive: Path,
    ) -> Mapping[str, object]:
        await asyncio.to_thread(_verify_staged_source, registration, source_archive)
        namespace = _namespace(registration)
        jobs: list[str] = []
        minimum_expiry = datetime.now(UTC) + timedelta(
            seconds=self.manifest_config.active_deadline_seconds + 60
        )
        for platform in PERSONAL_DEV_PLATFORMS:
            capability = await self.capabilities.issue(registration, platform=platform)
            if capability.expires_at.astimezone(UTC) < minimum_expiry:
                raise RuntimeError("personal-dev builder capability expires before its deadline")
            documents = personal_dev_builder_manifest_documents(
                registration,
                platform=platform,
                config=self.manifest_config,
            )
            job = documents[-1]
            job_name = str(job["metadata"]["name"])
            pod_volumes = job["spec"]["template"]["spec"]["volumes"]
            capability_volume = next(
                volume for volume in pod_volumes if volume["name"] == "attempt-capability"
            )
            secret_name = str(capability_volume["secret"]["secretName"])
            secret = {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": secret_name,
                    "namespace": namespace,
                    "labels": dict(job["metadata"]["labels"]),
                },
                "immutable": True,
                "type": "Opaque",
                "stringData": {
                    "artifact-upload.json": json.dumps(
                        {
                            "fields": dict(capability.artifact_upload_fields),
                            "max_bytes": capability.artifact_max_bytes,
                            "url": capability.artifact_upload_url,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=True,
                    ),
                    "source-get-url": capability.source_get_url,
                },
            }
            await self.cluster.apply(
                yaml.safe_dump_all(documents[:-1], sort_keys=False, explicit_start=True)
            )
            await self.cluster.apply(yaml.safe_dump(secret, sort_keys=False))
            await self.cluster.apply(yaml.safe_dump(job, sort_keys=False))
            jobs.append(job_name)
        await asyncio.gather(*(self.cluster.wait_job(namespace, name) for name in jobs))
        return await self.exporter.publish(registration)

    async def cleanup(self, registration: CandidateRegistration) -> None:
        await self.cluster.delete_namespace(_namespace(registration))


__all__ = [
    "KubectlPersonalDevBuildExecutor",
    "PersonalDevBuildCapability",
    "PersonalDevBuildCapabilityProvider",
    "PersonalDevBuildPublicationExporter",
    "PersonalDevBuilderCluster",
    "S3PersonalDevBuildCapabilityProvider",
    "personal_dev_build_artifact_key",
]
