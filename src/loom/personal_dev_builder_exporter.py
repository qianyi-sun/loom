"""Trusted scan-before-publish exporter for personal candidate images."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from loom.personal_dev_builder_artifact import (
    VerifiedPersonalDevBuildArtifact,
    VerifiedPersonalDevImageArtifact,
    verify_personal_dev_build_artifact,
)
from loom.personal_dev_builder_runtime import personal_dev_build_artifact_key
from loom.personal_dev_candidate import (
    PERSONAL_DEV_COMPONENTS,
    PERSONAL_DEV_PLATFORMS,
    PERSONAL_DEV_POOLS,
    CandidateRegistration,
    PersonalDevPlatform,
    personal_dev_image_set_manifest_digest,
    validate_personal_dev_candidate_publication,
)

_ARTIFACT_CONTENT_TYPE = "application/vnd.loom.personal-dev-build.v1+tar"
_REGISTRY_PREFIX_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,400}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_OCI_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
_PROTOCOL_KEY_RE = re.compile(r"[a-z][a-z0-9-]{0,63}")
_PROTOCOL_VALUE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,127}")
_DOWNLOAD_CHUNK_BYTES = 1024 * 1024
_MAX_SCAN_REPORT_BYTES = 16 * 1024 * 1024
_SCAN_REPORT_CONTENT_TYPE = "application/vnd.aquasec.trivy.report+json"
_SAFETY_EVIDENCE_CONTENT_TYPE = (
    "application/vnd.loom.personal-dev-safety-evidence.v1+json"
)


class SyncArtifactBody(Protocol):
    def read(self, size: int) -> bytes: ...

    def close(self) -> None: ...


class SyncArtifactObjectStore(Protocol):
    def get_object(self, **kwargs: Any) -> Mapping[str, object]: ...

    def put_object(self, **kwargs: Any) -> Mapping[str, object]: ...

    def head_object(self, **kwargs: Any) -> Mapping[str, object]: ...


@dataclass(frozen=True, slots=True)
class PersonalDevImageScanResult:
    """Exact trusted scanner output and its bounded publication summary."""

    report: bytes
    evidence: Mapping[str, object]


class PersonalDevImageScanner(Protocol):
    async def scan(
        self,
        image: VerifiedPersonalDevImageArtifact,
        *,
        registration: CandidateRegistration,
    ) -> PersonalDevImageScanResult: ...


class PersonalDevRegistryPublisher(Protocol):
    async def publish_platform(
        self,
        image: VerifiedPersonalDevImageArtifact,
        *,
        registration: CandidateRegistration,
        repository: str,
    ) -> str: ...

    async def publish_index(
        self,
        *,
        registration: CandidateRegistration,
        repository: str,
        platform_digests: Mapping[str, str],
    ) -> tuple[str, str]: ...


def _canonical_bytes(value: object, *, label: str) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise RuntimeError(f"personal-dev {label} is not canonical JSON data") from exc


def _artifact_metadata(
    registration: CandidateRegistration,
    *,
    platform: PersonalDevPlatform,
) -> dict[str, str]:
    attempt = registration.build_attempt
    if attempt is None:
        raise ValueError("personal-dev build attempt is unavailable")
    return {
        "attestation-scope": "personal-dev-only",
        "build-attempt-id": str(attempt.id),
        "build-lease-epoch": str(attempt.lease_epoch),
        "candidate-sha256": registration.candidate.candidate_sha,
        "platform": platform,
    }


@dataclass(slots=True)
class S3TrustedPersonalDevBuildPublicationExporter:
    """Download exact native outputs, verify/scan all, then publish immutably."""

    object_store: SyncArtifactObjectStore
    expected_bucket: str
    max_artifact_bytes: int
    max_image_archive_bytes: int
    scanner: PersonalDevImageScanner
    publisher: PersonalDevRegistryPublisher
    registry_prefix: str
    publisher_identity: str
    trusted_launcher_profile_sha256: str
    protocol_versions: Mapping[str, str]
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)

    def __post_init__(self) -> None:
        if (
            not self.expected_bucket
            or self.expected_bucket.strip() != self.expected_bucket
            or "/" in self.expected_bucket
        ):
            raise ValueError("personal-dev exporter artifact bucket is invalid")
        if (
            type(self.max_artifact_bytes) is not int
            or type(self.max_image_archive_bytes) is not int
            or self.max_artifact_bytes <= 0
            or not 0 < self.max_image_archive_bytes <= self.max_artifact_bytes
        ):
            raise ValueError("personal-dev exporter artifact limits are invalid")
        if (
            _REGISTRY_PREFIX_RE.fullmatch(self.registry_prefix) is None
            or self.registry_prefix.endswith("/")
            or "://" in self.registry_prefix
        ):
            raise ValueError("personal-dev exporter registry prefix is invalid")
        if (
            not self.publisher_identity
            or self.publisher_identity.strip() != self.publisher_identity
            or len(self.publisher_identity) > 256
            or any(
                ord(character) < 0x21 or ord(character) > 0x7E
                for character in self.publisher_identity
            )
        ):
            raise ValueError("personal-dev exporter publisher identity is invalid")
        if _SHA256_RE.fullmatch(self.trusted_launcher_profile_sha256) is None:
            raise ValueError("personal-dev exporter trusted launcher profile is invalid")
        protocols = dict(self.protocol_versions)
        if (
            not 1 <= len(protocols) <= 32
            or any(
                not isinstance(key, str)
                or _PROTOCOL_KEY_RE.fullmatch(key) is None
                or not isinstance(value, str)
                or _PROTOCOL_VALUE_RE.fullmatch(value) is None
                for key, value in protocols.items()
            )
        ):
            raise ValueError("personal-dev exporter protocol versions are invalid")
        object.__setattr__(self, "protocol_versions", protocols)

    def _download(
        self,
        registration: CandidateRegistration,
        *,
        platform: PersonalDevPlatform,
        destination: Path,
    ) -> str:
        candidate = registration.candidate
        if candidate.object_bucket != self.expected_bucket:
            raise RuntimeError("personal-dev build artifact bucket binding is invalid")
        key = personal_dev_build_artifact_key(registration, platform=platform)
        response = self.object_store.get_object(Bucket=self.expected_bucket, Key=key)
        body = response.get("Body")
        metadata = response.get("Metadata")
        content_length = response.get("ContentLength")
        if (
            not hasattr(body, "read")
            or not hasattr(body, "close")
            or type(content_length) is not int
            or not 0 < content_length <= self.max_artifact_bytes
            or response.get("ContentType") != _ARTIFACT_CONTENT_TYPE
            or not isinstance(metadata, Mapping)
            or any(
                metadata.get(key_name) != expected
                for key_name, expected in _artifact_metadata(
                    registration,
                    platform=platform,
                ).items()
            )
        ):
            if hasattr(body, "close"):
                body.close()
            raise RuntimeError("personal-dev build artifact object binding is invalid")
        descriptor = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
        )
        observed = 0
        digest = hashlib.sha256()
        typed_body = body
        try:
            while chunk := typed_body.read(_DOWNLOAD_CHUNK_BYTES):
                if not isinstance(chunk, bytes):
                    raise RuntimeError("personal-dev build artifact body is invalid")
                observed += len(chunk)
                if observed > content_length or observed > self.max_artifact_bytes:
                    raise RuntimeError("personal-dev build artifact exceeded its binding")
                digest.update(chunk)
                view = memoryview(chunk)
                while view:
                    written = os.write(descriptor, view)
                    view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
            typed_body.close()
        if observed != content_length:
            raise RuntimeError("personal-dev build artifact is truncated")
        metadata_after = os.stat(destination, follow_symlinks=False)
        if (
            not stat.S_ISREG(metadata_after.st_mode)
            or metadata_after.st_nlink != 1
            or metadata_after.st_size != observed
        ):
            raise RuntimeError("personal-dev build artifact file authority is invalid")
        return digest.hexdigest()

    def _put_evidence_object(
        self,
        registration: CandidateRegistration,
        *,
        key: str,
        payload: bytes,
        content_type: str,
        metadata: Mapping[str, str],
    ) -> dict[str, object]:
        if not payload:
            raise RuntimeError("personal-dev safety evidence is empty")
        attempt = registration.build_attempt
        if attempt is None:
            raise ValueError("personal-dev build attempt is unavailable")
        digest = hashlib.sha256(payload).hexdigest()
        expected_metadata = {
            "attestation-scope": "personal-dev-only",
            "build-attempt-id": str(attempt.id),
            "build-lease-epoch": str(attempt.lease_epoch),
            "candidate-sha256": registration.candidate.candidate_sha,
            "content-sha256": digest,
            **dict(metadata),
        }
        self.object_store.put_object(
            Bucket=self.expected_bucket,
            Key=key,
            Body=payload,
            ContentLength=len(payload),
            ContentType=content_type,
            Metadata=expected_metadata,
        )
        observed = self.object_store.head_object(Bucket=self.expected_bucket, Key=key)
        observed_metadata = observed.get("Metadata")
        if (
            observed.get("ContentLength") != len(payload)
            or observed.get("ContentType") != content_type
            or not isinstance(observed_metadata, Mapping)
            or any(
                observed_metadata.get(name) != value
                for name, value in expected_metadata.items()
            )
        ):
            raise RuntimeError("personal-dev safety evidence object binding is invalid")
        return {
            "bucket": self.expected_bucket,
            "content_type": content_type,
            "key": key,
            "sha256": digest,
            "size_bytes": len(payload),
        }

    @staticmethod
    def _evidence_prefix(registration: CandidateRegistration) -> str:
        attempt = registration.build_attempt
        assert attempt is not None
        return (
            f"personal-dev/evidence/{registration.candidate.candidate_sha}/"
            f"{attempt.id}/l{attempt.lease_epoch:016x}"
        )

    async def publish(
        self,
        registration: CandidateRegistration,
    ) -> Mapping[str, object]:
        attempt = registration.build_attempt
        candidate = registration.candidate
        if (
            attempt is None
            or attempt.candidate_id != candidate.id
            or attempt.state != "running"
            or attempt.lease_epoch <= 0
            or candidate.status != "building"
        ):
            raise ValueError("personal-dev exporter registration is not a running attempt")
        now = self.clock()
        if now.tzinfo is None:
            raise RuntimeError("personal-dev exporter clock must include a timezone")

        with tempfile.TemporaryDirectory(prefix="loom-personal-dev-export-") as directory:
            root = Path(directory)
            artifacts: dict[str, VerifiedPersonalDevBuildArtifact] = {}
            artifact_sha256: dict[str, str] = {}
            for platform in PERSONAL_DEV_PLATFORMS:
                architecture = platform.rsplit("/", 1)[1]
                bundle = root / f"artifact-{architecture}.tar"
                extracted = root / f"images-{architecture}"
                extracted.mkdir(mode=0o700)
                artifact_sha256[platform] = await asyncio.to_thread(
                    self._download,
                    registration,
                    platform=platform,
                    destination=bundle,
                )
                artifacts[platform] = await asyncio.to_thread(
                    verify_personal_dev_build_artifact,
                    bundle,
                    registration,
                    platform=platform,
                    output_directory=extracted,
                    max_artifact_bytes=self.max_artifact_bytes,
                    max_image_archive_bytes=self.max_image_archive_bytes,
                )

            scan_results: dict[str, PersonalDevImageScanResult] = {}
            for platform in PERSONAL_DEV_PLATFORMS:
                for component in PERSONAL_DEV_COMPONENTS:
                    image = artifacts[platform].images[component]
                    result = await self.scanner.scan(
                        image,
                        registration=registration,
                    )
                    evidence = dict(result.evidence)
                    _canonical_bytes(evidence, label="image scan evidence")
                    report_digest = hashlib.sha256(result.report).hexdigest()
                    if (
                        not 0 < len(result.report) <= _MAX_SCAN_REPORT_BYTES
                        or evidence.get("report_sha256") != report_digest
                    ):
                        raise RuntimeError("personal-dev image scan report binding is invalid")
                    scan_results[f"{component}:{platform}"] = result

            scan_evidence: dict[str, dict[str, object]] = {}
            evidence_prefix = self._evidence_prefix(registration)
            for platform in PERSONAL_DEV_PLATFORMS:
                architecture = platform.rsplit("/", 1)[1]
                for component in PERSONAL_DEV_COMPONENTS:
                    name = f"{component}:{platform}"
                    result = scan_results[name]
                    report_digest = hashlib.sha256(result.report).hexdigest()
                    report_object = await asyncio.to_thread(
                        self._put_evidence_object,
                        registration,
                        key=(
                            f"{evidence_prefix}/scan/{architecture}/{component}-"
                            f"{report_digest}.json"
                        ),
                        payload=result.report,
                        content_type=_SCAN_REPORT_CONTENT_TYPE,
                        metadata={
                            "component": component,
                            "platform": platform,
                            "evidence-kind": "image-scan-report",
                        },
                    )
                    scan_evidence[name] = {
                        **dict(result.evidence),
                        "report_object": report_object,
                    }

            platform_digests: dict[str, dict[str, str]] = {
                component: {} for component in PERSONAL_DEV_COMPONENTS
            }
            for platform in PERSONAL_DEV_PLATFORMS:
                for component in PERSONAL_DEV_COMPONENTS:
                    image = artifacts[platform].images[component]
                    repository = f"{self.registry_prefix}/loom-{component}"
                    published_digest = await self.publisher.publish_platform(
                        image,
                        registration=registration,
                        repository=repository,
                    )
                    if published_digest != image.manifest_digest:
                        raise RuntimeError(
                            "personal-dev registry changed a platform manifest digest"
                        )
                    platform_digests[component][platform] = published_digest

            images: dict[str, object] = {}
            for component in PERSONAL_DEV_COMPONENTS:
                repository = f"{self.registry_prefix}/loom-{component}"
                reference, index_digest = await self.publisher.publish_index(
                    registration=registration,
                    repository=repository,
                    platform_digests=platform_digests[component],
                )
                if (
                    _OCI_DIGEST_RE.fullmatch(index_digest) is None
                    or reference != f"{repository}@{index_digest}"
                ):
                    raise RuntimeError("personal-dev registry index reference is not immutable")
                images[component] = {
                    "index": reference,
                    "platforms": dict(platform_digests[component]),
                }

            image_set_manifest_digest = personal_dev_image_set_manifest_digest(images)
            safety_evidence = {
                "artifact_manifest_sha256": {
                    platform: artifacts[platform].manifest_sha256
                    for platform in PERSONAL_DEV_PLATFORMS
                },
                "artifact_sha256": artifact_sha256,
                "image_set_manifest_digest": image_set_manifest_digest,
                "scan_evidence": scan_evidence,
                "schema_version": 1,
            }
            safety_evidence_bytes = _canonical_bytes(
                safety_evidence,
                label="safety evidence",
            )
            safety_evidence_sha256 = hashlib.sha256(safety_evidence_bytes).hexdigest()
            safety_evidence_object = await asyncio.to_thread(
                self._put_evidence_object,
                registration,
                key=(
                    f"{evidence_prefix}/safety-evidence-"
                    f"{safety_evidence_sha256}.json"
                ),
                payload=safety_evidence_bytes,
                content_type=_SAFETY_EVIDENCE_CONTENT_TYPE,
                metadata={"evidence-kind": "aggregate-safety-evidence"},
            )
            publication: dict[str, object] = {
                "schema_version": 1,
                "attestation_scope": "personal-dev-only",
                "candidate_sha": candidate.candidate_sha,
                "source_sha256": candidate.source_sha256,
                "archive_sha256": candidate.archive_sha256,
                "build_contract_sha256": candidate.build_contract_sha256,
                "image_set_manifest_digest": image_set_manifest_digest,
                "images": images,
                "supported_pools": list(PERSONAL_DEV_POOLS),
                "supported_architectures": list(PERSONAL_DEV_PLATFORMS),
                "protocol_versions": dict(self.protocol_versions),
                "trusted_launcher_profile_sha256": (
                    self.trusted_launcher_profile_sha256
                ),
                "safety_evidence": safety_evidence_object,
                "safety_evidence_sha256": safety_evidence_sha256,
                "publisher_identity": self.publisher_identity,
                "published_at": now.astimezone(UTC).isoformat(
                    timespec="seconds"
                ).replace("+00:00", "Z"),
            }
            normalized, _publication_digest, _image_set_digest = (
                validate_personal_dev_candidate_publication(candidate, publication)
            )
            return normalized


__all__ = [
    "PersonalDevImageScanResult",
    "PersonalDevImageScanner",
    "PersonalDevRegistryPublisher",
    "S3TrustedPersonalDevBuildPublicationExporter",
    "SyncArtifactObjectStore",
]
