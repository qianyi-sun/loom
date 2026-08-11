"""Domain records and fixed build contract for personal-development candidates."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol
from uuid import UUID

PERSONAL_DEV_COMPONENTS = (
    "agent-sandbox",
    "control-plane",
    "egress-xds",
    "family-orchestrator",
    "llm-gateway",
    "llm-gateway-sandbox",
    "pipeline-orchestrator",
    "service",
    "web",
    "worker",
)
PersonalDevPlatform = Literal["linux/amd64", "linux/arm64"]
PERSONAL_DEV_PLATFORMS: tuple[PersonalDevPlatform, ...] = (
    "linux/amd64",
    "linux/arm64",
)
PERSONAL_DEV_POOLS = ("gb10", "oldlab")
_BUILD_CONTRACT = {
    "components": list(PERSONAL_DEV_COMPONENTS),
    "platforms": list(PERSONAL_DEV_PLATFORMS),
    "schema_version": 1,
    "scope": "personal-dev-only",
}
PERSONAL_DEV_BUILD_CONTRACT_SHA256 = hashlib.sha256(
    json.dumps(_BUILD_CONTRACT, sort_keys=True, separators=(",", ":")).encode("ascii"),
).hexdigest()

CandidateStatus = Literal["uploaded", "queued", "building", "ready", "failed"]
BuildAttemptState = Literal["queued", "claimed", "running", "succeeded", "failed"]
_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_OCI_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
_IMAGE_PATH_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,511}")
_PROTOCOL_KEY_RE = re.compile(r"[a-z][a-z0-9-]{0,63}")
_PROTOCOL_VALUE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,127}")


class PersonalDevCandidateQuotaError(RuntimeError):
    """An owner-scoped retained artifact limit is exhausted."""


@dataclass(frozen=True, slots=True)
class PersonalDevCandidateLimits:
    """Finite artifact and concurrent-build envelope for shared dev."""

    per_owner_retained_candidates: int = 8
    per_owner_retained_archive_bytes: int = 3 * 1024 * 1024 * 1024
    global_active_builds: int = 4
    per_owner_active_builds: int = 1

    def __post_init__(self) -> None:
        if (
            type(self.per_owner_retained_candidates) is not int
            or type(self.per_owner_retained_archive_bytes) is not int
            or self.per_owner_retained_candidates <= 0
            or self.per_owner_retained_archive_bytes <= 0
        ):
            raise ValueError("personal-dev retained candidate limits must be positive integers")
        if (
            type(self.global_active_builds) is not int
            or type(self.per_owner_active_builds) is not int
            or not 0 < self.per_owner_active_builds <= self.global_active_builds
        ):
            raise ValueError("personal-dev active build limits must be ordered positive integers")


@dataclass(frozen=True, slots=True)
class PersonalDevCandidateRecord:
    id: UUID
    owner_user_id: UUID
    owner_team_id: UUID
    candidate_sha: str
    source_sha256: str
    archive_sha256: str
    build_contract_sha256: str
    source_commit: str
    dirty: bool
    manifest_json: Mapping[str, object]
    object_bucket: str
    object_key: str
    archive_size_bytes: int
    status: CandidateStatus
    created_at: datetime
    updated_at: datetime
    image_manifest_digest: str | None = None
    publication_json: Mapping[str, object] | None = None
    publication_sha256: str | None = None
    failure_reason: str | None = None
    ready_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class PersonalDevCandidateBuildAttemptRecord:
    id: UUID
    candidate_id: UUID
    subject_id: UUID
    subject_incarnation: UUID
    operation_id: UUID
    operation_epoch: int
    attempt_sequence: int
    state: BuildAttemptState
    lease_epoch: int
    created_at: datetime
    updated_at: datetime
    claimed_by: str | None = None
    lease_expires_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    failure_reason: str | None = None


@dataclass(frozen=True, slots=True)
class CandidateRegistration:
    candidate: PersonalDevCandidateRecord
    build_attempt: PersonalDevCandidateBuildAttemptRecord | None
    created: bool

    @classmethod
    def from_candidate(
        cls,
        candidate: PersonalDevCandidateRecord,
    ) -> CandidateRegistration:
        return cls(
            candidate=candidate,
            build_attempt=None,
            created=True,
        )


class CandidateRegistry(Protocol):
    async def register(
        self,
        requested: PersonalDevCandidateRecord,
    ) -> CandidateRegistration: ...


def _publication_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("personal-dev publication timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise ValueError("personal-dev publication timestamp must include a timezone")
    normalized = parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if normalized != value:
        raise ValueError("personal-dev publication timestamp is not canonical UTC")
    return parsed


def _immutable_image_reference(value: object) -> str:
    if not isinstance(value, str) or value.count("@") != 1:
        raise ValueError("personal-dev publication image reference is not immutable")
    path, digest = value.rsplit("@", 1)
    if _IMAGE_PATH_RE.fullmatch(path) is None or _OCI_DIGEST_RE.fullmatch(digest) is None:
        raise ValueError("personal-dev publication image reference is not immutable")
    return value


def personal_dev_image_set_manifest_digest(images: Mapping[str, object]) -> str:
    """Return the canonical digest that binds all immutable image descriptors."""
    try:
        canonical = json.dumps(
            dict(images),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ValueError("personal-dev publication image set is invalid") from exc
    return "sha256:" + hashlib.sha256(canonical).hexdigest()


def validate_personal_dev_candidate_publication(
    candidate: PersonalDevCandidateRecord,
    publication: Mapping[str, object],
) -> tuple[dict[str, object], str, str]:
    """Validate and canonically bind the complete personal-dev safety output."""

    expected_fields = {
        "schema_version",
        "attestation_scope",
        "candidate_sha",
        "source_sha256",
        "archive_sha256",
        "build_contract_sha256",
        "image_set_manifest_digest",
        "images",
        "supported_pools",
        "supported_architectures",
        "protocol_versions",
        "trusted_launcher_profile_sha256",
        "safety_evidence",
        "safety_evidence_sha256",
        "publisher_identity",
        "published_at",
    }
    value = dict(publication)
    if set(value) != expected_fields:
        raise ValueError("personal-dev publication fields do not match schema v1")
    bindings = {
        "schema_version": 1,
        "attestation_scope": "personal-dev-only",
        "candidate_sha": candidate.candidate_sha,
        "source_sha256": candidate.source_sha256,
        "archive_sha256": candidate.archive_sha256,
        "build_contract_sha256": candidate.build_contract_sha256,
    }
    if any(value[key] != expected for key, expected in bindings.items()):
        raise ValueError("personal-dev publication binding does not match the candidate")
    image_set_digest = value["image_set_manifest_digest"]
    if not isinstance(image_set_digest, str) or _OCI_DIGEST_RE.fullmatch(image_set_digest) is None:
        raise ValueError("personal-dev publication image-set digest is invalid")
    if value["supported_pools"] != list(PERSONAL_DEV_POOLS) or value[
        "supported_architectures"
    ] != list(PERSONAL_DEV_PLATFORMS):
        raise ValueError("personal-dev publication does not cover both shared physical pools")
    raw_images = value["images"]
    if not isinstance(raw_images, dict) or set(raw_images) != set(PERSONAL_DEV_COMPONENTS):
        raise ValueError("personal-dev publication image set is incomplete")
    for component in PERSONAL_DEV_COMPONENTS:
        raw_image = raw_images[component]
        if not isinstance(raw_image, dict) or set(raw_image) != {"index", "platforms"}:
            raise ValueError("personal-dev publication image record is invalid")
        _immutable_image_reference(raw_image["index"])
        platforms = raw_image["platforms"]
        if not isinstance(platforms, dict) or set(platforms) != set(PERSONAL_DEV_PLATFORMS):
            raise ValueError("personal-dev publication platform manifests are incomplete")
        if any(
            not isinstance(digest, str) or _OCI_DIGEST_RE.fullmatch(digest) is None
            for digest in platforms.values()
        ):
            raise ValueError("personal-dev publication platform digest is invalid")
    expected_image_set_digest = personal_dev_image_set_manifest_digest(raw_images)
    if image_set_digest != expected_image_set_digest:
        raise ValueError("personal-dev publication image-set digest binding is invalid")
    protocols = value["protocol_versions"]
    if not isinstance(protocols, dict) or not 1 <= len(protocols) <= 32:
        raise ValueError("personal-dev publication protocol versions are invalid")
    if any(
        not isinstance(key, str)
        or _PROTOCOL_KEY_RE.fullmatch(key) is None
        or not isinstance(version, str)
        or _PROTOCOL_VALUE_RE.fullmatch(version) is None
        for key, version in protocols.items()
    ):
        raise ValueError("personal-dev publication protocol version is invalid")
    for field in ("trusted_launcher_profile_sha256", "safety_evidence_sha256"):
        digest = value[field]
        if not isinstance(digest, str) or _DIGEST_RE.fullmatch(digest) is None:
            raise ValueError(f"personal-dev publication {field} is invalid")
    evidence_object = value["safety_evidence"]
    if (
        not isinstance(evidence_object, dict)
        or set(evidence_object)
        != {"bucket", "content_type", "key", "sha256", "size_bytes"}
        or evidence_object["bucket"] != candidate.object_bucket
        or evidence_object["content_type"]
        != "application/vnd.loom.personal-dev-safety-evidence.v1+json"
        or not isinstance(evidence_object["key"], str)
        or not evidence_object["key"].startswith(
            f"personal-dev/evidence/{candidate.candidate_sha}/"
        )
        or len(evidence_object["key"]) > 1024
        or any(character in evidence_object["key"] for character in "\r\n\0")
        or evidence_object["sha256"] != value["safety_evidence_sha256"]
        or type(evidence_object["size_bytes"]) is not int
        or not 0 < evidence_object["size_bytes"] <= 64 * 1024 * 1024
    ):
        raise ValueError("personal-dev publication safety evidence object is invalid")
    publisher = value["publisher_identity"]
    if (
        not isinstance(publisher, str)
        or not 1 <= len(publisher) <= 256
        or publisher.strip() != publisher
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in publisher)
    ):
        raise ValueError("personal-dev publication publisher identity is invalid")
    published_at = value["published_at"]
    if not isinstance(published_at, str):
        raise ValueError("personal-dev publication timestamp is invalid")
    _publication_timestamp(published_at)
    try:
        canonical = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        normalized = json.loads(canonical)
    except (TypeError, ValueError) as exc:
        raise ValueError("personal-dev publication is not canonical JSON data") from exc
    if not isinstance(normalized, dict):  # pragma: no cover - value is already a dict
        raise ValueError("personal-dev publication is invalid")
    return normalized, hashlib.sha256(canonical).hexdigest(), image_set_digest


__all__ = [
    "PERSONAL_DEV_BUILD_CONTRACT_SHA256",
    "PERSONAL_DEV_COMPONENTS",
    "PERSONAL_DEV_PLATFORMS",
    "PERSONAL_DEV_POOLS",
    "BuildAttemptState",
    "CandidateRegistration",
    "CandidateRegistry",
    "CandidateStatus",
    "PersonalDevCandidateBuildAttemptRecord",
    "PersonalDevCandidateLimits",
    "PersonalDevCandidateQuotaError",
    "PersonalDevCandidateRecord",
    "PersonalDevPlatform",
    "personal_dev_image_set_manifest_digest",
    "validate_personal_dev_candidate_publication",
]
