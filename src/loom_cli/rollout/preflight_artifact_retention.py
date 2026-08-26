"""Immutable evidence and policy for bounded preflight artifact retirement."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ARTIFACT_FILE_NAMES = (
    "artifact.json",
    "migration.yaml",
    "production-defaults.json",
    "rendered.yaml",
)
RETENTION_GRACE = timedelta(days=7)
MAX_RETIREMENTS_PER_PLAN = 32
_GRACE_NS = int(RETENTION_GRACE.total_seconds()) * 1_000_000_000
_PROTECTION_REASONS = frozenset(
    {
        "active-rollout",
        "artifact-retention-claim",
        "backup-cleanup-pending",
        "backup-recovery-claim",
        "backup-retention-claim",
        "backup-rotation-active",
        "backup-rotation-candidate",
        "batch-deferred",
        "current-release",
        "grace-period",
        "lifecycle-capacity-claim",
        "manifest-ownership-claim",
        "nonterminal-preflight-backup",
        "opaque-store",
        "resume-eligible",
    }
)
_OPAQUE_KINDS = frozenset({"directory", "file", "other", "symlink"})
_OPAQUE_REASONS = frozenset({"changing-entry", "quarantine-entry", "unknown-entry", "unsafe-entry"})


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _sha256(value: object) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _utc_nanoseconds(value: datetime) -> int:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("preflight artifact inventory time must be UTC")
    normalized = value.astimezone(UTC)
    delta = normalized - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 24 * 60 * 60 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1000


def _integer(data: Mapping[str, object], key: str) -> int:
    value = data.get(key)
    if type(value) is not int:
        raise ValueError("preflight artifact retention metadata schema is invalid")
    return value


def _string(data: Mapping[str, object], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str):
        raise ValueError("preflight artifact retention metadata schema is invalid")
    return value


@dataclass(frozen=True, slots=True)
class ArtifactDirectoryIdentity:
    """Exact stable metadata for one private artifact directory."""

    device: int
    inode: int
    owner_uid: int
    owner_gid: int
    mode: int
    link_count: int
    size_bytes: int
    modified_ns: int
    changed_ns: int

    def __post_init__(self) -> None:
        if (
            self.device < 0
            or self.inode <= 0
            or self.owner_uid < 0
            or self.owner_gid < 0
            or self.mode != 0o700
            or self.link_count < 2
            or self.size_bytes < 0
            or self.modified_ns < 0
            or self.changed_ns < 0
        ):
            raise ValueError("preflight artifact directory identity is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "changed_ns": self.changed_ns,
            "device": self.device,
            "inode": self.inode,
            "link_count": self.link_count,
            "mode": self.mode,
            "modified_ns": self.modified_ns,
            "owner_gid": self.owner_gid,
            "owner_uid": self.owner_uid,
            "size_bytes": self.size_bytes,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> ArtifactDirectoryIdentity:
        expected = {
            "changed_ns",
            "device",
            "inode",
            "link_count",
            "mode",
            "modified_ns",
            "owner_gid",
            "owner_uid",
            "size_bytes",
        }
        if set(data) != expected:
            raise ValueError("preflight artifact directory schema is invalid")
        return cls(**{key: _integer(data, key) for key in expected})


@dataclass(frozen=True, slots=True)
class ArtifactFileIdentity:
    """Exact stable metadata and content digest for one bundle file."""

    name: str
    device: int
    inode: int
    owner_uid: int
    owner_gid: int
    mode: int
    link_count: int
    size_bytes: int
    modified_ns: int
    changed_ns: int
    sha256: str

    def __post_init__(self) -> None:
        if (
            self.name not in ARTIFACT_FILE_NAMES
            or self.device < 0
            or self.inode <= 0
            or self.owner_uid < 0
            or self.owner_gid < 0
            or self.mode != 0o600
            or self.link_count != 1
            or self.size_bytes <= 0
            or self.modified_ns < 0
            or self.changed_ns < 0
            or _SHA256_RE.fullmatch(self.sha256) is None
        ):
            raise ValueError("preflight artifact file identity is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "changed_ns": self.changed_ns,
            "device": self.device,
            "inode": self.inode,
            "link_count": self.link_count,
            "mode": self.mode,
            "modified_ns": self.modified_ns,
            "name": self.name,
            "owner_gid": self.owner_gid,
            "owner_uid": self.owner_uid,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> ArtifactFileIdentity:
        expected = {
            "changed_ns",
            "device",
            "inode",
            "link_count",
            "mode",
            "modified_ns",
            "name",
            "owner_gid",
            "owner_uid",
            "sha256",
            "size_bytes",
        }
        if set(data) != expected:
            raise ValueError("preflight artifact file schema is invalid")
        integers = expected - {"name", "sha256"}
        return cls(
            name=_string(data, "name"),
            sha256=_string(data, "sha256"),
            **{key: _integer(data, key) for key in integers},
        )


@dataclass(frozen=True, slots=True)
class PreflightArtifactInventoryRecord:
    """Complete four-file observation of one digest publication."""

    bundle_digest: str
    directory: ArtifactDirectoryIdentity
    files: tuple[ArtifactFileIdentity, ...]

    def __post_init__(self) -> None:
        if (
            _SHA256_RE.fullmatch(self.bundle_digest) is None
            or not isinstance(self.directory, ArtifactDirectoryIdentity)
            or tuple(item.name for item in self.files) != ARTIFACT_FILE_NAMES
            or len({item.inode for item in self.files}) != len(ARTIFACT_FILE_NAMES)
            or any(item.device != self.directory.device for item in self.files)
        ):
            raise ValueError("preflight artifact bundle or file identity is invalid")

    @property
    def newest_ns(self) -> int:
        return max(
            self.directory.modified_ns,
            self.directory.changed_ns,
            *(item.modified_ns for item in self.files),
            *(item.changed_ns for item in self.files),
        )

    @property
    def record_digest(self) -> str:
        return _sha256(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "bundle_digest": self.bundle_digest,
            "directory": self.directory.to_dict(),
            "files": [item.to_dict() for item in self.files],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> PreflightArtifactInventoryRecord:
        if set(data) != {"bundle_digest", "directory", "files"}:
            raise ValueError("preflight artifact inventory record schema is invalid")
        directory = data.get("directory")
        files = data.get("files")
        if (
            not isinstance(directory, Mapping)
            or not isinstance(files, list)
            or not all(isinstance(item, Mapping) for item in files)
        ):
            raise ValueError("preflight artifact inventory record schema is invalid")
        return cls(
            bundle_digest=_string(data, "bundle_digest"),
            directory=ArtifactDirectoryIdentity.from_dict(directory),
            files=tuple(ArtifactFileIdentity.from_dict(item) for item in files),
        )


@dataclass(frozen=True, slots=True)
class OpaqueArtifactEvidence:
    """Top-level store evidence that cannot authorize deletion."""

    name: str
    kind: str
    device: int
    inode: int
    owner_uid: int
    owner_gid: int
    mode: int
    link_count: int
    size_bytes: int
    modified_ns: int
    changed_ns: int
    reason: str
    sha256: str | None = None

    def __post_init__(self) -> None:
        if (
            not self.name
            or self.name in {".", ".."}
            or "/" in self.name
            or self.kind not in _OPAQUE_KINDS
            or self.reason not in _OPAQUE_REASONS
            or self.device < 0
            or self.inode <= 0
            or self.owner_uid < 0
            or self.owner_gid < 0
            or not 0 <= self.mode <= 0o7777
            or self.link_count <= 0
            or self.size_bytes < 0
            or self.modified_ns < 0
            or self.changed_ns < 0
            or (self.sha256 is not None and _SHA256_RE.fullmatch(self.sha256) is None)
            or (self.kind == "file") != (self.sha256 is not None)
        ):
            raise ValueError("opaque preflight artifact evidence is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "changed_ns": self.changed_ns,
            "device": self.device,
            "inode": self.inode,
            "kind": self.kind,
            "link_count": self.link_count,
            "mode": self.mode,
            "modified_ns": self.modified_ns,
            "name": self.name,
            "owner_gid": self.owner_gid,
            "owner_uid": self.owner_uid,
            "reason": self.reason,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> OpaqueArtifactEvidence:
        expected = {
            "changed_ns",
            "device",
            "inode",
            "kind",
            "link_count",
            "mode",
            "modified_ns",
            "name",
            "owner_gid",
            "owner_uid",
            "reason",
            "sha256",
            "size_bytes",
        }
        if set(data) != expected or (
            data.get("sha256") is not None and not isinstance(data.get("sha256"), str)
        ):
            raise ValueError("opaque preflight artifact evidence schema is invalid")
        integers = expected - {"kind", "name", "reason", "sha256"}
        return cls(
            name=_string(data, "name"),
            kind=_string(data, "kind"),
            reason=_string(data, "reason"),
            sha256=cast(str | None, data.get("sha256")),
            **{key: _integer(data, key) for key in integers},
        )


@dataclass(frozen=True, slots=True)
class PreflightArtifactProtection:
    bundle_digest: str
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            _SHA256_RE.fullmatch(self.bundle_digest) is None
            or not self.reasons
            or tuple(sorted(set(self.reasons))) != self.reasons
            or any(reason not in _PROTECTION_REASONS for reason in self.reasons)
        ):
            raise ValueError("preflight artifact protection is invalid")

    def to_dict(self) -> dict[str, object]:
        return {"bundle_digest": self.bundle_digest, "reasons": list(self.reasons)}

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> PreflightArtifactProtection:
        reasons = data.get("reasons")
        if (
            set(data) != {"bundle_digest", "reasons"}
            or not isinstance(reasons, list)
            or not all(isinstance(item, str) for item in reasons)
        ):
            raise ValueError("preflight artifact protection schema is invalid")
        return cls(_string(data, "bundle_digest"), tuple(reasons))


@dataclass(frozen=True, slots=True)
class PreflightArtifactRetentionPlan:
    """Approved inventory authority for one bounded local retirement batch."""

    root: ArtifactDirectoryIdentity
    inventory_at: datetime
    grace_cutoff_ns: int
    candidates: tuple[PreflightArtifactInventoryRecord, ...]
    protected: tuple[PreflightArtifactInventoryRecord, ...]
    protections: tuple[PreflightArtifactProtection, ...]
    opaque_evidence: tuple[OpaqueArtifactEvidence, ...]
    environment: str = "staging"
    namespace: str = "loom-staging"

    def __post_init__(self) -> None:
        inventory_ns = _utc_nanoseconds(self.inventory_at)
        candidate_digests = tuple(item.bundle_digest for item in self.candidates)
        protected_digests = tuple(item.bundle_digest for item in self.protected)
        protection_digests = tuple(item.bundle_digest for item in self.protections)
        all_digests = (*candidate_digests, *protected_digests)
        if (
            not isinstance(self.root, ArtifactDirectoryIdentity)
            or self.grace_cutoff_ns != inventory_ns - _GRACE_NS
            or self.environment != "staging"
            or self.namespace != "loom-staging"
            or len(self.candidates) > MAX_RETIREMENTS_PER_PLAN
            or tuple(sorted(self.candidates, key=lambda item: (item.newest_ns, item.bundle_digest)))
            != self.candidates
            or tuple(sorted(self.protected, key=lambda item: item.bundle_digest)) != self.protected
            or len(set(all_digests)) != len(all_digests)
            or set(candidate_digests) & set(protected_digests)
            or protection_digests != protected_digests
            or tuple(sorted(self.opaque_evidence, key=lambda item: item.name))
            != self.opaque_evidence
            or len({item.name for item in self.opaque_evidence}) != len(self.opaque_evidence)
            or (self.opaque_evidence and self.candidates)
        ):
            if set(candidate_digests) & set(protected_digests):
                raise ValueError("preflight artifact candidate and protected inventory overlap")
            raise ValueError("preflight artifact retention plan is invalid")

    @property
    def plan_digest(self) -> str:
        return _sha256(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "candidates": [item.to_dict() for item in self.candidates],
            "environment": self.environment,
            "grace_cutoff_ns": self.grace_cutoff_ns,
            "inventory_at": self.inventory_at.astimezone(UTC).isoformat(),
            "namespace": self.namespace,
            "opaque_evidence": [item.to_dict() for item in self.opaque_evidence],
            "protected": [item.to_dict() for item in self.protected],
            "protections": [item.to_dict() for item in self.protections],
            "root": self.root.to_dict(),
            "schema_version": 1,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> PreflightArtifactRetentionPlan:
        expected = {
            "candidates",
            "environment",
            "grace_cutoff_ns",
            "inventory_at",
            "namespace",
            "opaque_evidence",
            "protected",
            "protections",
            "root",
            "schema_version",
        }
        root = data.get("root")
        candidates = data.get("candidates")
        protected = data.get("protected")
        protections = data.get("protections")
        opaque = data.get("opaque_evidence")
        if (
            set(data) != expected
            or data.get("schema_version") != 1
            or not isinstance(root, Mapping)
            or not all(
                isinstance(value, list) for value in (candidates, protected, protections, opaque)
            )
        ):
            raise ValueError("preflight artifact retention plan schema is invalid")
        candidate_items = cast(list[object], candidates)
        protected_items = cast(list[object], protected)
        protection_items = cast(list[object], protections)
        opaque_items = cast(list[object], opaque)
        if not all(
            isinstance(item, Mapping)
            for item in (*candidate_items, *protected_items, *protection_items, *opaque_items)
        ):
            raise ValueError("preflight artifact retention plan schema is invalid")
        try:
            inventory_at = datetime.fromisoformat(_string(data, "inventory_at"))
        except ValueError as exc:
            raise ValueError("preflight artifact inventory time is invalid") from exc
        return cls(
            root=ArtifactDirectoryIdentity.from_dict(root),
            inventory_at=inventory_at,
            grace_cutoff_ns=_integer(data, "grace_cutoff_ns"),
            candidates=tuple(
                PreflightArtifactInventoryRecord.from_dict(cast(Mapping[str, object], item))
                for item in candidate_items
            ),
            protected=tuple(
                PreflightArtifactInventoryRecord.from_dict(cast(Mapping[str, object], item))
                for item in protected_items
            ),
            protections=tuple(
                PreflightArtifactProtection.from_dict(cast(Mapping[str, object], item))
                for item in protection_items
            ),
            opaque_evidence=tuple(
                OpaqueArtifactEvidence.from_dict(cast(Mapping[str, object], item))
                for item in opaque_items
            ),
            environment=_string(data, "environment"),
            namespace=_string(data, "namespace"),
        )


def build_preflight_artifact_retention_plan(
    *,
    root: ArtifactDirectoryIdentity,
    records: Sequence[PreflightArtifactInventoryRecord],
    references: Sequence[PreflightArtifactProtection],
    opaque_evidence: Sequence[OpaqueArtifactEvidence],
    inventory_at: datetime,
    environment: str,
    namespace: str,
) -> PreflightArtifactRetentionPlan:
    """Classify one complete snapshot with fixed grace and batch policy."""
    inventory_ns = _utc_nanoseconds(inventory_at)
    cutoff_ns = inventory_ns - _GRACE_NS
    by_digest = {item.bundle_digest: item for item in records}
    if len(by_digest) != len(records):
        raise ValueError("preflight artifact inventory contains duplicate bundles")
    reasons: dict[str, set[str]] = {}
    for reference in references:
        if reference.bundle_digest not in by_digest:
            raise ValueError("preflight artifact reference points to a missing bundle")
        reasons.setdefault(reference.bundle_digest, set()).update(reference.reasons)
    if opaque_evidence:
        for digest in by_digest:
            reasons.setdefault(digest, set()).add("opaque-store")
    else:
        for record in records:
            if record.newest_ns > cutoff_ns:
                reasons.setdefault(record.bundle_digest, set()).add("grace-period")
    eligible = sorted(
        (item for item in records if item.bundle_digest not in reasons),
        key=lambda item: (item.newest_ns, item.bundle_digest),
    )
    candidates = () if opaque_evidence else tuple(eligible[:MAX_RETIREMENTS_PER_PLAN])
    for deferred in eligible[len(candidates) :]:
        reasons.setdefault(deferred.bundle_digest, set()).add("batch-deferred")
    protected = tuple(
        sorted(
            (by_digest[digest] for digest in reasons),
            key=lambda item: item.bundle_digest,
        )
    )
    protections = tuple(
        PreflightArtifactProtection(digest, tuple(sorted(reason_set)))
        for digest, reason_set in sorted(reasons.items())
    )
    return PreflightArtifactRetentionPlan(
        root=root,
        inventory_at=inventory_at,
        grace_cutoff_ns=cutoff_ns,
        candidates=candidates,
        protected=protected,
        protections=protections,
        opaque_evidence=tuple(sorted(opaque_evidence, key=lambda item: item.name)),
        environment=environment,
        namespace=namespace,
    )


__all__ = [
    "ARTIFACT_FILE_NAMES",
    "MAX_RETIREMENTS_PER_PLAN",
    "RETENTION_GRACE",
    "ArtifactDirectoryIdentity",
    "ArtifactFileIdentity",
    "OpaqueArtifactEvidence",
    "PreflightArtifactInventoryRecord",
    "PreflightArtifactProtection",
    "PreflightArtifactRetentionPlan",
    "build_preflight_artifact_retention_plan",
]
