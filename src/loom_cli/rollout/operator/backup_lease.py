"""Fail-closed immutable backup lease identity and reuse eligibility."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_LEASE_ID_RE = re.compile(r"^lease-[a-z0-9][a-z0-9-]{7,63}$")
_REQUEST_ID_RE = re.compile(r"^req-[a-z0-9][a-z0-9-]{7,63}$")
_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_NAMESPACE_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")


def _bounded_identity(value: str, *, limit: int = 160) -> bool:
    return bool(
        value
        and value == value.strip()
        and len(value) <= limit
        and not any(ord(character) < 32 for character in value)
    )


def component_set_digest(component_sha256: Mapping[str, str]) -> str:
    """Bind an allowlisted component map without exposing payload contents."""
    components = dict(component_sha256)
    if (
        not components
        or len(components) > 32
        or any(
            _NAME_RE.fullmatch(name) is None or _SHA256_RE.fullmatch(digest) is None
            for name, digest in components.items()
        )
    ):
        raise ValueError("backup lease component hashes are invalid")
    return hashlib.sha256(
        json.dumps(components, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class BackupLease:
    lease_id: str
    source_request_id: str
    manifest_sha256: str
    component_sha256: Mapping[str, str]
    environment: str
    namespace: str
    mutation_epoch: int
    db_snapshot_identity: str
    schema_revision: str
    object_inventory_root: str
    created_at: datetime
    expires_at: datetime
    restore_verified_at: datetime

    def __post_init__(self) -> None:
        if (
            _LEASE_ID_RE.fullmatch(self.lease_id) is None
            or _REQUEST_ID_RE.fullmatch(self.source_request_id) is None
            or self.environment != "staging"
            or _NAMESPACE_RE.fullmatch(self.namespace) is None
            or self.mutation_epoch < 0
            or not _bounded_identity(self.db_snapshot_identity)
            or not _bounded_identity(self.schema_revision, limit=64)
            or _SHA256_RE.fullmatch(self.manifest_sha256) is None
            or _SHA256_RE.fullmatch(self.object_inventory_root) is None
        ):
            raise ValueError("backup lease identity is invalid")
        components = dict(self.component_sha256)
        component_set_digest(components)
        timestamps = (self.created_at, self.expires_at, self.restore_verified_at)
        if any(value.tzinfo is None or value.utcoffset() is None for value in timestamps):
            raise ValueError("backup lease timestamps must be timezone-aware")
        if not self.created_at <= self.restore_verified_at < self.expires_at:
            raise ValueError("backup lease restore or expiry ordering is invalid")
        object.__setattr__(self, "component_sha256", MappingProxyType(components))

    @property
    def evidence_digest(self) -> str:
        return hashlib.sha256(
            json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            "component_sha256": dict(self.component_sha256),
            "created_at": self.created_at.isoformat(),
            "db_snapshot_identity": self.db_snapshot_identity,
            "environment": self.environment,
            "expires_at": self.expires_at.isoformat(),
            "lease_id": self.lease_id,
            "manifest_sha256": self.manifest_sha256,
            "mutation_epoch": self.mutation_epoch,
            "namespace": self.namespace,
            "object_inventory_root": self.object_inventory_root,
            "restore_verified_at": self.restore_verified_at.isoformat(),
            "schema_revision": self.schema_revision,
            "source_request_id": self.source_request_id,
            "schema_version": 1,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> BackupLease:
        expected = {
            "component_sha256",
            "created_at",
            "db_snapshot_identity",
            "environment",
            "expires_at",
            "lease_id",
            "manifest_sha256",
            "mutation_epoch",
            "namespace",
            "object_inventory_root",
            "restore_verified_at",
            "schema_revision",
            "schema_version",
            "source_request_id",
        }
        components = data.get("component_sha256")
        if (
            set(data) != expected
            or data.get("schema_version") != 1
            or type(data.get("mutation_epoch")) is not int
            or not isinstance(components, Mapping)
            or not all(
                isinstance(key, str) and isinstance(value, str) for key, value in components.items()
            )
        ):
            raise ValueError("backup lease schema is invalid")
        string_fields = expected - {
            "component_sha256",
            "mutation_epoch",
            "schema_version",
        }
        if not all(isinstance(data[field], str) for field in string_fields):
            raise ValueError("backup lease schema is invalid")
        try:
            created_at = datetime.fromisoformat(data["created_at"])  # type: ignore[arg-type]
            expires_at = datetime.fromisoformat(data["expires_at"])  # type: ignore[arg-type]
            restore_verified_at = datetime.fromisoformat(
                data["restore_verified_at"]  # type: ignore[arg-type]
            )
        except ValueError as exc:
            raise ValueError("backup lease timestamps are invalid") from exc
        return cls(
            lease_id=data["lease_id"],  # type: ignore[arg-type]
            source_request_id=data["source_request_id"],  # type: ignore[arg-type]
            manifest_sha256=data["manifest_sha256"],  # type: ignore[arg-type]
            component_sha256=dict(components),
            environment=data["environment"],  # type: ignore[arg-type]
            namespace=data["namespace"],  # type: ignore[arg-type]
            mutation_epoch=data["mutation_epoch"],  # type: ignore[arg-type]
            db_snapshot_identity=data["db_snapshot_identity"],  # type: ignore[arg-type]
            schema_revision=data["schema_revision"],  # type: ignore[arg-type]
            object_inventory_root=data["object_inventory_root"],  # type: ignore[arg-type]
            created_at=created_at,
            expires_at=expires_at,
            restore_verified_at=restore_verified_at,
        )


@dataclass(frozen=True, slots=True)
class BackupLeaseEligibility:
    eligible: bool
    blockers: tuple[str, ...]
    lease_digest: str


def evaluate_backup_lease(
    lease: BackupLease,
    *,
    now: datetime,
    source_request_id: str,
    environment: str,
    namespace: str,
    mutation_epoch: int,
    db_snapshot_identity: str,
    schema_revision: str,
    object_inventory_root: str,
    manifest_sha256: str,
    component_sha256: Mapping[str, str],
) -> BackupLeaseEligibility:
    """Collect every provenance/freshness mismatch; absence of proof is ineligible."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("backup lease evaluation time must be timezone-aware")
    if (
        _REQUEST_ID_RE.fullmatch(source_request_id) is None
        or not _bounded_identity(environment, limit=32)
        or _NAMESPACE_RE.fullmatch(namespace) is None
        or mutation_epoch < 0
        or not _bounded_identity(db_snapshot_identity)
        or not _bounded_identity(schema_revision, limit=64)
        or _SHA256_RE.fullmatch(object_inventory_root) is None
        or _SHA256_RE.fullmatch(manifest_sha256) is None
    ):
        raise ValueError("backup lease expectation identity is invalid")
    component_set_digest(component_sha256)
    blockers: list[str] = []
    expectations = (
        (lease.source_request_id == source_request_id, "source-request"),
        (environment == "staging" and lease.environment == environment, "environment"),
        (lease.namespace == namespace, "namespace"),
        (lease.mutation_epoch == mutation_epoch, "mutation-epoch"),
        (lease.db_snapshot_identity == db_snapshot_identity, "db-snapshot"),
        (lease.schema_revision == schema_revision, "schema-revision"),
        (lease.object_inventory_root == object_inventory_root, "object-inventory"),
        (lease.manifest_sha256 == manifest_sha256, "manifest"),
        (dict(lease.component_sha256) == dict(component_sha256), "components"),
        (lease.restore_verified_at <= now < lease.expires_at, "freshness"),
    )
    blockers.extend(label for passed, label in expectations if not passed)
    stable = tuple(sorted(blockers))
    return BackupLeaseEligibility(
        eligible=not stable,
        blockers=stable,
        lease_digest=lease.evidence_digest,
    )


__all__ = [
    "BackupLease",
    "BackupLeaseEligibility",
    "component_set_digest",
    "evaluate_backup_lease",
]
