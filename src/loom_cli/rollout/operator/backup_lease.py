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
            not self.lease_id
            or not self.source_request_id
            or self.environment != "staging"
            or not self.namespace
            or self.mutation_epoch < 0
            or not self.db_snapshot_identity
            or not self.schema_revision
            or _SHA256_RE.fullmatch(self.manifest_sha256) is None
            or _SHA256_RE.fullmatch(self.object_inventory_root) is None
        ):
            raise ValueError("backup lease identity is invalid")
        components = dict(self.component_sha256)
        if not components or any(
            not name or _SHA256_RE.fullmatch(digest) is None for name, digest in components.items()
        ):
            raise ValueError("backup lease component hashes are invalid")
        timestamps = (self.created_at, self.expires_at, self.restore_verified_at)
        if any(value.tzinfo is None or value.utcoffset() is None for value in timestamps):
            raise ValueError("backup lease timestamps must be timezone-aware")
        if not self.created_at <= self.restore_verified_at < self.expires_at:
            raise ValueError("backup lease restore or expiry ordering is invalid")
        object.__setattr__(self, "component_sha256", MappingProxyType(components))

    @property
    def evidence_digest(self) -> str:
        payload = {
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
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class BackupLeaseEligibility:
    eligible: bool
    blockers: tuple[str, ...]
    lease_digest: str


def evaluate_backup_lease(
    lease: BackupLease,
    *,
    now: datetime,
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
    blockers: list[str] = []
    expectations = (
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
    "evaluate_backup_lease",
]
