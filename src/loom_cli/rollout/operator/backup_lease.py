"""Fail-closed immutable backup lease identity and reuse eligibility."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from uuid import UUID

from .checkpoint_database_authority import DatabaseAuthorityEvidence

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_LEASE_ID_RE = re.compile(r"^lease-[a-z0-9][a-z0-9-]{7,63}$")
_REQUEST_ID_RE = re.compile(r"^req-[a-z0-9][a-z0-9-]{7,63}$")
_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_NAMESPACE_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
_SCHEMA_THREE_COMPONENTS = frozenset(
    {"database_authority", "k8s_secrets", "object_inventory", "postgres"}
)


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
    checkpoint_schema_version: int | None = None
    database_authority_digest: str | None = None
    public_schema_revision: str | None = None
    capacity_guard_schema_revision: str | None = None
    manager_configuration_epoch: int | None = None
    manager_configuration_digest: str | None = None
    manager_authority_incarnation: UUID | None = None
    manager_writer_epoch: int | None = None
    manager_execution_state: str | None = None
    manager_execution_epoch: int | None = None
    manager_execution_manifest_sha256: str | None = None
    manager_executable_new_capacity_ceiling: int | None = None
    manager_increase_freeze: bool | None = None
    restore_report_sha256: str | None = None

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
        authority_fields = (
            self.database_authority_digest,
            self.public_schema_revision,
            self.capacity_guard_schema_revision,
            self.manager_configuration_epoch,
            self.manager_configuration_digest,
            self.manager_authority_incarnation,
            self.manager_writer_epoch,
            self.manager_execution_state,
            self.manager_execution_epoch,
            self.manager_execution_manifest_sha256,
            self.manager_executable_new_capacity_ceiling,
            self.manager_increase_freeze,
            self.restore_report_sha256,
        )
        if self.checkpoint_schema_version is None:
            if any(value is not None for value in authority_fields):
                raise ValueError("historical backup lease cannot carry schema-3 authority")
        else:
            if self.checkpoint_schema_version != 3 or set(components) != _SCHEMA_THREE_COMPONENTS:
                raise ValueError("backup lease schema-3 component authority is invalid")
            try:
                authority = DatabaseAuthorityEvidence(
                    public_schema_revision=self.public_schema_revision,  # type: ignore[arg-type]
                    capacity_guard_schema_revision=self.capacity_guard_schema_revision,
                    configuration_epoch=self.manager_configuration_epoch,  # type: ignore[arg-type]
                    configuration_digest=self.manager_configuration_digest,  # type: ignore[arg-type]
                    authority_incarnation=self.manager_authority_incarnation,  # type: ignore[arg-type]
                    writer_epoch=self.manager_writer_epoch,  # type: ignore[arg-type]
                    execution_state=self.manager_execution_state,  # type: ignore[arg-type]
                    execution_epoch=self.manager_execution_epoch,  # type: ignore[arg-type]
                    execution_manifest_sha256=self.manager_execution_manifest_sha256,  # type: ignore[arg-type]
                    executable_new_capacity_ceiling=(
                        self.manager_executable_new_capacity_ceiling  # type: ignore[arg-type]
                    ),
                    increase_freeze=self.manager_increase_freeze,  # type: ignore[arg-type]
                )
            except (TypeError, ValueError) as exc:
                raise ValueError("backup lease schema-3 database authority is invalid") from exc
            if (
                not isinstance(self.restore_report_sha256, str)
                or _SHA256_RE.fullmatch(self.restore_report_sha256) is None
                or authority.digest != self.database_authority_digest
                or components["database_authority"] != self.database_authority_digest
                or components["postgres"]
                != self.db_snapshot_identity.removeprefix("pgdump-sha256:")
                or self.public_schema_revision != self.schema_revision
            ):
                raise ValueError("backup lease schema-3 authority binding is invalid")
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
        payload: dict[str, object] = {
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
            "schema_version": 1 if self.checkpoint_schema_version is None else 2,
        }
        if self.checkpoint_schema_version is not None:
            payload.update(
                {
                    "checkpoint_schema_version": self.checkpoint_schema_version,
                    "database_authority_digest": self.database_authority_digest,
                    "public_schema_revision": self.public_schema_revision,
                    "capacity_guard_schema_revision": self.capacity_guard_schema_revision,
                    "manager_configuration_epoch": self.manager_configuration_epoch,
                    "manager_configuration_digest": self.manager_configuration_digest,
                    "manager_authority_incarnation": str(self.manager_authority_incarnation),
                    "manager_writer_epoch": self.manager_writer_epoch,
                    "manager_execution_state": self.manager_execution_state,
                    "manager_execution_epoch": self.manager_execution_epoch,
                    "manager_execution_manifest_sha256": self.manager_execution_manifest_sha256,
                    "manager_executable_new_capacity_ceiling": (
                        self.manager_executable_new_capacity_ceiling
                    ),
                    "manager_increase_freeze": self.manager_increase_freeze,
                    "restore_report_sha256": self.restore_report_sha256,
                }
            )
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> BackupLease:
        historical = {
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
        schema_three = historical | {
            "checkpoint_schema_version",
            "database_authority_digest",
            "public_schema_revision",
            "capacity_guard_schema_revision",
            "manager_configuration_epoch",
            "manager_configuration_digest",
            "manager_authority_incarnation",
            "manager_writer_epoch",
            "manager_execution_state",
            "manager_execution_epoch",
            "manager_execution_manifest_sha256",
            "manager_executable_new_capacity_ceiling",
            "manager_increase_freeze",
            "restore_report_sha256",
        }
        schema_version = data.get("schema_version")
        expected = historical if schema_version == 1 else schema_three
        components = data.get("component_sha256")
        if (
            set(data) != expected
            or schema_version not in {1, 2}
            or type(data.get("mutation_epoch")) is not int
            or not isinstance(components, Mapping)
            or not all(
                isinstance(key, str) and isinstance(value, str) for key, value in components.items()
            )
        ):
            raise ValueError("backup lease schema is invalid")
        string_fields = historical - {
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
        authority: dict[str, object] = {}
        if schema_version == 2:
            guard_revision = data["capacity_guard_schema_revision"]
            manifest_digest = data["manager_execution_manifest_sha256"]
            integer_fields = (
                "checkpoint_schema_version",
                "manager_configuration_epoch",
                "manager_writer_epoch",
                "manager_execution_epoch",
                "manager_executable_new_capacity_ceiling",
            )
            string_authority_fields = (
                "database_authority_digest",
                "public_schema_revision",
                "manager_configuration_digest",
                "manager_authority_incarnation",
                "manager_execution_state",
                "restore_report_sha256",
            )
            if (
                any(type(data[field]) is not int for field in integer_fields)
                or any(not isinstance(data[field], str) for field in string_authority_fields)
                or (guard_revision is not None and not isinstance(guard_revision, str))
                or manifest_digest is not None
                or type(data["manager_increase_freeze"]) is not bool
            ):
                raise ValueError("backup lease schema is invalid")
            try:
                incarnation = UUID(data["manager_authority_incarnation"])  # type: ignore[arg-type]
            except (TypeError, ValueError) as exc:
                raise ValueError("backup lease schema is invalid") from exc
            authority = {
                "checkpoint_schema_version": data["checkpoint_schema_version"],
                "database_authority_digest": data["database_authority_digest"],
                "public_schema_revision": data["public_schema_revision"],
                "capacity_guard_schema_revision": guard_revision,
                "manager_configuration_epoch": data["manager_configuration_epoch"],
                "manager_configuration_digest": data["manager_configuration_digest"],
                "manager_authority_incarnation": incarnation,
                "manager_writer_epoch": data["manager_writer_epoch"],
                "manager_execution_state": data["manager_execution_state"],
                "manager_execution_epoch": data["manager_execution_epoch"],
                "manager_execution_manifest_sha256": manifest_digest,
                "manager_executable_new_capacity_ceiling": data[
                    "manager_executable_new_capacity_ceiling"
                ],
                "manager_increase_freeze": data["manager_increase_freeze"],
                "restore_report_sha256": data["restore_report_sha256"],
            }
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
            **authority,  # type: ignore[arg-type]
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
