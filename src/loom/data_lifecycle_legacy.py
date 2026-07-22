"""Deterministic, fail-closed classification of pre-lifecycle staging history.

The migration intentionally left execution foreign keys nullable so an upgrade
would not guess retention or object-store authority.  This module is the
storage-independent half of the supported backfill protocol: inventory emits
exact row and object evidence, the planner binds it to one mutation epoch, and
only an explicitly approved digest may be applied by the SQL adapter.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid5

from loom.data_lifecycle import STAGING_EPHEMERAL_TTL, DataClass, OwnerKind
from loom.data_lifecycle_gc import GcScope

_AUTHORITY_NAMESPACE = UUID("24f4f0db-ec29-40ad-b65d-c5d6fb45b23a")
_ROW_TABLES = frozenset({"batches", "trials", "llm_calls", "trial_events", "artifacts"})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class LegacyClassificationError(RuntimeError):
    """Raised when legacy history cannot be classified without guessing."""


@dataclass(frozen=True, slots=True)
class LegacyRow:
    table: str
    row_id: UUID
    team_id: UUID
    data_class: DataClass
    owner_kind: OwnerKind
    owner_id: str
    created_at: datetime
    source_fingerprint: str
    pinned: bool = False

    def __post_init__(self) -> None:
        if self.table not in _ROW_TABLES:
            raise ValueError("legacy lifecycle row table is not allowlisted")
        if not self.owner_id or self.owner_id != self.owner_id.strip():
            raise ValueError("legacy lifecycle owner id must be normalized")
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("legacy lifecycle row time must be timezone-aware")
        if _SHA256_RE.fullmatch(self.source_fingerprint) is None:
            raise ValueError("legacy lifecycle row fingerprint is invalid")


@dataclass(frozen=True, slots=True)
class LegacyObject:
    row_table: str
    row_id: UUID
    bucket: str
    object_key: str
    version_id: str | None
    content_sha256: str
    size_bytes: int
    created_at: datetime

    def __post_init__(self) -> None:
        if self.row_table != "artifacts":
            raise ValueError("legacy object evidence must belong to an artifact row")
        if (
            not self.bucket
            or self.bucket != self.bucket.strip()
            or not self.object_key
            or self.object_key != self.object_key.strip()
            or self.object_key.startswith("/")
        ):
            raise ValueError("legacy object identity must be normalized and relative")
        if self.version_id is not None and (
            not self.version_id or self.version_id != self.version_id.strip()
        ):
            raise ValueError("legacy object version must be normalized")
        if _SHA256_RE.fullmatch(self.content_sha256) is None:
            raise ValueError("legacy object SHA-256 is invalid")
        if self.size_bytes < 0:
            raise ValueError("legacy object size must be non-negative")
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("legacy object time must be timezone-aware")

    @property
    def identity(self) -> tuple[str, str, str]:
        return self.bucket, self.object_key, self.version_id or ""


@dataclass(frozen=True, slots=True)
class LegacyAuthoritySeed:
    """An exact legacy owner that has no lifecycle-bearing source row.

    This is deliberately separate from :class:`LegacyRow`: applying a seed
    creates only the lifecycle authority and never grants permission to update
    an unrelated table.  It is used for durable catalog/system objects whose
    owning rows predate the execution-data lifecycle schema.
    """

    team_id: UUID | None
    data_class: DataClass
    owner_kind: OwnerKind
    owner_id: str
    created_at: datetime
    pinned: bool

    def __post_init__(self) -> None:
        if not self.owner_id or self.owner_id != self.owner_id.strip():
            raise ValueError("legacy lifecycle owner id must be normalized")
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("legacy lifecycle authority time must be timezone-aware")
        if self.team_id is None and not self.pinned:
            raise ValueError("unowned legacy authorities must be pinned")
        if self.data_class in {DataClass.CATALOG, DataClass.SYSTEM} and not self.pinned:
            raise ValueError("legacy catalog/system authorities must be pinned")


@dataclass(frozen=True, slots=True)
class LegacySupplementalObject:
    """Exact object evidence bound to an already classified logical owner."""

    authority_data_class: DataClass
    authority_owner_kind: OwnerKind
    authority_owner_id: str
    bucket: str
    object_key: str
    version_id: str | None
    content_sha256: str
    size_bytes: int
    created_at: datetime

    def __post_init__(self) -> None:
        if not self.authority_owner_id or (
            self.authority_owner_id != self.authority_owner_id.strip()
        ):
            raise ValueError("legacy supplemental object owner must be normalized")
        if (
            not self.bucket
            or self.bucket != self.bucket.strip()
            or not self.object_key
            or self.object_key != self.object_key.strip()
            or self.object_key.startswith("/")
        ):
            raise ValueError("legacy object identity must be normalized and relative")
        if self.version_id is not None and (
            not self.version_id or self.version_id != self.version_id.strip()
        ):
            raise ValueError("legacy object version must be normalized")
        if _SHA256_RE.fullmatch(self.content_sha256) is None:
            raise ValueError("legacy object SHA-256 is invalid")
        if self.size_bytes < 0:
            raise ValueError("legacy object size must be non-negative")
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("legacy object time must be timezone-aware")

    @property
    def authority_key(self) -> tuple[DataClass, OwnerKind, str]:
        return (
            self.authority_data_class,
            self.authority_owner_kind,
            self.authority_owner_id,
        )

    @property
    def identity(self) -> tuple[str, str, str]:
        return self.bucket, self.object_key, self.version_id or ""


@dataclass(frozen=True, slots=True)
class LegacyAbsentObject:
    """Exact legacy DB reference proven absent from the observed object store."""

    row_table: str
    row_id: UUID
    bucket: str
    object_key: str
    version_id: str | None
    created_at: datetime

    def __post_init__(self) -> None:
        if self.row_table != "artifacts":
            raise ValueError("legacy absent object evidence must belong to an artifact row")
        if (
            not self.bucket
            or self.bucket != self.bucket.strip()
            or not self.object_key
            or self.object_key != self.object_key.strip()
            or self.object_key.startswith("/")
        ):
            raise ValueError("legacy absent object identity must be normalized and relative")
        if self.version_id is not None and (
            not self.version_id or self.version_id != self.version_id.strip()
        ):
            raise ValueError("legacy absent object version must be normalized")
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("legacy absent object time must be timezone-aware")

    @property
    def identity(self) -> tuple[str, str, str]:
        return self.bucket, self.object_key, self.version_id or ""


@dataclass(frozen=True, slots=True)
class LegacyAuthority:
    id: UUID
    team_id: UUID | None
    data_class: DataClass
    owner_kind: OwnerKind
    owner_id: str
    created_at: datetime
    expires_at: datetime | None
    pinned: bool

    def __post_init__(self) -> None:
        if self.pinned == (self.expires_at is not None):
            raise ValueError(
                "pinned legacy authorities require no expiry; ephemeral authorities require one"
            )


@dataclass(frozen=True, slots=True)
class LegacyClassificationPlan:
    scope: GcScope
    mutation_epoch: int
    planned_at: datetime
    expire_created_before: datetime | None
    authorities: tuple[LegacyAuthority, ...]
    rows: tuple[LegacyRow, ...]
    objects: tuple[LegacyObject | LegacySupplementalObject, ...]
    absent_objects: tuple[LegacyAbsentObject, ...]
    blockers: tuple[str, ...]
    inventory_digest: str

    def require_applicable(self) -> None:
        if self.blockers:
            raise LegacyClassificationError("; ".join(self.blockers))
        if not self.rows:
            raise LegacyClassificationError("legacy classification contains no rows")


def _authority_id(
    scope: GcScope,
    data_class: DataClass,
    owner_kind: OwnerKind,
    owner_id: str,
) -> UUID:
    return uuid5(
        _AUTHORITY_NAMESPACE,
        "\0".join(
            (
                scope.environment,
                scope.namespace,
                data_class.value,
                owner_kind.value,
                owner_id,
            )
        ),
    )


def _payload(
    *,
    scope: GcScope,
    mutation_epoch: int,
    planned_at: datetime,
    expire_created_before: datetime | None,
    authorities: tuple[LegacyAuthority, ...],
    rows: tuple[LegacyRow, ...],
    objects: tuple[LegacyObject | LegacySupplementalObject, ...],
    absent_objects: tuple[LegacyAbsentObject, ...],
    blockers: tuple[str, ...],
) -> dict[str, object]:
    return {
        "schema_version": 2,
        "environment": scope.environment,
        "namespace": scope.namespace,
        "mutation_epoch": mutation_epoch,
        "planned_at": planned_at.isoformat(),
        "expire_created_before": (
            expire_created_before.isoformat() if expire_created_before else None
        ),
        "authorities": [
            {
                "id": str(item.id),
                "team_id": str(item.team_id),
                "data_class": item.data_class.value,
                "owner_kind": item.owner_kind.value,
                "owner_id": item.owner_id,
                "created_at": item.created_at.isoformat(),
                "expires_at": item.expires_at.isoformat() if item.expires_at else None,
                "pinned": item.pinned,
            }
            for item in authorities
        ],
        "rows": [
            {
                "table": item.table,
                "row_id": str(item.row_id),
                "team_id": str(item.team_id),
                "data_class": item.data_class.value,
                "owner_kind": item.owner_kind.value,
                "owner_id": item.owner_id,
                "created_at": item.created_at.isoformat(),
                "source_fingerprint": item.source_fingerprint,
                "pinned": item.pinned,
            }
            for item in rows
        ],
        "objects": [
            (
                {
                    "authority_data_class": item.authority_data_class.value,
                    "authority_owner_kind": item.authority_owner_kind.value,
                    "authority_owner_id": item.authority_owner_id,
                    "bucket": item.bucket,
                    "object_key": item.object_key,
                    "version_id": item.version_id,
                    "content_sha256": item.content_sha256,
                    "size_bytes": item.size_bytes,
                    "created_at": item.created_at.isoformat(),
                }
                if isinstance(item, LegacySupplementalObject)
                else {
                    "row_table": item.row_table,
                    "row_id": str(item.row_id),
                    "bucket": item.bucket,
                    "object_key": item.object_key,
                    "version_id": item.version_id,
                    "content_sha256": item.content_sha256,
                    "size_bytes": item.size_bytes,
                    "created_at": item.created_at.isoformat(),
                }
            )
            for item in objects
        ],
        "absent_objects": [
            {
                "row_table": item.row_table,
                "row_id": str(item.row_id),
                "bucket": item.bucket,
                "object_key": item.object_key,
                "version_id": item.version_id,
                "created_at": item.created_at.isoformat(),
            }
            for item in absent_objects
        ],
        "blockers": list(blockers),
    }


def build_legacy_classification_plan(
    *,
    scope: GcScope,
    mutation_epoch: int,
    planned_at: datetime,
    rows: Iterable[LegacyRow],
    objects: Iterable[LegacyObject],
    supplemental_authorities: Iterable[LegacyAuthoritySeed] = (),
    supplemental_objects: Iterable[LegacySupplementalObject] = (),
    absent_objects: Iterable[LegacyAbsentObject] = (),
    expire_created_before: datetime | None = None,
    additional_blockers: Iterable[str] = (),
) -> LegacyClassificationPlan:
    """Bind complete legacy evidence to one deterministic, reviewable digest."""
    if mutation_epoch < 0:
        raise ValueError("mutation_epoch must be non-negative")
    if planned_at.tzinfo is None or planned_at.utcoffset() is None:
        raise ValueError("planned_at must be timezone-aware")
    if expire_created_before is not None and (
        expire_created_before.tzinfo is None or expire_created_before.utcoffset() is None
    ):
        raise ValueError("legacy expiry cutoff must be timezone-aware")

    blockers = [value for value in additional_blockers if value]
    row_map: dict[tuple[str, UUID], LegacyRow] = {}
    authority_specs: dict[UUID, LegacyAuthority] = {}
    artifact_ids: set[UUID] = set()
    for row in rows:
        row_key = row.table, row.row_id
        if row_key in row_map:
            blockers.append(f"duplicate legacy row {row.table}/{row.row_id}")
            continue
        row_map[row_key] = row
        authority_id = _authority_id(scope, row.data_class, row.owner_kind, row.owner_id)
        existing = authority_specs.get(authority_id)
        if existing is not None and (
            existing.team_id != row.team_id
            or existing.data_class is not row.data_class
            or existing.owner_kind is not row.owner_kind
            or existing.owner_id != row.owner_id
            or existing.pinned != row.pinned
        ):
            blockers.append(f"legacy authority facts conflict for {row.owner_kind}/{row.owner_id}")
        else:
            created_at = min(existing.created_at, row.created_at) if existing else row.created_at
            default_expiry = created_at + STAGING_EPHEMERAL_TTL
            expires_at = None
            if not row.pinned:
                expires_at = (
                    min(default_expiry, expire_created_before)
                    if expire_created_before is not None and created_at < expire_created_before
                    else default_expiry
                )
            authority_specs[authority_id] = LegacyAuthority(
                id=authority_id,
                team_id=row.team_id,
                data_class=row.data_class,
                owner_kind=row.owner_kind,
                owner_id=row.owner_id,
                created_at=created_at,
                expires_at=expires_at,
                pinned=row.pinned,
            )
        if row.table == "artifacts":
            artifact_ids.add(row.row_id)

    for seed in supplemental_authorities:
        authority_id = _authority_id(
            scope,
            seed.data_class,
            seed.owner_kind,
            seed.owner_id,
        )
        existing = authority_specs.get(authority_id)
        if existing is not None and (
            existing.team_id != seed.team_id
            or existing.data_class is not seed.data_class
            or existing.owner_kind is not seed.owner_kind
            or existing.owner_id != seed.owner_id
            or existing.pinned != seed.pinned
        ):
            blockers.append(
                f"legacy authority facts conflict for {seed.owner_kind}/{seed.owner_id}"
            )
            continue
        created_at = min(existing.created_at, seed.created_at) if existing else seed.created_at
        default_expiry = created_at + STAGING_EPHEMERAL_TTL
        expires_at = None
        if not seed.pinned:
            expires_at = (
                min(default_expiry, expire_created_before)
                if expire_created_before is not None and created_at < expire_created_before
                else default_expiry
            )
        authority_specs[authority_id] = LegacyAuthority(
            id=authority_id,
            team_id=seed.team_id,
            data_class=seed.data_class,
            owner_kind=seed.owner_kind,
            owner_id=seed.owner_id,
            created_at=created_at,
            expires_at=expires_at,
            pinned=seed.pinned,
        )

    selected_objects: list[LegacyObject | LegacySupplementalObject] = []
    selected_absent: list[LegacyAbsentObject] = []
    seen_objects: set[tuple[str, str, str]] = set()
    object_rows: set[UUID] = set()
    for object_item in objects:
        if object_item.row_id not in artifact_ids:
            blockers.append(f"legacy object has no classified artifact row {object_item.row_id}")
            continue
        if object_item.identity in seen_objects:
            blockers.append(
                f"duplicate legacy object identity {object_item.bucket}/{object_item.object_key}"
            )
            continue
        seen_objects.add(object_item.identity)
        object_rows.add(object_item.row_id)
        selected_objects.append(object_item)

    authority_keys = {
        (item.data_class, item.owner_kind, item.owner_id) for item in authority_specs.values()
    }
    for supplemental_object in supplemental_objects:
        if supplemental_object.authority_key not in authority_keys:
            blockers.append(
                "legacy supplemental object has no classified authority "
                f"{supplemental_object.authority_owner_kind}/"
                f"{supplemental_object.authority_owner_id}"
            )
            continue
        if supplemental_object.identity in seen_objects:
            blockers.append(
                "duplicate legacy object identity "
                f"{supplemental_object.bucket}/{supplemental_object.object_key}"
            )
            continue
        seen_objects.add(supplemental_object.identity)
        selected_objects.append(supplemental_object)

    for absent_item in absent_objects:
        if absent_item.row_id not in artifact_ids:
            blockers.append(
                f"legacy absent object has no classified artifact row {absent_item.row_id}"
            )
            continue
        if absent_item.identity in seen_objects:
            blockers.append(
                f"duplicate legacy object identity {absent_item.bucket}/{absent_item.object_key}"
            )
            continue
        seen_objects.add(absent_item.identity)
        object_rows.add(absent_item.row_id)
        selected_absent.append(absent_item)

    missing_object_rows = artifact_ids - object_rows
    blockers.extend(
        f"legacy artifact {row_id} lacks exact object evidence"
        for row_id in sorted(missing_object_rows)
    )

    stable_rows = tuple(sorted(row_map.values(), key=lambda row: (row.table, str(row.row_id))))
    stable_authorities = tuple(
        sorted(
            authority_specs.values(),
            key=lambda item: (item.data_class.value, item.owner_kind.value, item.owner_id),
        )
    )
    stable_objects = tuple(
        sorted(
            selected_objects,
            key=lambda item: (
                item.identity,
                str(item.row_id)
                if isinstance(item, LegacyObject)
                else "/".join(
                    (
                        item.authority_data_class.value,
                        item.authority_owner_kind.value,
                        item.authority_owner_id,
                    )
                ),
            ),
        )
    )
    stable_absent = tuple(
        sorted(selected_absent, key=lambda item: (item.identity, str(item.row_id)))
    )
    stable_blockers = tuple(sorted(set(blockers)))
    payload = _payload(
        scope=scope,
        mutation_epoch=mutation_epoch,
        planned_at=planned_at,
        expire_created_before=expire_created_before,
        authorities=stable_authorities,
        rows=stable_rows,
        objects=stable_objects,
        absent_objects=stable_absent,
        blockers=stable_blockers,
    )
    digest_payload = {key: value for key, value in payload.items() if key != "planned_at"}
    digest = hashlib.sha256(
        json.dumps(digest_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return LegacyClassificationPlan(
        scope=scope,
        mutation_epoch=mutation_epoch,
        planned_at=planned_at,
        expire_created_before=expire_created_before,
        authorities=stable_authorities,
        rows=stable_rows,
        objects=stable_objects,
        absent_objects=stable_absent,
        blockers=stable_blockers,
        inventory_digest=digest,
    )


def classification_plan_document(plan: LegacyClassificationPlan) -> Mapping[str, object]:
    """Return canonical non-secret inventory evidence for operator review."""
    payload = _payload(
        scope=plan.scope,
        mutation_epoch=plan.mutation_epoch,
        planned_at=plan.planned_at,
        expire_created_before=plan.expire_created_before,
        authorities=plan.authorities,
        rows=plan.rows,
        objects=plan.objects,
        absent_objects=plan.absent_objects,
        blockers=plan.blockers,
    )
    return {**payload, "inventory_digest": plan.inventory_digest}


def classification_plan_summary(plan: LegacyClassificationPlan) -> Mapping[str, object]:
    """Return row/object-payload-bounded evidence for an exact digest-bound plan.

    The canonical inventory digest continues to bind every row fingerprint and
    exact object identity.  This summary intentionally does not duplicate that
    potentially million-row authority into stdout or a second evidence file.
    All classification blockers remain visible together. ``apply`` rebuilds
    the complete live plan and requires the same digest.
    """
    row_counts = Counter(item.table for item in plan.rows)
    class_counts = Counter(item.data_class.value for item in plan.authorities)
    pinned = sum(1 for item in plan.authorities if item.pinned)
    return {
        "schema_version": 2,
        "evidence_kind": "digest-bound-legacy-classification-summary",
        "environment": plan.scope.environment,
        "namespace": plan.scope.namespace,
        "mutation_epoch": plan.mutation_epoch,
        "planned_at": plan.planned_at.isoformat(),
        "expire_created_before": (
            plan.expire_created_before.isoformat() if plan.expire_created_before else None
        ),
        "inventory_digest": plan.inventory_digest,
        "authority_count": len(plan.authorities),
        "ephemeral_authority_count": len(plan.authorities) - pinned,
        "pinned_authority_count": pinned,
        "authority_class_counts": dict(sorted(class_counts.items())),
        "row_count": len(plan.rows),
        "row_table_counts": dict(sorted(row_counts.items())),
        "present_object_count": len(plan.objects),
        "present_object_bytes": sum(item.size_bytes for item in plan.objects),
        "verified_absent_object_count": len(plan.absent_objects),
        "blockers": list(plan.blockers),
    }


__all__ = [
    "LegacyAbsentObject",
    "LegacyAuthority",
    "LegacyAuthoritySeed",
    "LegacyClassificationError",
    "LegacyClassificationPlan",
    "LegacyObject",
    "LegacyRow",
    "LegacySupplementalObject",
    "build_legacy_classification_plan",
    "classification_plan_document",
    "classification_plan_summary",
]
