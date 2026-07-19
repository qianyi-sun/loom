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
class LegacyAuthority:
    id: UUID
    team_id: UUID
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
    authorities: tuple[LegacyAuthority, ...]
    rows: tuple[LegacyRow, ...]
    objects: tuple[LegacyObject, ...]
    blockers: tuple[str, ...]
    inventory_digest: str

    def require_applicable(self) -> None:
        if self.blockers:
            raise LegacyClassificationError("; ".join(self.blockers))
        if not self.rows:
            raise LegacyClassificationError("legacy classification contains no rows")


def _authority_id(scope: GcScope, row: LegacyRow) -> UUID:
    return uuid5(
        _AUTHORITY_NAMESPACE,
        "\0".join(
            (
                scope.environment,
                scope.namespace,
                row.data_class.value,
                row.owner_kind.value,
                row.owner_id,
            )
        ),
    )


def _payload(
    *,
    scope: GcScope,
    mutation_epoch: int,
    planned_at: datetime,
    authorities: tuple[LegacyAuthority, ...],
    rows: tuple[LegacyRow, ...],
    objects: tuple[LegacyObject, ...],
    blockers: tuple[str, ...],
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "environment": scope.environment,
        "namespace": scope.namespace,
        "mutation_epoch": mutation_epoch,
        "planned_at": planned_at.isoformat(),
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
            {
                "row_table": item.row_table,
                "row_id": str(item.row_id),
                "bucket": item.bucket,
                "object_key": item.object_key,
                "version_id": item.version_id,
                "content_sha256": item.content_sha256,
                "size_bytes": item.size_bytes,
                "created_at": item.created_at.isoformat(),
            }
            for item in objects
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
    additional_blockers: Iterable[str] = (),
) -> LegacyClassificationPlan:
    """Bind complete legacy evidence to one deterministic, reviewable digest."""
    if mutation_epoch < 0:
        raise ValueError("mutation_epoch must be non-negative")
    if planned_at.tzinfo is None or planned_at.utcoffset() is None:
        raise ValueError("planned_at must be timezone-aware")

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
        authority_id = _authority_id(scope, row)
        authority = LegacyAuthority(
            id=authority_id,
            team_id=row.team_id,
            data_class=row.data_class,
            owner_kind=row.owner_kind,
            owner_id=row.owner_id,
            created_at=row.created_at,
            expires_at=None if row.pinned else row.created_at + STAGING_EPHEMERAL_TTL,
            pinned=row.pinned,
        )
        existing = authority_specs.get(authority_id)
        if existing is not None and existing != authority:
            blockers.append(f"legacy authority facts conflict for {row.owner_kind}/{row.owner_id}")
        else:
            authority_specs[authority_id] = authority
        if row.table == "artifacts":
            artifact_ids.add(row.row_id)

    selected_objects: list[LegacyObject] = []
    seen_objects: set[tuple[str, str, str]] = set()
    object_rows: set[UUID] = set()
    for item in objects:
        if item.row_id not in artifact_ids:
            blockers.append(f"legacy object has no classified artifact row {item.row_id}")
            continue
        if item.identity in seen_objects:
            blockers.append(f"duplicate legacy object identity {item.bucket}/{item.object_key}")
            continue
        seen_objects.add(item.identity)
        object_rows.add(item.row_id)
        selected_objects.append(item)

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
        sorted(selected_objects, key=lambda item: (item.identity, str(item.row_id)))
    )
    stable_blockers = tuple(sorted(set(blockers)))
    payload = _payload(
        scope=scope,
        mutation_epoch=mutation_epoch,
        planned_at=planned_at,
        authorities=stable_authorities,
        rows=stable_rows,
        objects=stable_objects,
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
        authorities=stable_authorities,
        rows=stable_rows,
        objects=stable_objects,
        blockers=stable_blockers,
        inventory_digest=digest,
    )


def classification_plan_document(plan: LegacyClassificationPlan) -> Mapping[str, object]:
    """Return canonical non-secret inventory evidence for operator review."""
    payload = _payload(
        scope=plan.scope,
        mutation_epoch=plan.mutation_epoch,
        planned_at=plan.planned_at,
        authorities=plan.authorities,
        rows=plan.rows,
        objects=plan.objects,
        blockers=plan.blockers,
    )
    return {**payload, "inventory_digest": plan.inventory_digest}


__all__ = [
    "LegacyAuthority",
    "LegacyClassificationError",
    "LegacyClassificationPlan",
    "LegacyObject",
    "LegacyRow",
    "build_legacy_classification_plan",
    "classification_plan_document",
]
