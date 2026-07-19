"""Typed authority for bounded staging execution data.

The database schema stores these values as text so migrations remain portable,
but writers and operators use the enums and validation helpers in this module.
Unknown values fail closed: they are never eligible for garbage collection.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from uuid import UUID

STAGING_EPHEMERAL_TTL = timedelta(days=7)
STAGING_GC_OBJECT_TRIGGER = 200_000
STAGING_GC_BYTES_TRIGGER = 12 * 1024**3
STAGING_GC_FREE_PERCENT_TRIGGER = 25
STAGING_ADMISSION_OBJECT_LIMIT = 250_000
STAGING_ADMISSION_BYTES_LIMIT = 16 * 1024**3
STAGING_ADMISSION_FREE_PERCENT_LIMIT = 20


def staging_capacity_policy_digest() -> str:
    """Return the immutable digest for the reviewed staging high-water policy."""
    payload = json.dumps(
        {
            "admission_bytes_limit": STAGING_ADMISSION_BYTES_LIMIT,
            "admission_free_percent_limit": STAGING_ADMISSION_FREE_PERCENT_LIMIT,
            "admission_object_limit": STAGING_ADMISSION_OBJECT_LIMIT,
            "gc_bytes_trigger": STAGING_GC_BYTES_TRIGGER,
            "gc_free_percent_trigger": STAGING_GC_FREE_PERCENT_TRIGGER,
            "gc_object_trigger": STAGING_GC_OBJECT_TRIGGER,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


class DataClass(StrEnum):
    RUN = "run"
    TRIAL = "trial"
    EVENT = "event"
    ARTIFACT = "artifact"
    BENCHMARK = "benchmark"
    CATALOG = "catalog"
    SYSTEM = "system"


class OwnerKind(StrEnum):
    BATCH = "batch"
    TRIAL = "trial"
    ARTIFACT = "artifact"
    BENCHMARK = "benchmark"
    SYSTEM = "system"


class LifecycleState(StrEnum):
    ACTIVE = "active"
    DELETING = "deleting"
    QUARANTINED = "quarantined"


class ObjectLifecycleState(StrEnum):
    ACTIVE = "active"
    DELETE_PENDING = "delete_pending"
    DELETED = "deleted"
    QUARANTINED = "quarantined"


class GcRunState(StrEnum):
    PLANNED = "planned"
    APPLYING = "applying"
    VERIFYING = "verifying"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class LifecycleAuthoritySpec:
    """Complete retention authority for one logical owner."""

    environment: str
    namespace: str
    team_id: UUID | None
    data_class: DataClass
    owner_kind: OwnerKind
    owner_id: str
    created_at: datetime
    expires_at: datetime | None
    pinned: bool = False

    def __post_init__(self) -> None:
        for name, value in (
            ("environment", self.environment),
            ("namespace", self.namespace),
            ("owner_id", self.owner_id),
        ):
            if not value or value != value.strip():
                raise ValueError(f"{name} must be non-empty and normalized")
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")
        if self.expires_at is not None:
            if self.expires_at.tzinfo is None:
                raise ValueError("expires_at must be timezone-aware")
            if self.expires_at <= self.created_at:
                raise ValueError("expires_at must follow created_at")
        if self.pinned == (self.expires_at is not None):
            raise ValueError(
                "pinned authorities require no expiry; ephemeral authorities require one"
            )
        if self.data_class in {DataClass.CATALOG, DataClass.SYSTEM} and not self.pinned:
            raise ValueError("catalog and system authorities must be pinned")

    @classmethod
    def staging_ephemeral(
        cls,
        *,
        namespace: str,
        team_id: UUID,
        data_class: DataClass,
        owner_kind: OwnerKind,
        owner_id: str,
        created_at: datetime | None = None,
    ) -> LifecycleAuthoritySpec:
        if data_class in {DataClass.CATALOG, DataClass.SYSTEM}:
            raise ValueError("catalog and system data cannot use ephemeral retention")
        now = created_at or datetime.now(UTC)
        return cls(
            environment="staging",
            namespace=namespace,
            team_id=team_id,
            data_class=data_class,
            owner_kind=owner_kind,
            owner_id=owner_id,
            created_at=now,
            expires_at=now + STAGING_EPHEMERAL_TTL,
        )

    def eligible_at(self, now: datetime) -> bool:
        """Return deletion eligibility; unexpired/pinned authorities stay false."""
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        return not self.pinned and self.expires_at is not None and self.expires_at <= now


@dataclass(frozen=True, slots=True)
class StagingCapacity:
    object_count: int
    bytes_used: int
    disk_free_percent: int
    inode_free_percent: int

    def __post_init__(self) -> None:
        if self.object_count < 0 or self.bytes_used < 0:
            raise ValueError("capacity counters must be non-negative")
        for value in (self.disk_free_percent, self.inode_free_percent):
            if not 0 <= value <= 100:
                raise ValueError("free percentages must be in [0, 100]")

    @property
    def gc_required(self) -> bool:
        return (
            self.object_count >= STAGING_GC_OBJECT_TRIGGER
            or self.bytes_used >= STAGING_GC_BYTES_TRIGGER
            or self.disk_free_percent < STAGING_GC_FREE_PERCENT_TRIGGER
            or self.inode_free_percent < STAGING_GC_FREE_PERCENT_TRIGGER
        )

    @property
    def admission_allowed(self) -> bool:
        return (
            self.object_count < STAGING_ADMISSION_OBJECT_LIMIT
            and self.bytes_used < STAGING_ADMISSION_BYTES_LIMIT
            and self.disk_free_percent >= STAGING_ADMISSION_FREE_PERCENT_LIMIT
            and self.inode_free_percent >= STAGING_ADMISSION_FREE_PERCENT_LIMIT
        )

    @property
    def evidence_digest(self) -> str:
        payload = json.dumps(
            {
                "bytes_used": self.bytes_used,
                "disk_free_percent": self.disk_free_percent,
                "inode_free_percent": self.inode_free_percent,
                "object_count": self.object_count,
                "policy_digest": staging_capacity_policy_digest(),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(payload).hexdigest()
