"""Transactional lifecycle-authority registration for execution writers."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.data_lifecycle import (
    STAGING_EPHEMERAL_TTL,
    DataClass,
    LifecycleAuthoritySpec,
    OwnerKind,
)
from loom.data_lifecycle_capacity_sql import require_staging_capacity_admission
from loom.db.schema import (
    Batch,
    DataLifecycleAuthority,
    DataLifecycleObject,
    Trial,
)

_PROTECTED_ENVIRONMENTS = frozenset({"staging", "production"})


@dataclass(frozen=True, slots=True)
class RuntimeLifecycleScope:
    environment: str
    namespace: str

    def __post_init__(self) -> None:
        if not self.environment or self.environment != self.environment.strip().lower():
            raise ValueError("runtime lifecycle environment must be normalized")
        if not self.namespace or self.namespace != self.namespace.strip():
            raise ValueError("runtime lifecycle namespace must be normalized")

    @classmethod
    def from_environ(cls) -> RuntimeLifecycleScope:
        environment = os.environ.get("LOOM_ENV", "development").strip().lower()
        namespace_value = os.environ.get("LOOM_NAMESPACE")
        if namespace_value is None:
            if environment in _PROTECTED_ENVIRONMENTS:
                raise RuntimeError("LOOM_NAMESPACE is required in protected environments")
            namespace_value = "loom"
        return cls(environment=environment, namespace=namespace_value)

    def authority_spec(
        self,
        *,
        team_id: UUID | None,
        data_class: DataClass,
        owner_kind: OwnerKind,
        owner_id: str,
        created_at: datetime | None = None,
    ) -> LifecycleAuthoritySpec:
        now = created_at or datetime.now(UTC)
        if self.environment == "staging":
            if team_id is None:
                raise ValueError("staging execution authority requires a team")
            return LifecycleAuthoritySpec.staging_ephemeral(
                namespace=self.namespace,
                team_id=team_id,
                data_class=data_class,
                owner_kind=owner_kind,
                owner_id=owner_id,
                created_at=now,
            )
        return LifecycleAuthoritySpec(
            environment=self.environment,
            namespace=self.namespace,
            team_id=team_id,
            data_class=data_class,
            owner_kind=owner_kind,
            owner_id=owner_id,
            created_at=now,
            expires_at=None,
            pinned=True,
        )


def _validate_existing(
    row: DataLifecycleAuthority,
    spec: LifecycleAuthoritySpec,
) -> None:
    if (
        row.environment != spec.environment
        or row.namespace != spec.namespace
        or row.team_id != spec.team_id
        or row.data_class != spec.data_class
        or row.owner_kind != spec.owner_kind
        or row.owner_id != spec.owner_id
        or row.pinned != spec.pinned
        or row.state != "active"
    ):
        raise RuntimeError("existing lifecycle authority conflicts with requested owner")
    if spec.environment == "staging":
        if row.expires_at != row.created_at + STAGING_EPHEMERAL_TTL:
            raise RuntimeError("existing staging lifecycle authority retention drifted")
    elif row.expires_at is not None:
        raise RuntimeError("existing pinned lifecycle authority unexpectedly expires")


async def ensure_lifecycle_authority(
    session: AsyncSession,
    *,
    spec: LifecycleAuthoritySpec,
) -> UUID:
    """Create or verify one owner authority in the caller's transaction."""
    values = {
        "environment": spec.environment,
        "namespace": spec.namespace,
        "team_id": spec.team_id,
        "data_class": spec.data_class,
        "owner_kind": spec.owner_kind,
        "owner_id": spec.owner_id,
        "created_at": spec.created_at,
        "expires_at": spec.expires_at,
        "pinned": spec.pinned,
        "state": "active",
    }
    result = await session.execute(
        pg_insert(DataLifecycleAuthority)
        .values(**values)
        .on_conflict_do_nothing(
            index_elements=[
                "environment",
                "namespace",
                "data_class",
                "owner_kind",
                "owner_id",
            ]
        )
        .returning(DataLifecycleAuthority.id)
    )
    authority_id = result.scalar_one_or_none()
    if authority_id is not None:
        return authority_id
    row = (
        await session.execute(
            select(DataLifecycleAuthority).where(
                DataLifecycleAuthority.environment == spec.environment,
                DataLifecycleAuthority.namespace == spec.namespace,
                DataLifecycleAuthority.data_class == spec.data_class,
                DataLifecycleAuthority.owner_kind == spec.owner_kind,
                DataLifecycleAuthority.owner_id == spec.owner_id,
            )
        )
    ).scalar_one()
    _validate_existing(row, spec)
    return row.id


async def ensure_batch_lifecycle_authority(
    session: AsyncSession,
    *,
    batch_id: UUID,
    team_id: UUID,
    created_at: datetime,
    enforce_admission: bool = True,
) -> UUID:
    scope = RuntimeLifecycleScope.from_environ()
    if scope.environment == "staging" and enforce_admission:
        await require_staging_capacity_admission(
            session,
            namespace=scope.namespace,
            now=created_at,
        )
    return await ensure_lifecycle_authority(
        session,
        spec=scope.authority_spec(
            team_id=team_id,
            data_class=DataClass.RUN,
            owner_kind=OwnerKind.BATCH,
            owner_id=str(batch_id),
            created_at=created_at,
        ),
    )


async def bind_existing_batch_lifecycle_authority(
    session: AsyncSession,
    *,
    batch_id: UUID,
    expected_team_id: UUID | None = None,
) -> UUID:
    batch = (
        await session.execute(select(Batch).where(Batch.id == batch_id).with_for_update())
    ).scalar_one_or_none()
    if batch is None:
        raise RuntimeError("batch lifecycle owner does not exist")
    if expected_team_id is not None and batch.team_id != expected_team_id:
        raise RuntimeError("batch lifecycle owner team conflicts with writer")
    authority_id = await ensure_batch_lifecycle_authority(
        session,
        batch_id=batch.id,
        team_id=batch.team_id,
        created_at=batch.created_at,
        enforce_admission=False,
    )
    if batch.lifecycle_authority_id is None:
        result = await session.execute(
            update(Batch)
            .where(Batch.id == batch.id, Batch.lifecycle_authority_id.is_(None))
            .values(lifecycle_authority_id=authority_id)
            .returning(Batch.id)
        )
        if result.scalar_one_or_none() != batch.id:
            raise RuntimeError("batch lifecycle parent binding raced")
    elif batch.lifecycle_authority_id != authority_id:
        raise RuntimeError("batch lifecycle parent authority conflicts")
    return authority_id


async def ensure_trial_lifecycle_authority(
    session: AsyncSession,
    *,
    trial_id: UUID,
    team_id: UUID,
    created_at: datetime,
) -> UUID:
    scope = RuntimeLifecycleScope.from_environ()
    return await ensure_lifecycle_authority(
        session,
        spec=scope.authority_spec(
            team_id=team_id,
            data_class=DataClass.TRIAL,
            owner_kind=OwnerKind.TRIAL,
            owner_id=str(trial_id),
            created_at=created_at,
        ),
    )


async def ensure_trial_event_lifecycle_authority(
    session: AsyncSession,
    *,
    trial_id: UUID,
    expected_team_id: UUID | None = None,
) -> UUID:
    """Bind a trial and return its shared event-stream authority.

    The lazy parent bind lets workers safely continue trials that were admitted
    immediately before the lifecycle migration, without permitting a child row
    to commit unclassified. A conflicting team or parent authority fails the
    caller's transaction.
    """
    _parent_authority_id, team_id, submitted_at = (
        await bind_existing_trial_lifecycle_authority(
            session,
            trial_id=trial_id,
            expected_team_id=expected_team_id,
        )
    )

    scope = RuntimeLifecycleScope.from_environ()
    return await ensure_lifecycle_authority(
        session,
        spec=scope.authority_spec(
            team_id=team_id,
            data_class=DataClass.EVENT,
            owner_kind=OwnerKind.TRIAL,
            owner_id=str(trial_id),
            created_at=submitted_at,
        ),
    )


async def bind_existing_trial_lifecycle_authority(
    session: AsyncSession,
    *,
    trial_id: UUID,
    expected_team_id: UUID | None = None,
) -> tuple[UUID, UUID, datetime]:
    """Return the exact parent authority, team, and creation time."""
    trial = (
        await session.execute(select(Trial).where(Trial.id == trial_id).with_for_update())
    ).scalar_one_or_none()
    if trial is None:
        raise RuntimeError("trial lifecycle owner does not exist")
    if expected_team_id is not None and trial.team_id != expected_team_id:
        raise RuntimeError("trial lifecycle owner team conflicts with writer")
    parent_authority_id = await ensure_trial_lifecycle_authority(
        session,
        trial_id=trial.id,
        team_id=trial.team_id,
        created_at=trial.submitted_at,
    )
    if trial.lifecycle_authority_id is None:
        result = await session.execute(
            update(Trial)
            .where(Trial.id == trial.id, Trial.lifecycle_authority_id.is_(None))
            .values(lifecycle_authority_id=parent_authority_id)
            .returning(Trial.id)
        )
        if result.scalar_one_or_none() != trial.id:
            raise RuntimeError("trial lifecycle parent binding raced")
    elif trial.lifecycle_authority_id != parent_authority_id:
        raise RuntimeError("trial lifecycle parent authority conflicts")
    return parent_authority_id, trial.team_id, trial.submitted_at


async def ensure_artifact_lifecycle_authority(
    session: AsyncSession,
    *,
    artifact_id: UUID,
    team_id: UUID,
    created_at: datetime,
) -> UUID:
    scope = RuntimeLifecycleScope.from_environ()
    return await ensure_lifecycle_authority(
        session,
        spec=scope.authority_spec(
            team_id=team_id,
            data_class=DataClass.ARTIFACT,
            owner_kind=OwnerKind.ARTIFACT,
            owner_id=str(artifact_id),
            created_at=created_at,
        ),
    )


async def register_lifecycle_object(
    session: AsyncSession,
    *,
    authority_id: UUID,
    bucket: str,
    object_key: str,
    version_id: str | None,
    content_sha256: str | None,
    size_bytes: int,
    created_at: datetime,
) -> UUID:
    """Register one exact object identity or verify an idempotent retry."""
    scope = RuntimeLifecycleScope.from_environ()
    if not bucket or bucket != bucket.strip():
        raise ValueError("lifecycle object bucket must be normalized")
    if not object_key or object_key != object_key.strip():
        raise ValueError("lifecycle object key must be normalized")
    if version_id is not None and (not version_id or version_id != version_id.strip()):
        raise ValueError("lifecycle object version must be normalized")
    if content_sha256 is not None and re.fullmatch(r"[0-9a-f]{64}", content_sha256) is None:
        raise ValueError("lifecycle object SHA-256 must be lowercase hexadecimal")
    if version_id is None and content_sha256 is None:
        raise ValueError("unversioned lifecycle object requires a SHA-256")
    if size_bytes < 0:
        raise ValueError("lifecycle object size must be non-negative")
    if created_at.tzinfo is None:
        raise ValueError("lifecycle object created_at must be timezone-aware")

    authority = await session.get(DataLifecycleAuthority, authority_id)
    if authority is None or authority.state != "active":
        raise RuntimeError("lifecycle object authority is missing or inactive")
    if (authority.environment, authority.namespace) != (
        scope.environment,
        scope.namespace,
    ):
        raise RuntimeError("lifecycle object authority scope conflicts with runtime")

    values = {
        "authority_id": authority_id,
        "environment": scope.environment,
        "namespace": scope.namespace,
        "bucket": bucket,
        "object_key": object_key,
        "version_id": version_id,
        "content_sha256": content_sha256,
        "size_bytes": size_bytes,
        "created_at": created_at,
        "state": "active",
    }
    result = await session.execute(
        pg_insert(DataLifecycleObject)
        .values(**values)
        .on_conflict_do_nothing()
        .returning(DataLifecycleObject.id)
    )
    object_id = result.scalar_one_or_none()
    if object_id is not None:
        return object_id
    version_predicate = (
        DataLifecycleObject.version_id.is_(None)
        if version_id is None
        else DataLifecycleObject.version_id == version_id
    )
    row = (
        await session.execute(
            select(DataLifecycleObject).where(
                DataLifecycleObject.environment == scope.environment,
                DataLifecycleObject.namespace == scope.namespace,
                DataLifecycleObject.bucket == bucket,
                DataLifecycleObject.object_key == object_key,
                version_predicate,
            )
        )
    ).scalar_one()
    if (
        row.authority_id != authority_id
        or row.content_sha256 != content_sha256
        or row.size_bytes != size_bytes
        or row.created_at != created_at
        or row.state != "active"
    ):
        raise RuntimeError("existing lifecycle object conflicts with exact registration")
    return row.id


__all__ = [
    "RuntimeLifecycleScope",
    "bind_existing_batch_lifecycle_authority",
    "bind_existing_trial_lifecycle_authority",
    "ensure_artifact_lifecycle_authority",
    "ensure_batch_lifecycle_authority",
    "ensure_lifecycle_authority",
    "ensure_trial_event_lifecycle_authority",
    "ensure_trial_lifecycle_authority",
    "register_lifecycle_object",
]
