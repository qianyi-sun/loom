"""Transactional lifecycle-authority registration for execution writers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.data_lifecycle import (
    STAGING_EPHEMERAL_TTL,
    DataClass,
    LifecycleAuthoritySpec,
    OwnerKind,
)
from loom.db.schema import DataLifecycleAuthority

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
) -> UUID:
    scope = RuntimeLifecycleScope.from_environ()
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


__all__ = [
    "RuntimeLifecycleScope",
    "ensure_batch_lifecycle_authority",
    "ensure_lifecycle_authority",
    "ensure_trial_lifecycle_authority",
]
