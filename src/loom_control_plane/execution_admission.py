"""Persisted, race-safe hybrid execution concurrency admission (#1552)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import ExecutionAdmissionPolicy

AdmissionScopeKind = Literal[
    "global",
    "environment",
    "region",
    "team",
    "batch",
    "execution_class",
    "pool",
]

_SCOPE_KINDS = frozenset(
    {"global", "environment", "region", "team", "batch", "execution_class", "pool"}
)

_BLOCKER_SQL = text(
    """
    SELECT * FROM loom_execution_admission_blocker(
      (:team_id)::uuid, (:batch_id)::uuid, :environment, :region,
      :execution_class_id, :pool_id
    )
    """
)


@dataclass(frozen=True)
class ExecutionAdmissionIdentity:
    trial_id: UUID
    attempt: int
    execution_role: Literal["attempt", "verifier"]
    team_id: UUID
    batch_id: UUID | None
    environment: str | None
    region: str | None
    execution_class_id: str | None
    pool_id: str
    owner_kind: Literal["legacy_worker_claim", "service_execution_lease"]
    owner_id: UUID


@dataclass(frozen=True)
class ExecutionAdmissionBlockedError(Exception):
    scope_kind: str
    scope_key: str
    max_concurrent: int
    active_count: int

    @property
    def reason(self) -> str:
        return f"execution_admission_{self.scope_kind}_ceiling_reached"


def _clean_scope(scope_kind: str, scope_key: str) -> tuple[str, str]:
    kind = str(scope_kind).strip()
    key = str(scope_key).strip()
    if kind not in _SCOPE_KINDS:
        raise ValueError(
            "scope_kind must be global, environment, region, team, batch, execution_class, or pool"
        )
    if not key or len(key) > 120:
        raise ValueError("scope_key must contain 1 to 120 characters")
    if kind == "global" and key != "*":
        raise ValueError("global admission policy scope_key must be '*'")
    if kind in {"team", "batch"}:
        try:
            UUID(key)
        except ValueError as exc:
            raise ValueError(f"{kind} admission policy scope_key must be a UUID") from exc
    return kind, key


async def upsert_execution_admission_policy(
    session: AsyncSession,
    *,
    scope_kind: str,
    scope_key: str,
    max_concurrent: int,
    enabled: bool,
    reason: str | None,
    now: datetime | None = None,
) -> ExecutionAdmissionPolicy:
    kind, key = _clean_scope(scope_kind, scope_key)
    if max_concurrent <= 0:
        raise ValueError("max_concurrent must be > 0")
    clean_reason = reason.strip() if isinstance(reason, str) else None
    if clean_reason == "":
        clean_reason = None
    if clean_reason is not None and len(clean_reason) > 500:
        raise ValueError("reason must contain at most 500 characters")
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('execution-admission-policy-mutation', 1552))"
        ),
    )
    row = (
        await session.execute(
            select(ExecutionAdmissionPolicy)
            .where(
                ExecutionAdmissionPolicy.scope_kind == kind,
                ExecutionAdmissionPolicy.scope_key == key,
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    ledger_active_count = int(
        (
            await session.execute(
                text(
                    """
                    SELECT count(*)
                      FROM execution_admission_reservations reservation
                     WHERE reservation.state = 'active'
                       AND CASE :scope_kind
                             WHEN 'global' THEN :scope_key = '*'
                             WHEN 'environment' THEN reservation.environment = :scope_key
                             WHEN 'region' THEN reservation.region = :scope_key
                             WHEN 'team' THEN reservation.team_id::text = :scope_key
                             WHEN 'batch' THEN reservation.batch_id::text = :scope_key
                             WHEN 'execution_class' THEN
                               reservation.execution_class_id = :scope_key
                             WHEN 'pool' THEN reservation.pool_id = :scope_key
                             ELSE false
                           END
                    """
                ),
                {"scope_kind": kind, "scope_key": key},
            )
        ).scalar_one()
    )
    if enabled and ledger_active_count > max_concurrent:
        raise ValueError(
            "max_concurrent cannot be enabled below the current active reservation count",
        )
    current_time = now or datetime.now(UTC)
    if row is None:
        row = ExecutionAdmissionPolicy(
            scope_kind=kind,
            scope_key=key,
            max_concurrent=max_concurrent,
            active_count=ledger_active_count,
            counter_updated_at=current_time if ledger_active_count else None,
            enabled=enabled,
            reason=clean_reason,
            updated_at=current_time,
        )
        session.add(row)
    else:
        row.max_concurrent = max_concurrent
        row.active_count = ledger_active_count
        row.counter_updated_at = current_time
        row.enabled = enabled
        row.reason = clean_reason
        row.version += 1
        row.updated_at = current_time
    await session.flush()
    return row


def execution_admission_policy_to_dict(
    row: ExecutionAdmissionPolicy,
    *,
    ledger_active_count: int,
) -> dict[str, object]:
    return {
        "id": str(row.id),
        "scope_kind": row.scope_kind,
        "scope_key": row.scope_key,
        "max_concurrent": row.max_concurrent,
        "active_count": row.active_count,
        "ledger_active_count": ledger_active_count,
        "counter_in_sync": row.active_count == ledger_active_count,
        "counter_updated_at": (
            row.counter_updated_at.isoformat() if row.counter_updated_at else None
        ),
        "available": max(0, row.max_concurrent - row.active_count) if row.enabled else None,
        "enabled": row.enabled,
        "reason": row.reason,
        "version": row.version,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


async def fetch_execution_admission_status(
    session: AsyncSession,
) -> dict[str, list[dict[str, object]]]:
    rows = (
        (
            await session.execute(
                select(ExecutionAdmissionPolicy).order_by(
                    ExecutionAdmissionPolicy.scope_kind,
                    ExecutionAdmissionPolicy.scope_key,
                )
            )
        )
        .scalars()
        .all()
    )
    counts = {
        (str(kind), str(key)): int(count)
        for kind, key, count in (
            await session.execute(
                text(
                    """
                    SELECT policy.scope_kind, policy.scope_key, count(reservation.id)
                      FROM execution_admission_policies policy
                      LEFT JOIN execution_admission_reservations reservation
                        ON reservation.state = 'active'
                       AND CASE policy.scope_kind
                             WHEN 'global' THEN true
                             WHEN 'environment' THEN reservation.environment=policy.scope_key
                             WHEN 'region' THEN reservation.region=policy.scope_key
                             WHEN 'team' THEN reservation.team_id::text=policy.scope_key
                             WHEN 'batch' THEN reservation.batch_id::text=policy.scope_key
                             WHEN 'execution_class' THEN
                               reservation.execution_class_id=policy.scope_key
                             WHEN 'pool' THEN reservation.pool_id=policy.scope_key
                             ELSE false
                           END
                     GROUP BY policy.scope_kind, policy.scope_key
                    """
                )
            )
        ).all()
    }
    return {
        "policies": [
            execution_admission_policy_to_dict(
                row,
                ledger_active_count=counts.get((row.scope_kind, row.scope_key), 0),
            )
            for row in rows
        ]
    }


async def reserve_execution_admission(
    session: AsyncSession,
    identity: ExecutionAdmissionIdentity,
    *,
    now: datetime | None = None,
) -> UUID:
    if identity.attempt <= 0:
        raise ValueError("admission attempt must be > 0")
    if not identity.pool_id.strip():
        raise ValueError("admission pool_id must be non-empty")
    params = {
        "trial_id": identity.trial_id,
        "attempt": identity.attempt,
        "execution_role": identity.execution_role,
        "team_id": identity.team_id,
        "batch_id": identity.batch_id,
        "environment": identity.environment,
        "region": identity.region,
        "execution_class_id": identity.execution_class_id,
        "pool_id": identity.pool_id,
        "owner_kind": identity.owner_kind,
        "owner_id": identity.owner_id,
        "acquired_at": now or datetime.now(UTC),
    }
    blocker = (await session.execute(_BLOCKER_SQL, params)).mappings().one_or_none()
    if blocker is not None:
        raise ExecutionAdmissionBlockedError(
            scope_kind=str(blocker["scope_kind"]),
            scope_key=str(blocker["scope_key"]),
            max_concurrent=int(blocker["max_concurrent"]),
            active_count=int(blocker["active_count"]),
        )
    reservation_id = (
        await session.execute(
            text(
                """
                SELECT loom_execution_admission_reserve(
                  (:trial_id)::uuid, :attempt, :execution_role,
                  (:team_id)::uuid, (:batch_id)::uuid, :environment, :region,
                  :execution_class_id, :pool_id, :owner_kind,
                  (:owner_id)::uuid, :acquired_at
                )
                """
            ),
            params,
        )
    ).scalar_one_or_none()
    if reservation_id is None:
        blocker = (await session.execute(_BLOCKER_SQL, params)).mappings().one_or_none()
        if blocker is not None:
            raise ExecutionAdmissionBlockedError(
                scope_kind=str(blocker["scope_kind"]),
                scope_key=str(blocker["scope_key"]),
                max_concurrent=int(blocker["max_concurrent"]),
                active_count=int(blocker["active_count"]),
            )
        raise ExecutionAdmissionBlockedError(
            scope_kind="reservation",
            scope_key=identity.pool_id,
            max_concurrent=0,
            active_count=0,
        )
    return UUID(str(reservation_id))


__all__ = [
    "ExecutionAdmissionBlockedError",
    "ExecutionAdmissionIdentity",
    "fetch_execution_admission_status",
    "reserve_execution_admission",
    "upsert_execution_admission_policy",
]
