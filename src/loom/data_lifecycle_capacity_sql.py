"""PostgreSQL persistence and runtime checks for staging capacity authority."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Engine, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.data_lifecycle import StagingCapacity, staging_capacity_policy_digest
from loom.data_lifecycle_capacity import (
    CAPACITY_SOURCE,
    StagingAdmissionError,
    StagingCapacityEvidence,
)
from loom.db.schema import StagingLifecycleCapacity


class SqlAlchemyStagingCapacityStore:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def publish(self, evidence: StagingCapacityEvidence) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO staging_lifecycle_capacity "
                    "(environment,namespace,object_count,bytes_used,disk_free_percent,"
                    "inode_free_percent,policy_sha256,evidence_sha256,source,observed_at) "
                    "VALUES ('staging',:namespace,:object_count,:bytes_used,"
                    ":disk_free_percent,:inode_free_percent,:policy_sha256,"
                    ":evidence_sha256,:source,:observed_at) "
                    "ON CONFLICT (environment) DO UPDATE SET "
                    "namespace=EXCLUDED.namespace, object_count=EXCLUDED.object_count, "
                    "bytes_used=EXCLUDED.bytes_used, "
                    "disk_free_percent=EXCLUDED.disk_free_percent, "
                    "inode_free_percent=EXCLUDED.inode_free_percent, "
                    "policy_sha256=EXCLUDED.policy_sha256, "
                    "evidence_sha256=EXCLUDED.evidence_sha256, source=EXCLUDED.source, "
                    "observed_at=EXCLUDED.observed_at "
                    "WHERE staging_lifecycle_capacity.observed_at < EXCLUDED.observed_at"
                ),
                {
                    "namespace": evidence.namespace,
                    "object_count": evidence.capacity.object_count,
                    "bytes_used": evidence.capacity.bytes_used,
                    "disk_free_percent": evidence.capacity.disk_free_percent,
                    "inode_free_percent": evidence.capacity.inode_free_percent,
                    "policy_sha256": evidence.policy_sha256,
                    "evidence_sha256": evidence.evidence_sha256,
                    "source": evidence.source,
                    "observed_at": evidence.observed_at,
                },
            )
            published = connection.execute(
                text(
                    "SELECT namespace,object_count,bytes_used,disk_free_percent,"
                    "inode_free_percent,policy_sha256,evidence_sha256,source,observed_at "
                    "FROM staging_lifecycle_capacity WHERE environment='staging'"
                )
            ).one_or_none()
            expected = (
                evidence.namespace,
                evidence.capacity.object_count,
                evidence.capacity.bytes_used,
                evidence.capacity.disk_free_percent,
                evidence.capacity.inode_free_percent,
                evidence.policy_sha256,
                evidence.evidence_sha256,
                evidence.source,
                evidence.observed_at,
            )
            if published is None or tuple(published) != expected:
                raise RuntimeError("staging capacity publication lost freshness authority")


async def require_staging_capacity_admission(
    session: AsyncSession,
    *,
    namespace: str,
    now: datetime,
) -> None:
    row = await session.scalar(
        select(StagingLifecycleCapacity).where(
            StagingLifecycleCapacity.environment == "staging",
            StagingLifecycleCapacity.namespace == namespace,
        )
    )
    if row is None:
        raise StagingAdmissionError("staging_capacity_evidence_missing")
    if row.policy_sha256 != staging_capacity_policy_digest() or row.source != CAPACITY_SOURCE:
        raise StagingAdmissionError("staging_capacity_policy_drift")
    try:
        evidence = StagingCapacityEvidence(
            namespace=row.namespace,
            capacity=StagingCapacity(
                object_count=row.object_count,
                bytes_used=row.bytes_used,
                disk_free_percent=row.disk_free_percent,
                inode_free_percent=row.inode_free_percent,
            ),
            policy_sha256=row.policy_sha256,
            evidence_sha256=row.evidence_sha256,
            observed_at=row.observed_at,
            source=row.source,
        )
    except ValueError as exc:
        raise StagingAdmissionError("staging_capacity_evidence_corrupt") from exc
    evidence.require_fresh_admission(now=now)


__all__ = ["SqlAlchemyStagingCapacityStore", "require_staging_capacity_admission"]
