"""PostgreSQL inventory and transactional apply for legacy lifecycle history."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import Connection, Engine, text

from loom.data_lifecycle import DataClass, OwnerKind
from loom.data_lifecycle_gc import GcScope
from loom.data_lifecycle_legacy import (
    LegacyAbsentObject,
    LegacyClassificationError,
    LegacyClassificationPlan,
    LegacyObject,
    LegacyRow,
    build_legacy_classification_plan,
)
from loom.staging_mutation_epoch import (
    MutationEpochAdvance,
    MutationEpochState,
    ProtectedMutationClass,
    advance_mutation_epoch,
)
from loom.staging_mutation_epoch_sql import SqlAlchemyMutationEpochStore


class LegacyObjectInspector(Protocol):
    """Read and hash one exact object without mutating its store."""

    def inspect(
        self,
        *,
        bucket: str,
        object_key: str,
        version_id: str | None,
    ) -> tuple[str | None, str, int] | None: ...


def _fingerprint(values: Iterable[object]) -> str:
    payload = json.dumps(
        list(values),
        sort_keys=True,
        separators=(",", ":"),
        default=lambda value: value.isoformat() if isinstance(value, datetime) else str(value),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _legacy_artifact_authority(
    artifact_type: str,
) -> tuple[DataClass, OwnerKind, bool]:
    """Classify retention from the durable data class, never a sharing hint."""
    if artifact_type == "benchmark":
        return DataClass.BENCHMARK, OwnerKind.BENCHMARK, True
    if artifact_type == "catalog":
        return DataClass.CATALOG, OwnerKind.SYSTEM, True
    if artifact_type in {"bootstrap", "system"}:
        return DataClass.SYSTEM, OwnerKind.SYSTEM, True
    return DataClass.ARTIFACT, OwnerKind.ARTIFACT, False


def _row(
    *,
    table: str,
    row_id: UUID,
    team_id: UUID,
    data_class: DataClass,
    owner_kind: OwnerKind,
    owner_id: str,
    created_at: datetime,
    source_values: Iterable[object],
    pinned: bool = False,
) -> LegacyRow:
    return LegacyRow(
        table=table,
        row_id=row_id,
        team_id=team_id,
        data_class=data_class,
        owner_kind=owner_kind,
        owner_id=owner_id,
        created_at=created_at,
        source_fingerprint=_fingerprint(source_values),
        pinned=pinned,
    )


def _load_rows(
    connection: Connection,
) -> tuple[list[LegacyRow], list[tuple[LegacyRow, Any]], list[str]]:
    rows: list[LegacyRow] = []
    artifacts: list[tuple[LegacyRow, Any]] = []
    blockers: list[str] = []
    for item in connection.execute(
        text(
            "SELECT id, team_id, created_at FROM batches "
            "WHERE lifecycle_authority_id IS NULL ORDER BY id"
        )
    ):
        rows.append(
            _row(
                table="batches",
                row_id=item.id,
                team_id=item.team_id,
                data_class=DataClass.RUN,
                owner_kind=OwnerKind.BATCH,
                owner_id=str(item.id),
                created_at=item.created_at,
                source_values=item,
            )
        )
    for item in connection.execute(
        text(
            "SELECT id, team_id, submitted_at FROM trials "
            "WHERE lifecycle_authority_id IS NULL ORDER BY id"
        )
    ):
        rows.append(
            _row(
                table="trials",
                row_id=item.id,
                team_id=item.team_id,
                data_class=DataClass.TRIAL,
                owner_kind=OwnerKind.TRIAL,
                owner_id=str(item.id),
                created_at=item.submitted_at,
                source_values=item,
            )
        )
    for item in connection.execute(
        text(
            "SELECT l.id, l.team_id, l.trial_id, t.team_id AS trial_team_id, "
            "t.submitted_at, l.captured_at FROM llm_calls l "
            "LEFT JOIN trials t ON t.id=l.trial_id "
            "WHERE l.lifecycle_authority_id IS NULL ORDER BY l.id"
        )
    ):
        if item.trial_team_id is None or item.team_id != item.trial_team_id:
            blockers.append(f"legacy llm_call {item.id} has missing or cross-team trial owner")
            continue
        rows.append(
            _row(
                table="llm_calls",
                row_id=item.id,
                team_id=item.team_id,
                data_class=DataClass.EVENT,
                owner_kind=OwnerKind.TRIAL,
                owner_id=str(item.trial_id),
                created_at=item.submitted_at,
                source_values=item,
            )
        )
    for item in connection.execute(
        text(
            "SELECT e.id, e.trial_id, t.team_id, t.submitted_at, e.created_at "
            "FROM trial_events e LEFT JOIN trials t ON t.id=e.trial_id "
            "WHERE e.lifecycle_authority_id IS NULL ORDER BY e.id"
        )
    ):
        if item.team_id is None:
            blockers.append(f"legacy trial_event {item.id} has no trial owner")
            continue
        rows.append(
            _row(
                table="trial_events",
                row_id=item.id,
                team_id=item.team_id,
                data_class=DataClass.EVENT,
                owner_kind=OwnerKind.TRIAL,
                owner_id=str(item.trial_id),
                created_at=item.submitted_at,
                source_values=item,
            )
        )
    for item in connection.execute(
        text(
            "SELECT a.id, a.team_id, a.batch_id, a.trial_id, a.created_at, "
            "a.content_hash, a.storage, a.retention, a.artifact_type, "
            "b.team_id AS batch_team_id, t.team_id AS trial_team_id "
            "FROM artifacts a LEFT JOIN batches b ON b.id=a.batch_id "
            "LEFT JOIN trials t ON t.id=a.trial_id "
            "WHERE a.lifecycle_authority_id IS NULL ORDER BY a.id"
        )
    ):
        data_class, owner_kind, pinned = _legacy_artifact_authority(item.artifact_type)
        if item.batch_id is not None and item.batch_team_id != item.team_id:
            blockers.append(f"legacy artifact {item.id} has missing or cross-team batch owner")
        if item.trial_id is not None and item.trial_team_id != item.team_id:
            blockers.append(f"legacy artifact {item.id} has missing or cross-team trial owner")
        row = _row(
            table="artifacts",
            row_id=item.id,
            team_id=item.team_id,
            data_class=data_class,
            owner_kind=owner_kind,
            owner_id=str(item.id),
            created_at=item.created_at,
            source_values=item,
            pinned=pinned,
        )
        rows.append(row)
        artifacts.append((row, item))
    return rows, artifacts, blockers


def _artifact_object(
    row: LegacyRow,
    source: Any,
    inspector: LegacyObjectInspector,
    *,
    bucket_aliases: Mapping[str, str] | None = None,
) -> LegacyObject | LegacyAbsentObject:
    storage = source.storage
    if not isinstance(storage, dict):
        raise LegacyClassificationError(f"legacy artifact {row.row_id} storage is unclassified")
    bucket = storage.get("bucket")
    object_key = storage.get("key")
    version_id = storage.get("version_id")
    if not isinstance(bucket, str) or not isinstance(object_key, str):
        raise LegacyClassificationError(
            f"legacy artifact {row.row_id} lacks exact bucket/key authority"
        )
    aliases = {} if bucket_aliases is None else dict(bucket_aliases)
    if any(
        not key or key != key.strip() or not value or value != value.strip()
        for key, value in aliases.items()
    ):
        raise LegacyClassificationError("legacy bucket alias authority is invalid")
    bucket = aliases.get(bucket, bucket)
    if version_id is not None and not isinstance(version_id, str):
        raise LegacyClassificationError(
            f"legacy artifact {row.row_id} has invalid version authority"
        )
    observation = inspector.inspect(
        bucket=bucket,
        object_key=object_key,
        version_id=version_id,
    )
    if observation is None:
        return LegacyAbsentObject(
            row_table="artifacts",
            row_id=row.row_id,
            bucket=bucket,
            object_key=object_key,
            version_id=version_id,
            created_at=row.created_at,
        )
    observed_version, observed_sha, observed_size = observation
    if version_id is not None and observed_version != version_id:
        raise LegacyClassificationError(f"legacy artifact {row.row_id} object version drifted")
    content_hash = source.content_hash
    if isinstance(content_hash, str) and content_hash.startswith("sha256:"):
        expected_sha = content_hash.removeprefix("sha256:")
        if expected_sha != observed_sha:
            raise LegacyClassificationError(f"legacy artifact {row.row_id} object digest drifted")
    elif content_hash != "pending:legacy-unhashed":
        raise LegacyClassificationError(
            f"legacy artifact {row.row_id} content hash is unclassified"
        )
    expected_size = storage.get("size_bytes")
    if expected_size is not None:
        if type(expected_size) is not int or expected_size < 0:
            raise LegacyClassificationError(f"legacy artifact {row.row_id} object size is invalid")
        # Migration 0047 deliberately used zero as the sentinel for a
        # legacy object whose exact size had never been recorded.  Only the
        # paired pending-hash sentinel may authorize replacing that unknown
        # value with the exact, single-GET observed identity.
        size_unknown = expected_size == 0 and content_hash == "pending:legacy-unhashed"
        if not size_unknown and expected_size != observed_size:
            raise LegacyClassificationError(f"legacy artifact {row.row_id} object size drifted")
    return LegacyObject(
        row_table="artifacts",
        row_id=row.row_id,
        bucket=bucket,
        object_key=object_key,
        version_id=observed_version,
        content_sha256=observed_sha,
        size_bytes=observed_size,
        created_at=row.created_at,
    )


class SqlAlchemyLegacyClassifier:
    """Inventory and apply exact legacy classification without prefix authority."""

    def __init__(
        self,
        engine: Engine,
        inspector: LegacyObjectInspector,
        *,
        bucket_aliases: Mapping[str, str] | None = None,
    ) -> None:
        self._engine = engine
        self._inspector = inspector
        self._bucket_aliases = {} if bucket_aliases is None else dict(bucket_aliases)
        if any(
            not key or key != key.strip() or not value or value != value.strip()
            for key, value in self._bucket_aliases.items()
        ):
            raise ValueError("legacy bucket alias authority is invalid")

    def inventory(
        self,
        *,
        scope: GcScope,
        planned_at: datetime,
    ) -> LegacyClassificationPlan:
        blockers: list[str] = []
        objects: list[LegacyObject] = []
        absent_objects: list[LegacyAbsentObject] = []
        with self._engine.connect().execution_options(
            isolation_level="REPEATABLE READ"
        ) as connection:
            with connection.begin():
                epoch = connection.execute(
                    text(
                        "SELECT epoch FROM staging_mutation_epochs "
                        "WHERE environment=:environment AND namespace=:namespace"
                    ),
                    {"environment": scope.environment, "namespace": scope.namespace},
                ).scalar_one_or_none()
                if epoch is None:
                    raise LegacyClassificationError(
                        "staging mutation epoch authority is unavailable"
                    )
                rows, artifacts, row_blockers = _load_rows(connection)
                blockers.extend(row_blockers)
        for row, source in artifacts:
            try:
                observation = _artifact_object(
                    row,
                    source,
                    self._inspector,
                    bucket_aliases=self._bucket_aliases,
                )
                if isinstance(observation, LegacyAbsentObject):
                    absent_objects.append(observation)
                else:
                    objects.append(observation)
            except LegacyClassificationError as exc:
                blockers.append(str(exc))
        return build_legacy_classification_plan(
            scope=scope,
            mutation_epoch=int(epoch),
            planned_at=planned_at,
            rows=rows,
            objects=objects,
            absent_objects=absent_objects,
            additional_blockers=blockers,
        )

    def apply(
        self,
        *,
        plan: LegacyClassificationPlan,
        approved_inventory_digest: str,
        request_id: str,
        applied_at: datetime,
    ) -> MutationEpochState:
        plan.require_applicable()
        if approved_inventory_digest != plan.inventory_digest:
            raise LegacyClassificationError("approved legacy inventory digest does not match")
        if applied_at.tzinfo is None or applied_at.utcoffset() is None:
            raise ValueError("legacy apply time must be timezone-aware")

        with self._engine.begin() as connection:
            epoch = connection.execute(
                text(
                    "SELECT epoch FROM staging_mutation_epochs "
                    "WHERE environment=:environment AND namespace=:namespace FOR UPDATE"
                ),
                {
                    "environment": plan.scope.environment,
                    "namespace": plan.scope.namespace,
                },
            ).scalar_one_or_none()
            if epoch != plan.mutation_epoch:
                raise LegacyClassificationError("legacy classification mutation epoch is stale")
            live_rows, _artifacts, live_blockers = _load_rows(connection)
            if live_blockers:
                raise LegacyClassificationError("legacy classification row inventory drifted")
            planned_rows = {(item.table, item.row_id): item for item in plan.rows}
            live_map = {(item.table, item.row_id): item for item in live_rows}
            if live_map != planned_rows:
                raise LegacyClassificationError("legacy classification row inventory drifted")

            absent_owner_ids = {str(item.row_id) for item in plan.absent_objects}
            for authority in plan.authorities:
                connection.execute(
                    text(
                        "INSERT INTO data_lifecycle_authorities "
                        "(id, environment, namespace, team_id, data_class, owner_kind, "
                        "owner_id, created_at, expires_at, pinned, state, metadata) VALUES "
                        "(:id,:environment,:namespace,:team_id,:data_class,:owner_kind,"
                        ":owner_id,:created_at,:expires_at,:pinned,'active',"
                        "CAST(:metadata AS jsonb)) ON CONFLICT DO NOTHING"
                    ),
                    {
                        "id": authority.id,
                        "environment": plan.scope.environment,
                        "namespace": plan.scope.namespace,
                        "team_id": authority.team_id,
                        "data_class": authority.data_class.value,
                        "owner_kind": authority.owner_kind.value,
                        "owner_id": authority.owner_id,
                        "created_at": authority.created_at,
                        "expires_at": authority.expires_at,
                        "pinned": authority.pinned,
                        "metadata": json.dumps(
                            {
                                "classification": "legacy-staging-v1",
                                "inventory_digest": plan.inventory_digest,
                                "object_state": (
                                    "verified_absent"
                                    if authority.owner_kind is OwnerKind.ARTIFACT
                                    and authority.owner_id in absent_owner_ids
                                    else "registered_or_not_applicable"
                                ),
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    },
                )
                existing = connection.execute(
                    text(
                        "SELECT environment, namespace, team_id, data_class, owner_kind, "
                        "owner_id, created_at, expires_at, pinned, state FROM "
                        "data_lifecycle_authorities WHERE id=:id"
                    ),
                    {"id": authority.id},
                ).one_or_none()
                expected_authority = (
                    plan.scope.environment,
                    plan.scope.namespace,
                    authority.team_id,
                    authority.data_class.value,
                    authority.owner_kind.value,
                    authority.owner_id,
                    authority.created_at,
                    authority.expires_at,
                    authority.pinned,
                    "active",
                )
                if existing is None or tuple(existing) != expected_authority:
                    raise LegacyClassificationError("legacy authority identity conflicts")

            authority_by_owner = {
                (item.data_class, item.owner_kind, item.owner_id): item.id
                for item in plan.authorities
            }
            for table in sorted({row.table for row in plan.rows}):
                table_rows = [row for row in plan.rows if row.table == table]
                for row in table_rows:
                    authority_id = authority_by_owner[
                        (row.data_class, row.owner_kind, row.owner_id)
                    ]
                    result = connection.execute(
                        text(
                            f"UPDATE {table} SET lifecycle_authority_id=:authority_id "
                            "WHERE id=:row_id AND lifecycle_authority_id IS NULL"
                        ),
                        {"authority_id": authority_id, "row_id": row.row_id},
                    )
                    if result.rowcount != 1:
                        raise LegacyClassificationError("legacy row binding raced")

            artifact_authority = {
                row.row_id: authority_by_owner[(row.data_class, row.owner_kind, row.owner_id)]
                for row in plan.rows
                if row.table == "artifacts"
            }
            for item in plan.objects:
                connection.execute(
                    text(
                        "INSERT INTO data_lifecycle_objects "
                        "(authority_id, environment, namespace, bucket, object_key, "
                        "version_id, content_sha256, size_bytes, created_at, state) VALUES "
                        "(:authority_id,:environment,:namespace,:bucket,:object_key,"
                        ":version_id,:content_sha256,:size_bytes,:created_at,'active') "
                        "ON CONFLICT DO NOTHING"
                    ),
                    {
                        "authority_id": artifact_authority[item.row_id],
                        "environment": plan.scope.environment,
                        "namespace": plan.scope.namespace,
                        "bucket": item.bucket,
                        "object_key": item.object_key,
                        "version_id": item.version_id,
                        "content_sha256": item.content_sha256,
                        "size_bytes": item.size_bytes,
                        "created_at": item.created_at,
                    },
                )
                version_clause = (
                    "version_id IS NULL" if item.version_id is None else "version_id=:version_id"
                )
                existing = connection.execute(
                    text(
                        "SELECT authority_id, content_sha256, size_bytes, created_at, state "
                        "FROM data_lifecycle_objects WHERE environment=:environment "
                        "AND namespace=:namespace AND bucket=:bucket AND object_key=:object_key "
                        f"AND {version_clause}"
                    ),
                    {
                        "environment": plan.scope.environment,
                        "namespace": plan.scope.namespace,
                        "bucket": item.bucket,
                        "object_key": item.object_key,
                        "version_id": item.version_id,
                    },
                ).one_or_none()
                expected_object = (
                    artifact_authority[item.row_id],
                    item.content_sha256,
                    item.size_bytes,
                    item.created_at,
                    "active",
                )
                if existing is None or tuple(existing) != expected_object:
                    raise LegacyClassificationError("legacy object registry identity conflicts")

            return advance_mutation_epoch(
                SqlAlchemyMutationEpochStore(connection),
                MutationEpochAdvance(
                    environment=plan.scope.environment,
                    namespace=plan.scope.namespace,
                    expected_epoch=plan.mutation_epoch,
                    mutation_class=ProtectedMutationClass.OBJECT_REWRITE,
                    request_id=request_id,
                    evidence_sha256=plan.inventory_digest,
                    occurred_at=applied_at,
                ),
            )


__all__ = ["LegacyObjectInspector", "SqlAlchemyLegacyClassifier"]
