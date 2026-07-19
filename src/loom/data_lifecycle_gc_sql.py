"""Transactional PostgreSQL journal for exact staging lifecycle GC."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Protocol
from uuid import UUID, uuid4

from sqlalchemy import Engine, bindparam, text
from sqlalchemy.engine import Connection

from loom.data_lifecycle_gc import GcPlan
from loom.staging_mutation_epoch import (
    MutationEpochAdvance,
    MutationEpochState,
    advance_mutation_epoch,
)
from loom.staging_mutation_epoch_sql import SqlAlchemyMutationEpochStore


class BusinessMetadataPurger(Protocol):
    """Delete only business rows bound to the exact lifecycle authorities."""

    def delete_exact(self, connection: Connection, authority_ids: Sequence[UUID]) -> None: ...


class ExecutionMetadataPurger:
    """Fail-closed deletion order for execution-history tables only."""

    _TABLES = ("trial_events", "llm_calls", "artifacts", "trials", "batches")

    def delete_exact(self, connection: Connection, authority_ids: Sequence[UUID]) -> None:
        parameters = {"authority_ids": list(authority_ids)}
        for table in self._TABLES:
            connection.execute(
                text(
                    f"DELETE FROM {table} WHERE lifecycle_authority_id IN :authority_ids"
                ).bindparams(bindparam("authority_ids", expanding=True)),
                parameters,
            )


def _plan_inventory(plan: GcPlan) -> dict[str, object]:
    return {
        "authority_ids": [str(value) for value in plan.authority_ids],
        "blockers": list(plan.blockers),
        "bytes_total": plan.bytes_total,
        "inventory_digest": plan.inventory_digest,
        "mutation_epoch": plan.mutation_epoch,
        "object_count": plan.object_count,
        "objects": [
            {
                "bucket": item.bucket,
                "content_sha256": item.content_sha256,
                "id": str(item.id),
                "object_key": item.object_key,
                "size_bytes": item.size_bytes,
                "version_id": item.version_id,
            }
            for item in plan.objects
        ],
        "planned_at": plan.planned_at.isoformat(),
    }


class SqlAlchemyGcJournal:
    """Persist each external side-effect boundary as a committed transition."""

    def __init__(
        self,
        engine: Engine,
        *,
        metadata_purger: BusinessMetadataPurger | None = None,
    ) -> None:
        self._engine = engine
        self._metadata_purger = metadata_purger or ExecutionMetadataPurger()

    def record_dry_run(self, *, plan: GcPlan, requested_by: str) -> UUID:
        run_id = uuid4()
        with self._engine.begin() as connection:
            self._insert_run(
                connection,
                run_id=run_id,
                plan=plan,
                requested_by=requested_by,
                dry_run=True,
                state="completed",
            )
        return run_id

    def begin_apply(
        self,
        *,
        plan: GcPlan,
        requested_by: str,
        deletion_token: UUID,
    ) -> UUID:
        run_id = uuid4()
        authority_ids = list(plan.authority_ids)
        object_ids = [item.id for item in plan.objects]
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
                raise RuntimeError("GC plan mutation epoch is stale")
            self._insert_run(
                connection,
                run_id=run_id,
                plan=plan,
                requested_by=requested_by,
                dry_run=False,
                state="applying",
            )
            authority_result = connection.execute(
                text(
                    "UPDATE data_lifecycle_authorities "
                    "SET state='deleting', deletion_token=:token "
                    "WHERE id IN :ids AND environment=:environment "
                    "AND namespace=:namespace AND state='active' AND NOT pinned"
                ).bindparams(bindparam("ids", expanding=True)),
                {
                    "ids": authority_ids,
                    "token": deletion_token,
                    "environment": plan.scope.environment,
                    "namespace": plan.scope.namespace,
                },
            )
            if authority_result.rowcount != len(authority_ids):
                raise RuntimeError("GC authority mark count does not match plan")
            object_result = connection.execute(
                text(
                    "UPDATE data_lifecycle_objects "
                    "SET state='delete_pending', deletion_token=:token "
                    "WHERE id IN :ids AND state='active'"
                ).bindparams(bindparam("ids", expanding=True)),
                {"ids": object_ids, "token": deletion_token},
            )
            if object_result.rowcount != len(object_ids):
                raise RuntimeError("GC object mark count does not match plan")
            connection.execute(
                text(
                    "INSERT INTO data_lifecycle_gc_items "
                    "(gc_run_id, object_id, deletion_token, state) "
                    "VALUES (:run_id, :object_id, :token, 'marked')"
                ),
                [
                    {
                        "run_id": run_id,
                        "object_id": object_id,
                        "token": deletion_token,
                    }
                    for object_id in object_ids
                ],
            )
        return run_id

    def record_object_deleted(
        self,
        *,
        run_id: UUID,
        object_id: UUID,
        deletion_token: UUID,
    ) -> None:
        self._transition_item(
            run_id=run_id,
            object_id=object_id,
            deletion_token=deletion_token,
            expected="marked",
            target="object_deleted",
        )

    def record_object_verified(
        self,
        *,
        run_id: UUID,
        object_id: UUID,
        deletion_token: UUID,
    ) -> None:
        with self._engine.begin() as connection:
            item_result = connection.execute(
                text(
                    "UPDATE data_lifecycle_gc_items SET state='verified', updated_at=now() "
                    "WHERE gc_run_id=:run_id AND object_id=:object_id "
                    "AND deletion_token=:token AND state='object_deleted'"
                ),
                {"run_id": run_id, "object_id": object_id, "token": deletion_token},
            )
            object_result = connection.execute(
                text(
                    "UPDATE data_lifecycle_objects "
                    "SET state='deleted', verified_deleted_at=now() "
                    "WHERE id=:object_id AND deletion_token=:token "
                    "AND state='delete_pending'"
                ),
                {"object_id": object_id, "token": deletion_token},
            )
            if item_result.rowcount != 1 or object_result.rowcount != 1:
                raise RuntimeError("GC object verification transition is stale")

    def delete_business_metadata(
        self,
        *,
        run_id: UUID,
        authority_ids: Sequence[UUID],
        deletion_token: UUID,
    ) -> None:
        ids = list(authority_ids)
        with self._engine.begin() as connection:
            unverified = connection.execute(
                text(
                    "SELECT count(*) FROM data_lifecycle_gc_items "
                    "WHERE gc_run_id=:run_id AND deletion_token=:token "
                    "AND state <> 'verified'"
                ),
                {"run_id": run_id, "token": deletion_token},
            ).scalar_one()
            if unverified != 0:
                raise RuntimeError("GC metadata deletion requires all objects verified absent")
            self._metadata_purger.delete_exact(connection, authority_ids)
            connection.execute(
                text(
                    "DELETE FROM data_lifecycle_objects "
                    "WHERE authority_id IN :ids AND deletion_token=:token AND state='deleted'"
                ).bindparams(bindparam("ids", expanding=True)),
                {"ids": ids, "token": deletion_token},
            )
            authority_result = connection.execute(
                text(
                    "DELETE FROM data_lifecycle_authorities "
                    "WHERE id IN :ids AND deletion_token=:token AND state='deleting'"
                ).bindparams(bindparam("ids", expanding=True)),
                {"ids": ids, "token": deletion_token},
            )
            if authority_result.rowcount != len(ids):
                raise RuntimeError("GC metadata authority delete count does not match plan")
            connection.execute(
                text(
                    "UPDATE data_lifecycle_gc_items "
                    "SET state='metadata_deleted', updated_at=now() "
                    "WHERE gc_run_id=:run_id AND deletion_token=:token AND state='verified'"
                ),
                {"run_id": run_id, "token": deletion_token},
            )

    def complete_apply(
        self,
        *,
        run_id: UUID,
        mutation: MutationEpochAdvance,
        deletion_token: UUID,
    ) -> MutationEpochState:
        with self._engine.begin() as connection:
            remaining = connection.execute(
                text(
                    "SELECT count(*) FROM data_lifecycle_gc_items "
                    "WHERE gc_run_id=:run_id AND deletion_token=:token "
                    "AND state <> 'metadata_deleted'"
                ),
                {"run_id": run_id, "token": deletion_token},
            ).scalar_one()
            if remaining != 0:
                raise RuntimeError("GC run cannot complete with unfinished items")
            state = advance_mutation_epoch(
                SqlAlchemyMutationEpochStore(connection),
                mutation,
            )
            result = connection.execute(
                text(
                    "UPDATE data_lifecycle_gc_runs "
                    "SET state='completed', mutation_epoch_after=:epoch, finished_at=now() "
                    "WHERE id=:run_id AND state='applying' "
                    "AND mutation_epoch_before=:expected_epoch"
                ),
                {
                    "epoch": state.epoch,
                    "run_id": run_id,
                    "expected_epoch": mutation.expected_epoch,
                },
            )
            if result.rowcount != 1:
                raise RuntimeError("GC completion journal transition is stale")
        return state

    def fail_apply(self, *, run_id: UUID, reason: str) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE data_lifecycle_gc_runs "
                    "SET state='failed', failure_reason=:reason, finished_at=now() "
                    "WHERE id=:run_id AND state <> 'completed'"
                ),
                {"run_id": run_id, "reason": reason[:500]},
            )

    def _insert_run(
        self,
        connection: Connection,
        *,
        run_id: UUID,
        plan: GcPlan,
        requested_by: str,
        dry_run: bool,
        state: str,
    ) -> None:
        connection.execute(
            text(
                "INSERT INTO data_lifecycle_gc_runs "
                "(id, environment, namespace, mutation_epoch_before, mutation_epoch_after, "
                "state, dry_run, requested_by, policy, inventory, finished_at) VALUES "
                "(:id, :environment, :namespace, :epoch, NULL, :state, :dry_run, "
                ":requested_by, CAST(:policy AS jsonb), CAST(:inventory AS jsonb), "
                "CASE WHEN :dry_run THEN now() ELSE NULL END)"
            ),
            {
                "id": run_id,
                "environment": plan.scope.environment,
                "namespace": plan.scope.namespace,
                "epoch": plan.mutation_epoch,
                "state": state,
                "dry_run": dry_run,
                "requested_by": requested_by,
                "policy": '{"protocol":"two-phase-exact-v1"}',
                "inventory": json.dumps(
                    _plan_inventory(plan),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
        )

    def _transition_item(
        self,
        *,
        run_id: UUID,
        object_id: UUID,
        deletion_token: UUID,
        expected: str,
        target: str,
    ) -> None:
        with self._engine.begin() as connection:
            result = connection.execute(
                text(
                    "UPDATE data_lifecycle_gc_items "
                    "SET state=:target, updated_at=now() "
                    "WHERE gc_run_id=:run_id AND object_id=:object_id "
                    "AND deletion_token=:token AND state=:expected"
                ),
                {
                    "target": target,
                    "run_id": run_id,
                    "object_id": object_id,
                    "token": deletion_token,
                    "expected": expected,
                },
            )
            if result.rowcount != 1:
                raise RuntimeError("GC item transition is stale")


__all__ = ["ExecutionMetadataPurger", "SqlAlchemyGcJournal"]
