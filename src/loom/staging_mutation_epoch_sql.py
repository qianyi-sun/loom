"""Transactional PostgreSQL adapter for staging mutation epoch authority."""

from __future__ import annotations

from sqlalchemy import Connection, text

from loom.staging_mutation_epoch import (
    MutationEpochAdvance,
    MutationEpochState,
)


class SqlAlchemyMutationEpochStore:
    """Run the epoch CAS and append-only event in one database transaction."""

    def __init__(self, connection: Connection) -> None:
        if not connection.in_transaction():
            raise ValueError("mutation epoch store requires an active transaction")
        self._connection = connection

    def compare_and_swap(self, advance: MutationEpochAdvance) -> MutationEpochState:
        parameters = {
            "environment": advance.environment,
            "namespace": advance.namespace,
            "expected_epoch": advance.expected_epoch,
            "mutation_class": advance.mutation_class.value,
            "request_id": advance.request_id,
            "evidence_sha256": advance.evidence_sha256,
            "occurred_at": advance.occurred_at,
        }
        with self._connection.begin_nested():
            row = self._connection.execute(
                text(
                    "UPDATE staging_mutation_epochs "
                    "SET epoch = epoch + 1, reason = :mutation_class, "
                    "request_id = :request_id, evidence_sha256 = :evidence_sha256, "
                    "updated_at = :occurred_at "
                    "WHERE environment = :environment AND namespace = :namespace "
                    "AND epoch = :expected_epoch "
                    "RETURNING epoch"
                ),
                parameters,
            ).one_or_none()
            if row is None:
                raise RuntimeError("stale or unavailable staging mutation epoch")
            epoch = row[0]
            if type(epoch) is not int or epoch != advance.expected_epoch + 1:
                raise RuntimeError("mutation epoch update returned an invalid generation")
            self._connection.execute(
                text(
                    "INSERT INTO staging_mutation_epoch_events "
                    "(environment, namespace, epoch, mutation_class, request_id, "
                    "evidence_sha256, occurred_at) VALUES "
                    "(:environment, :namespace, :epoch, :mutation_class, :request_id, "
                    ":evidence_sha256, :occurred_at)"
                ),
                {**parameters, "epoch": epoch},
            )
        return MutationEpochState(
            environment=advance.environment,
            namespace=advance.namespace,
            epoch=epoch,
            mutation_class=advance.mutation_class,
            request_id=advance.request_id,
            evidence_sha256=advance.evidence_sha256,
            updated_at=advance.occurred_at,
        )


__all__ = ["SqlAlchemyMutationEpochStore"]
