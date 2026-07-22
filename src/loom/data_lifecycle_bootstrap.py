"""Digest-approved initialization of staging lifecycle mutation authority."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

from sqlalchemy import Connection, Engine, text

from loom.data_lifecycle_gc import GcScope

_REVISION_RE = re.compile(r"^(?P<number>[0-9]{4})(?:_[a-z0-9_]+)?$")
_EXECUTION_TABLES = ("batches", "trials", "llm_calls", "trial_events", "artifacts")


class LifecycleBootstrapError(RuntimeError):
    """The first staging mutation-epoch row cannot be created safely."""


@dataclass(frozen=True, slots=True)
class LifecycleBootstrapPlan:
    scope: GcScope
    schema_revision: str
    epoch_rows: tuple[tuple[str, str, int, str, str | None, str | None], ...]
    epoch_event_count: int
    authority_count: int
    object_count: int
    gc_run_count: int
    classified_row_counts: tuple[tuple[str, int], ...]
    blockers: tuple[str, ...]
    inventory_digest: str

    @property
    def converged(self) -> bool:
        return (
            self.epoch_rows == (("staging", self.scope.namespace, 0, "bootstrap", None, None),)
            and not self.blockers
        )

    @property
    def applicable(self) -> bool:
        return not self.epoch_rows and not self.blockers

    def require_applicable_or_converged(self) -> None:
        if self.blockers:
            raise LifecycleBootstrapError("; ".join(self.blockers))
        if not self.applicable and not self.converged:
            raise LifecycleBootstrapError("staging lifecycle bootstrap authority is ambiguous")


def _payload(
    *,
    scope: GcScope,
    schema_revision: str,
    epoch_rows: tuple[tuple[str, str, int, str, str | None, str | None], ...],
    epoch_event_count: int,
    authority_count: int,
    object_count: int,
    gc_run_count: int,
    classified_row_counts: tuple[tuple[str, int], ...],
    blockers: tuple[str, ...],
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "environment": scope.environment,
        "namespace": scope.namespace,
        "schema_revision": schema_revision,
        "epoch_rows": [
            {
                "environment": row[0],
                "namespace": row[1],
                "epoch": row[2],
                "reason": row[3],
                "request_id": row[4],
                "evidence_sha256": row[5],
            }
            for row in epoch_rows
        ],
        "epoch_event_count": epoch_event_count,
        "authority_count": authority_count,
        "object_count": object_count,
        "gc_run_count": gc_run_count,
        "classified_row_counts": dict(classified_row_counts),
        "blockers": list(blockers),
    }


def build_lifecycle_bootstrap_plan(
    *,
    scope: GcScope,
    schema_revision: str,
    epoch_rows: tuple[tuple[str, str, int, str, str | None, str | None], ...] = (),
    epoch_event_count: int = 0,
    authority_count: int = 0,
    object_count: int = 0,
    gc_run_count: int = 0,
    classified_row_counts: tuple[tuple[str, int], ...] = (),
    additional_blockers: tuple[str, ...] = (),
) -> LifecycleBootstrapPlan:
    blockers = [value for value in additional_blockers if value]
    revision = _REVISION_RE.fullmatch(schema_revision)
    if revision is None or int(revision.group("number")) < 69:
        blockers.append("lifecycle bootstrap requires schema revision 0069 or later")
    if (scope.environment, scope.namespace) != ("staging", "loom-staging"):
        blockers.append("lifecycle bootstrap is fixed to staging/loom-staging")
    if any(value < 0 for value in (epoch_event_count, authority_count, object_count, gc_run_count)):
        raise ValueError("lifecycle bootstrap counts must be non-negative")
    if any(count < 0 or table not in _EXECUTION_TABLES for table, count in classified_row_counts):
        raise ValueError("lifecycle bootstrap classified-row evidence is invalid")
    if epoch_event_count:
        blockers.append("mutation epoch events already exist")
    if authority_count or object_count or gc_run_count:
        blockers.append("lifecycle registry or GC journal is not empty")
    if any(count for _table, count in classified_row_counts):
        blockers.append("execution rows already carry lifecycle authority")

    stable_epoch_rows = tuple(sorted(epoch_rows))
    expected = (("staging", scope.namespace, 0, "bootstrap", None, None),)
    if stable_epoch_rows and stable_epoch_rows != expected:
        blockers.append("existing mutation epoch authority is not the exact bootstrap row")
    stable_counts = tuple(sorted(classified_row_counts))
    stable_blockers = tuple(sorted(set(blockers)))
    payload = _payload(
        scope=scope,
        schema_revision=schema_revision,
        epoch_rows=stable_epoch_rows,
        epoch_event_count=epoch_event_count,
        authority_count=authority_count,
        object_count=object_count,
        gc_run_count=gc_run_count,
        classified_row_counts=stable_counts,
        blockers=stable_blockers,
    )
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return LifecycleBootstrapPlan(
        scope=scope,
        schema_revision=schema_revision,
        epoch_rows=stable_epoch_rows,
        epoch_event_count=epoch_event_count,
        authority_count=authority_count,
        object_count=object_count,
        gc_run_count=gc_run_count,
        classified_row_counts=stable_counts,
        blockers=stable_blockers,
        inventory_digest=digest,
    )


def lifecycle_bootstrap_plan_document(plan: LifecycleBootstrapPlan) -> dict[str, object]:
    document = _payload(
        scope=plan.scope,
        schema_revision=plan.schema_revision,
        epoch_rows=plan.epoch_rows,
        epoch_event_count=plan.epoch_event_count,
        authority_count=plan.authority_count,
        object_count=plan.object_count,
        gc_run_count=plan.gc_run_count,
        classified_row_counts=plan.classified_row_counts,
        blockers=plan.blockers,
    )
    return {
        **document,
        "inventory_digest": plan.inventory_digest,
        "applicable": plan.applicable,
        "converged": plan.converged,
    }


def _scalar_count(connection: Connection, table: str) -> int:
    return int(connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one())


def _inventory_connection(connection: Connection, *, scope: GcScope) -> LifecycleBootstrapPlan:
    revision_rows = tuple(
        str(value)
        for value in connection.execute(text("SELECT version_num FROM alembic_version")).scalars()
    )
    if len(revision_rows) != 1:
        raise LifecycleBootstrapError("database schema revision authority is ambiguous")
    epoch_rows = tuple(
        (
            str(row.environment),
            str(row.namespace),
            int(row.epoch),
            str(row.reason),
            str(row.request_id) if row.request_id is not None else None,
            str(row.evidence_sha256) if row.evidence_sha256 is not None else None,
        )
        for row in connection.execute(
            text(
                "SELECT environment,namespace,epoch,reason,request_id,evidence_sha256 "
                "FROM staging_mutation_epochs ORDER BY environment,namespace"
            )
        )
    )
    classified = tuple(
        (
            table,
            int(
                connection.execute(
                    text(f"SELECT count(*) FROM {table} WHERE lifecycle_authority_id IS NOT NULL")
                ).scalar_one()
            ),
        )
        for table in _EXECUTION_TABLES
    )
    return build_lifecycle_bootstrap_plan(
        scope=scope,
        schema_revision=revision_rows[0],
        epoch_rows=epoch_rows,
        epoch_event_count=_scalar_count(connection, "staging_mutation_epoch_events"),
        authority_count=_scalar_count(connection, "data_lifecycle_authorities"),
        object_count=_scalar_count(connection, "data_lifecycle_objects"),
        gc_run_count=_scalar_count(connection, "data_lifecycle_gc_runs"),
        classified_row_counts=classified,
    )


class SqlAlchemyLifecycleBootstrap:
    """Inventory and initialize only one exact empty staging epoch authority."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def inventory(self, *, scope: GcScope) -> LifecycleBootstrapPlan:
        with self._engine.connect().execution_options(
            isolation_level="REPEATABLE READ"
        ) as connection:
            with connection.begin():
                return _inventory_connection(connection, scope=scope)

    def apply(
        self,
        *,
        plan: LifecycleBootstrapPlan,
        approved_inventory_digest: str,
    ) -> LifecycleBootstrapPlan:
        plan.require_applicable_or_converged()
        if approved_inventory_digest != plan.inventory_digest:
            raise LifecycleBootstrapError("approved bootstrap inventory digest does not match")
        if plan.converged:
            return plan
        with self._engine.begin() as connection:
            connection.execute(
                text(
                    "LOCK TABLE staging_mutation_epochs, staging_mutation_epoch_events, "
                    "data_lifecycle_authorities, data_lifecycle_objects, data_lifecycle_gc_runs "
                    "IN SHARE ROW EXCLUSIVE MODE"
                )
            )
            live = _inventory_connection(connection, scope=plan.scope)
            if live.inventory_digest != plan.inventory_digest or not live.applicable:
                raise LifecycleBootstrapError("lifecycle bootstrap inventory drifted")
            result = connection.execute(
                text(
                    "INSERT INTO staging_mutation_epochs "
                    "(environment,namespace,epoch,reason) "
                    "VALUES ('staging',:namespace,0,'bootstrap') "
                    "ON CONFLICT (environment) DO NOTHING"
                ),
                {"namespace": plan.scope.namespace},
            )
            if result.rowcount != 1:
                raise LifecycleBootstrapError("lifecycle bootstrap publication raced")
        converged = self.inventory(scope=plan.scope)
        if not converged.converged:
            raise LifecycleBootstrapError("lifecycle bootstrap did not converge")
        return converged


__all__ = [
    "LifecycleBootstrapError",
    "LifecycleBootstrapPlan",
    "SqlAlchemyLifecycleBootstrap",
    "build_lifecycle_bootstrap_plan",
    "lifecycle_bootstrap_plan_document",
]
