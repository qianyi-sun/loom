"""Digest-approved recovery for the one dirty pre-bootstrap staging database."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from sqlalchemy import Connection, Engine, text

from loom.data_lifecycle_gc import GcScope
from loom.staging_mutation_epoch import (
    MutationEpochAdvance,
    MutationEpochState,
    ProtectedMutationClass,
    advance_mutation_epoch,
)
from loom.staging_mutation_epoch_sql import SqlAlchemyMutationEpochStore

_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_REQUIRED_SCHEMA_REVISION = "0075"
_ADVISORY_LOCK_KEY = 0x4C4F4F4D4C494645  # ``LOOMLIFE`` as one signed-safe bigint.
_EXECUTION_TABLES = ("artifacts", "batches", "llm_calls", "trial_events", "trials")
_LOCK_TABLES = tuple(
    sorted(
        (
            *_EXECUTION_TABLES,
            "alembic_version",
            "data_lifecycle_authorities",
            "data_lifecycle_gc_authorities",
            "data_lifecycle_gc_items",
            "data_lifecycle_gc_runs",
            "data_lifecycle_objects",
            "staging_lifecycle_capacity",
            "staging_mutation_epoch_events",
            "staging_mutation_epochs",
        )
    )
)
_FINGERPRINT_QUERIES = (
    (
        "data_lifecycle_authorities",
        """
SELECT id::text, environment, namespace, coalesce(team_id::text, ''), data_class,
       owner_kind, owner_id, created_at::text, coalesce(expires_at::text, ''),
       pinned::text, state, coalesce(deletion_token::text, ''), metadata::text
FROM data_lifecycle_authorities
ORDER BY id
""".strip(),
    ),
    (
        "data_lifecycle_objects",
        """
SELECT id::text, authority_id::text, environment, namespace, bucket, object_key,
       version_id, content_sha256, size_bytes::text, created_at::text,
       state, coalesce(deletion_token::text, ''), coalesce(verified_deleted_at::text, '')
FROM data_lifecycle_objects
ORDER BY id
""".strip(),
    ),
    (
        "staging_lifecycle_capacity",
        """
SELECT environment, namespace, object_count::text, bytes_used::text,
       disk_free_percent::text, inode_free_percent::text, policy_sha256,
       evidence_sha256, source, observed_at::text
FROM staging_lifecycle_capacity
ORDER BY environment, namespace
""".strip(),
    ),
    *tuple(
        (
            table,
            (
                f"SELECT id::text, coalesce(lifecycle_authority_id::text, '') "
                f"FROM {table} ORDER BY id"
            ),
        )
        for table in _EXECUTION_TABLES
    ),
)


class DirtyEpochReconcileError(RuntimeError):
    """The dirty staging epoch cannot be reconciled safely."""


@dataclass(frozen=True, slots=True)
class DirtyEpochReconcilePlan:
    scope: GcScope
    schema_revision: str
    state_fingerprint: str
    epoch_count: int
    epoch_event_count: int
    gc_authority_count: int
    gc_item_count: int
    gc_run_count: int
    authority_count: int
    object_count: int
    capacity_count: int
    unsafe_authority_count: int
    unsafe_object_count: int
    execution_counts: tuple[tuple[str, int, int], ...]
    blockers: tuple[str, ...]
    inventory_digest: str

    @property
    def classified_execution_count(self) -> int:
        return sum(classified for _table, _total, classified in self.execution_counts)

    @property
    def unclassified_execution_count(self) -> int:
        return sum(total - classified for _table, total, classified in self.execution_counts)

    @property
    def applicable(self) -> bool:
        return not self.blockers

    def require_applicable(self) -> None:
        if self.blockers:
            raise DirtyEpochReconcileError("; ".join(self.blockers))


def _payload(plan: DirtyEpochReconcilePlan, *, include_digest: bool) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": 1,
        "environment": plan.scope.environment,
        "namespace": plan.scope.namespace,
        "schema_revision": plan.schema_revision,
        "state_fingerprint": plan.state_fingerprint,
        "epoch_count": plan.epoch_count,
        "epoch_event_count": plan.epoch_event_count,
        "gc_authority_count": plan.gc_authority_count,
        "gc_item_count": plan.gc_item_count,
        "gc_run_count": plan.gc_run_count,
        "authority_count": plan.authority_count,
        "object_count": plan.object_count,
        "capacity_count": plan.capacity_count,
        "unsafe_authority_count": plan.unsafe_authority_count,
        "unsafe_object_count": plan.unsafe_object_count,
        "execution_counts": {
            table: {"total": total, "classified": classified}
            for table, total, classified in plan.execution_counts
        },
        "blockers": list(plan.blockers),
    }
    if include_digest:
        value.update(
            {
                "classified_execution_count": plan.classified_execution_count,
                "unclassified_execution_count": plan.unclassified_execution_count,
                "inventory_digest": plan.inventory_digest,
                "applicable": plan.applicable,
            }
        )
    return value


def dirty_epoch_reconcile_plan_document(plan: DirtyEpochReconcilePlan) -> dict[str, object]:
    return _payload(plan, include_digest=True)


def build_dirty_epoch_reconcile_plan(
    *,
    scope: GcScope,
    schema_revision: str,
    state_fingerprint: str,
    epoch_count: int,
    epoch_event_count: int,
    gc_authority_count: int,
    gc_item_count: int,
    gc_run_count: int,
    authority_count: int,
    object_count: int,
    capacity_count: int,
    unsafe_authority_count: int,
    unsafe_object_count: int,
    execution_counts: tuple[tuple[str, int, int], ...],
) -> DirtyEpochReconcilePlan:
    counts = (
        epoch_count,
        epoch_event_count,
        gc_authority_count,
        gc_item_count,
        gc_run_count,
        authority_count,
        object_count,
        capacity_count,
        unsafe_authority_count,
        unsafe_object_count,
    )
    stable_execution = tuple(sorted(execution_counts))
    if (
        _DIGEST_RE.fullmatch(state_fingerprint) is None
        or any(type(value) is not int or value < 0 for value in counts)
        or tuple(table for table, _total, _classified in stable_execution) != _EXECUTION_TABLES
        or any(
            type(total) is not int
            or type(classified) is not int
            or total < 0
            or classified < 0
            or classified > total
            for _table, total, classified in stable_execution
        )
    ):
        raise ValueError("dirty epoch reconciliation inventory is invalid")
    blockers: list[str] = []
    if schema_revision != _REQUIRED_SCHEMA_REVISION:
        blockers.append("dirty epoch reconciliation requires exact schema revision 0075")
    if (scope.environment, scope.namespace) != ("staging", "loom-staging"):
        blockers.append("dirty epoch reconciliation is fixed to staging/loom-staging")
    if epoch_count:
        blockers.append("mutation epoch authority already exists")
    if epoch_event_count:
        blockers.append("mutation epoch events already exist")
    if gc_authority_count or gc_item_count or gc_run_count:
        blockers.append("lifecycle GC state already exists")
    if unsafe_authority_count:
        blockers.append("lifecycle authority deletion state already exists")
    if unsafe_object_count:
        blockers.append("lifecycle object deletion state already exists")
    classified_count = sum(classified for _table, _total, classified in stable_execution)
    if authority_count == 0 and object_count == 0 and classified_count == 0:
        blockers.append("lifecycle state is clean; use standard bootstrap")
    placeholder = DirtyEpochReconcilePlan(
        scope=scope,
        schema_revision=schema_revision,
        state_fingerprint=state_fingerprint,
        epoch_count=epoch_count,
        epoch_event_count=epoch_event_count,
        gc_authority_count=gc_authority_count,
        gc_item_count=gc_item_count,
        gc_run_count=gc_run_count,
        authority_count=authority_count,
        object_count=object_count,
        capacity_count=capacity_count,
        unsafe_authority_count=unsafe_authority_count,
        unsafe_object_count=unsafe_object_count,
        execution_counts=stable_execution,
        blockers=tuple(sorted(set(blockers))),
        inventory_digest="0" * 64,
    )
    digest = hashlib.sha256(
        json.dumps(
            _payload(placeholder, include_digest=False),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return replace(placeholder, inventory_digest=digest)


def _scalar_count(connection: Connection, table: str, where: str = "") -> int:
    return int(connection.execute(text(f"SELECT count(*) FROM {table} {where}")).scalar_one())


def _state_fingerprint(connection: Connection) -> str:
    digest = hashlib.sha256()
    for label, sql in _FINGERPRINT_QUERIES:
        digest.update(label.encode())
        digest.update(b"\0")
        for row in connection.execute(text(sql)):
            payload = json.dumps(list(row), separators=(",", ":"), ensure_ascii=True).encode()
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
    return digest.hexdigest()


def _inventory_connection(connection: Connection, *, scope: GcScope) -> DirtyEpochReconcilePlan:
    revisions = tuple(
        str(value)
        for value in connection.execute(text("SELECT version_num FROM alembic_version")).scalars()
    )
    if len(revisions) != 1:
        raise DirtyEpochReconcileError("database schema revision authority is ambiguous")
    execution_counts = tuple(
        (
            table,
            int(row.total),
            int(row.classified),
        )
        for table in _EXECUTION_TABLES
        for row in (
            connection.execute(
                text(
                    f"SELECT count(*) AS total, count(lifecycle_authority_id) AS classified "
                    f"FROM {table}"
                )
            ).one(),
        )
    )
    return build_dirty_epoch_reconcile_plan(
        scope=scope,
        schema_revision=revisions[0],
        state_fingerprint=_state_fingerprint(connection),
        epoch_count=_scalar_count(connection, "staging_mutation_epochs"),
        epoch_event_count=_scalar_count(connection, "staging_mutation_epoch_events"),
        gc_authority_count=_scalar_count(connection, "data_lifecycle_gc_authorities"),
        gc_item_count=_scalar_count(connection, "data_lifecycle_gc_items"),
        gc_run_count=_scalar_count(connection, "data_lifecycle_gc_runs"),
        authority_count=_scalar_count(connection, "data_lifecycle_authorities"),
        object_count=_scalar_count(connection, "data_lifecycle_objects"),
        capacity_count=_scalar_count(connection, "staging_lifecycle_capacity"),
        unsafe_authority_count=_scalar_count(
            connection,
            "data_lifecycle_authorities",
            "WHERE state <> 'active' OR deletion_token IS NOT NULL",
        ),
        unsafe_object_count=_scalar_count(
            connection,
            "data_lifecycle_objects",
            (
                "WHERE state <> 'active' OR deletion_token IS NOT NULL "
                "OR verified_deleted_at IS NOT NULL"
            ),
        ),
        execution_counts=execution_counts,
    )


class SqlAlchemyDirtyEpochReconciler:
    """Atomically adopt a dirty state that has never had protected mutation authority."""

    def __init__(self, engine: Engine, *, now: Callable[[], datetime] | None = None) -> None:
        self._engine = engine
        self._now = now or (lambda: datetime.now(UTC))

    def inventory(self, *, scope: GcScope) -> DirtyEpochReconcilePlan:
        with self._engine.connect().execution_options(
            isolation_level="REPEATABLE READ"
        ) as connection:
            with connection.begin():
                return _inventory_connection(connection, scope=scope)

    def apply(
        self,
        *,
        plan: DirtyEpochReconcilePlan,
        approved_inventory_digest: str,
        request_id: str,
    ) -> MutationEpochState:
        plan.require_applicable()
        if approved_inventory_digest != plan.inventory_digest:
            raise DirtyEpochReconcileError("approved reconciliation inventory digest does not match")
        occurred_at = self._now()
        advance = MutationEpochAdvance(
            environment=plan.scope.environment,
            namespace=plan.scope.namespace,
            expected_epoch=0,
            mutation_class=ProtectedMutationClass.OBJECT_REWRITE,
            request_id=request_id,
            evidence_sha256=plan.inventory_digest,
            occurred_at=occurred_at,
        )
        with self._engine.begin() as connection:
            connection.execute(
                text("SELECT pg_advisory_xact_lock(:key)"), {"key": _ADVISORY_LOCK_KEY}
            )
            connection.execute(
                text(
                    "LOCK TABLE "
                    + ", ".join(_LOCK_TABLES)
                    + " IN SHARE ROW EXCLUSIVE MODE"
                )
            )
            live = _inventory_connection(connection, scope=plan.scope)
            if live.inventory_digest != plan.inventory_digest or not live.applicable:
                raise DirtyEpochReconcileError("dirty epoch reconciliation inventory drifted")
            inserted = connection.execute(
                text(
                    "INSERT INTO staging_mutation_epochs "
                    "(environment,namespace,epoch,reason) "
                    "VALUES ('staging','loom-staging',0,'bootstrap') "
                    "ON CONFLICT (environment) DO NOTHING"
                )
            )
            if inserted.rowcount != 1:
                raise DirtyEpochReconcileError("dirty epoch reconciliation publication raced")
            state = advance_mutation_epoch(SqlAlchemyMutationEpochStore(connection), advance)
        if state.epoch != 1 or state.evidence_sha256 != plan.inventory_digest:
            raise DirtyEpochReconcileError("dirty epoch reconciliation did not converge")
        return state


__all__ = [
    "DirtyEpochReconcileError",
    "DirtyEpochReconcilePlan",
    "SqlAlchemyDirtyEpochReconciler",
    "build_dirty_epoch_reconcile_plan",
    "dirty_epoch_reconcile_plan_document",
]
