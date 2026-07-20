"""Fail-closed two-phase garbage collection for staging execution data.

This module contains the storage-independent protocol.  Database and object
store adapters are deliberately separate: the planner can be exercised without
credentials, while the executor only accepts exact registered object identities
and an epoch-bound plan.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID, uuid4

from loom.data_lifecycle import LifecycleState, ObjectLifecycleState
from loom.staging_mutation_epoch import (
    MutationEpochAdvance,
    MutationEpochState,
    ProtectedMutationClass,
)


class LifecycleGcError(RuntimeError):
    """Base class for fail-closed lifecycle GC errors."""


class LifecycleGcPlanError(LifecycleGcError):
    """Raised when an inventory cannot produce a deletion-safe plan."""


class LifecycleGcExecutionError(LifecycleGcError):
    """Raised after an apply run is journaled as failed."""


@dataclass(frozen=True, slots=True)
class GcScope:
    """Exact staging environment and namespace authority."""

    environment: str
    namespace: str

    def __post_init__(self) -> None:
        if self.environment != "staging":
            raise ValueError("lifecycle GC is staging-only")
        if not self.namespace or self.namespace != self.namespace.strip():
            raise ValueError("namespace must be non-empty and normalized")


@dataclass(frozen=True, slots=True)
class AuthorityInventory:
    id: UUID
    environment: str
    namespace: str
    owner_kind: str
    owner_id: str
    expires_at: datetime | None
    pinned: bool
    state: str


@dataclass(frozen=True, slots=True)
class RegisteredObject:
    id: UUID
    authority_id: UUID
    environment: str
    namespace: str
    bucket: str
    object_key: str
    version_id: str | None
    content_sha256: str | None
    size_bytes: int
    state: str

    @property
    def identity(self) -> tuple[str, str, str]:
        return (self.bucket, self.object_key, self.version_id or "")


@dataclass(frozen=True, slots=True)
class ObservedObject:
    """Read-only object inventory entry used for orphan reconciliation."""

    bucket: str
    object_key: str
    version_id: str | None
    size_bytes: int

    @property
    def identity(self) -> tuple[str, str, str]:
        return (self.bucket, self.object_key, self.version_id or "")


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    registered_missing: tuple[tuple[str, str, str], ...]
    observed_unregistered: tuple[tuple[str, str, str], ...]
    registered_size_drift: tuple[tuple[str, str, str], ...] = ()

    @property
    def clean(self) -> bool:
        return (
            not self.registered_missing
            and not self.observed_unregistered
            and not self.registered_size_drift
        )


@dataclass(frozen=True, slots=True)
class GcPlan:
    scope: GcScope
    mutation_epoch: int
    planned_at: datetime
    authority_ids: tuple[UUID, ...]
    objects: tuple[RegisteredObject, ...]
    blockers: tuple[str, ...]
    inventory_digest: str

    @property
    def object_count(self) -> int:
        return len(self.objects)

    @property
    def bytes_total(self) -> int:
        return sum(item.size_bytes for item in self.objects)

    def require_applicable(self) -> None:
        if self.blockers:
            raise LifecycleGcPlanError("; ".join(self.blockers))
        if not self.authority_ids:
            raise LifecycleGcPlanError("plan contains no eligible lifecycle authorities")


@dataclass(frozen=True, slots=True)
class GcExecutionResult:
    run_id: UUID
    deletion_token: UUID
    mutation_epoch_before: int
    mutation_epoch_after: int | None
    deleted_objects: int
    deleted_bytes: int
    dry_run: bool


@dataclass(frozen=True, slots=True)
class GcResumeSnapshot:
    run_id: UUID
    deletion_token: UUID
    plan: GcPlan
    item_states: tuple[tuple[UUID, str], ...]

    def __post_init__(self) -> None:
        expected_ids = {item.id for item in self.plan.objects}
        actual_ids = {object_id for object_id, _state in self.item_states}
        allowed = {"marked", "object_deleted", "verified", "metadata_deleted"}
        if (
            expected_ids != actual_ids
            or len(self.item_states) != len(actual_ids)
            or any(state not in allowed for _object_id, state in self.item_states)
        ):
            raise ValueError("GC resume snapshot item authority is invalid")


class ExactObjectDeleter(Protocol):
    """Object adapter that never broadens an exact registered identity."""

    def delete_exact(self, item: RegisteredObject) -> None: ...

    def exact_absent(self, item: RegisteredObject) -> bool: ...

    def delete_exact_many(self, items: Sequence[RegisteredObject]) -> None: ...

    def exact_absent_many(self, items: Sequence[RegisteredObject]) -> Mapping[UUID, bool]: ...


class GcJournal(Protocol):
    """Transactional DB adapter for the resumable two-phase protocol."""

    def record_dry_run(self, *, plan: GcPlan, requested_by: str) -> UUID: ...

    def begin_apply(
        self,
        *,
        plan: GcPlan,
        requested_by: str,
        deletion_token: UUID,
    ) -> UUID: ...

    def record_object_deleted(
        self,
        *,
        run_id: UUID,
        object_id: UUID,
        deletion_token: UUID,
    ) -> None: ...

    def record_objects_deleted(
        self,
        *,
        run_id: UUID,
        object_ids: Sequence[UUID],
        deletion_token: UUID,
    ) -> None: ...

    def record_object_verified(
        self,
        *,
        run_id: UUID,
        object_id: UUID,
        deletion_token: UUID,
    ) -> None: ...

    def record_objects_verified(
        self,
        *,
        run_id: UUID,
        object_ids: Sequence[UUID],
        deletion_token: UUID,
    ) -> None: ...

    def delete_business_metadata(
        self,
        *,
        run_id: UUID,
        authority_ids: Sequence[UUID],
        deletion_token: UUID,
    ) -> None: ...

    def complete_apply(
        self,
        *,
        run_id: UUID,
        mutation: MutationEpochAdvance,
        deletion_token: UUID,
    ) -> MutationEpochState: ...

    def fail_apply(self, *, run_id: UUID, reason: str) -> None: ...

    def load_resume(self, run_id: UUID) -> GcResumeSnapshot: ...

    def begin_resume(self, *, run_id: UUID, deletion_token: UUID) -> None: ...


def _normalized_sha256(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.lower()
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        return None
    return normalized


def _inventory_payload(
    *,
    scope: GcScope,
    mutation_epoch: int,
    planned_at: datetime,
    authority_ids: Sequence[UUID],
    objects: Sequence[RegisteredObject],
    blockers: Sequence[str],
) -> dict[str, object]:
    return {
        "environment": scope.environment,
        "namespace": scope.namespace,
        "mutation_epoch": mutation_epoch,
        "planned_at": planned_at.isoformat(),
        "authority_ids": [str(value) for value in authority_ids],
        "objects": [
            {
                "id": str(item.id),
                "authority_id": str(item.authority_id),
                "bucket": item.bucket,
                "object_key": item.object_key,
                "version_id": item.version_id,
                "content_sha256": item.content_sha256,
                "size_bytes": item.size_bytes,
            }
            for item in objects
        ],
        "blockers": list(blockers),
    }


def _digest_payload(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _inventory_digest(payload: Mapping[str, object]) -> str:
    """Digest deletion authority while excluding report-only wall clock time."""
    return _digest_payload({key: value for key, value in payload.items() if key != "planned_at"})


def serialize_gc_plan(plan: GcPlan) -> dict[str, object]:
    """Serialize the exact epoch-bound plan for durable resume evidence."""
    payload = _inventory_payload(
        scope=plan.scope,
        mutation_epoch=plan.mutation_epoch,
        planned_at=plan.planned_at,
        authority_ids=plan.authority_ids,
        objects=plan.objects,
        blockers=plan.blockers,
    )
    if _inventory_digest(payload) != plan.inventory_digest:
        raise LifecycleGcPlanError("GC plan inventory digest is not canonical")
    return {**payload, "inventory_digest": plan.inventory_digest}


def deserialize_gc_plan(document: Mapping[str, object]) -> GcPlan:
    """Rebuild only an exact canonical plan; tampered resume evidence fails closed."""
    expected = {
        "environment",
        "namespace",
        "mutation_epoch",
        "planned_at",
        "authority_ids",
        "objects",
        "blockers",
        "inventory_digest",
    }
    if set(document) != expected:
        raise LifecycleGcPlanError("GC plan document fields do not match schema")
    try:
        scope = GcScope(
            environment=str(document["environment"]),
            namespace=str(document["namespace"]),
        )
        mutation_epoch = document["mutation_epoch"]
        if type(mutation_epoch) is not int:
            raise ValueError("mutation epoch is not an integer")
        planned_at = datetime.fromisoformat(str(document["planned_at"]))
        authority_values = document["authority_ids"]
        object_values = document["objects"]
        blocker_values = document["blockers"]
        if (
            not isinstance(authority_values, list)
            or not all(isinstance(value, str) for value in authority_values)
            or not isinstance(object_values, list)
            or not all(isinstance(value, dict) for value in object_values)
            or not isinstance(blocker_values, list)
            or not all(isinstance(value, str) for value in blocker_values)
            or not isinstance(document["inventory_digest"], str)
        ):
            raise ValueError("GC plan collection schema is invalid")
        authority_ids = tuple(UUID(value) for value in authority_values)
        objects: list[RegisteredObject] = []
        object_expected = {
            "id",
            "authority_id",
            "bucket",
            "object_key",
            "version_id",
            "content_sha256",
            "size_bytes",
        }
        for raw in object_values:
            if set(raw) != object_expected or type(raw["size_bytes"]) is not int:
                raise ValueError("GC plan object schema is invalid")
            objects.append(
                RegisteredObject(
                    id=UUID(str(raw["id"])),
                    authority_id=UUID(str(raw["authority_id"])),
                    environment=scope.environment,
                    namespace=scope.namespace,
                    bucket=str(raw["bucket"]),
                    object_key=str(raw["object_key"]),
                    version_id=(str(raw["version_id"]) if raw["version_id"] is not None else None),
                    content_sha256=(
                        str(raw["content_sha256"]) if raw["content_sha256"] is not None else None
                    ),
                    size_bytes=raw["size_bytes"],
                    state=ObjectLifecycleState.ACTIVE,
                )
            )
    except (TypeError, ValueError) as exc:
        raise LifecycleGcPlanError("GC plan document is invalid") from exc
    payload = {key: document[key] for key in expected - {"inventory_digest"}}
    digest = _inventory_digest(payload)
    if digest != document["inventory_digest"]:
        raise LifecycleGcPlanError("GC plan document digest does not match")
    plan = GcPlan(
        scope=scope,
        mutation_epoch=mutation_epoch,
        planned_at=planned_at,
        authority_ids=authority_ids,
        objects=tuple(objects),
        blockers=tuple(blocker_values),
        inventory_digest=digest,
    )
    plan.require_applicable()
    return plan


def reconcile_object_inventory(
    *,
    registered: Iterable[RegisteredObject],
    observed: Iterable[ObservedObject],
) -> ReconciliationReport:
    """Report both orphan directions without authorizing either for deletion."""
    registered_items = {item.identity: item for item in registered}
    observed_items = {item.identity: item for item in observed}
    registered_keys = set(registered_items)
    observed_keys = set(observed_items)
    return ReconciliationReport(
        registered_missing=tuple(sorted(registered_keys - observed_keys)),
        observed_unregistered=tuple(sorted(observed_keys - registered_keys)),
        registered_size_drift=tuple(
            sorted(
                identity
                for identity in registered_keys & observed_keys
                if registered_items[identity].size_bytes != observed_items[identity].size_bytes
            )
        ),
    )


def build_gc_plan(
    *,
    scope: GcScope,
    mutation_epoch: int,
    now: datetime,
    authorities: Iterable[AuthorityInventory],
    objects: Iterable[RegisteredObject],
    additional_blockers: Iterable[str] = (),
) -> GcPlan:
    """Build an exact, deterministic plan; ambiguity becomes a blocker."""
    if mutation_epoch < 0:
        raise ValueError("mutation_epoch must be non-negative")
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    blockers = [value for value in additional_blockers if value]
    eligible: dict[UUID, AuthorityInventory] = {}
    seen_authority_ids: set[UUID] = set()
    for authority in authorities:
        if authority.id in seen_authority_ids:
            blockers.append(f"duplicate authority id {authority.id}")
            continue
        seen_authority_ids.add(authority.id)
        if (authority.environment, authority.namespace) != (
            scope.environment,
            scope.namespace,
        ):
            blockers.append(f"authority {authority.id} crosses GC scope")
            continue
        if authority.state != LifecycleState.ACTIVE:
            continue
        if authority.pinned or authority.expires_at is None:
            continue
        if authority.expires_at.tzinfo is None:
            blockers.append(f"authority {authority.id} has naive expiry")
            continue
        if authority.expires_at <= now:
            eligible[authority.id] = authority

    selected_objects: list[RegisteredObject] = []
    seen_object_ids: set[UUID] = set()
    seen_identities: set[tuple[str, str, str]] = set()
    for item in objects:
        if item.id in seen_object_ids:
            blockers.append(f"duplicate object id {item.id}")
            continue
        seen_object_ids.add(item.id)
        if item.authority_id not in seen_authority_ids:
            blockers.append(f"object {item.id} references unknown authority")
            continue
        if item.authority_id not in eligible:
            continue
        if (item.environment, item.namespace) != (scope.environment, scope.namespace):
            blockers.append(f"object {item.id} crosses GC scope")
            continue
        if not item.bucket or not item.object_key or item.object_key.startswith("/"):
            blockers.append(f"object {item.id} has invalid storage identity")
            continue
        if item.size_bytes < 0:
            blockers.append(f"object {item.id} has negative size")
            continue
        if item.state != ObjectLifecycleState.ACTIVE:
            blockers.append(f"object {item.id} is not active")
            continue
        if item.identity in seen_identities:
            blockers.append(f"duplicate object identity {item.bucket}/{item.object_key}")
            continue
        seen_identities.add(item.identity)
        normalized_sha = _normalized_sha256(item.content_sha256)
        if item.content_sha256 is not None and normalized_sha is None:
            blockers.append(f"object {item.id} has an invalid SHA-256")
            continue
        if item.version_id is None and normalized_sha is None:
            blockers.append(
                f"unversioned object {item.id} lacks an exact SHA-256 deletion authority"
            )
            continue
        if normalized_sha != item.content_sha256:
            item = RegisteredObject(
                id=item.id,
                authority_id=item.authority_id,
                environment=item.environment,
                namespace=item.namespace,
                bucket=item.bucket,
                object_key=item.object_key,
                version_id=item.version_id,
                content_sha256=normalized_sha,
                size_bytes=item.size_bytes,
                state=item.state,
            )
        selected_objects.append(item)

    authority_ids = tuple(sorted(eligible))
    selected = tuple(sorted(selected_objects, key=lambda item: (item.identity, str(item.id))))
    stable_blockers = tuple(sorted(set(blockers)))
    payload = _inventory_payload(
        scope=scope,
        mutation_epoch=mutation_epoch,
        planned_at=now,
        authority_ids=authority_ids,
        objects=selected,
        blockers=stable_blockers,
    )
    return GcPlan(
        scope=scope,
        mutation_epoch=mutation_epoch,
        planned_at=now,
        authority_ids=authority_ids,
        objects=selected,
        blockers=stable_blockers,
        inventory_digest=_inventory_digest(payload),
    )


def execute_gc(
    *,
    plan: GcPlan,
    requested_by: str,
    journal: GcJournal,
    object_deleter: ExactObjectDeleter,
    dry_run: bool,
    request_id: str | None = None,
    completed_at: datetime | None = None,
    batch_size: int = 1,
) -> GcExecutionResult:
    """Execute exact object deletion, verification, metadata removal, and epoch bump."""
    plan.require_applicable()
    if not requested_by or requested_by != requested_by.strip():
        raise ValueError("requested_by must be non-empty and normalized")
    if not 1 <= batch_size <= 1000:
        raise ValueError("GC batch size must be in [1, 1000]")
    if dry_run:
        run_id = journal.record_dry_run(plan=plan, requested_by=requested_by)
        return GcExecutionResult(
            run_id=run_id,
            deletion_token=UUID(int=0),
            mutation_epoch_before=plan.mutation_epoch,
            mutation_epoch_after=None,
            deleted_objects=0,
            deleted_bytes=0,
            dry_run=True,
        )
    if request_id is None or completed_at is None:
        raise ValueError("GC apply requires request and completion authority")
    if batch_size > 1 and any(
        not callable(getattr(target, method, None))
        for target, method in (
            (journal, "record_objects_deleted"),
            (journal, "record_objects_verified"),
            (object_deleter, "delete_exact_many"),
            (object_deleter, "exact_absent_many"),
        )
    ):
        raise TypeError("batched GC requires batch-capable journal and object deleter")
    mutation = MutationEpochAdvance(
        environment=plan.scope.environment,
        namespace=plan.scope.namespace,
        expected_epoch=plan.mutation_epoch,
        mutation_class=ProtectedMutationClass.LIFECYCLE_GC,
        request_id=request_id,
        evidence_sha256=plan.inventory_digest,
        occurred_at=completed_at,
    )

    deletion_token = uuid4()
    run_id = journal.begin_apply(
        plan=plan,
        requested_by=requested_by,
        deletion_token=deletion_token,
    )
    try:
        for offset in range(0, len(plan.objects), batch_size):
            batch = plan.objects[offset : offset + batch_size]
            if batch_size == 1:
                item = batch[0]
                object_deleter.delete_exact(item)
                journal.record_object_deleted(
                    run_id=run_id,
                    object_id=item.id,
                    deletion_token=deletion_token,
                )
            else:
                object_deleter.delete_exact_many(batch)
                journal.record_objects_deleted(
                    run_id=run_id,
                    object_ids=tuple(item.id for item in batch),
                    deletion_token=deletion_token,
                )
        for offset in range(0, len(plan.objects), batch_size):
            batch = plan.objects[offset : offset + batch_size]
            if batch_size == 1:
                item = batch[0]
                absent = {item.id: object_deleter.exact_absent(item)}
            else:
                absent = dict(object_deleter.exact_absent_many(batch))
            expected_ids = {item.id for item in batch}
            if set(absent) != expected_ids or not all(absent.values()):
                present = next(
                    (item for item in batch if item.id not in absent or not absent[item.id]),
                    batch[0],
                )
                raise LifecycleGcExecutionError(
                    f"object still present after exact delete: "
                    f"{present.bucket}/{present.object_key}"
                )
            if batch_size == 1:
                journal.record_object_verified(
                    run_id=run_id,
                    object_id=batch[0].id,
                    deletion_token=deletion_token,
                )
            else:
                journal.record_objects_verified(
                    run_id=run_id,
                    object_ids=tuple(item.id for item in batch),
                    deletion_token=deletion_token,
                )
        journal.delete_business_metadata(
            run_id=run_id,
            authority_ids=plan.authority_ids,
            deletion_token=deletion_token,
        )
        epoch_state = journal.complete_apply(
            run_id=run_id,
            mutation=mutation,
            deletion_token=deletion_token,
        )
        if epoch_state.epoch != plan.mutation_epoch + 1:
            raise LifecycleGcExecutionError("journal completed with a non-monotonic mutation epoch")
    except Exception as exc:
        reason = f"{type(exc).__name__}: {exc}"
        journal.fail_apply(run_id=run_id, reason=reason)
        if isinstance(exc, LifecycleGcExecutionError):
            raise
        raise LifecycleGcExecutionError(reason) from exc

    return GcExecutionResult(
        run_id=run_id,
        deletion_token=deletion_token,
        mutation_epoch_before=plan.mutation_epoch,
        mutation_epoch_after=epoch_state.epoch,
        deleted_objects=plan.object_count,
        deleted_bytes=plan.bytes_total,
        dry_run=False,
    )


def resume_gc(
    *,
    run_id: UUID,
    request_id: str,
    completed_at: datetime,
    journal: GcJournal,
    object_deleter: ExactObjectDeleter,
    batch_size: int = 1,
) -> GcExecutionResult:
    """Resume the exact journaled phase without broadening or repeating metadata work."""
    snapshot = journal.load_resume(run_id)
    if snapshot.run_id != run_id:
        raise LifecycleGcExecutionError("GC resume journal returned another run")
    plan = snapshot.plan
    plan.require_applicable()
    if not 1 <= batch_size <= 1000:
        raise ValueError("GC batch size must be in [1, 1000]")
    if batch_size > 1 and any(
        not callable(getattr(target, method, None))
        for target, method in (
            (journal, "record_objects_deleted"),
            (journal, "record_objects_verified"),
            (object_deleter, "delete_exact_many"),
            (object_deleter, "exact_absent_many"),
        )
    ):
        raise TypeError("batched GC requires batch-capable journal and object deleter")
    journal.begin_resume(run_id=run_id, deletion_token=snapshot.deletion_token)
    mutation = MutationEpochAdvance(
        environment=plan.scope.environment,
        namespace=plan.scope.namespace,
        expected_epoch=plan.mutation_epoch,
        mutation_class=ProtectedMutationClass.LIFECYCLE_GC,
        request_id=request_id,
        evidence_sha256=plan.inventory_digest,
        occurred_at=completed_at,
    )
    states = dict(snapshot.item_states)
    try:
        marked = [item for item in plan.objects if states[item.id] == "marked"]
        for offset in range(0, len(marked), batch_size):
            batch = marked[offset : offset + batch_size]
            if batch_size == 1:
                absent = {batch[0].id: object_deleter.exact_absent(batch[0])}
            else:
                absent = dict(object_deleter.exact_absent_many(batch))
            if set(absent) != {item.id for item in batch}:
                raise LifecycleGcExecutionError("GC resume absence evidence is incomplete")
            remaining = [item for item in batch if not absent[item.id]]
            if remaining:
                if batch_size == 1:
                    object_deleter.delete_exact(remaining[0])
                else:
                    object_deleter.delete_exact_many(remaining)
            if batch_size == 1:
                journal.record_object_deleted(
                    run_id=run_id,
                    object_id=batch[0].id,
                    deletion_token=snapshot.deletion_token,
                )
            else:
                journal.record_objects_deleted(
                    run_id=run_id,
                    object_ids=tuple(item.id for item in batch),
                    deletion_token=snapshot.deletion_token,
                )
            for item in batch:
                states[item.id] = "object_deleted"
        deleted = [item for item in plan.objects if states[item.id] == "object_deleted"]
        for offset in range(0, len(deleted), batch_size):
            batch = deleted[offset : offset + batch_size]
            if batch_size == 1:
                absent = {batch[0].id: object_deleter.exact_absent(batch[0])}
            else:
                absent = dict(object_deleter.exact_absent_many(batch))
            if set(absent) != {item.id for item in batch} or not all(absent.values()):
                present = next(item for item in batch if not absent.get(item.id, False))
                raise LifecycleGcExecutionError(
                    f"object still present during resume: {present.bucket}/{present.object_key}"
                )
            if batch_size == 1:
                journal.record_object_verified(
                    run_id=run_id,
                    object_id=batch[0].id,
                    deletion_token=snapshot.deletion_token,
                )
            else:
                journal.record_objects_verified(
                    run_id=run_id,
                    object_ids=tuple(item.id for item in batch),
                    deletion_token=snapshot.deletion_token,
                )
            for item in batch:
                states[item.id] = "verified"
        phases = set(states.values())
        if phases == {"verified"}:
            journal.delete_business_metadata(
                run_id=run_id,
                authority_ids=plan.authority_ids,
                deletion_token=snapshot.deletion_token,
            )
        elif phases != {"metadata_deleted"}:
            raise LifecycleGcExecutionError("GC resume contains mixed metadata phases")
        epoch_state = journal.complete_apply(
            run_id=run_id,
            mutation=mutation,
            deletion_token=snapshot.deletion_token,
        )
    except Exception as exc:
        journal.fail_apply(run_id=run_id, reason=f"{type(exc).__name__}: {exc}")
        if isinstance(exc, LifecycleGcExecutionError):
            raise
        raise LifecycleGcExecutionError(f"{type(exc).__name__}: {exc}") from exc
    return GcExecutionResult(
        run_id=run_id,
        deletion_token=snapshot.deletion_token,
        mutation_epoch_before=plan.mutation_epoch,
        mutation_epoch_after=epoch_state.epoch,
        deleted_objects=plan.object_count,
        deleted_bytes=plan.bytes_total,
        dry_run=False,
    )
