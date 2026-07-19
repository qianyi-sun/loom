"""Supported staging lifecycle inventory, dry-run, apply, and resume orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from loom.data_lifecycle_gc import (
    ExactObjectDeleter,
    GcExecutionResult,
    GcJournal,
    GcPlan,
    GcScope,
    LifecycleGcPlanError,
    execute_gc,
    resume_gc,
)
from loom.data_lifecycle_inventory_sql import LifecycleInventorySnapshot


class OperatorAction(StrEnum):
    INVENTORY = "inventory"
    DRY_RUN = "dry-run"
    APPLY = "apply"
    RESUME = "resume"
    AUTO = "auto"


class LifecycleInventoryLoader(Protocol):
    def load(self, *, scope: GcScope) -> LifecycleInventorySnapshot: ...


@dataclass(frozen=True, slots=True)
class LifecycleOperatorRequest:
    action: OperatorAction
    requested_by: str
    now: datetime
    approved_inventory_digest: str | None = None
    request_id: str | None = None
    resume_run_id: UUID | None = None

    def __post_init__(self) -> None:
        if not self.requested_by or self.requested_by != self.requested_by.strip():
            raise ValueError("requested_by must be non-empty and normalized")
        if self.now.tzinfo is None:
            raise ValueError("operator time must be timezone-aware")
        if self.action == OperatorAction.APPLY:
            if self.approved_inventory_digest is None or self.request_id is None:
                raise ValueError("apply requires inventory digest and request id")
        elif self.action == OperatorAction.RESUME:
            if self.resume_run_id is None or self.request_id is None:
                raise ValueError("resume requires run id and request id")
        elif self.action == OperatorAction.AUTO:
            if self.request_id is None:
                raise ValueError("auto requires request id")
            if self.approved_inventory_digest is not None or self.resume_run_id is not None:
                raise ValueError("auto does not accept manual approval or resume authority")
        elif any(
            value is not None
            for value in (
                self.approved_inventory_digest,
                self.request_id,
                self.resume_run_id,
            )
        ):
            raise ValueError("inventory and dry-run do not accept mutation authority")


def _plan_document(*, snapshot: LifecycleInventorySnapshot, plan: GcPlan) -> dict[str, object]:
    denial: str | None
    try:
        plan.require_applicable()
    except LifecycleGcPlanError as exc:
        applicable = False
        denial = str(exc)
    else:
        applicable = True
        denial = None
    return {
        "schema_version": 1,
        "environment": plan.scope.environment,
        "namespace": plan.scope.namespace,
        "mutation_epoch": plan.mutation_epoch,
        "planned_at": plan.planned_at.isoformat(),
        "inventory_digest": plan.inventory_digest,
        "registered_authority_count": len(snapshot.authorities),
        "registered_object_count": len(snapshot.objects),
        "eligible_authority_count": len(plan.authority_ids),
        "eligible_object_count": plan.object_count,
        "eligible_bytes": plan.bytes_total,
        "unclassified_rows": dict(snapshot.unclassified_rows),
        "blockers": list(plan.blockers),
        "applicable": applicable,
        "denial": denial,
    }


def _execution_document(result: GcExecutionResult) -> dict[str, object]:
    return {
        "run_id": str(result.run_id),
        "deletion_token": str(result.deletion_token),
        "mutation_epoch_before": result.mutation_epoch_before,
        "mutation_epoch_after": result.mutation_epoch_after,
        "deleted_objects": result.deleted_objects,
        "deleted_bytes": result.deleted_bytes,
        "dry_run": result.dry_run,
    }


def run_lifecycle_operator(
    *,
    request: LifecycleOperatorRequest,
    scope: GcScope,
    inventory: LifecycleInventoryLoader,
    journal: GcJournal,
    object_deleter: ExactObjectDeleter,
) -> dict[str, object]:
    """Run one supported operation without broadening its mutation authority."""
    if request.action == OperatorAction.RESUME:
        assert request.resume_run_id is not None
        assert request.request_id is not None
        result = resume_gc(
            run_id=request.resume_run_id,
            request_id=request.request_id,
            completed_at=request.now,
            journal=journal,
            object_deleter=object_deleter,
        )
        return {
            "schema_version": 1,
            "action": request.action,
            "execution": _execution_document(result),
        }

    snapshot = inventory.load(scope=scope)
    plan = snapshot.build_plan(now=request.now)
    document = _plan_document(snapshot=snapshot, plan=plan)
    if request.action == OperatorAction.INVENTORY:
        return {**document, "action": request.action}
    if request.action == OperatorAction.AUTO:
        if plan.blockers:
            plan.require_applicable()
        if not plan.authority_ids:
            return {
                **document,
                "action": request.action,
                "execution": None,
                "no_op": True,
            }
        assert request.request_id is not None
        result = execute_gc(
            plan=plan,
            requested_by=request.requested_by,
            journal=journal,
            object_deleter=object_deleter,
            dry_run=False,
            request_id=request.request_id,
            completed_at=request.now,
        )
        return {
            **document,
            "action": request.action,
            "execution": _execution_document(result),
            "no_op": False,
        }
    if request.action == OperatorAction.APPLY:
        if request.approved_inventory_digest != plan.inventory_digest:
            raise LifecycleGcPlanError("approved inventory digest does not match live plan")
        assert request.request_id is not None
        result = execute_gc(
            plan=plan,
            requested_by=request.requested_by,
            journal=journal,
            object_deleter=object_deleter,
            dry_run=False,
            request_id=request.request_id,
            completed_at=request.now,
        )
    else:
        result = execute_gc(
            plan=plan,
            requested_by=request.requested_by,
            journal=journal,
            object_deleter=object_deleter,
            dry_run=True,
        )
    return {
        **document,
        "action": request.action,
        "execution": _execution_document(result),
    }


__all__ = [
    "LifecycleOperatorRequest",
    "OperatorAction",
    "run_lifecycle_operator",
]
