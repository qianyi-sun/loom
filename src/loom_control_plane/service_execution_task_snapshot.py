"""Resolve the task snapshot already authorized by a persisted execution lease."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ServiceExecutionLease,
    Task,
    TaskImageMaterialization,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.execution_runtime_contract import ExecutionRuntimePlanV1


class ServiceExecutionTaskSnapshotError(ValueError):
    pass


@dataclass(frozen=True)
class ServiceExecutionTaskSnapshot:
    config: dict[str, Any]
    source: str | None
    source_provenance: dict[str, Any]


async def resolve_service_execution_task_snapshot(
    session: AsyncSession,
    *,
    lease: ServiceExecutionLease,
    trial: Trial | None = None,
) -> ServiceExecutionTaskSnapshot:
    if trial is None:
        trial = await session.get(Trial, lease.trial_id)
    if trial is None or trial.id != lease.trial_id or trial.team_id != lease.team_id:
        raise ServiceExecutionTaskSnapshotError("task_snapshot_trial_mismatch")

    contract = lease.runtime_contract_json or {}
    if contract.get("task_image_materialization_id") is None:
        task = await session.get(Task, trial.task_id)
        if task is None:
            raise ServiceExecutionTaskSnapshotError("task_snapshot_unavailable")
        return ServiceExecutionTaskSnapshot(task.config, task.source, task.source_provenance or {})

    try:
        plan = ExecutionRuntimePlanV1.model_validate(contract)
    except ValueError as exc:
        raise ServiceExecutionTaskSnapshotError("task_snapshot_identity_invalid") from exc
    # Retirement can follow execution; only the immutable snapshot association
    # matters here, not readiness or registry fields used for new claims.
    row = await session.scalar(
        select(TaskImageMaterialization)
        .join(
            TrialTaskImageMaterialization,
            TrialTaskImageMaterialization.materialization_id == TaskImageMaterialization.id,
        )
        .where(
            TrialTaskImageMaterialization.trial_id == trial.id,
            TaskImageMaterialization.id == plan.task_image_materialization_id,
            TaskImageMaterialization.task_id == trial.task_id,
            TaskImageMaterialization.task_checksum == plan.task_revision_sha256.removeprefix("sha256:"),
        )
    )
    if row is None:
        raise ServiceExecutionTaskSnapshotError("task_snapshot_binding_mismatch")
    return ServiceExecutionTaskSnapshot(
        row.task_config, row.task_source, row.task_source_provenance or {},
    )
