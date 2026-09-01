"""Normal queued-Trial admission into the durable service-execution path."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import ServiceExecutionLease, ServiceExecutionTarget
from loom.execution_contract import ExecutionRoutingReason, workload_requirements_from_task
from loom.execution_image_admission import ImageAdmissionKeyring
from loom.execution_runtime_contract import ExecutionRuntimePlanV1
from loom.models.task import TaskConfig, bind_service_execution_runtime_plan
from loom.pipeline.keys import canonical_digest, canonical_uuid5
from loom_control_plane.service_execution import reserve_trial_execution

_LOG = logging.getLogger(__name__)
_RESERVATION_REQUEST_NAMESPACE = UUID("aaf78d09-4268-4dc5-81ee-4c2408ce2611")

_NEXT_SERVICE_TRIAL = text("""
SELECT t.id,
       t.attempt_count,
       task_definition.checksum AS task_checksum,
       task_definition.config AS task_config
  FROM trials t
  JOIN batches b ON b.id = t.batch_id
  JOIN tasks task_definition ON task_definition.id = t.task_id
  JOIN team_quotas q ON q.team_id = t.team_id
 WHERE t.state = 'queued'
   AND t.attempt_count < q.max_attempts_ceiling
   AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= :now)
   AND t.family_key IS NULL
   AND b.backend = 'nebius'
   AND task_definition.config ? 'service_execution'
   AND t.requires_caps->>'worker_pool' = :pool_id
   AND (
         t.execution_route_pool_name IS NULL
         OR t.execution_route_pool_name = :pool_id
       )
   AND NOT EXISTS (
         SELECT 1 FROM execution_leases lease
          WHERE lease.trial_id = t.id
            AND lease.execution_role = 'attempt'
            AND lease.revoked_at IS NULL
       )
 ORDER BY (q.in_flight_count::double precision / q.fair_share_weight) ASC,
          t.submit_priority DESC,
          t.submitted_at ASC,
          t.id ASC
 FOR UPDATE OF t SKIP LOCKED
 LIMIT 1
""")


def _task_revision(checksum: str) -> str:
    value = str(checksum).lower()
    if value.startswith("sha256:"):
        value = value.removeprefix("sha256:")
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("service-execution task checksum is not sha256")
    return "sha256:" + value


def _deadline(
    plan: ExecutionRuntimePlanV1,
    *,
    now: datetime,
    maximum_seconds: int,
) -> datetime:
    phase_seconds = sum(item.timeout_seconds for item in plan.setup) + plan.main.timeout_seconds
    if plan.verifier is not None:
        phase_seconds += plan.verifier.timeout_seconds
    requested_seconds = phase_seconds + plan.termination_grace_seconds + 600
    if requested_seconds > maximum_seconds:
        raise ValueError("service-execution runtime exceeds the scheduler deadline bound")
    return now + timedelta(seconds=requested_seconds)


async def _ready_target(
    session: AsyncSession,
    *,
    environment: str,
    pool_id: str,
    execution_class_id: str,
    now: datetime,
) -> ServiceExecutionTarget | None:
    targets = (
        (
            await session.execute(
                select(ServiceExecutionTarget)
                .where(
                    ServiceExecutionTarget.environment == environment,
                    ServiceExecutionTarget.logical_pool_id == pool_id,
                    ServiceExecutionTarget.execution_class_id == execution_class_id,
                    ServiceExecutionTarget.desired_state == "active",
                    ServiceExecutionTarget.observed_state == "ready",
                    ServiceExecutionTarget.health_status == "healthy",
                )
                .order_by(ServiceExecutionTarget.region, ServiceExecutionTarget.id)
            )
        )
        .scalars()
        .all()
    )
    for target in targets:
        if target.health_observed_at is None:
            continue
        stale_after = int(target.spec_json["health_stale_after_seconds"])
        if target.health_observed_at + timedelta(seconds=stale_after) > now:
            return target
    return None


async def reserve_next_service_execution(
    session: AsyncSession,
    *,
    environment: str,
    pool_id: str,
    image_admission_keyring: ImageAdmissionKeyring,
    maximum_deadline_seconds: int = 7200,
    now: datetime | None = None,
) -> ServiceExecutionLease | None:
    """Reserve one normally queued, explicitly converted service task."""

    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    row = (
        (await session.execute(_NEXT_SERVICE_TRIAL, {"now": current_time, "pool_id": pool_id}))
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    task = TaskConfig.model_validate(row["task_config"])
    binding = task.service_execution
    if binding is None or binding.logical_pool_id != pool_id:
        raise ValueError("queued service-execution task binding drift")
    task_revision = _task_revision(row["task_checksum"])
    runtime_plan = bind_service_execution_runtime_plan(
        binding.runtime_template,
        task_revision_sha256=task_revision,
    )
    target = await _ready_target(
        session,
        environment=environment,
        pool_id=pool_id,
        execution_class_id=runtime_plan.execution_class_id,
        now=current_time,
    )
    if target is None:
        return None
    request_id = canonical_uuid5(
        _RESERVATION_REQUEST_NAMESPACE,
        {
            "schema_version": "loom.service-execution-reservation-request.v1",
            "trial_id": str(row["id"]),
            "attempt": int(row["attempt_count"]) + 1,
            "target_id": target.id,
            "task_revision_sha256": task_revision,
            "runtime_contract_sha256": canonical_digest(runtime_plan.canonical_payload()),
        },
    )
    return await reserve_trial_execution(
        session,
        request_id=request_id,
        trial_id=row["id"],
        execution_class_id=runtime_plan.execution_class_id,
        target_id=target.id,
        requirements=workload_requirements_from_task(task),
        runtime_contract=runtime_plan,
        image_admission_keyring=image_admission_keyring,
        routing_reason=ExecutionRoutingReason.PREEXISTING_ASSIGNMENT,
        deadline_at=_deadline(
            runtime_plan,
            now=current_time,
            maximum_seconds=maximum_deadline_seconds,
        ),
        now=current_time,
    )


async def run_service_execution_scheduler_loop(
    *,
    session_factory: Any,
    environment: str,
    pool_id: str,
    image_admission_keyring: ImageAdmissionKeyring,
    interval_seconds: float,
    maximum_deadline_seconds: int,
) -> None:
    """Continuously reserve converted service tasks; cancellation stops the loop."""

    while True:
        try:
            async with session_factory() as session:
                lease = await reserve_next_service_execution(
                    session,
                    environment=environment,
                    pool_id=pool_id,
                    image_admission_keyring=image_admission_keyring,
                    maximum_deadline_seconds=maximum_deadline_seconds,
                )
                await session.commit()
            if lease is not None:
                _LOG.info(
                    "service_execution_reserved",
                    extra={"target_id": lease.target_id, "pool_id": lease.selected_pool_id},
                )
                continue
        except asyncio.CancelledError:
            return
        except Exception as exc:
            _LOG.warning("service_execution_scheduler_error", extra={"err": str(exc)})
        await asyncio.sleep(interval_seconds)


__all__ = ["reserve_next_service_execution", "run_service_execution_scheduler_loop"]
