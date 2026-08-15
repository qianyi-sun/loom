"""One-shot fenced reconciliation with exact executable promotion."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_manager.allocator import (
    ExecutableEpochV2,
    ShadowAllocatorError,
    allocate_shadow,
    promote_shadow_epoch,
)
from loom_capacity_manager.contracts import (
    AllocationInputV1,
    CapacityContractError,
    ShadowEpochV1,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import ExecutionAuthorityV2
from loom_capacity_manager.models import (
    CapacityAllocation,
    CapacityAllocationEpoch,
    CapacityAuditEvent,
    CapacityAuthorityState,
    CapacityFairnessState,
)
from loom_capacity_manager.store import (
    CapacityManagementStore,
    CapacityStoreError,
    StaleAllocationInputError,
    StaleWriterError,
    WriterFence,
)

AllocatorResult = ShadowEpochV1 | Awaitable[ShadowEpochV1]
ShadowAllocator = Callable[[AllocationInputV1], AllocatorResult]
_EXECUTABLE_COMMIT_SAFETY_MARGIN = timedelta(seconds=1)


@dataclass(frozen=True, slots=True)
class ShadowRunResult:
    status: Literal["committed", "input-contention", "failed"]
    allocation_epoch: int | None
    input_digest: str
    reason: str | None
    attempt_count: int


class ReconciliationFailurePersistenceError(CapacityStoreError):
    """Failure evidence and its increase freeze could not be persisted."""


async def _commit_reconciled_epoch(
    session: AsyncSession,
    store: CapacityManagementStore,
    writer: WriterFence,
    shadow: ShadowEpochV1,
) -> tuple[int, ShadowEpochV1 | ExecutableEpochV2]:
    """Commit the fresh plan under the writer, input, and execution fences."""

    async with session.begin():
        connection = await session.connection()
        isolation_level = await connection.get_isolation_level()
        if isolation_level.upper() != "SERIALIZABLE":
            raise CapacityStoreError("capacity mutations require a SERIALIZABLE database session")
        authority_row = (
            await session.execute(
                select(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if authority_row is None:
            raise CapacityStoreError("capacity authority row is missing")
        if (
            authority_row.authority_incarnation != writer.authority_incarnation
            or authority_row.writer_epoch != writer.writer_epoch
        ):
            raise StaleWriterError("writer is no longer current")

        current_input = await store.load_allocation_input(session, writer)
        if canonical_digest(current_input) != shadow.input_digest:
            raise StaleAllocationInputError("allocation input changed before commit")
        if current_input.configuration != shadow.configuration:
            raise StaleAllocationInputError("configuration changed before commit")

        authority = await store.execution_authority(session)
        executable_authority = (
            authority
            if isinstance(authority, ExecutionAuthorityV2)
            and authority.execution_state == "active"
            and authority.executable_new_capacity_ceiling > 0
            and authority.executable_new_capacity_rate_per_minute > 0
            else None
        )
        if executable_authority is None:
            raise StaleAllocationInputError("active execution authority changed before commit")
        if (
            executable_authority.authority_incarnation != writer.authority_incarnation
            or executable_authority.writer_epoch != writer.writer_epoch
            or executable_authority.execution_epoch != authority_row.execution_epoch
            or executable_authority.execution_manifest_sha256
            != authority_row.execution_manifest_sha256
        ):
            raise StaleWriterError("execution fence changed before commit")
        if authority_row.increase_freeze:
            raise CapacityStoreError("capacity increases are frozen")

        now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        valid_until = store.allocation_input_valid_until(current_input)
        if valid_until is not None and now + _EXECUTABLE_COMMIT_SAFETY_MARGIN >= valid_until:
            raise StaleAllocationInputError("allocation input freshness expired before commit")
        if valid_until is None:
            raise StaleAllocationInputError(
                "allocation input freshness deadline is required for executable commit"
            )
        allocation_epoch = (
            await session.execute(
                text(
                    "SELECT nextval(pg_get_serial_sequence("
                    "'public.capacity_allocation_epochs', 'allocation_epoch'))"
                )
            )
        ).scalar_one()
        committed = promote_shadow_epoch(
            shadow,
            executable_authority,
            allocation_epoch=allocation_epoch,
        )
        row = CapacityAllocationEpoch(
            allocation_epoch=allocation_epoch,
            writer_epoch=writer.writer_epoch,
            configuration_epoch=shadow.configuration.configuration_epoch,
            input_digest=shadow.input_digest,
            status="executable",
            failure_reason=None,
            complete_payload=committed.model_dump(mode="json", exclude_none=False),
            executable=True,
            execution_epoch=executable_authority.execution_epoch,
            execution_manifest_sha256=executable_authority.execution_manifest_sha256,
            input_valid_until=valid_until,
            sealed=False,
            allocation_count=len(shadow.allocations),
            committed_at=now,
        )
        session.add(row)
        await session.flush()
        for allocation in shadow.allocations:
            session.add(
                CapacityAllocation(
                    allocation_epoch=row.allocation_epoch,
                    subject_id=allocation.subject_id,
                    subject_incarnation=allocation.subject_incarnation,
                    deployment_generation=allocation.deployment_generation,
                    pool_id=allocation.pool_id,
                    desired_shapes=[
                        item.model_dump(mode="json", exclude_none=False)
                        for item in allocation.desired_shapes
                    ],
                    desired_resources={},
                    commitments=[
                        match.model_dump(mode="json", exclude_none=False)
                        for match in allocation.claim_slot_matches
                    ],
                    drains=[{"shape_id": shape_id} for shape_id in allocation.draining_shape_ids],
                    allowances=[
                        allowance.model_dump(mode="json", exclude_none=False)
                        for allowance in allocation.placement_allowances
                    ],
                    witness=(
                        {}
                        if allocation.matching_witness is None
                        else allocation.matching_witness.model_dump(mode="json", exclude_none=False)
                    ),
                    mode="executable",
                    executable=True,
                    execution_epoch=executable_authority.execution_epoch,
                    execution_manifest_sha256=(executable_authority.execution_manifest_sha256),
                )
            )

        await session.flush()
        row.sealed = True
        await session.execute(delete(CapacityFairnessState))
        for cursor in shadow.next_fairness_cursors:
            session.add(
                CapacityFairnessState(
                    configuration_epoch=shadow.configuration.configuration_epoch,
                    mode="shadow",
                    scope="tier_account" if cursor.subject_id is None else "account_subject",
                    phase=cursor.phase,
                    tier_id=cursor.tier_id,
                    account_id=cursor.account_id,
                    subject_id=cursor.subject_id,
                    cursor_id=str(
                        cursor.subject_id if cursor.subject_id is not None else cursor.account_id
                    )
                    if (cursor.subject_id is not None or cursor.account_id is not None)
                    else None,
                    last_shadow_epoch=row.allocation_epoch,
                )
            )

        authority_row.increase_freeze = False
        authority_row.increase_freeze_reason = None
        authority_row.updated_at = now
        session.add(
            CapacityAuditEvent(
                actor_kind="manager",
                actor_id=str(writer.authority_incarnation),
                event_kind="capacity_executable_epoch_committed",
                object_binding={"allocation_epoch": row.allocation_epoch},
                detail={
                    "input_digest": shadow.input_digest,
                    "allocation_count": len(shadow.allocations),
                    "execution_epoch": row.execution_epoch,
                    "execution_manifest_sha256": row.execution_manifest_sha256,
                },
            )
        )
        await session.flush()
        await session.execute(
            text("SET CONSTRAINTS public.capacity_executable_allocation_seal_guard IMMEDIATE")
        )
        final_now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        final_valid_until = store.allocation_input_valid_until(current_input)
        if (
            final_valid_until is None
            or final_now + _EXECUTABLE_COMMIT_SAFETY_MARGIN >= final_valid_until
        ):
            raise StaleAllocationInputError(
                "allocation input freshness expired before durable commit"
            )
        return row.allocation_epoch, committed


def _is_async_callable(allocator: ShadowAllocator) -> bool:
    return inspect.iscoroutinefunction(allocator) or inspect.iscoroutinefunction(
        type(allocator).__call__
    )


async def _invoke_allocator(
    allocator: ShadowAllocator,
    value: AllocationInputV1,
) -> ShadowEpochV1:
    if _is_async_callable(allocator):
        result = allocator(value)
    else:
        result = await asyncio.to_thread(allocator, value)
    if inspect.isawaitable(result):
        result = await result
    if not isinstance(result, ShadowEpochV1):
        raise ShadowAllocatorError("allocator returned an invalid shadow epoch")
    return result


def _validate_complete_epoch(
    value: AllocationInputV1,
    epoch: ShadowEpochV1,
) -> None:
    expected_allocations = {
        (
            subject.configuration.subject_id,
            subject.configuration.subject_incarnation,
            subject.configuration.deployment_generation,
            profile.pool_id,
        )
        for subject in value.subjects
        for profile in subject.configuration.profiles
    }
    actual_allocations = {
        (
            allocation.subject_id,
            allocation.subject_incarnation,
            allocation.deployment_generation,
            allocation.pool_id,
        )
        for allocation in epoch.allocations
    }
    if actual_allocations != expected_allocations:
        raise ShadowAllocatorError("allocator output is missing a subject-pool allocation")

    expected_pools = {pool.pool_id for pool in value.fleet.pools}
    witnesses = {witness.pool_id: witness for witness in epoch.pool_witnesses}
    if set(witnesses) != expected_pools:
        raise ShadowAllocatorError("allocator output is missing a complete pool witness")
    for pool_id, witness in witnesses.items():
        expected_commitments = {
            commitment.commitment_id
            for commitment in value.observed_commitments
            if commitment.pool_id == pool_id and commitment.kind != "claim"
        }
        if not expected_commitments <= set(witness.charged_commitment_ids):
            raise ShadowAllocatorError("pool witness omits fixed capacity evidence")
    placement_ids = {
        placement.instance_id
        for witness in epoch.pool_witnesses
        for placement in witness.placements
    }
    if any(
        launch.shape_instance_id not in placement_ids for launch in epoch.hypothetical_launch_rank
    ):
        raise ShadowAllocatorError("hypothetical launch has no topology placement")


async def _record_failure(
    session_factory: async_sessionmaker[AsyncSession],
    store: CapacityManagementStore,
    writer: WriterFence,
    *,
    event_kind: Literal[
        "shadow_allocation_timeout",
        "shadow_allocation_invalid",
        "shadow_allocation_failure",
        "shadow_allocation_input_contention",
    ],
    reason: str,
    input_digest: str | None,
    persist_failed_epoch: bool = True,
) -> int | None:
    async with session_factory() as session:
        try:
            recorded = await store.record_reconcile_failure(
                session,
                writer,
                event_kind=event_kind,
                reason=reason,
                expected_input_digest=input_digest,
                persist_failed_epoch=persist_failed_epoch,
            )
        except Exception as exc:
            raise ReconciliationFailurePersistenceError(
                "failed to persist reconciliation failure evidence and increase freeze"
            ) from exc
        return recorded.allocation_epoch


async def reconcile_shadow_once(
    session_factory: async_sessionmaker[AsyncSession],
    writer: WriterFence,
    *,
    allocator: ShadowAllocator = allocate_shadow,
    max_attempts: int = 3,
    allocation_timeout_seconds: float = 1.0,
    store: CapacityManagementStore | None = None,
) -> ShadowRunResult:
    """Calculate and atomically commit one complete diagnostic allocation epoch."""

    if type(max_attempts) is not int or not 1 <= max_attempts <= 10:
        raise ValueError("max_attempts must be between 1 and 10")
    if (
        isinstance(allocation_timeout_seconds, bool)
        or not isinstance(allocation_timeout_seconds, (int, float))
        or not 0 < allocation_timeout_seconds <= 60
    ):
        raise ValueError("allocation_timeout_seconds must be between 0 and 60")
    resolved_store = store or CapacityManagementStore()
    last_input_digest = "0" * 64

    for attempt_count in range(1, max_attempts + 1):
        try:
            async with session_factory() as session:
                await resolved_store.execution_authority(session)
                allocation_input = await resolved_store.load_allocation_input(
                    session,
                    writer,
                )
            last_input_digest = canonical_digest(allocation_input)
        except StaleWriterError:
            return ShadowRunResult(
                status="failed",
                allocation_epoch=None,
                input_digest=last_input_digest,
                reason="capacity writer fence changed",
                attempt_count=attempt_count,
            )
        except Exception:
            reason = "capacity allocation input is invalid"
            allocation_epoch = await _record_failure(
                session_factory,
                resolved_store,
                writer,
                event_kind="shadow_allocation_invalid",
                reason=reason,
                input_digest=None,
            )
            return ShadowRunResult(
                status="failed",
                allocation_epoch=allocation_epoch,
                input_digest=last_input_digest,
                reason=reason,
                attempt_count=attempt_count,
            )

        try:
            async with asyncio.timeout(allocation_timeout_seconds):
                epoch = await _invoke_allocator(allocator, allocation_input)
            if epoch.input_digest != last_input_digest:
                raise ShadowAllocatorError("allocator output input digest is inconsistent")
            if epoch.configuration != allocation_input.configuration:
                raise ShadowAllocatorError("allocator output configuration is inconsistent")
            if epoch.executable or epoch.executable_new_capacity_ceiling != 0:
                raise ShadowAllocatorError("allocator output is not shadow-only")
            _validate_complete_epoch(allocation_input, epoch)
        except TimeoutError:
            reason = "shadow allocation exceeded its configured deadline"
            allocation_epoch = await _record_failure(
                session_factory,
                resolved_store,
                writer,
                event_kind="shadow_allocation_timeout",
                reason=reason,
                input_digest=last_input_digest,
            )
            return ShadowRunResult(
                status="failed",
                allocation_epoch=allocation_epoch,
                input_digest=last_input_digest,
                reason=reason,
                attempt_count=attempt_count,
            )
        except (ShadowAllocatorError, CapacityContractError, ArithmeticError, ValueError):
            reason = "shadow allocator rejected the complete global input"
            allocation_epoch = await _record_failure(
                session_factory,
                resolved_store,
                writer,
                event_kind="shadow_allocation_invalid",
                reason=reason,
                input_digest=last_input_digest,
            )
            return ShadowRunResult(
                status="failed",
                allocation_epoch=allocation_epoch,
                input_digest=last_input_digest,
                reason=reason,
                attempt_count=attempt_count,
            )
        except Exception:
            reason = "shadow allocator failed unexpectedly"
            allocation_epoch = await _record_failure(
                session_factory,
                resolved_store,
                writer,
                event_kind="shadow_allocation_failure",
                reason=reason,
                input_digest=last_input_digest,
            )
            return ShadowRunResult(
                status="failed",
                allocation_epoch=allocation_epoch,
                input_digest=last_input_digest,
                reason=reason,
                attempt_count=attempt_count,
            )

        try:
            async with session_factory() as session:
                execution = await resolved_store.execution_authority(session)
            if (
                isinstance(execution, ExecutionAuthorityV2)
                and execution.execution_state == "active"
            ):
                async with session_factory() as session:
                    allocation_epoch, committed = await _commit_reconciled_epoch(
                        session,
                        resolved_store,
                        writer,
                        epoch,
                    )
            else:
                async with session_factory() as session:
                    shadow_committed = await resolved_store.commit_shadow_epoch(
                        session,
                        writer,
                        epoch,
                    )
                allocation_epoch = shadow_committed.allocation_epoch
                committed = epoch
            return ShadowRunResult(
                status="committed",
                allocation_epoch=allocation_epoch,
                input_digest=committed.input_digest,
                reason=None,
                attempt_count=attempt_count,
            )
        except StaleAllocationInputError:
            continue
        except StaleWriterError:
            return ShadowRunResult(
                status="failed",
                allocation_epoch=None,
                input_digest=last_input_digest,
                reason="capacity writer fence changed",
                attempt_count=attempt_count,
            )
        except Exception:
            reason = "shadow epoch transaction failed"
            allocation_epoch = await _record_failure(
                session_factory,
                resolved_store,
                writer,
                event_kind="shadow_allocation_failure",
                reason=reason,
                input_digest=last_input_digest,
            )
            return ShadowRunResult(
                status="failed",
                allocation_epoch=allocation_epoch,
                input_digest=last_input_digest,
                reason=reason,
                attempt_count=attempt_count,
            )

    reason = "allocation input changed during every reconciliation attempt"
    await _record_failure(
        session_factory,
        resolved_store,
        writer,
        event_kind="shadow_allocation_input_contention",
        reason=reason,
        input_digest=None,
        persist_failed_epoch=False,
    )
    return ShadowRunResult(
        status="input-contention",
        allocation_epoch=None,
        input_digest=last_input_digest,
        reason=reason,
        attempt_count=max_attempts,
    )


__all__ = [
    "ReconciliationFailurePersistenceError",
    "ShadowAllocator",
    "ShadowRunResult",
    "reconcile_shadow_once",
]
