"""One-shot fenced reconciliation for non-executable capacity shadow epochs."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_manager.allocator import (
    ShadowAllocatorError,
    allocate_shadow,
)
from loom_capacity_manager.contracts import (
    AllocationInputV1,
    CapacityContractError,
    ShadowEpochV1,
    canonical_digest,
)
from loom_capacity_manager.store import (
    CapacityManagementStore,
    StaleAllocationInputError,
    StaleWriterError,
    WriterFence,
)

AllocatorResult = ShadowEpochV1 | Awaitable[ShadowEpochV1]
ShadowAllocator = Callable[[AllocationInputV1], AllocatorResult]


@dataclass(frozen=True, slots=True)
class ShadowRunResult:
    status: Literal["committed", "input-contention", "failed"]
    allocation_epoch: int | None
    input_digest: str
    reason: str | None
    attempt_count: int


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
        recorded = await store.record_shadow_failure(
            session,
            writer,
            event_kind=event_kind,
            reason=reason,
            expected_input_digest=input_digest,
            persist_failed_epoch=persist_failed_epoch,
        )
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
            try:
                allocation_epoch = await _record_failure(
                    session_factory,
                    resolved_store,
                    writer,
                    event_kind="shadow_allocation_invalid",
                    reason=reason,
                    input_digest=None,
                )
            except StaleWriterError:
                allocation_epoch = None
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
            try:
                allocation_epoch = await _record_failure(
                    session_factory,
                    resolved_store,
                    writer,
                    event_kind="shadow_allocation_timeout",
                    reason=reason,
                    input_digest=last_input_digest,
                )
            except StaleAllocationInputError:
                continue
            except StaleWriterError:
                allocation_epoch = None
            return ShadowRunResult(
                status="failed",
                allocation_epoch=allocation_epoch,
                input_digest=last_input_digest,
                reason=reason,
                attempt_count=attempt_count,
            )
        except (ShadowAllocatorError, CapacityContractError, ArithmeticError, ValueError):
            reason = "shadow allocator rejected the complete global input"
            try:
                allocation_epoch = await _record_failure(
                    session_factory,
                    resolved_store,
                    writer,
                    event_kind="shadow_allocation_invalid",
                    reason=reason,
                    input_digest=last_input_digest,
                )
            except StaleAllocationInputError:
                continue
            except StaleWriterError:
                allocation_epoch = None
            return ShadowRunResult(
                status="failed",
                allocation_epoch=allocation_epoch,
                input_digest=last_input_digest,
                reason=reason,
                attempt_count=attempt_count,
            )
        except Exception:
            reason = "shadow allocator failed unexpectedly"
            try:
                allocation_epoch = await _record_failure(
                    session_factory,
                    resolved_store,
                    writer,
                    event_kind="shadow_allocation_failure",
                    reason=reason,
                    input_digest=last_input_digest,
                )
            except StaleAllocationInputError:
                continue
            except StaleWriterError:
                allocation_epoch = None
            return ShadowRunResult(
                status="failed",
                allocation_epoch=allocation_epoch,
                input_digest=last_input_digest,
                reason=reason,
                attempt_count=attempt_count,
            )

        try:
            async with session_factory() as session:
                committed = await resolved_store.commit_shadow_epoch(
                    session,
                    writer,
                    epoch,
                )
            return ShadowRunResult(
                status="committed",
                allocation_epoch=committed.allocation_epoch,
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
            try:
                allocation_epoch = await _record_failure(
                    session_factory,
                    resolved_store,
                    writer,
                    event_kind="shadow_allocation_failure",
                    reason=reason,
                    input_digest=last_input_digest,
                )
            except StaleAllocationInputError:
                continue
            except Exception:
                allocation_epoch = None
            return ShadowRunResult(
                status="failed",
                allocation_epoch=allocation_epoch,
                input_digest=last_input_digest,
                reason=reason,
                attempt_count=attempt_count,
            )

    reason = "allocation input changed during every reconciliation attempt"
    try:
        await _record_failure(
            session_factory,
            resolved_store,
            writer,
            event_kind="shadow_allocation_input_contention",
            reason=reason,
            input_digest=None,
            persist_failed_epoch=False,
        )
    except StaleWriterError:
        pass
    return ShadowRunResult(
        status="input-contention",
        allocation_epoch=None,
        input_digest=last_input_digest,
        reason=reason,
        attempt_count=max_attempts,
    )


__all__ = ["ShadowAllocator", "ShadowRunResult", "reconcile_shadow_once"]
