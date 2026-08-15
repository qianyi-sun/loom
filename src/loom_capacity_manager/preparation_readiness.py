"""Read-only, database-derived readiness for a zero-ceiling execution epoch."""

from __future__ import annotations

import json
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Literal, cast
from uuid import UUID

from pydantic import field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.contracts import Digest, Identifier, PositiveQuantity, Quantity
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorInventoryV2,
    ExecutionContextV2,
    ExecutionPreparationPolicyV2,
    ExecutionPreparationV2,
    PreparedExecutorBindingV2,
    StrictV2Model,
    canonical_executable_digest,
    canonical_inventory_confirmation_journal_head,
)
from loom_capacity_manager.models import (
    CapacityAuthorityState,
    CapacityCandidate,
    CapacityConfigurationEpoch,
    CapacityExecutableExecutorState,
    CapacityExecutionEpoch,
    CapacityExecutionExecutor,
    CapacitySubject,
)

PreparedReadinessBlocker = Literal[
    "execution-policy-disabled",
    "manager-shadow",
    "manager-not-prepared",
    "nonzero-executable-ceiling",
    "increase-freeze-missing",
    "subject-acknowledgements-incomplete",
    "executor-registration-missing",
    "executor-binding-changed",
    "executor-lease-expired",
    "executor-inventory-missing",
    "executor-inventory-invalid",
    "executor-inventory-stale",
    "executor-post-inventory-heartbeat-missing",
    "executor-inventory-foreign",
    "executor-inventory-unknown",
    "executor-inventory-quarantined",
    "executor-inventory-ownership-missing",
]

_DIGEST = re.compile(r"[0-9a-f]{64}")


def _canonical_blockers(
    blockers: list[PreparedReadinessBlocker],
) -> tuple[PreparedReadinessBlocker, ...]:
    return tuple(sorted(set(blockers)))


class PreparedExecutorReadinessV1(StrictV2Model):
    """Bounded readiness evidence for one expected controller-local executor."""

    pool_id: Literal["gb10", "oldlab"]
    expected_executor_id: Identifier
    expected_executor_incarnation: UUID
    expected_pool_generation: PositiveQuantity
    registered: bool
    registered_executor_id: Identifier | None
    registered_executor_incarnation: UUID | None
    registered_pool_generation: PositiveQuantity | None
    current: bool
    lease_expires_at: datetime | None
    lease_fresh: bool
    last_heartbeat_at: datetime | None
    heartbeat_sequence: Quantity
    journal_sequence: Quantity
    journal_digest: Digest
    inventory_sequence: Quantity
    inventory_digest: Digest | None
    inventory_observed_at: datetime | None
    inventory_fresh: bool
    post_inventory_heartbeat: bool
    inventory_record_count: Quantity
    foreign_record_count: Quantity
    unknown_record_count: Quantity
    ownership_missing_record_count: Quantity
    quarantined_record_count: Quantity
    blockers: tuple[PreparedReadinessBlocker, ...]

    @field_validator("blockers")
    @classmethod
    def _blockers_are_canonical(
        cls,
        value: tuple[PreparedReadinessBlocker, ...],
    ) -> tuple[PreparedReadinessBlocker, ...]:
        if value != tuple(sorted(set(value))):
            raise ValueError("prepared executor readiness blockers are not canonical")
        return value


class PreparedExecutionReadinessV2(StrictV2Model):
    """Canonical proof that a prepared epoch is safe to rehearse at zero."""

    ready: bool
    policy_mode: Literal["disabled", "pinned"]
    policy_sha256: Digest | None
    execution: ExecutionContextV2 | None
    expected_subject_count: Quantity
    acknowledged_subject_count: Quantity
    executors: tuple[PreparedExecutorReadinessV1, ...]
    blockers: tuple[PreparedReadinessBlocker, ...]
    executable: Literal[False] = False

    @field_validator("executors")
    @classmethod
    def _executors_are_canonical(
        cls,
        value: tuple[PreparedExecutorReadinessV1, ...],
    ) -> tuple[PreparedExecutorReadinessV1, ...]:
        pools = tuple(item.pool_id for item in value)
        if pools not in {(), ("gb10", "oldlab")}:
            raise ValueError("prepared executor readiness is not in canonical pool order")
        return value

    @field_validator("blockers")
    @classmethod
    def _blockers_are_canonical(
        cls,
        value: tuple[PreparedReadinessBlocker, ...],
    ) -> tuple[PreparedReadinessBlocker, ...]:
        if value != tuple(sorted(set(value))):
            raise ValueError("prepared execution readiness blockers are not canonical")
        return value


@asynccontextmanager
async def _read_transaction(session: AsyncSession) -> AsyncIterator[None]:
    transaction = session.begin_nested() if session.in_transaction() else session.begin()
    async with transaction:
        yield


async def _database_now(session: AsyncSession) -> datetime:
    value = cast(datetime, (await session.execute(select(func.clock_timestamp()))).scalar_one())
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _execution_context(
    authority: CapacityAuthorityState,
    epoch: CapacityExecutionEpoch,
) -> ExecutionContextV2 | None:
    try:
        return ExecutionContextV2(
            authority_incarnation=authority.authority_incarnation,
            writer_epoch=authority.writer_epoch,
            configuration_epoch=epoch.configuration_epoch,
            execution_epoch=epoch.execution_epoch,
            execution_manifest_sha256=epoch.execution_manifest_sha256,
            execution_state=cast(
                Literal["prepared", "active", "drain-only"],
                epoch.state,
            ),
            executable_new_capacity_ceiling=epoch.effective_ceiling,
            executable_new_capacity_rate_per_minute=epoch.effective_rate_per_minute,
            trusted_fleet_release_sha256=epoch.trusted_fleet_release_sha256,
        )
    except ValueError:
        return None


def _binding_matches(
    registration: CapacityExecutionExecutor,
    expected: PreparedExecutorBindingV2,
) -> bool:
    return bool(
        registration.executor_id == expected.executor_id
        and registration.executor_incarnation == expected.executor_incarnation
        and registration.pool_id == expected.pool_id
        and registration.pool_generation == expected.pool_generation
        and registration.signing_key_sha256 == expected.signing_key_sha256
        and registration.local_authority_sha256 == expected.local_authority_sha256
        and registration.controller_authority_sha256 == expected.controller_authority_sha256
    )


def _runtime_matches(
    runtime: CapacityExecutableExecutorState,
    registration: CapacityExecutionExecutor,
) -> bool:
    return bool(
        runtime.execution_epoch == registration.execution_epoch
        and runtime.execution_manifest_sha256 == registration.execution_manifest_sha256
        and runtime.executor_id == registration.executor_id
        and runtime.executor_incarnation == registration.executor_incarnation
        and runtime.pool_id == registration.pool_id
        and runtime.pool_generation == registration.pool_generation
    )


def _validated_inventory(
    runtime: CapacityExecutableExecutorState,
    execution: ExecutionContextV2,
) -> tuple[ExecutableExecutorInventoryV2 | None, bool]:
    if runtime.inventory_payload is None or runtime.last_inventory_digest is None:
        return None, False
    try:
        inventory = ExecutableExecutorInventoryV2.model_validate_json(
            json.dumps(runtime.inventory_payload)
        )
    except ValueError:
        return None, False
    confirmation_sequence, confirmation_digest = canonical_inventory_confirmation_journal_head(
        inventory
    )
    if (
        inventory.execution != execution
        or inventory.executor_id != runtime.executor_id
        or inventory.executor_incarnation != runtime.executor_incarnation
        or inventory.pool_id != runtime.pool_id
        or inventory.pool_generation != runtime.pool_generation
        or inventory.inventory_sequence != runtime.inventory_high_water
        or canonical_executable_digest(inventory) != runtime.last_inventory_digest
    ):
        return None, False
    journal_confirmed = bool(
        runtime.journal_high_water == confirmation_sequence
        and runtime.journal_digest == confirmation_digest
        and runtime.inventory_confirmation_journal_digest == confirmation_digest
    )
    return inventory, journal_confirmed


async def _subject_readiness(
    session: AsyncSession,
    *,
    epoch: CapacityExecutionEpoch,
    preparation: ExecutionPreparationV2 | None,
    execution_policy: ExecutionPreparationPolicyV2,
) -> tuple[int, int]:
    configuration = (
        await session.execute(
            select(CapacityConfigurationEpoch).where(
                CapacityConfigurationEpoch.configuration_epoch == epoch.configuration_epoch
            )
        )
    ).scalar_one_or_none()
    subjects = (
        (
            await session.execute(
                select(CapacitySubject)
                .where(CapacitySubject.configuration_epoch == epoch.configuration_epoch)
                .order_by(CapacitySubject.subject_id)
            )
        )
        .scalars()
        .all()
    )
    expected_count = len(subjects)
    if preparation is None or configuration is None:
        return expected_count, 0
    if (
        configuration.fleet_generation != epoch.fleet_generation
        or configuration.fleet_digest != epoch.fleet_digest
        or preparation.configuration_epoch != epoch.configuration_epoch
        or preparation.fleet_generation != epoch.fleet_generation
        or preparation.fleet_digest != epoch.fleet_digest
        or preparation.subject_acknowledgements != execution_policy.subject_acknowledgements
        or len(preparation.subject_acknowledgements) != expected_count
    ):
        return expected_count, 0
    acknowledgements = {
        acknowledgement.subject_id: acknowledgement
        for acknowledgement in preparation.subject_acknowledgements
    }
    acknowledged_count = 0
    for subject in subjects:
        acknowledgement = acknowledgements.get(subject.subject_id)
        if acknowledgement is None or (
            acknowledgement.subject_incarnation != subject.subject_incarnation
            or acknowledgement.configuration_generation != subject.configuration_generation
            or acknowledgement.deployment_generation != subject.deployment_generation
            or acknowledgement.reporter_incarnation != subject.demand_reporter_incarnation
        ):
            continue
        candidate = (
            await session.execute(
                select(CapacityCandidate).where(
                    CapacityCandidate.subject_id == subject.subject_id,
                    CapacityCandidate.subject_incarnation == subject.subject_incarnation,
                    CapacityCandidate.candidate_generation == subject.candidate_generation,
                )
            )
        ).scalar_one_or_none()
        if candidate is None or (
            candidate.candidate_identity_algorithm != acknowledgement.candidate.algorithm
            or candidate.candidate_identity != acknowledgement.candidate.identity
            or candidate.source_payload.get("publication_sha256")
            != acknowledgement.candidate.publication_sha256
        ):
            continue
        acknowledged_count += 1
    return expected_count, acknowledged_count


def _missing_executor(expected: PreparedExecutorBindingV2) -> PreparedExecutorReadinessV1:
    return PreparedExecutorReadinessV1(
        pool_id=expected.pool_id,
        expected_executor_id=expected.executor_id,
        expected_executor_incarnation=expected.executor_incarnation,
        expected_pool_generation=expected.pool_generation,
        registered=False,
        registered_executor_id=None,
        registered_executor_incarnation=None,
        registered_pool_generation=None,
        current=False,
        lease_expires_at=None,
        lease_fresh=False,
        last_heartbeat_at=None,
        heartbeat_sequence=0,
        journal_sequence=0,
        journal_digest="0" * 64,
        inventory_sequence=0,
        inventory_digest=None,
        inventory_observed_at=None,
        inventory_fresh=False,
        post_inventory_heartbeat=False,
        inventory_record_count=0,
        foreign_record_count=0,
        unknown_record_count=0,
        ownership_missing_record_count=0,
        quarantined_record_count=0,
        blockers=("executor-registration-missing",),
    )


def _executor_readiness(
    *,
    expected: PreparedExecutorBindingV2,
    registration: CapacityExecutionExecutor,
    runtime: CapacityExecutableExecutorState | None,
    execution: ExecutionContextV2,
    now: datetime,
    freshness: timedelta,
) -> PreparedExecutorReadinessV1:
    blockers: list[PreparedReadinessBlocker] = []
    binding_matches = _binding_matches(registration, expected)
    if not binding_matches:
        blockers.append("executor-binding-changed")
    current = bool(
        runtime is not None
        and runtime.state == "current"
        and _runtime_matches(runtime, registration)
    )
    if runtime is not None and not current:
        blockers.append("executor-binding-changed")
    lease_fresh = bool(current and runtime is not None and runtime.lease_expires_at > now)
    if not lease_fresh:
        blockers.append("executor-lease-expired")

    inventory: ExecutableExecutorInventoryV2 | None = None
    if runtime is None or runtime.inventory_high_water == 0:
        blockers.append("executor-inventory-missing")
    else:
        inventory, journal_confirmed = _validated_inventory(runtime, execution)
        if inventory is None or not journal_confirmed:
            blockers.append("executor-inventory-invalid")

    inventory_fresh = bool(
        inventory is not None
        and runtime is not None
        and runtime.last_inventory_at is not None
        and runtime.last_inventory_at + freshness > now
    )
    if inventory is not None and not inventory_fresh:
        blockers.append("executor-inventory-stale")
    post_inventory_heartbeat = bool(
        inventory is not None
        and runtime is not None
        and runtime.last_inventory_at is not None
        and runtime.last_heartbeat_at > runtime.last_inventory_at
    )
    if inventory is not None and not post_inventory_heartbeat:
        blockers.append("executor-post-inventory-heartbeat-missing")

    records = () if inventory is None else inventory.records
    foreign_count = sum(record.authority_scope == "foreign" for record in records)
    unknown_count = sum(record.state == "unknown" for record in records)
    ownership_missing_count = sum(
        record.authority_scope != "foreign" and record.ownership_proof is None for record in records
    )
    # A prepared epoch cannot legitimately own physical work: executable
    # intents are prohibited until activation.  Preserve the more specific
    # classifications below, while quarantining every nonempty observation.
    quarantined_count = len(records)
    if foreign_count:
        blockers.append("executor-inventory-foreign")
    if unknown_count:
        blockers.append("executor-inventory-unknown")
    if ownership_missing_count:
        blockers.append("executor-inventory-ownership-missing")
    if quarantined_count:
        blockers.append("executor-inventory-quarantined")

    return PreparedExecutorReadinessV1(
        pool_id=expected.pool_id,
        expected_executor_id=expected.executor_id,
        expected_executor_incarnation=expected.executor_incarnation,
        expected_pool_generation=expected.pool_generation,
        registered=True,
        registered_executor_id=registration.executor_id,
        registered_executor_incarnation=registration.executor_incarnation,
        registered_pool_generation=registration.pool_generation,
        current=current,
        lease_expires_at=None if runtime is None else runtime.lease_expires_at,
        lease_fresh=lease_fresh,
        last_heartbeat_at=None if runtime is None else runtime.last_heartbeat_at,
        heartbeat_sequence=0 if runtime is None else runtime.heartbeat_high_water,
        journal_sequence=0 if runtime is None else runtime.journal_high_water,
        journal_digest="0" * 64 if runtime is None else runtime.journal_digest,
        inventory_sequence=0 if runtime is None else runtime.inventory_high_water,
        inventory_digest=None if runtime is None else runtime.last_inventory_digest,
        inventory_observed_at=None if runtime is None else runtime.last_inventory_at,
        inventory_fresh=inventory_fresh,
        post_inventory_heartbeat=post_inventory_heartbeat,
        inventory_record_count=len(records),
        foreign_record_count=foreign_count,
        unknown_record_count=unknown_count,
        ownership_missing_record_count=ownership_missing_count,
        quarantined_record_count=quarantined_count,
        blockers=_canonical_blockers(blockers),
    )


async def load_prepared_execution_readiness(
    session: AsyncSession,
    *,
    execution_policy: ExecutionPreparationPolicyV2 | None,
    execution_policy_sha256: str | None,
    freshness_seconds: int,
) -> PreparedExecutionReadinessV2:
    """Load one bounded readiness snapshot without credentials or scheduler access."""

    if type(freshness_seconds) is not int or freshness_seconds <= 0:
        raise ValueError("prepared readiness freshness must be a positive integer")
    if (execution_policy is None) != (execution_policy_sha256 is None):
        raise ValueError("prepared readiness policy and digest must be configured together")
    if execution_policy_sha256 is not None and (
        _DIGEST.fullmatch(execution_policy_sha256) is None or execution_policy_sha256 == "0" * 64
    ):
        raise ValueError("prepared readiness policy digest is invalid")

    blockers: list[PreparedReadinessBlocker] = []
    if execution_policy is None:
        blockers.append("execution-policy-disabled")
    async with _read_transaction(session):
        now = await _database_now(session)
        authority = (
            await session.execute(
                select(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .with_for_update(read=True)
            )
        ).scalar_one()
        if authority.execution_state == "shadow":
            blockers.append("manager-shadow")
        elif authority.execution_state != "prepared":
            blockers.append("manager-not-prepared")
        if authority.executable_new_capacity_ceiling != 0:
            blockers.append("nonzero-executable-ceiling")
        if not authority.increase_freeze:
            blockers.append("increase-freeze-missing")

        epoch: CapacityExecutionEpoch | None = None
        execution: ExecutionContextV2 | None = None
        preparation: ExecutionPreparationV2 | None = None
        if authority.execution_epoch > 0:
            epoch = (
                await session.execute(
                    select(CapacityExecutionEpoch)
                    .where(CapacityExecutionEpoch.execution_epoch == authority.execution_epoch)
                    .with_for_update(read=True)
                )
            ).scalar_one_or_none()
        if epoch is not None:
            execution = _execution_context(authority, epoch)
            if (
                execution is None
                or execution.execution_state != "prepared"
                or authority.execution_manifest_sha256 != epoch.execution_manifest_sha256
                or authority.writer_epoch != epoch.current_writer_epoch
                or epoch.effective_ceiling != 0
                or epoch.effective_rate_per_minute != 0
            ):
                blockers.append("manager-not-prepared")
                execution = None
            try:
                preparation = ExecutionPreparationV2.model_validate_json(
                    json.dumps(epoch.manifest_payload)
                )
            except ValueError:
                blockers.append("manager-not-prepared")
            if preparation is not None and (
                canonical_executable_digest(preparation) != epoch.execution_manifest_sha256
            ):
                blockers.append("manager-not-prepared")
        elif authority.execution_state != "shadow":
            blockers.append("manager-not-prepared")

        expected_subject_count = 0
        acknowledged_subject_count = 0
        executor_items: tuple[PreparedExecutorReadinessV1, ...] = ()
        if epoch is not None and execution_policy is not None:
            expected_subject_count, acknowledged_subject_count = await _subject_readiness(
                session,
                epoch=epoch,
                preparation=preparation,
                execution_policy=execution_policy,
            )
            if acknowledged_subject_count != expected_subject_count:
                blockers.append("subject-acknowledgements-incomplete")
            if preparation is None or (
                preparation.trusted_fleet_release_sha256
                != execution_policy.trusted_fleet_release_sha256
                or preparation.requested_ceiling != execution_policy.executable_new_capacity_ceiling
                or preparation.requested_rate_per_minute
                != execution_policy.executable_new_capacity_rate_per_minute
                or preparation.rollback_evidence_sha256 != execution_policy.rollback_evidence_sha256
                or preparation.legacy_writer_fences != execution_policy.legacy_writer_fences
            ):
                blockers.append("manager-not-prepared")
            if preparation is None or preparation.executors != execution_policy.executors:
                blockers.append("executor-binding-changed")
            controller_authorities = {
                item.pool_id: item.controller_authority_sha256
                for item in execution_policy.controller_authorities
            }
            if any(
                expected.controller_authority_sha256 != controller_authorities[expected.pool_id]
                for expected in execution_policy.executors
            ):
                blockers.append("executor-binding-changed")

            registrations = (
                (
                    await session.execute(
                        select(CapacityExecutionExecutor)
                        .where(CapacityExecutionExecutor.execution_epoch == epoch.execution_epoch)
                        .order_by(CapacityExecutionExecutor.pool_id)
                        .with_for_update(read=True)
                    )
                )
                .scalars()
                .all()
            )
            runtimes = (
                (
                    await session.execute(
                        select(CapacityExecutableExecutorState)
                        .where(
                            CapacityExecutableExecutorState.execution_epoch == epoch.execution_epoch
                        )
                        .order_by(CapacityExecutableExecutorState.pool_id)
                        .with_for_update(read=True)
                    )
                )
                .scalars()
                .all()
            )
            registrations_by_pool = {row.pool_id: row for row in registrations}
            runtimes_by_pool = {row.pool_id: row for row in runtimes}
            items: list[PreparedExecutorReadinessV1] = []
            for expected in execution_policy.executors:
                registration = registrations_by_pool.get(expected.pool_id)
                if registration is None:
                    item = _missing_executor(expected)
                elif execution is None:
                    item = _missing_executor(expected).model_copy(
                        update={
                            "registered": True,
                            "registered_executor_id": registration.executor_id,
                            "registered_executor_incarnation": registration.executor_incarnation,
                            "registered_pool_generation": registration.pool_generation,
                            "blockers": ("executor-binding-changed",),
                        }
                    )
                else:
                    item = _executor_readiness(
                        expected=expected,
                        registration=registration,
                        runtime=runtimes_by_pool.get(expected.pool_id),
                        execution=execution,
                        now=now,
                        freshness=timedelta(seconds=freshness_seconds),
                    )
                items.append(item)
                blockers.extend(item.blockers)
            executor_items = tuple(items)

    canonical_blockers = _canonical_blockers(blockers)
    return PreparedExecutionReadinessV2(
        ready=not canonical_blockers,
        policy_mode="disabled" if execution_policy is None else "pinned",
        policy_sha256=execution_policy_sha256,
        execution=execution,
        expected_subject_count=expected_subject_count,
        acknowledged_subject_count=acknowledged_subject_count,
        executors=executor_items,
        blockers=canonical_blockers,
    )


__all__ = [
    "PreparedExecutionReadinessV2",
    "PreparedExecutorReadinessV1",
    "PreparedReadinessBlocker",
    "load_prepared_execution_readiness",
]
