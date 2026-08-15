"""Durable executable-v2 reservation and pool work transitions."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, cast
from uuid import UUID, uuid5

from sqlalchemy import func, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.contracts import (
    MICROTOKENS_PER_LAUNCH,
    PackingRequestV1,
    PackingShapeRequestV1,
    PoolObservationV1,
    ResourceDomainV1,
    ResourceVectorV1,
    WorkerShapeV1,
    checked_add_vectors,
    checked_sum_vectors,
    vector_fits,
)
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableBootstrapAcknowledgementV2,
    ExecutableBootstrapProposalV2,
    ExecutableBootstrapRegistrationV2,
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorInventoryV2,
    ExecutableIntentBindingV2,
    ExecutableIntentCloseV2,
    ExecutableLaunchPermitV2,
    ExecutablePartialReleaseV2,
    ExecutablePermitConsumptionV2,
    ExecutableProtectedReleaseV2,
    ExecutableReleasedShapeV2,
    ExecutableReservationAcceptanceV2,
    ExecutableReservationProposalV2,
    ExecutableSubmissionRecoveryV2,
    ExecutionContextV2,
    ExecutionFenceV2,
    ExecutionPreparationV2,
    PreparedExecutorBindingV2,
    StrictV2Model,
    canonical_executable_digest,
)
from loom_capacity_manager.grant_contracts import ReservationShapeV1
from loom_capacity_manager.models import (
    CapacityAccountPolicy,
    CapacityAllocation,
    CapacityAllocationEpoch,
    CapacityAuthorityState,
    CapacityCandidate,
    CapacityDemandReporter,
    CapacityExecutableBootstrapAcknowledgement,
    CapacityExecutableBootstrapProposal,
    CapacityExecutableCommandReceipt,
    CapacityExecutableExecutorState,
    CapacityExecutableIntent,
    CapacityExecutableLaunchRateBucket,
    CapacityExecutableProtectedReleaseReceipt,
    CapacityExecutionEpoch,
    CapacityExecutionExecutor,
    CapacityPool,
    CapacityPoolObservation,
    CapacitySubject,
    CapacityTier,
    CapacityWorkerProfile,
)
from loom_capacity_manager.ownership import OwnershipKeyring
from loom_capacity_manager.store import CapacityStoreError, ExecutionConflictError
from loom_capacity_manager.topology import TopologyInfeasible, TopologySearchLimit, pack_topology

_EXECUTION_NAMESPACE = UUID("82e6e16b-6c44-4af2-894b-af8fbb3fead2")


class _ExecutorFenceError(RuntimeError):
    def __init__(
        self,
        executor_incarnation: UUID,
        state: Literal["fenced", "equivocal"],
        message: str,
    ) -> None:
        super().__init__(message)
        self.executor_incarnation = executor_incarnation
        self.state = state


@dataclass(frozen=True, slots=True)
class HeartbeatedExecutableExecutor:
    heartbeat_sequence: int
    lease_expires_at: datetime
    replayed: bool
    executable: Literal[True] = True


@dataclass(frozen=True, slots=True)
class IngestedExecutableInventory:
    inventory_sequence: int
    inventory_digest: str
    replayed: bool
    executable: Literal[True] = True


@dataclass(frozen=True, slots=True)
class ExecutableExecutorCheckpoint:
    execution_epoch: int
    execution_manifest_sha256: str
    executor_id: str
    executor_incarnation: UUID
    pool_id: str
    pool_generation: int
    command_sequence: int
    journal_sequence: int
    journal_digest: str
    inventory_sequence: int
    lease_expires_at: datetime
    executable: Literal[True] = True


@dataclass(frozen=True, slots=True)
class AcceptedExecutableReservation:
    tranche_id: UUID
    intent_ids: tuple[UUID, ...]
    receipt_digest: str
    replayed: bool
    executable: Literal[True] = True


@dataclass(frozen=True, slots=True)
class RegisteredExecutableBootstrap:
    intent_id: UUID
    bootstrap_registration_epoch: int
    receipt_digest: str
    replayed: bool
    executable: Literal[True] = True


@dataclass(frozen=True, slots=True)
class ProposedExecutableBootstrap:
    intent_id: UUID
    proposal_epoch: int
    receipt_digest: str
    replayed: bool
    executable: Literal[True] = True


@dataclass(frozen=True, slots=True)
class ConsumedExecutablePermit:
    permit_id: UUID
    intent_id: UUID
    receipt_digest: str
    replayed: bool
    executable: Literal[True] = True


@dataclass(frozen=True, slots=True)
class RecoveredExecutableSubmission:
    intent_id: UUID
    receipt_digest: str
    replayed: bool
    executable: Literal[True] = True


@dataclass(frozen=True, slots=True)
class AcknowledgedExecutableProtectedRelease:
    intent_id: UUID
    protected_release_sha256: str
    receipt_digest: str
    replayed: bool
    executable: Literal[True] = True


@dataclass(frozen=True, slots=True)
class ClosingExecutableIntent:
    intent_id: UUID
    receipt_digest: str
    replayed: bool
    executable: Literal[True] = True


@dataclass(frozen=True, slots=True)
class ReleasedExecutableShapes:
    tranche_id: UUID
    released_shape_ids: tuple[str, ...]
    receipt_digest: str
    replayed: bool
    executable: Literal[True] = True


@dataclass(frozen=True, slots=True)
class LockedExecutionContext:
    authority: CapacityAuthorityState
    epoch: CapacityExecutionEpoch
    registration: CapacityExecutionExecutor
    executor: CapacityExecutableExecutorState


@dataclass(frozen=True, slots=True)
class DurableLaunchDeadlines:
    permit_expires_at: datetime | None
    executor_lease_expires_at: datetime
    executor_inventory_fresh_until: datetime
    pool_inventory_fresh_until: datetime
    allocation_input_valid_until: datetime


def _payload_digest(payload: object) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


@asynccontextmanager
async def _write_transaction(session: AsyncSession) -> AsyncIterator[None]:
    transaction = session.begin_nested() if session.in_transaction() else session.begin()
    try:
        async with transaction:
            connection = await session.connection()
            if (await connection.get_isolation_level()).upper() != "SERIALIZABLE":
                raise CapacityStoreError(
                    "capacity mutations require a SERIALIZABLE database session"
                )
            yield
    except _ExecutorFenceError as exc:
        recovery = session.begin_nested() if session.in_transaction() else session.begin()
        async with recovery:
            await session.execute(
                update(CapacityExecutableExecutorState)
                .where(
                    CapacityExecutableExecutorState.executor_incarnation == exc.executor_incarnation
                )
                .values(state=exc.state)
            )
        raise ExecutionConflictError(str(exc)) from exc
    except DBAPIError as exc:
        if getattr(exc.orig, "sqlstate", None) in {"40001", "40P01"}:
            raise CapacityStoreError("serializable capacity transaction must be retried") from exc
        raise


async def _database_now(session: AsyncSession) -> datetime:
    value = cast(datetime, (await session.execute(select(func.clock_timestamp()))).scalar_one())
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _context_matches(
    authority: CapacityAuthorityState,
    epoch: CapacityExecutionEpoch,
    execution: ExecutionContextV2,
) -> bool:
    return bool(
        authority.authority_incarnation == execution.authority_incarnation
        and authority.writer_epoch == execution.writer_epoch
        and authority.execution_epoch == execution.execution_epoch
        and authority.execution_state == execution.execution_state
        and authority.execution_manifest_sha256 == execution.execution_manifest_sha256
        and authority.executable_new_capacity_ceiling == execution.executable_new_capacity_ceiling
        and epoch.configuration_epoch == execution.configuration_epoch
        and epoch.execution_epoch == execution.execution_epoch
        and epoch.execution_manifest_sha256 == execution.execution_manifest_sha256
        and epoch.state == execution.execution_state
        and epoch.effective_rate_per_minute == execution.executable_new_capacity_rate_per_minute
        and epoch.trusted_fleet_release_sha256 == execution.trusted_fleet_release_sha256
    )


def _retained_drain_context_matches(
    authority: CapacityAuthorityState,
    epoch: CapacityExecutionEpoch,
    execution: ExecutionContextV2,
) -> bool:
    """Allow monotonic drain receipts to retain their original active fence."""

    return bool(
        authority.execution_state == "drain-only"
        and authority.executable_new_capacity_ceiling == 0
        and epoch.state == "drain-only"
        and epoch.effective_ceiling == 0
        and epoch.effective_rate_per_minute == 0
        and authority.authority_incarnation == execution.authority_incarnation
        and execution.writer_epoch == epoch.prepared_writer_epoch
        and authority.execution_epoch == execution.execution_epoch
        and authority.execution_manifest_sha256 == execution.execution_manifest_sha256
        and execution.execution_state == "active"
        and execution.executable_new_capacity_ceiling == epoch.requested_ceiling
        and execution.executable_new_capacity_rate_per_minute == epoch.requested_rate_per_minute
        and epoch.configuration_epoch == execution.configuration_epoch
        and epoch.execution_epoch == execution.execution_epoch
        and epoch.execution_manifest_sha256 == execution.execution_manifest_sha256
        and epoch.trusted_fleet_release_sha256 == execution.trusted_fleet_release_sha256
    )


class CapacityExecutionStore:
    """Own executable-v2 work without widening the dry-run-v1 ledger."""

    def __init__(
        self,
        *,
        executor_lease_seconds: int = 120,
        permit_ttl_seconds: int = 15,
        inventory_freshness_seconds: int = 120,
        ownership_keyring: OwnershipKeyring | None = None,
    ) -> None:
        if not 1 <= executor_lease_seconds <= 3_600:
            raise ValueError("executor lease must be between 1 and 3600 seconds")
        if not 1 <= permit_ttl_seconds <= 300:
            raise ValueError("permit ttl must be between 1 and 300 seconds")
        if not 1 <= inventory_freshness_seconds <= 3_600:
            raise ValueError("inventory freshness must be between 1 and 3600 seconds")
        self._executor_lease = timedelta(seconds=executor_lease_seconds)
        self._permit_ttl = timedelta(seconds=permit_ttl_seconds)
        self._inventory_freshness = timedelta(seconds=inventory_freshness_seconds)
        self._ownership_keyring = ownership_keyring or OwnershipKeyring()

    @staticmethod
    def contract_digest(contract: StrictV2Model) -> str:
        return canonical_executable_digest(contract)

    async def executor_checkpoint(
        self,
        session: AsyncSession,
        executor: PreparedExecutorBindingV2,
    ) -> ExecutableExecutorCheckpoint:
        authority = await self._lock_authority(session)
        if authority.execution_state == "shadow":
            raise ExecutionConflictError("execution authority is shadow-only")
        epoch = await self._lock_current_epoch(session, authority)
        registration = await self._exact_registration(session, epoch, executor)
        state = await self._runtime_state(session, registration, epoch, create=False)
        if state is None or state.state != "current":
            raise ExecutionConflictError("executor checkpoint is unavailable")
        return ExecutableExecutorCheckpoint(
            execution_epoch=state.execution_epoch,
            execution_manifest_sha256=state.execution_manifest_sha256,
            executor_id=state.executor_id,
            executor_incarnation=state.executor_incarnation,
            pool_id=state.pool_id,
            pool_generation=state.pool_generation,
            command_sequence=state.command_high_water,
            journal_sequence=state.journal_high_water,
            journal_digest=state.journal_digest,
            inventory_sequence=state.inventory_high_water,
            lease_expires_at=state.lease_expires_at,
        )

    async def heartbeat_executor(
        self,
        session: AsyncSession,
        heartbeat: ExecutableExecutorHeartbeatV2,
    ) -> HeartbeatedExecutableExecutor:
        digest = canonical_executable_digest(heartbeat)
        async with _write_transaction(session):
            _authority, epoch, registration = await self._locked_epoch_and_registration(
                session,
                heartbeat.execution,
                executor_id=heartbeat.executor_id,
                executor_incarnation=heartbeat.executor_incarnation,
                pool_id=heartbeat.pool_id,
                pool_generation=heartbeat.pool_generation,
            )
            state = await self._runtime_state(
                session,
                registration,
                epoch,
                create=True,
            )
            assert state is not None
            if heartbeat.heartbeat_sequence < state.heartbeat_high_water:
                raise ExecutionConflictError("executor heartbeat sequence regressed")
            if heartbeat.heartbeat_sequence == state.heartbeat_high_water:
                if state.last_heartbeat_digest != digest:
                    raise _ExecutorFenceError(
                        state.executor_incarnation,
                        "equivocal",
                        "executor heartbeat equivocated",
                    )
                return HeartbeatedExecutableExecutor(
                    state.heartbeat_high_water,
                    state.lease_expires_at,
                    True,
                )
            if heartbeat.heartbeat_sequence != state.heartbeat_high_water + 1:
                raise ExecutionConflictError("executor heartbeat sequence has a gap")
            if heartbeat.journal_sequence < state.journal_high_water:
                raise _ExecutorFenceError(
                    state.executor_incarnation,
                    "fenced",
                    "executor journal regressed",
                )
            if (
                heartbeat.journal_checkpoint_sequence != state.journal_high_water
                or heartbeat.journal_checkpoint_digest != state.journal_digest
            ):
                raise _ExecutorFenceError(
                    state.executor_incarnation,
                    "fenced",
                    "executor journal checkpoint diverged",
                )
            if (
                heartbeat.journal_sequence == state.journal_high_water
                and heartbeat.journal_digest != state.journal_digest
            ):
                raise _ExecutorFenceError(
                    state.executor_incarnation,
                    "fenced",
                    "executor journal digest changed at same sequence",
                )
            now = await _database_now(session)
            state.heartbeat_high_water = heartbeat.heartbeat_sequence
            state.last_heartbeat_digest = digest
            state.journal_high_water = heartbeat.journal_sequence
            state.journal_digest = heartbeat.journal_digest
            state.last_heartbeat_at = now
            state.lease_expires_at = now + self._executor_lease
            return HeartbeatedExecutableExecutor(
                state.heartbeat_high_water,
                state.lease_expires_at,
                False,
            )

    async def ingest_executor_inventory(
        self,
        session: AsyncSession,
        inventory: ExecutableExecutorInventoryV2,
    ) -> IngestedExecutableInventory:
        digest = canonical_executable_digest(inventory)
        async with _write_transaction(session):
            authority, epoch, registration = await self._locked_epoch_and_registration(
                session,
                inventory.execution,
                executor_id=inventory.executor_id,
                executor_incarnation=inventory.executor_incarnation,
                pool_id=inventory.pool_id,
                pool_generation=inventory.pool_generation,
            )
            del authority
            state = await self._runtime_state(session, registration, epoch, create=False)
            if state is None or state.state != "current":
                raise ExecutionConflictError("executor lease is unavailable")
            now = await _database_now(session)
            if state.lease_expires_at <= now:
                raise ExecutionConflictError("executor lease expired")
            if inventory.inventory_sequence < state.inventory_high_water:
                raise ExecutionConflictError("executor inventory sequence regressed")
            if inventory.inventory_sequence == state.inventory_high_water:
                if state.last_inventory_digest != digest:
                    raise _ExecutorFenceError(
                        state.executor_incarnation,
                        "equivocal",
                        "executor inventory equivocated",
                    )
                return IngestedExecutableInventory(
                    inventory.inventory_sequence,
                    digest,
                    True,
                )
            if inventory.inventory_sequence != state.inventory_high_water + 1:
                raise ExecutionConflictError("executor inventory sequence has a gap")
            if (
                inventory.journal_checkpoint_sequence != state.journal_high_water
                or inventory.journal_checkpoint_digest != state.journal_digest
            ):
                raise _ExecutorFenceError(
                    state.executor_incarnation,
                    "fenced",
                    "executor inventory journal diverged",
                )
            if (
                inventory.journal_sequence == state.journal_high_water
                and inventory.journal_digest != state.journal_digest
            ):
                raise _ExecutorFenceError(
                    state.executor_incarnation,
                    "fenced",
                    "executor journal digest changed at same sequence",
                )
            state.journal_high_water = inventory.journal_sequence
            state.journal_digest = inventory.journal_digest
            state.inventory_high_water = inventory.inventory_sequence
            state.last_inventory_digest = digest
            state.inventory_payload = inventory.model_dump(mode="json", exclude_none=False)
            state.last_inventory_at = now
            intents_by_id = await self._locked_inventory_intents(session, inventory)
            claimed_intent_ids: list[UUID] = []
            for record in inventory.records:
                proof = record.ownership_proof
                if proof is None:
                    continue
                intent = intents_by_id.get(proof.metadata.binding.intent_id)
                if intent is None:
                    continue
                if proof.metadata.binding != ExecutableIntentBindingV2.model_validate_json(
                    json.dumps(intent.binding_payload)
                ):
                    continue
                claimed_intent_ids.append(intent.intent_id)
            if len(claimed_intent_ids) != len(set(claimed_intent_ids)):
                raise _ExecutorFenceError(
                    state.executor_incarnation,
                    "equivocal",
                    "duplicate executable inventory claim",
                )
            pool = (
                await session.execute(
                    select(CapacityPool).where(
                        CapacityPool.configuration_epoch == epoch.configuration_epoch,
                        CapacityPool.pool_id == inventory.pool_id,
                        CapacityPool.pool_generation == inventory.pool_generation,
                    )
                )
            ).scalar_one()
            for record in inventory.records:
                proof = record.ownership_proof
                if proof is None:
                    continue
                binding = proof.metadata.binding
                intent = intents_by_id.get(binding.intent_id)
                if intent is None or binding != ExecutableIntentBindingV2.model_validate_json(
                    json.dumps(intent.binding_payload)
                ):
                    continue
                if (
                    not self._ownership_keyring.verify_executable(
                        proof,
                        expected_public_key_sha256=registration.signing_key_sha256,
                    )
                    or proof.signing_key_id != registration.signing_key_id
                    or proof.metadata.controller_authority_sha256
                    != registration.controller_authority_sha256
                    or proof.metadata.trusted_launcher_sha256 != epoch.trusted_fleet_release_sha256
                    or record.authority_scope != "dedicated-loom-association"
                    or proof.metadata.slurm_cluster != pool.controller
                    or proof.metadata.association != pool.association
                    or proof.metadata.submitter_identity != "loom"
                    or record.resources != binding.resources
                    or record.node_ids != binding.node_ids
                ):
                    if intent.state == "released":
                        continue
                    if self._inventory_terminal_conflicts(intent, record):
                        intent.state = "quarantined"
                        continue
                    if intent.state not in {"closing", "terminal"}:
                        intent.state = "quarantined"
                    continue
                if not self._apply_inventory_observation(intent, inventory, record):
                    continue
            return IngestedExecutableInventory(inventory.inventory_sequence, digest, False)

    async def next_pool_work(
        self,
        session: AsyncSession,
        executor: PreparedExecutorBindingV2,
    ) -> (
        ExecutableReservationProposalV2
        | ExecutableIntentBindingV2
        | ExecutableLaunchPermitV2
        | ExecutableIntentCloseV2
        | ExecutablePartialReleaseV2
        | None
    ):
        async with _write_transaction(session):
            authority = await self._lock_authority(session)
            if authority.execution_state == "shadow":
                return None
            epoch = await self._lock_current_epoch(session, authority)
            context = await self._locked_execution_context(
                session,
                self._fence_from_rows(authority, epoch, allocation_epoch=1),
                executor,
                require_active=False,
                ignore_allocation=True,
            )
            latest_epoch = await self._locked_latest_sealed_allocation_epoch(session, epoch)
            now = await _database_now(session)
            latest_increase_allowed = (
                latest_epoch is not None
                and latest_epoch.input_valid_until is not None
                and latest_epoch.input_valid_until > now
            )
            while True:
                current = (
                    (
                        await session.execute(
                            select(CapacityExecutableIntent)
                            .where(
                                CapacityExecutableIntent.execution_epoch == epoch.execution_epoch,
                                CapacityExecutableIntent.executor_incarnation
                                == executor.executor_incarnation,
                                CapacityExecutableIntent.pool_id == executor.pool_id,
                                CapacityExecutableIntent.state != "released",
                            )
                            .order_by(CapacityExecutableIntent.launch_rank)
                            .with_for_update()
                        )
                    )
                    .scalars()
                    .first()
                )
                if current is None:
                    break
                if latest_epoch is not None and current.allocation_epoch != latest_epoch.allocation_epoch:
                    if current.state == "proposed":
                        current.state = "released"
                        current.released_at = now
                        continue
                    if current.state in {"accepted", "launch-ready", "permitted"}:
                        binding = ExecutableIntentBindingV2.model_validate_json(
                            json.dumps(current.binding_payload)
                        )
                        return ExecutableIntentCloseV2(
                            binding=binding,
                            command_sequence=context.executor.command_high_water + 1,
                        )
                increase_allowed = (
                    authority.execution_state == "active"
                    and authority.executable_new_capacity_ceiling > 0
                    and not authority.increase_freeze
                    and latest_increase_allowed
                )
                if current.state == "proposed":
                    if not increase_allowed:
                        current.state = "released"
                        current.released_at = now
                        continue
                    return ExecutableReservationProposalV2.model_validate_json(
                        json.dumps(current.proposal_payload)
                    )
                if current.state == "accepted":
                    if not increase_allowed:
                        binding = ExecutableIntentBindingV2.model_validate_json(
                            json.dumps(current.binding_payload)
                        )
                        return ExecutableIntentCloseV2(
                            binding=binding,
                            command_sequence=context.executor.command_high_water + 1,
                        )
                    bootstrap = await self._latest_bootstrap_proposal(
                        session, current.intent_id, lock=True
                    )
                    if bootstrap is None or bootstrap.expires_at <= now:
                        return ExecutableIntentBindingV2.model_validate_json(
                            json.dumps(current.binding_payload)
                        )
                    return None
                if current.state in {"launch-ready", "permitted"}:
                    if not increase_allowed:
                        binding = ExecutableIntentBindingV2.model_validate_json(
                            json.dumps(current.binding_payload)
                        )
                        return ExecutableIntentCloseV2(
                            binding=binding,
                            command_sequence=context.executor.command_high_water + 1,
                        )
                    if (
                        current.permit_payload is None
                        or current.permit_expires_at is None
                        or current.permit_expires_at <= now
                    ):
                        await self._assert_increase_eligible(session, context, current=current)
                        permit = self._new_permit(current, now)
                        current.permit_id = permit.permit_id
                        current.permit_epoch = permit.permit_epoch
                        current.permit_digest = canonical_executable_digest(permit)
                        current.permit_payload = permit.model_dump(mode="json", exclude_none=False)
                        current.permit_expires_at = permit.expires_at
                        current.state = "permitted"
                        return permit
                    return ExecutableLaunchPermitV2.model_validate_json(
                        json.dumps(current.permit_payload)
                    )
                if current.state == "terminal":
                    binding = ExecutableIntentBindingV2.model_validate_json(
                        json.dumps(current.binding_payload)
                    )
                    return ExecutableIntentCloseV2(
                        binding=binding,
                        command_sequence=context.executor.command_high_water + 1,
                    )
                if current.state == "observed" and not increase_allowed:
                    binding = ExecutableIntentBindingV2.model_validate_json(
                        json.dumps(current.binding_payload)
                    )
                    return ExecutableIntentCloseV2(
                        binding=binding,
                        command_sequence=context.executor.command_high_water + 1,
                    )
                protected_release = (
                    await self._latest_protected_release_receipt(session, current.intent_id)
                    if current.state == "closing"
                    else None
                )
                if (
                    protected_release is not None
                    and current.terminal_evidence_sha256 is not None
                    and current.inventory_sequence is not None
                    and current.terminal_kind is not None
                    and current.terminal_identity is not None
                ):
                    binding = ExecutableIntentBindingV2.model_validate_json(
                        json.dumps(current.binding_payload)
                    )
                    released = ExecutableReleasedShapeV2(
                        binding=binding,
                        inventory_sequence=current.inventory_sequence,
                        terminal_kind=cast(
                            Literal["unused", "slurm-job", "worker"], current.terminal_kind
                        ),
                        terminal_identity=current.terminal_identity,
                        terminal_evidence_sha256=current.terminal_evidence_sha256,
                        protected_registration_epoch=(
                            protected_release.protected_registration_epoch
                        ),
                        bootstrap_revoked=True,
                        protected_release_sha256=protected_release.protected_release_sha256,
                    )
                    return ExecutablePartialReleaseV2(
                        execution=binding.execution,
                        tranche_id=binding.tranche_id,
                        executor_id=binding.executor_id,
                        executor_incarnation=binding.executor_incarnation,
                        command_sequence=context.executor.command_high_water + 1,
                        releases=(released,),
                    )
                return None
            if (
                authority.execution_state != "active"
                or authority.executable_new_capacity_ceiling <= 0
                or not latest_increase_allowed
            ):
                return None
            return await self._create_next_proposal(session, context)

    async def accept_reservation(
        self,
        session: AsyncSession,
        acceptance: ExecutableReservationAcceptanceV2,
    ) -> AcceptedExecutableReservation:
        digest = canonical_executable_digest(acceptance)
        async with _write_transaction(session):
            context = await self._locked_execution_context(
                session,
                acceptance.execution,
                self._executor_binding_from_acceptance(acceptance),
            )
            del context
            row = await self._locked_intent_by_tranche(session, acceptance.tranche_id)
            if (
                acceptance.pool_id != row.pool_id
                or acceptance.executor_id != row.executor_id
                or acceptance.executor_incarnation != row.executor_incarnation
                or acceptance.pool_generation != row.pool_generation
            ):
                raise ExecutionConflictError("reservation executor binding changed")
            if row.proposal_digest != acceptance.proposal_digest:
                raise ExecutionConflictError("reservation proposal digest changed")
            payload = {
                "tranche_id": str(row.tranche_id),
                "intent_ids": [str(row.intent_id)],
                "executable": True,
            }
            replay = await self._command_replay(
                session,
                row,
                sequence=acceptance.command_sequence,
                operation_kind="accept",
                request_digest=digest,
                result_payload=payload,
            )
            if replay is None:
                if row.state != "proposed":
                    raise ExecutionConflictError("reservation proposal is not current")
                row.state = "accepted"
                row.accepted_at = await _database_now(session)
                await self._record_command(
                    session,
                    row,
                    sequence=acceptance.command_sequence,
                    operation_kind="accept",
                    request_digest=digest,
                    result_payload=payload,
                )
            return AcceptedExecutableReservation(
                row.tranche_id,
                (row.intent_id,),
                _payload_digest(payload),
                replay is not None,
            )

    async def propose_bootstrap(
        self,
        session: AsyncSession,
        proposal: ExecutableBootstrapProposalV2,
    ) -> ProposedExecutableBootstrap:
        """Persist an executor bootstrap hash without granting launch readiness."""

        digest = canonical_executable_digest(proposal)
        async with _write_transaction(session):
            await self._locked_execution_context(
                session,
                proposal.binding.execution,
                self._executor_binding_from_contract(proposal.binding),
            )
            row = await self._locked_intent(session, proposal.binding.intent_id)
            if proposal.binding != ExecutableIntentBindingV2.model_validate_json(
                json.dumps(row.binding_payload)
            ):
                raise ExecutionConflictError("bootstrap proposal intent binding changed")
            payload = {
                "intent_id": str(row.intent_id),
                "proposal_epoch": proposal.proposal_epoch,
                "proposal_digest": digest,
                "executable": True,
            }
            replay = await self._command_replay(
                session,
                row,
                sequence=proposal.command_sequence,
                operation_kind="bootstrap-propose",
                request_digest=digest,
                result_payload=payload,
            )
            if replay is None:
                if row.state != "accepted":
                    raise ExecutionConflictError("bootstrap proposal intent is not accepted")
                now = await _database_now(session)
                latest = await self._latest_bootstrap_proposal(
                    session, row.intent_id, lock=True
                )
                expected_epoch = 1 if latest is None else latest.proposal_epoch + 1
                if proposal.proposal_epoch != expected_epoch:
                    raise ExecutionConflictError("bootstrap proposal epoch changed")
                if latest is not None and latest.expires_at > now:
                    raise ExecutionConflictError("bootstrap proposal is still current")
                if proposal.expires_at <= now or proposal.expires_at > now + timedelta(minutes=10):
                    raise ExecutionConflictError("bootstrap proposal expiry is invalid")
                session.add(
                    CapacityExecutableBootstrapProposal(
                        intent_id=row.intent_id,
                        execution_epoch=row.execution_epoch,
                        execution_manifest_sha256=row.execution_manifest_sha256,
                        proposal_epoch=proposal.proposal_epoch,
                        command_sequence=proposal.command_sequence,
                        bootstrap_sha256=proposal.bootstrap_sha256,
                        expires_at=proposal.expires_at,
                        proposal_digest=digest,
                        proposal_payload=proposal.model_dump(mode="json", exclude_none=False),
                    )
                )
                await session.flush()
                await self._record_command(
                    session,
                    row,
                    sequence=proposal.command_sequence,
                    operation_kind="bootstrap-propose",
                    request_digest=digest,
                    result_payload=payload,
                )
            return ProposedExecutableBootstrap(
                row.intent_id,
                proposal.proposal_epoch,
                _payload_digest(payload),
                replay is not None,
            )

    async def next_subject_bootstrap(
        self,
        session: AsyncSession,
        *,
        subject_id: UUID,
        subject_incarnation: UUID,
        reporter_incarnation: UUID,
    ) -> ExecutableBootstrapProposalV2 | None:
        """Return one current bootstrap hash only to its protected subject reporter."""

        async with _write_transaction(session):
            authority = await self._lock_authority(session)
            if authority.execution_state not in {"active", "drain-only"}:
                return None
            epoch = await self._lock_current_epoch(session, authority)
            reporter = (
                await session.execute(
                    select(CapacityDemandReporter)
                    .where(
                        CapacityDemandReporter.subject_id == subject_id,
                        CapacityDemandReporter.subject_incarnation == subject_incarnation,
                        CapacityDemandReporter.reporter_incarnation == reporter_incarnation,
                        CapacityDemandReporter.state == "current",
                    )
                    .with_for_update(read=True)
                )
            ).scalar_one_or_none()
            if reporter is None:
                raise ExecutionConflictError("bootstrap subject reporter changed")
            now = await _database_now(session)
            proposals = (
                (
                    await session.execute(
                        select(CapacityExecutableBootstrapProposal)
                        .join(
                            CapacityExecutableIntent,
                            CapacityExecutableIntent.intent_id
                            == CapacityExecutableBootstrapProposal.intent_id,
                        )
                        .where(
                            CapacityExecutableIntent.execution_epoch == epoch.execution_epoch,
                            CapacityExecutableIntent.execution_manifest_sha256
                            == epoch.execution_manifest_sha256,
                            CapacityExecutableIntent.subject_id == subject_id,
                            CapacityExecutableIntent.subject_incarnation == subject_incarnation,
                            CapacityExecutableIntent.state == "accepted",
                            CapacityExecutableBootstrapProposal.expires_at > now,
                        )
                        .order_by(
                            CapacityExecutableIntent.launch_rank,
                            CapacityExecutableBootstrapProposal.proposal_epoch.desc(),
                        )
                        .with_for_update(read=True)
                    )
                )
                .scalars()
                .all()
            )
            for proposal in proposals:
                latest = await self._latest_bootstrap_proposal(
                    session, proposal.intent_id, lock=False
                )
                if latest is not None and latest.id == proposal.id:
                    return ExecutableBootstrapProposalV2.model_validate_json(
                        json.dumps(proposal.proposal_payload)
                    )
            return None

    async def acknowledge_bootstrap(
        self,
        session: AsyncSession,
        acknowledgement: ExecutableBootstrapAcknowledgementV2,
        *,
        actor: str,
        idempotency_key: UUID,
    ) -> RegisteredExecutableBootstrap:
        """Make one intent launch-ready only after exact protected subject evidence."""

        if not actor or len(actor.encode("utf-8")) > 256:
            raise ValueError("bootstrap acknowledgement actor is invalid")
        digest = canonical_executable_digest(acknowledgement)
        async with _write_transaction(session):
            context = await self._locked_execution_context(
                session,
                acknowledgement.binding.execution,
                self._executor_binding_from_contract(acknowledgement.binding),
            )
            row = await self._locked_intent(session, acknowledgement.binding.intent_id)
            if acknowledgement.binding != ExecutableIntentBindingV2.model_validate_json(
                json.dumps(row.binding_payload)
            ):
                raise ExecutionConflictError("bootstrap acknowledgement binding changed")
            stored_payload = acknowledgement.model_dump(mode="json", exclude_none=False)
            replay = (
                await session.execute(
                    select(CapacityExecutableBootstrapAcknowledgement)
                    .where(
                        CapacityExecutableBootstrapAcknowledgement.idempotency_key
                        == idempotency_key
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if replay is not None:
                if (
                    replay.intent_id != row.intent_id
                    or replay.acknowledgement_digest != digest
                    or replay.actor_id != actor
                    or replay.acknowledgement_payload != stored_payload
                ):
                    raise ExecutionConflictError(
                        "bootstrap acknowledgement idempotency key was reused"
                    )
                return RegisteredExecutableBootstrap(
                    row.intent_id,
                    replay.bootstrap_registration_epoch,
                    replay.acknowledgement_digest,
                    True,
                )
            if row.state != "accepted":
                raise ExecutionConflictError("bootstrap acknowledgement intent is not accepted")
            reporter = (
                await session.execute(
                    select(CapacityDemandReporter)
                    .where(
                        CapacityDemandReporter.subject_id == row.subject_id,
                        CapacityDemandReporter.subject_incarnation == row.subject_incarnation,
                        CapacityDemandReporter.reporter_incarnation
                        == acknowledgement.reporter_incarnation,
                        CapacityDemandReporter.state == "current",
                    )
                    .with_for_update(read=True)
                )
            ).scalar_one_or_none()
            if reporter is None:
                raise ExecutionConflictError("bootstrap acknowledgement reporter changed")
            try:
                preparation = ExecutionPreparationV2.model_validate_json(
                    json.dumps(context.epoch.manifest_payload)
                )
            except ValueError as exc:
                raise ExecutionConflictError("execution preparation manifest is invalid") from exc
            subject_acknowledgement = next(
                (
                    item
                    for item in preparation.subject_acknowledgements
                    if item.subject_id == row.subject_id
                ),
                None,
            )
            if (
                subject_acknowledgement is None
                or subject_acknowledgement.subject_incarnation != row.subject_incarnation
                or subject_acknowledgement.reporter_incarnation
                != acknowledgement.reporter_incarnation
                or subject_acknowledgement.protected_admission_sha256
                != acknowledgement.protected_admission_sha256
            ):
                raise ExecutionConflictError("bootstrap protected admission changed")
            proposal = await self._latest_bootstrap_proposal(session, row.intent_id, lock=True)
            now = await _database_now(session)
            if (
                proposal is None
                or proposal.proposal_epoch != acknowledgement.proposal_epoch
                or proposal.proposal_digest != acknowledgement.proposal_digest
                or proposal.expires_at <= now
            ):
                raise ExecutionConflictError("bootstrap proposal changed or expired")
            session.add(
                CapacityExecutableBootstrapAcknowledgement(
                    idempotency_key=idempotency_key,
                    intent_id=row.intent_id,
                    execution_epoch=row.execution_epoch,
                    execution_manifest_sha256=row.execution_manifest_sha256,
                    proposal_epoch=acknowledgement.proposal_epoch,
                    proposal_digest=acknowledgement.proposal_digest,
                    reporter_incarnation=acknowledgement.reporter_incarnation,
                    bootstrap_registration_epoch=(
                        acknowledgement.bootstrap_registration_epoch
                    ),
                    bootstrap_evidence_sha256=(
                        acknowledgement.bootstrap_evidence_sha256
                    ),
                    protected_admission_sha256=(
                        acknowledgement.protected_admission_sha256
                    ),
                    acknowledgement_digest=digest,
                    actor_id=actor,
                    acknowledgement_payload=stored_payload,
                )
            )
            await session.flush()
            row.state = "launch-ready"
            row.bootstrap_registration_epoch = acknowledgement.bootstrap_registration_epoch
            row.bootstrap_evidence_sha256 = acknowledgement.bootstrap_evidence_sha256
            row.launch_ready_at = now
            await session.flush()
            return RegisteredExecutableBootstrap(
                row.intent_id,
                acknowledgement.bootstrap_registration_epoch,
                digest,
                False,
            )

    async def register_bootstrap(
        self,
        session: AsyncSession,
        registration: ExecutableBootstrapRegistrationV2,
    ) -> RegisteredExecutableBootstrap:
        del session, registration
        raise ExecutionConflictError("protected bootstrap acknowledgement is required")

    async def consume_launch_permit(
        self,
        session: AsyncSession,
        consumption: ExecutablePermitConsumptionV2,
    ) -> ConsumedExecutablePermit:
        digest = canonical_executable_digest(consumption)
        async with _write_transaction(session):
            context = await self._locked_execution_context(
                session,
                consumption.binding.execution,
                self._executor_binding_from_contract(consumption.binding),
            )
            locked_intents = await self._locked_allocation_intents(session, consumption.binding)
            row = next(
                (item for item in locked_intents if item.intent_id == consumption.binding.intent_id),
                None,
            )
            if row is None:
                raise ExecutionConflictError("executable intent is unknown")
            if consumption.binding != ExecutableIntentBindingV2.model_validate_json(
                json.dumps(row.binding_payload)
            ):
                raise ExecutionConflictError("launch permit binding changed")
            if (
                row.permit_id != consumption.permit_id
                or row.permit_digest != consumption.permit_digest
            ):
                raise ExecutionConflictError("launch permit digest changed")
            payload = {
                "permit_id": str(consumption.permit_id),
                "intent_id": str(row.intent_id),
                "executable": True,
            }
            replay = await self._command_replay(
                session,
                row,
                sequence=consumption.command_sequence,
                operation_kind="permit-consumption",
                request_digest=digest,
                result_payload=payload,
            )
            if replay is None:
                now = await _database_now(session)
                if row.state != "permitted" or row.permit_expires_at is None:
                    raise ExecutionConflictError("launch permit is not current")
                if row.permit_expires_at <= now:
                    raise ExecutionConflictError("launch permit expired")
                deadlines = await self._assert_increase_eligible(session, context, current=row)
                self._assert_central_launch_order(locked_intents, row)
                pending_deadline = await self._assert_pending_limits(session, context, row)
                deadlines = replace(
                    deadlines,
                    pool_inventory_fresh_until=min(
                        deadlines.pool_inventory_fresh_until,
                        pending_deadline,
                    ),
                )
                await self._consume_rate_tokens(session, context, row, now=now)
                final_now = await _database_now(session)
                self._assert_final_launch_deadlines(deadlines, final_now=final_now)
                row.state = "submitting-unknown"
                row.permit_consumed_at = now
                await self._record_command(
                    session,
                    row,
                    sequence=consumption.command_sequence,
                    operation_kind="permit-consumption",
                    request_digest=digest,
                    result_payload=payload,
                )
            return ConsumedExecutablePermit(
                consumption.permit_id,
                row.intent_id,
                _payload_digest(payload),
                replay is not None,
            )

    async def recover_unsubmitted_permit(
        self,
        session: AsyncSession,
        recovery: ExecutableSubmissionRecoveryV2,
    ) -> RecoveredExecutableSubmission:
        """Close an ambiguous consumed permit only after exact absence evidence."""

        digest = canonical_executable_digest(recovery)
        async with _write_transaction(session):
            context = await self._locked_execution_context(
                session,
                recovery.binding.execution,
                self._executor_binding_from_contract(recovery.binding),
                require_active=False,
            )
            row = await self._locked_intent(session, recovery.binding.intent_id)
            if recovery.binding != ExecutableIntentBindingV2.model_validate_json(
                json.dumps(row.binding_payload)
            ):
                raise ExecutionConflictError("submission recovery binding changed")
            if row.permit_id != recovery.permit_id or row.permit_digest != recovery.permit_digest:
                raise ExecutionConflictError("submission recovery permit changed")
            payload = {
                "intent_id": str(row.intent_id),
                "recovery": recovery.model_dump(mode="json", exclude_none=False),
                "executable": True,
            }
            replay = await self._command_replay(
                session,
                row,
                sequence=recovery.command_sequence,
                operation_kind="submission-recovery",
                request_digest=digest,
                result_payload=payload,
            )
            if replay is None:
                if row.state != "submitting-unknown" or row.permit_consumed_at is None:
                    raise ExecutionConflictError("submission recovery requires unknown submission")
                runtime = context.executor
                if (
                    runtime.inventory_payload is None
                    or runtime.last_inventory_digest is None
                    or runtime.last_inventory_at is None
                    or runtime.inventory_high_water != recovery.inventory_sequence
                    or runtime.last_inventory_digest != recovery.inventory_digest
                ):
                    raise ExecutionConflictError("submission recovery inventory changed")
                now = await _database_now(session)
                if (
                    runtime.last_inventory_at <= row.permit_consumed_at
                    or recovery.controller_query_completed_at < runtime.last_inventory_at
                ):
                    raise ExecutionConflictError(
                        "post-consumption inventory is required for submission recovery"
                    )
                if (
                    runtime.last_inventory_at + self._inventory_freshness <= now
                    or recovery.controller_query_completed_at > now + timedelta(seconds=1)
                ):
                    raise ExecutionConflictError("submission recovery evidence is not fresh")
                inventory = ExecutableExecutorInventoryV2.model_validate_json(
                    json.dumps(runtime.inventory_payload)
                )
                if canonical_executable_digest(inventory) != recovery.inventory_digest:
                    raise ExecutionConflictError("submission recovery inventory digest changed")
                if any(
                    record.ownership_proof is not None
                    and record.ownership_proof.metadata.binding.intent_id == row.intent_id
                    for record in inventory.records
                ):
                    raise ExecutionConflictError("submission recovery found physical work")
                row.inventory_sequence = recovery.inventory_sequence
                row.observed_state = "terminal"
                row.terminal_kind = "unused"
                row.terminal_identity = row.shape_instance_id
                row.terminal_evidence_sha256 = digest
                row.state = "closing"
                await self._record_command(
                    session,
                    row,
                    sequence=recovery.command_sequence,
                    operation_kind="submission-recovery",
                    request_digest=digest,
                    result_payload=payload,
                )
            return RecoveredExecutableSubmission(
                row.intent_id,
                _payload_digest(payload),
                replay is not None,
            )

    async def begin_intent_close(
        self,
        session: AsyncSession,
        close: ExecutableIntentCloseV2,
    ) -> ClosingExecutableIntent:
        digest = canonical_executable_digest(close)
        async with _write_transaction(session):
            await self._locked_execution_context(
                session,
                close.binding.execution,
                self._executor_binding_from_contract(close.binding),
                require_active=False,
            )
            row = await self._locked_intent(session, close.binding.intent_id)
            if close.binding != ExecutableIntentBindingV2.model_validate_json(
                json.dumps(row.binding_payload)
            ):
                raise ExecutionConflictError("intent close binding changed")
            payload = {"intent_id": str(row.intent_id), "executable": True}
            replay = await self._command_replay(
                session,
                row,
                sequence=close.command_sequence,
                operation_kind="close",
                request_digest=digest,
                result_payload=payload,
            )
            if replay is None:
                if row.state not in {
                    "accepted",
                    "launch-ready",
                    "permitted",
                    "observed",
                    "terminal",
                }:
                    raise ExecutionConflictError("intent cannot begin close")
                if row.state in {"accepted", "launch-ready", "permitted"}:
                    runtime = (
                        await session.execute(
                            select(CapacityExecutableExecutorState).where(
                                CapacityExecutableExecutorState.executor_incarnation
                                == row.executor_incarnation
                            )
                        )
                    ).scalar_one()
                    if runtime.inventory_payload is None:
                        raise ExecutionConflictError("unused close requires complete inventory")
                    inventory = ExecutableExecutorInventoryV2.model_validate_json(
                        json.dumps(runtime.inventory_payload)
                    )
                    if any(
                        record.ownership_proof is not None
                        and record.ownership_proof.metadata.binding.intent_id == row.intent_id
                        for record in inventory.records
                    ):
                        raise ExecutionConflictError("unused intent has physical inventory")
                    row.inventory_sequence = inventory.inventory_sequence
                    row.terminal_kind = "unused"
                    row.terminal_identity = row.shape_instance_id
                    row.terminal_evidence_sha256 = canonical_executable_digest(inventory)
                row.state = "closing"
                await self._record_command(
                    session,
                    row,
                    sequence=close.command_sequence,
                    operation_kind="close",
                    request_digest=digest,
                    result_payload=payload,
                )
            return ClosingExecutableIntent(
                row.intent_id,
                _payload_digest(payload),
                replay is not None,
            )

    async def acknowledge_protected_release(
        self,
        session: AsyncSession,
        release: ExecutableProtectedReleaseV2,
        *,
        actor: str,
        idempotency_key: UUID,
    ) -> AcknowledgedExecutableProtectedRelease:
        if not actor or len(actor.encode("utf-8")) > 256:
            raise ValueError("protected release actor is invalid")
        digest = canonical_executable_digest(release)
        async with _write_transaction(session):
            await self._locked_execution_context(
                session,
                release.binding.execution,
                self._executor_binding_from_contract(release.binding),
                require_active=False,
            )
            row = await self._locked_intent(session, release.binding.intent_id)
            if release.binding != ExecutableIntentBindingV2.model_validate_json(
                json.dumps(row.binding_payload)
            ):
                raise ExecutionConflictError("protected release binding changed")
            replay = (
                await session.execute(
                    select(CapacityExecutableProtectedReleaseReceipt)
                    .where(
                        CapacityExecutableProtectedReleaseReceipt.idempotency_key
                        == idempotency_key
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if replay is not None:
                if (
                    replay.intent_id != row.intent_id
                    or replay.acknowledgement_digest != digest
                    or replay.actor_id != actor
                ):
                    raise ExecutionConflictError("protected release idempotency key was reused")
                return AcknowledgedExecutableProtectedRelease(
                    replay.intent_id,
                    replay.protected_release_sha256,
                    _payload_digest(replay.release_payload),
                    True,
                )
            prior_receipts = await self._locked_protected_release_receipts(
                session, (row.intent_id,)
            )
            prior = prior_receipts.get(row.intent_id)
            if (
                prior is not None
                and release.protected_registration_epoch <= prior.protected_registration_epoch
            ):
                raise ExecutionConflictError(
                    "protected release registration epoch must advance"
                )
            reporter = (
                await session.execute(
                    select(CapacityDemandReporter).where(
                        CapacityDemandReporter.subject_id == row.subject_id,
                        CapacityDemandReporter.subject_incarnation == row.subject_incarnation,
                        CapacityDemandReporter.reporter_incarnation == release.reporter_incarnation,
                        CapacityDemandReporter.state == "current",
                    )
                )
            ).scalar_one_or_none()
            if reporter is None:
                raise ExecutionConflictError("protected release reporter changed")
            if row.bootstrap_registration_epoch != release.bootstrap_registration_epoch:
                raise ExecutionConflictError("protected release bootstrap binding changed")
            payload = release.model_dump(mode="json", exclude_none=False)
            session.add(
                CapacityExecutableProtectedReleaseReceipt(
                    idempotency_key=idempotency_key,
                    intent_id=row.intent_id,
                    execution_epoch=row.execution_epoch,
                    execution_manifest_sha256=row.execution_manifest_sha256,
                    reporter_incarnation=release.reporter_incarnation,
                    bootstrap_registration_epoch=release.bootstrap_registration_epoch,
                    protected_registration_epoch=release.protected_registration_epoch,
                    protected_release_sha256=release.protected_release_sha256,
                    acknowledgement_digest=digest,
                    actor_id=actor,
                    release_payload=payload,
                )
            )
            await session.flush()
            return AcknowledgedExecutableProtectedRelease(
                row.intent_id,
                release.protected_release_sha256,
                _payload_digest(payload),
                False,
            )

    async def release_shapes(
        self,
        session: AsyncSession,
        release: ExecutablePartialReleaseV2,
    ) -> ReleasedExecutableShapes:
        digest = canonical_executable_digest(release)
        async with _write_transaction(session):
            first_binding = release.releases[0].binding
            await self._locked_execution_context(
                session,
                release.execution,
                self._executor_binding_from_contract(first_binding),
                require_active=False,
            )
            intent_ids = tuple(item.binding.intent_id for item in release.releases)
            ordered_intent_ids = tuple(
                (
                    await session.execute(
                    select(CapacityExecutableIntent.intent_id)
                    .where(CapacityExecutableIntent.intent_id.in_(intent_ids))
                    .order_by(
                        CapacityExecutableIntent.allocation_epoch,
                        CapacityExecutableIntent.launch_rank,
                        CapacityExecutableIntent.intent_id,
                    )
                )
            )
            .scalars()
            .all()
            )
            if set(ordered_intent_ids) != set(intent_ids):
                raise ExecutionConflictError("executable intent is unknown")
            rows_by_id = {
                intent_id: await self._locked_intent(session, intent_id)
                for intent_id in ordered_intent_ids
            }
            protected_releases = await self._locked_protected_release_receipts(
                session, ordered_intent_ids
            )
            rows = [(rows_by_id[item.binding.intent_id], item) for item in release.releases]
            first = rows[0][0]
            released_ids = tuple(sorted(item.binding.shape_instance_id for _, item in rows))
            payload = {
                "tranche_id": str(release.tranche_id),
                "released_shape_ids": list(released_ids),
                "executable": True,
            }
            replay = await self._command_replay(
                session,
                first,
                sequence=release.command_sequence,
                operation_kind="release",
                request_digest=digest,
                result_payload=payload,
            )
            if replay is None:
                for row, item in rows:
                    protected_release = protected_releases.get(row.intent_id)
                    if (
                        row.state != "closing"
                        or item.binding
                        != ExecutableIntentBindingV2.model_validate_json(
                            json.dumps(row.binding_payload)
                        )
                        or protected_release is None
                        or protected_release.protected_registration_epoch
                        != item.protected_registration_epoch
                        or protected_release.protected_release_sha256
                        != item.protected_release_sha256
                        or row.inventory_sequence != item.inventory_sequence
                        or row.terminal_kind != item.terminal_kind
                        or row.terminal_identity != item.terminal_identity
                        or row.terminal_evidence_sha256 != item.terminal_evidence_sha256
                    ):
                        raise ExecutionConflictError(
                            "release requires exact protected and physical terminal evidence"
                        )
                now = await _database_now(session)
                for row, _item in rows:
                    row.state = "released"
                    row.released_at = now
                await self._record_command(
                    session,
                    first,
                    sequence=release.command_sequence,
                    operation_kind="release",
                    request_digest=digest,
                    result_payload=payload,
                )
            return ReleasedExecutableShapes(
                first.tranche_id,
                released_ids,
                _payload_digest(payload),
                replay is not None,
            )

    async def _locked_execution_context(
        self,
        session: AsyncSession,
        fence: ExecutionFenceV2,
        executor: PreparedExecutorBindingV2,
        *,
        require_active: bool = True,
        ignore_allocation: bool = False,
    ) -> LockedExecutionContext:
        authority = await self._lock_authority(session)
        epoch = await self._lock_current_epoch(session, authority)
        context_matches = _context_matches(authority, epoch, fence) or (
            not require_active and _retained_drain_context_matches(authority, epoch, fence)
        )
        if not context_matches or (
            require_active and (authority.execution_state != "active" or authority.increase_freeze)
        ):
            raise ExecutionConflictError("execution fence changed")
        if not ignore_allocation:
            allocation = (
                await session.execute(
                    select(CapacityAllocationEpoch).where(
                        CapacityAllocationEpoch.allocation_epoch == fence.allocation_epoch,
                        CapacityAllocationEpoch.execution_epoch == fence.execution_epoch,
                        CapacityAllocationEpoch.execution_manifest_sha256
                        == fence.execution_manifest_sha256,
                        CapacityAllocationEpoch.status == "executable",
                    )
                )
            ).scalar_one_or_none()
            if allocation is None:
                raise ExecutionConflictError("execution allocation fence changed")
        registration = await self._exact_registration(session, epoch, executor)
        state = await self._runtime_state(session, registration, epoch, create=False)
        now = await _database_now(session)
        if state is None or state.state != "current" or state.lease_expires_at <= now:
            raise ExecutionConflictError("executor lease expired")
        return LockedExecutionContext(authority, epoch, registration, state)

    async def _locked_epoch_and_registration(
        self,
        session: AsyncSession,
        execution: ExecutionContextV2,
        *,
        executor_id: str,
        executor_incarnation: UUID,
        pool_id: str,
        pool_generation: int,
    ) -> tuple[CapacityAuthorityState, CapacityExecutionEpoch, CapacityExecutionExecutor]:
        authority = await self._lock_authority(session)
        epoch = await self._lock_current_epoch(session, authority)
        matches = (
            _retained_drain_context_matches(authority, epoch, execution)
            if authority.execution_state == "drain-only"
            else _context_matches(authority, epoch, execution)
        )
        if not matches:
            raise ExecutionConflictError("execution fence changed")
        registration = (
            await session.execute(
                select(CapacityExecutionExecutor)
                .where(
                    CapacityExecutionExecutor.execution_epoch == execution.execution_epoch,
                    CapacityExecutionExecutor.execution_manifest_sha256
                    == execution.execution_manifest_sha256,
                    CapacityExecutionExecutor.executor_id == executor_id,
                    CapacityExecutionExecutor.executor_incarnation == executor_incarnation,
                    CapacityExecutionExecutor.pool_id == pool_id,
                    CapacityExecutionExecutor.pool_generation == pool_generation,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if registration is None:
            raise ExecutionConflictError("executor registration changed")
        return authority, epoch, registration

    @staticmethod
    async def _lock_authority(session: AsyncSession) -> CapacityAuthorityState:
        row = (
            await session.execute(
                select(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if row is None:
            raise ExecutionConflictError("execution authority is unavailable")
        return row

    @staticmethod
    async def _lock_current_epoch(
        session: AsyncSession,
        authority: CapacityAuthorityState,
    ) -> CapacityExecutionEpoch:
        row = (
            await session.execute(
                select(CapacityExecutionEpoch)
                .where(CapacityExecutionEpoch.execution_epoch == authority.execution_epoch)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if row is None:
            raise ExecutionConflictError("execution fence changed")
        return row

    @staticmethod
    async def _exact_registration(
        session: AsyncSession,
        epoch: CapacityExecutionEpoch,
        executor: PreparedExecutorBindingV2,
    ) -> CapacityExecutionExecutor:
        row = (
            await session.execute(
                select(CapacityExecutionExecutor)
                .where(
                    CapacityExecutionExecutor.execution_epoch == epoch.execution_epoch,
                    CapacityExecutionExecutor.execution_manifest_sha256
                    == epoch.execution_manifest_sha256,
                    CapacityExecutionExecutor.executor_id == executor.executor_id,
                    CapacityExecutionExecutor.executor_incarnation == executor.executor_incarnation,
                    CapacityExecutionExecutor.pool_id == executor.pool_id,
                    CapacityExecutionExecutor.pool_generation == executor.pool_generation,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if row is None:
            raise ExecutionConflictError("executor registration changed")
        return row

    async def _runtime_state(
        self,
        session: AsyncSession,
        registration: CapacityExecutionExecutor,
        epoch: CapacityExecutionEpoch,
        *,
        create: bool,
    ) -> CapacityExecutableExecutorState | None:
        row = (
            await session.execute(
                select(CapacityExecutableExecutorState)
                .where(
                    CapacityExecutableExecutorState.execution_epoch == epoch.execution_epoch,
                    CapacityExecutableExecutorState.pool_id == registration.pool_id,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if row is None and create:
            now = await _database_now(session)
            row = CapacityExecutableExecutorState(
                execution_epoch=epoch.execution_epoch,
                execution_manifest_sha256=epoch.execution_manifest_sha256,
                executor_id=registration.executor_id,
                executor_incarnation=registration.executor_incarnation,
                pool_id=registration.pool_id,
                pool_generation=registration.pool_generation,
                state="current",
                heartbeat_high_water=0,
                command_high_water=0,
                journal_high_water=0,
                journal_digest="0" * 64,
                inventory_high_water=0,
                lease_expires_at=now,
                last_heartbeat_at=now,
            )
            session.add(row)
            await session.flush()
        if row is not None and (
            row.execution_manifest_sha256 != epoch.execution_manifest_sha256
            or row.executor_id != registration.executor_id
            or row.executor_incarnation != registration.executor_incarnation
            or row.pool_generation != registration.pool_generation
        ):
            raise ExecutionConflictError("executor runtime binding changed")
        return row

    @staticmethod
    def _fence_from_rows(
        authority: CapacityAuthorityState,
        epoch: CapacityExecutionEpoch,
        *,
        allocation_epoch: int,
    ) -> ExecutionFenceV2:
        return ExecutionFenceV2(
            authority_incarnation=authority.authority_incarnation,
            writer_epoch=authority.writer_epoch,
            configuration_epoch=epoch.configuration_epoch,
            execution_epoch=epoch.execution_epoch,
            execution_manifest_sha256=epoch.execution_manifest_sha256,
            execution_state=cast(Literal["active", "drain-only"], epoch.state),
            executable_new_capacity_ceiling=epoch.effective_ceiling,
            executable_new_capacity_rate_per_minute=epoch.effective_rate_per_minute,
            trusted_fleet_release_sha256=epoch.trusted_fleet_release_sha256,
            allocation_epoch=allocation_epoch,
        )

    @staticmethod
    async def _locked_latest_sealed_allocation_epoch(
        session: AsyncSession,
        epoch: CapacityExecutionEpoch,
    ) -> CapacityAllocationEpoch | None:
        return (
            await session.execute(
                select(CapacityAllocationEpoch)
                .where(
                    CapacityAllocationEpoch.status == "executable",
                    CapacityAllocationEpoch.executable.is_(True),
                    CapacityAllocationEpoch.sealed.is_(True),
                    CapacityAllocationEpoch.execution_epoch == epoch.execution_epoch,
                    CapacityAllocationEpoch.execution_manifest_sha256
                    == epoch.execution_manifest_sha256,
                )
                .order_by(CapacityAllocationEpoch.allocation_epoch.desc())
                .limit(1)
                .with_for_update()
            )
        ).scalar_one_or_none()

    async def _create_next_proposal(
        self,
        session: AsyncSession,
        context: LockedExecutionContext,
    ) -> ExecutableReservationProposalV2 | None:
        epoch_row = await self._locked_latest_sealed_allocation_epoch(session, context.epoch)
        if epoch_row is None:
            return None
        now = await _database_now(session)
        if epoch_row.input_valid_until is None or epoch_row.input_valid_until <= now:
            return None
        complete = epoch_row.complete_payload
        ranks = complete.get("hypothetical_launch_rank", [])
        for rank in ranks:
            earlier = (
                (
                    await session.execute(
                        select(CapacityExecutableIntent).where(
                            CapacityExecutableIntent.execution_epoch
                            == context.epoch.execution_epoch,
                            CapacityExecutableIntent.allocation_epoch == epoch_row.allocation_epoch,
                            CapacityExecutableIntent.launch_rank < rank["rank"],
                        )
                    )
                )
                .scalars()
                .all()
            )
            if len(earlier) != rank["rank"] - 1 or any(
                item.state
                not in {"submitting-unknown", "observed", "terminal", "closing", "released"}
                for item in earlier
            ):
                return None
            if rank.get("pool_id") != context.executor.pool_id:
                continue
            existing = (
                await session.execute(
                    select(CapacityExecutableIntent.id).where(
                        CapacityExecutableIntent.execution_epoch == context.epoch.execution_epoch,
                        CapacityExecutableIntent.allocation_epoch == epoch_row.allocation_epoch,
                        CapacityExecutableIntent.launch_rank == rank["rank"],
                    )
                )
            ).scalar_one_or_none()
            if existing is not None:
                continue
            proposal, binding = await self._proposal_for_rank(
                session,
                context,
                epoch_row,
                rank,
            )
            await self._assert_increase_eligible(session, context, proposed=binding)
            session.add(
                CapacityExecutableIntent(
                    intent_id=binding.intent_id,
                    tranche_id=binding.tranche_id,
                    shape_instance_id=binding.shape_instance_id,
                    execution_epoch=binding.execution.execution_epoch,
                    execution_manifest_sha256=binding.execution.execution_manifest_sha256,
                    configuration_epoch=binding.execution.configuration_epoch,
                    allocation_epoch=binding.execution.allocation_epoch,
                    executor_id=binding.executor_id,
                    executor_incarnation=binding.executor_incarnation,
                    pool_id=binding.pool_id,
                    pool_generation=binding.pool_generation,
                    subject_id=binding.subject_id,
                    subject_incarnation=binding.subject_incarnation,
                    launch_rank=rank["rank"],
                    proposal_digest=canonical_executable_digest(proposal),
                    proposal_payload=proposal.model_dump(mode="json", exclude_none=False),
                    binding_digest=canonical_executable_digest(binding),
                    binding_payload=binding.model_dump(mode="json", exclude_none=False),
                    state="proposed",
                )
            )
            return proposal
        return None

    async def _proposal_for_rank(
        self,
        session: AsyncSession,
        context: LockedExecutionContext,
        epoch_row: CapacityAllocationEpoch,
        rank: dict[str, Any],
    ) -> tuple[ExecutableReservationProposalV2, ExecutableIntentBindingV2]:
        allocation = (
            await session.execute(
                select(CapacityAllocation).where(
                    CapacityAllocation.allocation_epoch == epoch_row.allocation_epoch,
                    CapacityAllocation.subject_id == UUID(rank["subject_id"]),
                    CapacityAllocation.pool_id == rank["pool_id"],
                    CapacityAllocation.mode == "executable",
                )
            )
        ).scalar_one_or_none()
        if allocation is None:
            raise ExecutionConflictError("executable allocation changed")
        subject = await self._current_subject(session, context.epoch, allocation)
        candidate = await self._current_candidate(session, subject)
        profile = await self._current_profile(session, allocation)
        shape = next(
            (
                item
                for item in profile.shape_catalog
                if rank["shape_instance_id"].startswith(
                    "shape-"
                    + hashlib.sha256(
                        f"{allocation.subject_id}:{allocation.pool_id}:{item['shape_id']}".encode()
                    ).hexdigest()[:24]
                    + "-"
                )
            ),
            None,
        )
        if shape is None:
            raise ExecutionConflictError("worker profile shape changed")
        witness = next(
            item
            for item in epoch_row.complete_payload["pool_witnesses"]
            if item["pool_id"] == allocation.pool_id
        )
        placement = next(
            item
            for item in witness["placements"]
            if item["instance_id"] == rank["shape_instance_id"]
        )
        stable = (
            f"{context.epoch.execution_epoch}:{epoch_row.allocation_epoch}:"
            f"{rank['rank']}:{rank['shape_instance_id']}"
        )
        tranche_id = uuid5(_EXECUTION_NAMESPACE, f"tranche:{stable}")
        intent_id = uuid5(_EXECUTION_NAMESPACE, f"intent:{stable}")
        fence = self._fence_from_rows(
            context.authority,
            context.epoch,
            allocation_epoch=epoch_row.allocation_epoch,
        )
        resources = ResourceVectorV1.model_validate(shape["total_resources"])
        binding = ExecutableIntentBindingV2(
            execution=fence,
            tranche_id=tranche_id,
            intent_id=intent_id,
            shape_instance_id=rank["shape_instance_id"],
            subject_id=allocation.subject_id,
            subject_incarnation=allocation.subject_incarnation,
            account_id=subject.account_id,
            tier_id=cast(Literal["production", "staging", "development"], subject.tier_id),
            candidate=candidate,
            candidate_generation=subject.candidate_generation,
            deployment_generation=allocation.deployment_generation,
            pool_id=allocation.pool_id,
            pool_generation=profile.pool_generation,
            executor_id=context.executor.executor_id,
            executor_incarnation=context.executor.executor_incarnation,
            shape_id=shape["shape_id"],
            profile_id=shape["shape_id"],
            profile_generation=profile.profile_generation,
            profile_digest=profile.profile_digest,
            concurrency_slots=shape["concurrency_slots"],
            resources=resources,
            node_ids=tuple(placement["node_ids"]),
        )
        proposal = ExecutableReservationProposalV2(
            tranche_id=tranche_id,
            execution=fence,
            subject_id=binding.subject_id,
            subject_incarnation=binding.subject_incarnation,
            account_id=binding.account_id,
            tier_id=binding.tier_id,
            candidate=binding.candidate,
            candidate_generation=binding.candidate_generation,
            deployment_generation=binding.deployment_generation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            shapes=(
                ReservationShapeV1(
                    shape_instance_id=binding.shape_instance_id,
                    intent_id=binding.intent_id,
                    shape_id=binding.shape_id,
                    profile_id=binding.profile_id,
                    profile_generation=binding.profile_generation,
                    profile_digest=binding.profile_digest,
                    concurrency_slots=binding.concurrency_slots,
                    resources=binding.resources,
                    node_ids=binding.node_ids,
                ),
            ),
        )
        return proposal, binding

    async def _assert_increase_eligible(
        self,
        session: AsyncSession,
        context: LockedExecutionContext,
        *,
        proposed: ExecutableIntentBindingV2 | None = None,
        current: CapacityExecutableIntent | None = None,
    ) -> DurableLaunchDeadlines:
        if context.authority.execution_state != "active" or context.authority.increase_freeze:
            raise ExecutionConflictError("execution fence changed")
        now = await _database_now(session)
        if context.executor.inventory_payload is None:
            raise ExecutionConflictError("fresh complete executor inventory is required")
        inventory = ExecutableExecutorInventoryV2.model_validate_json(
            json.dumps(context.executor.inventory_payload)
        )
        if context.executor.last_inventory_at is None:
            raise ExecutionConflictError("fresh complete executor inventory is required")
        executor_inventory_fresh_until = context.executor.last_inventory_at + self._inventory_freshness
        if (
            inventory.inventory_sequence != context.executor.inventory_high_water
            or executor_inventory_fresh_until <= now
        ):
            raise ExecutionConflictError("fresh complete executor inventory is required")
        pool_observation = (
            await session.execute(
                select(CapacityPoolObservation)
                .where(
                    CapacityPoolObservation.pool_id == context.executor.pool_id,
                    CapacityPoolObservation.validity == "valid",
                )
                .order_by(CapacityPoolObservation.database_received_at.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if pool_observation is None:
            raise ExecutionConflictError("fresh complete pool inventory is required")
        pool_inventory_fresh_until = (
            pool_observation.database_received_at + self._inventory_freshness
        )
        if (
            pool_observation.payload.get("pool_generation") != context.executor.pool_generation
            or pool_inventory_fresh_until <= now
        ):
            raise ExecutionConflictError("fresh complete pool inventory is required")
        observation = PoolObservationV1.model_validate_json(json.dumps(pool_observation.payload))
        binding = proposed
        if binding is None and current is not None:
            binding = ExecutableIntentBindingV2.model_validate_json(
                json.dumps(current.binding_payload)
            )
        if binding is None:
            raise ExecutionConflictError("increase binding is unavailable")
        latest_epoch = await self._locked_latest_sealed_allocation_epoch(session, context.epoch)
        if (
            latest_epoch is None
            or latest_epoch.allocation_epoch != binding.execution.allocation_epoch
        ):
            raise ExecutionConflictError("latest sealed executable allocation changed")
        if latest_epoch.input_valid_until is None or latest_epoch.input_valid_until <= now:
            raise ExecutionConflictError("allocation input expired")
        committed = checked_sum_vectors(tuple(item.resources for item in observation.commitments))
        allocation = (
            await session.execute(
                select(CapacityAllocation).where(
                    CapacityAllocation.allocation_epoch == binding.execution.allocation_epoch,
                    CapacityAllocation.subject_id == binding.subject_id,
                    CapacityAllocation.subject_incarnation == binding.subject_incarnation,
                    CapacityAllocation.pool_id == binding.pool_id,
                    CapacityAllocation.mode == "executable",
                )
            )
        ).scalar_one_or_none()
        if allocation is None:
            raise ExecutionConflictError("executable allocation changed")
        subject = await self._current_subject(session, context.epoch, allocation)
        candidate = await self._current_candidate(session, subject)
        profile = await self._current_profile(session, allocation)
        if (
            binding.candidate != candidate
            or binding.candidate_generation != subject.candidate_generation
            or binding.deployment_generation != subject.deployment_generation
            or binding.profile_generation != profile.profile_generation
            or binding.profile_digest != profile.profile_digest
            or binding.execution.trusted_fleet_release_sha256
            != context.epoch.trusted_fleet_release_sha256
        ):
            raise ExecutionConflictError("increase binding changed")
        charged_rows = (
            (
                await session.execute(
                    select(CapacityExecutableIntent).where(
                        CapacityExecutableIntent.execution_epoch == context.epoch.execution_epoch,
                        CapacityExecutableIntent.state != "released",
                    )
                )
            )
            .scalars()
            .all()
        )
        charged_slots = sum(
            ExecutableIntentBindingV2.model_validate_json(
                json.dumps(charged.binding_payload)
            ).concurrency_slots
            for charged in charged_rows
            if current is None or charged.id != current.id
        )
        if charged_slots + binding.concurrency_slots > context.epoch.effective_ceiling:
            raise ExecutionConflictError("executable capacity ceiling exhausted")
        pool = (
            await session.execute(
                select(CapacityPool).where(
                    CapacityPool.configuration_epoch == context.epoch.configuration_epoch,
                    CapacityPool.pool_id == binding.pool_id,
                    CapacityPool.pool_generation == binding.pool_generation,
                    CapacityPool.health == "eligible",
                )
            )
        ).scalar_one_or_none()
        if pool is None:
            raise ExecutionConflictError("pool profile is not eligible")
        total = checked_sum_vectors(
            tuple(
                ResourceVectorV1.model_validate(node["allocatable"])
                for domain in pool.topology["resource_domains"]
                for node in domain["nodes"]
            )
        )
        if not vector_fits(checked_add_vectors(committed, binding.resources), total):
            raise ExecutionConflictError("pool headroom changed")
        shape_payload = next(
            (item for item in profile.shape_catalog if item["shape_id"] == binding.shape_id),
            None,
        )
        if shape_payload is None:
            raise ExecutionConflictError("selected node headroom changed")
        shape = WorkerShapeV1.model_validate(shape_payload)
        selected = frozenset(binding.node_ids)
        configured_domains = tuple(
            ResourceDomainV1.model_validate(item) for item in pool.topology["resource_domains"]
        )
        domains = tuple(
            domain.model_copy(
                update={"nodes": tuple(node for node in domain.nodes if node.node_id in selected)}
            )
            for domain in configured_domains
            if any(node.node_id in selected for node in domain.nodes)
        )
        if len(shape.node_resources) != len(selected) or sum(
            len(domain.nodes) for domain in domains
        ) != len(selected):
            raise ExecutionConflictError("selected node headroom changed")
        overlapping = tuple(
            item
            for item in observation.commitments
            if not item.node_ids or selected.intersection(item.node_ids)
        )
        try:
            witness = pack_topology(
                PackingRequestV1(
                    pool_id=binding.pool_id,
                    domains=domains,
                    fixed_commitments=overlapping,
                    desired_shapes=(
                        PackingShapeRequestV1(
                            instance_id=binding.shape_instance_id,
                            shape=shape,
                        ),
                    ),
                )
            )
        except (TopologyInfeasible, TopologySearchLimit) as exc:
            raise ExecutionConflictError("selected node headroom changed") from exc
        if set(witness.placements[0].node_ids) != selected:
            raise ExecutionConflictError("selected node headroom changed")
        account = (
            await session.execute(
                select(CapacityAccountPolicy).where(
                    CapacityAccountPolicy.configuration_epoch == context.epoch.configuration_epoch,
                    CapacityAccountPolicy.account_id == binding.account_id,
                )
            )
        ).scalar_one_or_none()
        if (
            account is None
            or min(
                context.epoch.effective_rate_per_minute,
                context.authority.global_submission_rate_ceiling,
                pool.submission_rate_per_minute,
                subject.submission_rate_per_minute,
                account.submission_rate_per_minute,
            )
            <= 0
        ):
            raise ExecutionConflictError("launch rate is exhausted")
        return DurableLaunchDeadlines(
            permit_expires_at=(None if current is None else current.permit_expires_at),
            executor_lease_expires_at=context.executor.lease_expires_at,
            executor_inventory_fresh_until=executor_inventory_fresh_until,
            pool_inventory_fresh_until=pool_inventory_fresh_until,
            allocation_input_valid_until=latest_epoch.input_valid_until,
        )

    async def _consume_rate_tokens(
        self,
        session: AsyncSession,
        context: LockedExecutionContext,
        row: CapacityExecutableIntent,
        *,
        now: datetime,
    ) -> None:
        binding = ExecutableIntentBindingV2.model_validate_json(json.dumps(row.binding_payload))
        pool = (
            await session.execute(
                select(CapacityPool).where(
                    CapacityPool.configuration_epoch == context.epoch.configuration_epoch,
                    CapacityPool.pool_id == binding.pool_id,
                    CapacityPool.pool_generation == binding.pool_generation,
                )
            )
        ).scalar_one()
        subject = (
            await session.execute(
                select(CapacitySubject).where(
                    CapacitySubject.configuration_epoch == context.epoch.configuration_epoch,
                    CapacitySubject.subject_id == binding.subject_id,
                    CapacitySubject.subject_incarnation == binding.subject_incarnation,
                )
            )
        ).scalar_one()
        account = (
            await session.execute(
                select(CapacityAccountPolicy).where(
                    CapacityAccountPolicy.configuration_epoch == context.epoch.configuration_epoch,
                    CapacityAccountPolicy.account_id == binding.account_id,
                )
            )
        ).scalar_one()
        global_rate = min(
            context.epoch.effective_rate_per_minute,
            context.authority.global_submission_rate_ceiling,
        )
        scopes = (
            ("global", "fleet", global_rate),
            ("account", binding.account_id, account.submission_rate_per_minute),
            ("subject", str(binding.subject_id), subject.submission_rate_per_minute),
            ("pool", binding.pool_id, pool.submission_rate_per_minute),
        )
        buckets: list[CapacityExecutableLaunchRateBucket] = []
        for scope, identity, rate in scopes:
            bucket = (
                await session.execute(
                    select(CapacityExecutableLaunchRateBucket)
                    .where(
                        CapacityExecutableLaunchRateBucket.execution_epoch
                        == context.epoch.execution_epoch,
                        CapacityExecutableLaunchRateBucket.scope == scope,
                        CapacityExecutableLaunchRateBucket.scope_identity == identity,
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if bucket is None:
                capacity = rate * MICROTOKENS_PER_LAUNCH
                bucket = CapacityExecutableLaunchRateBucket(
                    execution_epoch=context.epoch.execution_epoch,
                    configuration_epoch=context.epoch.configuration_epoch,
                    scope=scope,
                    scope_identity=identity,
                    rate_per_minute=rate,
                    capacity_microtokens=capacity,
                    available_microtokens=capacity,
                    refill_remainder=0,
                    last_refill_at=now,
                )
                session.add(bucket)
            elif (
                bucket.configuration_epoch != context.epoch.configuration_epoch
                or bucket.rate_per_minute != rate
            ):
                raise ExecutionConflictError("durable executable launch rate changed")
            else:
                self._refill_rate_bucket(bucket, now)
            buckets.append(bucket)
        if any(bucket.available_microtokens < MICROTOKENS_PER_LAUNCH for bucket in buckets):
            raise ExecutionConflictError("launch rate is exhausted")
        for bucket in buckets:
            bucket.available_microtokens -= MICROTOKENS_PER_LAUNCH

    @staticmethod
    def _assert_final_launch_deadlines(
        deadlines: DurableLaunchDeadlines,
        *,
        final_now: datetime,
    ) -> None:
        permit_expires_at = deadlines.permit_expires_at
        if permit_expires_at is None:
            raise ExecutionConflictError("launch permit is not current")
        if final_now + timedelta(seconds=1) >= min(
            permit_expires_at,
            deadlines.executor_lease_expires_at,
            deadlines.executor_inventory_fresh_until,
            deadlines.pool_inventory_fresh_until,
            deadlines.allocation_input_valid_until,
        ):
            raise ExecutionConflictError("final deadline changed")

    @staticmethod
    def _refill_rate_bucket(
        bucket: CapacityExecutableLaunchRateBucket,
        now: datetime,
    ) -> None:
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        elapsed = now - bucket.last_refill_at
        elapsed_microseconds = max(
            0,
            elapsed.days * 86_400_000_000 + elapsed.seconds * 1_000_000 + elapsed.microseconds,
        )
        if elapsed_microseconds == 0:
            return
        numerator = elapsed_microseconds * bucket.rate_per_minute + bucket.refill_remainder
        added, remainder = divmod(numerator, 60)
        bucket.available_microtokens = min(
            bucket.capacity_microtokens,
            bucket.available_microtokens + added,
        )
        bucket.refill_remainder = (
            0 if bucket.available_microtokens == bucket.capacity_microtokens else remainder
        )
        bucket.last_refill_at = now

    async def _assert_pending_limits(
        self,
        session: AsyncSession,
        context: LockedExecutionContext,
        target: CapacityExecutableIntent,
    ) -> datetime:
        binding = ExecutableIntentBindingV2.model_validate_json(json.dumps(target.binding_payload))
        now = await _database_now(session)
        tier = (
            await session.execute(
                select(CapacityTier)
                .where(
                    CapacityTier.configuration_epoch == context.epoch.configuration_epoch,
                    CapacityTier.tier_id == binding.tier_id,
                )
                .with_for_update()
            )
        ).scalar_one()
        account = (
            await session.execute(
                select(CapacityAccountPolicy)
                .where(
                    CapacityAccountPolicy.configuration_epoch == context.epoch.configuration_epoch,
                    CapacityAccountPolicy.account_id == binding.account_id,
                )
                .with_for_update()
            )
        ).scalar_one()
        subject = (
            await session.execute(
                select(CapacitySubject)
                .where(
                    CapacitySubject.configuration_epoch == context.epoch.configuration_epoch,
                    CapacitySubject.subject_id == binding.subject_id,
                    CapacitySubject.subject_incarnation == binding.subject_incarnation,
                )
                .with_for_update()
            )
        ).scalar_one()
        pool = (
            await session.execute(
                select(CapacityPool)
                .where(
                    CapacityPool.configuration_epoch == context.epoch.configuration_epoch,
                    CapacityPool.pool_id == binding.pool_id,
                    CapacityPool.pool_generation == binding.pool_generation,
                )
                .with_for_update()
            )
        ).scalar_one()
        pools = tuple(
            (
                await session.execute(
                    select(CapacityPool)
                    .where(CapacityPool.configuration_epoch == context.epoch.configuration_epoch)
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        observations: dict[str, PoolObservationV1] = {}
        earliest_fresh_until: datetime | None = None
        for configured_pool in pools:
            observation_row = (
                await session.execute(
                    select(CapacityPoolObservation)
                    .where(CapacityPoolObservation.pool_id == configured_pool.pool_id)
                    .order_by(CapacityPoolObservation.database_received_at.desc())
                    .limit(1)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            fresh_until = (
                None
                if observation_row is None
                else observation_row.database_received_at + self._inventory_freshness
            )
            if (
                observation_row is None
                or observation_row.validity != "valid"
                or fresh_until is None
                or fresh_until <= now
            ):
                raise ExecutionConflictError("pending pool inventory changed")
            observation = PoolObservationV1.model_validate_json(json.dumps(observation_row.payload))
            if observation.pool_generation != configured_pool.pool_generation:
                raise ExecutionConflictError("pending pool inventory changed")
            if earliest_fresh_until is None or fresh_until < earliest_fresh_until:
                earliest_fresh_until = fresh_until
            observations[configured_pool.pool_id] = observation
        subject_rows = tuple(
            (
                await session.execute(
                    select(CapacitySubject)
                    .where(CapacitySubject.configuration_epoch == context.epoch.configuration_epoch)
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        subject_scopes = {
            (row.subject_id, row.subject_incarnation): (row.account_id, row.tier_id)
            for row in subject_rows
        }
        pending = tuple(
            (
                await session.execute(
                    select(CapacityExecutableIntent)
                    .where(
                        CapacityExecutableIntent.execution_epoch == context.epoch.execution_epoch,
                        CapacityExecutableIntent.id != target.id,
                        (
                            (CapacityExecutableIntent.state == "submitting-unknown")
                            | (CapacityExecutableIntent.observed_state == "pending")
                        ),
                    )
                    .order_by(
                        CapacityExecutableIntent.allocation_epoch,
                        CapacityExecutableIntent.launch_rank,
                        CapacityExecutableIntent.intent_id,
                    )
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        totals = {scope: [0, 0] for scope in ("global", "tier", "account", "subject", "pool")}

        def add(
            *,
            subject_id: UUID | None,
            account_id: str | None,
            tier_id: str | None,
            pool_id: str,
            slots: int,
        ) -> None:
            totals["global"][0] += slots
            totals["global"][1] += 1
            for scope, applies in (
                ("tier", tier_id == binding.tier_id),
                ("account", account_id == binding.account_id),
                ("subject", subject_id == binding.subject_id),
                ("pool", pool_id == binding.pool_id),
            ):
                if applies:
                    totals[scope][0] += slots
                    totals[scope][1] += 1

        counted_reservation_identities: set[str] = set()
        for pending_row in pending:
            pending_binding = ExecutableIntentBindingV2.model_validate_json(
                json.dumps(pending_row.binding_payload)
            )
            counted_reservation_identities.add(str(pending_binding.intent_id))
            add(
                subject_id=pending_binding.subject_id,
                account_id=pending_binding.account_id,
                tier_id=pending_binding.tier_id,
                pool_id=pending_binding.pool_id,
                slots=pending_binding.concurrency_slots,
            )
        for observation in observations.values():
            for commitment in observation.commitments:
                if commitment.state not in {
                    "proposed",
                    "accepted",
                    "pending",
                    "submitting-unknown",
                }:
                    continue
                if (
                    commitment.kind == "physical"
                    and commitment.ownership_state == "authenticated"
                    and commitment.reservation_identity in counted_reservation_identities
                ):
                    continue
                account_id: str | None = None
                tier_id: str | None = None
                if commitment.subject_id is not None and commitment.subject_incarnation is not None:
                    scope_binding = subject_scopes.get(
                        (commitment.subject_id, commitment.subject_incarnation)
                    )
                    if scope_binding is not None:
                        account_id, tier_id = scope_binding
                add(
                    subject_id=commitment.subject_id,
                    account_id=account_id,
                    tier_id=tier_id,
                    pool_id=commitment.pool_id,
                    slots=commitment.resources.slots,
                )
        add(
            subject_id=binding.subject_id,
            account_id=binding.account_id,
            tier_id=binding.tier_id,
            pool_id=binding.pool_id,
            slots=binding.concurrency_slots,
        )
        limits = {
            "global": (
                context.authority.global_pending_slot_ceiling,
                context.authority.global_pending_job_ceiling,
            ),
            "tier": (tier.max_pending_slots, tier.max_pending_jobs),
            "account": (account.max_pending_slots, account.max_pending_jobs),
            "subject": (subject.max_pending_slots, subject.max_pending_jobs),
            "pool": (pool.max_pending_slots, pool.max_pending_jobs),
        }
        for scope, (slot_limit, job_limit) in limits.items():
            slots, jobs = totals[scope]
            if slots > slot_limit or jobs > job_limit:
                raise ExecutionConflictError(f"{scope} pending limit changed")
        assert earliest_fresh_until is not None
        return earliest_fresh_until

    @staticmethod
    def _assert_central_launch_order(
        locked_intents: tuple[CapacityExecutableIntent, ...],
        target: CapacityExecutableIntent,
    ) -> None:
        earlier = tuple(item for item in locked_intents if item.launch_rank < target.launch_rank)
        if len(earlier) != target.launch_rank - 1 or any(
            item.state not in {"submitting-unknown", "observed", "terminal", "closing", "released"}
            for item in earlier
        ):
            raise ExecutionConflictError("an earlier global launch is unresolved")

    @staticmethod
    async def _current_subject(
        session: AsyncSession,
        epoch: CapacityExecutionEpoch,
        allocation: CapacityAllocation,
    ) -> CapacitySubject:
        row = (
            await session.execute(
                select(CapacitySubject).where(
                    CapacitySubject.configuration_epoch == epoch.configuration_epoch,
                    CapacitySubject.subject_id == allocation.subject_id,
                    CapacitySubject.subject_incarnation == allocation.subject_incarnation,
                    CapacitySubject.deployment_generation == allocation.deployment_generation,
                    CapacitySubject.lifecycle_state == "active",
                )
            )
        ).scalar_one_or_none()
        if row is None:
            raise ExecutionConflictError("subject lifecycle changed")
        return row

    @staticmethod
    async def _current_candidate(
        session: AsyncSession,
        subject: CapacitySubject,
    ) -> CandidateBindingV2:
        row = (
            await session.execute(
                select(CapacityCandidate).where(
                    CapacityCandidate.subject_id == subject.subject_id,
                    CapacityCandidate.subject_incarnation == subject.subject_incarnation,
                    CapacityCandidate.candidate_generation == subject.candidate_generation,
                )
            )
        ).scalar_one_or_none()
        if row is None:
            raise ExecutionConflictError("candidate binding changed")
        publication = row.source_payload.get("publication_sha256")
        if not isinstance(publication, str):
            raise ExecutionConflictError("candidate publication binding changed")
        return CandidateBindingV2(
            algorithm=cast(Literal["git-sha1", "source-sha256"], row.candidate_identity_algorithm),
            identity=row.candidate_identity,
            publication_sha256=publication,
        )

    @staticmethod
    async def _current_profile(
        session: AsyncSession,
        allocation: CapacityAllocation,
    ) -> CapacityWorkerProfile:
        row = (
            await session.execute(
                select(CapacityWorkerProfile).where(
                    CapacityWorkerProfile.subject_id == allocation.subject_id,
                    CapacityWorkerProfile.subject_incarnation == allocation.subject_incarnation,
                    CapacityWorkerProfile.deployment_generation == allocation.deployment_generation,
                    CapacityWorkerProfile.pool_id == allocation.pool_id,
                )
            )
        ).scalar_one_or_none()
        if row is None:
            raise ExecutionConflictError("worker profile changed")
        return row

    def _new_permit(
        self,
        row: CapacityExecutableIntent,
        now: datetime,
    ) -> ExecutableLaunchPermitV2:
        binding = ExecutableIntentBindingV2.model_validate_json(json.dumps(row.binding_payload))
        permit_epoch = (row.permit_epoch or 0) + 1
        return ExecutableLaunchPermitV2(
            permit_id=uuid5(_EXECUTION_NAMESPACE, f"permit:{row.intent_id}:{permit_epoch}"),
            binding=binding,
            permit_epoch=permit_epoch,
            launch_rank=row.launch_rank,
            expires_at=now + self._permit_ttl,
        )

    @staticmethod
    async def _locked_intent(
        session: AsyncSession,
        intent_id: UUID,
    ) -> CapacityExecutableIntent:
        row = (
            await session.execute(
                select(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.intent_id == intent_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if row is None:
            raise ExecutionConflictError("executable intent is unknown")
        return row

    @staticmethod
    async def _locked_intent_by_tranche(
        session: AsyncSession,
        tranche_id: UUID,
    ) -> CapacityExecutableIntent:
        row = (
            await session.execute(
                select(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.tranche_id == tranche_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if row is None:
            raise ExecutionConflictError("reservation proposal is unknown")
        return row

    @staticmethod
    async def _locked_allocation_intents(
        session: AsyncSession,
        target: ExecutableIntentBindingV2,
    ) -> tuple[CapacityExecutableIntent, ...]:
        return tuple(
            (
                await session.execute(
                    select(CapacityExecutableIntent)
                    .where(
                        CapacityExecutableIntent.execution_epoch == target.execution.execution_epoch,
                        CapacityExecutableIntent.allocation_epoch == target.execution.allocation_epoch,
                    )
                    .order_by(
                        CapacityExecutableIntent.allocation_epoch,
                        CapacityExecutableIntent.launch_rank,
                        CapacityExecutableIntent.intent_id,
                    )
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )

    @staticmethod
    async def _locked_protected_release_receipts(
        session: AsyncSession,
        intent_ids: tuple[UUID, ...],
    ) -> dict[UUID, CapacityExecutableProtectedReleaseReceipt]:
        if not intent_ids:
            return {}
        rows = tuple(
            (
                await session.execute(
                    select(CapacityExecutableProtectedReleaseReceipt)
                    .where(
                        CapacityExecutableProtectedReleaseReceipt.intent_id.in_(intent_ids)
                    )
                    .order_by(
                        CapacityExecutableProtectedReleaseReceipt.intent_id,
                        CapacityExecutableProtectedReleaseReceipt.protected_registration_epoch,
                    )
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        latest: dict[UUID, CapacityExecutableProtectedReleaseReceipt] = {}
        for row in rows:
            latest[row.intent_id] = row
        return latest

    @staticmethod
    async def _latest_bootstrap_proposal(
        session: AsyncSession,
        intent_id: UUID,
        *,
        lock: bool,
    ) -> CapacityExecutableBootstrapProposal | None:
        statement = (
            select(CapacityExecutableBootstrapProposal)
            .where(CapacityExecutableBootstrapProposal.intent_id == intent_id)
            .order_by(CapacityExecutableBootstrapProposal.proposal_epoch.desc())
            .limit(1)
        )
        if lock:
            statement = statement.with_for_update()
        return (await session.execute(statement)).scalar_one_or_none()

    @classmethod
    async def _latest_protected_release_receipt(
        cls,
        session: AsyncSession,
        intent_id: UUID,
    ) -> CapacityExecutableProtectedReleaseReceipt | None:
        return (await cls._locked_protected_release_receipts(session, (intent_id,))).get(intent_id)

    @staticmethod
    async def _locked_inventory_intents(
        session: AsyncSession,
        inventory: ExecutableExecutorInventoryV2,
    ) -> dict[UUID, CapacityExecutableIntent]:
        intent_ids = tuple(
            sorted(
                {
                    record.ownership_proof.metadata.binding.intent_id
                    for record in inventory.records
                    if record.ownership_proof is not None
                },
                key=str,
            )
        )
        if not intent_ids:
            return {}
        rows = tuple(
            (
                await session.execute(
                    select(CapacityExecutableIntent)
                    .where(
                        CapacityExecutableIntent.intent_id.in_(intent_ids),
                        CapacityExecutableIntent.executor_incarnation
                        == inventory.executor_incarnation,
                        CapacityExecutableIntent.pool_id == inventory.pool_id,
                    )
                    .order_by(
                        CapacityExecutableIntent.allocation_epoch,
                        CapacityExecutableIntent.launch_rank,
                        CapacityExecutableIntent.intent_id,
                    )
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        return {row.intent_id: row for row in rows}

    @staticmethod
    def _inventory_terminal_conflicts(
        intent: CapacityExecutableIntent,
        record: Any,
    ) -> bool:
        if intent.state not in {"closing", "terminal"}:
            return False
        if intent.terminal_kind == "unused":
            return True
        return not (
            record.state == "terminal"
            and intent.terminal_kind == record.physical_kind
            and intent.terminal_identity == record.physical_identity
            and intent.terminal_evidence_sha256 == record.terminal_evidence_sha256
        )

    @staticmethod
    def _apply_inventory_observation(
        intent: CapacityExecutableIntent,
        inventory: ExecutableExecutorInventoryV2,
        record: Any,
    ) -> bool:
        if intent.state == "released":
            return False
        if CapacityExecutionStore._inventory_terminal_conflicts(intent, record):
            intent.state = "quarantined"
            return False
        if intent.state in {"closing", "terminal"}:
            return False
        if intent.terminal_identity is not None and intent.terminal_identity != record.physical_identity:
            intent.state = "quarantined"
            return False
        if intent.state in {"proposed", "accepted", "launch-ready", "permitted", "bound"}:
            intent.state = "quarantined"
            return False
        intent.inventory_sequence = inventory.inventory_sequence
        intent.observed_state = record.state
        intent.terminal_kind = record.physical_kind
        intent.terminal_identity = record.physical_identity
        if record.state == "terminal":
            intent.state = "terminal"
            intent.terminal_evidence_sha256 = record.terminal_evidence_sha256
        elif intent.state == "submitting-unknown":
            intent.state = "observed"
        return True

    @staticmethod
    def _executor_binding_from_row(row: CapacityExecutableIntent) -> PreparedExecutorBindingV2:
        return PreparedExecutorBindingV2(
            pool_id=cast(Literal["gb10", "oldlab"], row.pool_id),
            pool_generation=row.pool_generation,
            executor_id=row.executor_id,
            executor_incarnation=row.executor_incarnation,
            signing_key_sha256="0" * 64,
            local_authority_sha256="0" * 64,
            controller_authority_sha256="0" * 64,
        )

    @staticmethod
    def _executor_binding_from_contract(
        binding: ExecutableIntentBindingV2,
    ) -> PreparedExecutorBindingV2:
        return PreparedExecutorBindingV2(
            pool_id=cast(Literal["gb10", "oldlab"], binding.pool_id),
            pool_generation=binding.pool_generation,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            signing_key_sha256="0" * 64,
            local_authority_sha256="0" * 64,
            controller_authority_sha256="0" * 64,
        )

    @staticmethod
    def _executor_binding_from_acceptance(
        acceptance: ExecutableReservationAcceptanceV2,
    ) -> PreparedExecutorBindingV2:
        return PreparedExecutorBindingV2(
            pool_id=cast(Literal["gb10", "oldlab"], acceptance.pool_id),
            pool_generation=acceptance.pool_generation,
            executor_id=acceptance.executor_id,
            executor_incarnation=acceptance.executor_incarnation,
            signing_key_sha256="0" * 64,
            local_authority_sha256="0" * 64,
            controller_authority_sha256="0" * 64,
        )

    @staticmethod
    async def _command_replay(
        session: AsyncSession,
        row: CapacityExecutableIntent,
        *,
        sequence: int,
        operation_kind: str,
        request_digest: str,
        result_payload: dict[str, Any],
    ) -> CapacityExecutableCommandReceipt | None:
        receipt = (
            await session.execute(
                select(CapacityExecutableCommandReceipt).where(
                    CapacityExecutableCommandReceipt.executor_incarnation
                    == row.executor_incarnation,
                    CapacityExecutableCommandReceipt.command_sequence == sequence,
                )
            )
        ).scalar_one_or_none()
        if receipt is not None:
            if (
                receipt.operation_kind != operation_kind
                or receipt.request_digest != request_digest
                or receipt.result_digest != _payload_digest(result_payload)
            ):
                raise _ExecutorFenceError(
                    row.executor_incarnation,
                    "equivocal",
                    "executor command equivocated",
                )
            return receipt
        state = (
            await session.execute(
                select(CapacityExecutableExecutorState)
                .where(
                    CapacityExecutableExecutorState.executor_incarnation == row.executor_incarnation
                )
                .with_for_update()
            )
        ).scalar_one()
        if sequence != state.command_high_water + 1:
            raise ExecutionConflictError("executor command sequence is not next")
        return None

    @staticmethod
    async def _record_command(
        session: AsyncSession,
        row: CapacityExecutableIntent,
        *,
        sequence: int,
        operation_kind: str,
        request_digest: str,
        result_payload: dict[str, Any],
    ) -> None:
        result_digest = _payload_digest(result_payload)
        session.add(
            CapacityExecutableCommandReceipt(
                execution_epoch=row.execution_epoch,
                executor_incarnation=row.executor_incarnation,
                command_sequence=sequence,
                operation_kind=operation_kind,
                request_digest=request_digest,
                result_digest=result_digest,
                result_payload=result_payload,
            )
        )
        state = (
            await session.execute(
                select(CapacityExecutableExecutorState)
                .where(
                    CapacityExecutableExecutorState.executor_incarnation == row.executor_incarnation
                )
                .with_for_update()
            )
        ).scalar_one()
        state.command_high_water = sequence
        state.last_command_digest = request_digest


__all__ = [
    "AcceptedExecutableReservation",
    "AcknowledgedExecutableProtectedRelease",
    "CapacityExecutionStore",
    "ClosingExecutableIntent",
    "ConsumedExecutablePermit",
    "ExecutableExecutorCheckpoint",
    "HeartbeatedExecutableExecutor",
    "IngestedExecutableInventory",
    "LockedExecutionContext",
    "RecoveredExecutableSubmission",
    "RegisteredExecutableBootstrap",
    "ReleasedExecutableShapes",
]
