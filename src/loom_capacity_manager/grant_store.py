"""Serializable, zero-executable reservation protocol persistence.

This module deliberately has no scheduler client, subprocess call, or live
capacity transition.  It proves central ordering, identity, replay, and
accounting invariants while the authority ceiling remains permanently zero.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, cast
from uuid import UUID

from sqlalchemy import delete, func, or_, select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.contracts import (
    MICROTOKENS_PER_LAUNCH,
    ObservedCommitmentV1,
    PoolObservationV1,
    ResourceVectorV1,
    canonical_digest,
    canonical_digest_excluding,
)
from loom_capacity_manager.grant_contracts import (
    DryRunBootstrapRegistrationV1,
    DryRunExecutorHeartbeatV1,
    DryRunExecutorInventoryV1,
    DryRunExecutorRegistrationV1,
    DryRunIntentCloseV1,
    DryRunLaunchPermitV1,
    DryRunPartialReleaseV1,
    DryRunPermitConsumptionV1,
    DryRunProtectedReleaseAcknowledgementV1,
    DryRunReservationAcceptanceV1,
    DryRunReservationProposalV1,
    ExecutorInventoryRecordV1,
    OwnershipMetadataV1,
    ReleasedShapeV1,
    canonical_grant_digest,
)
from loom_capacity_manager.models import (
    CapacityAccountPolicy,
    CapacityAllocation,
    CapacityAllocationEpoch,
    CapacityAuthorityState,
    CapacityConfigurationEpoch,
    CapacityDemandReporter,
    CapacityExecutor,
    CapacityExecutorObservation,
    CapacityLaunchPermit,
    CapacityLaunchRateBucket,
    CapacityObservedCommitment,
    CapacityPool,
    CapacityPoolObservation,
    CapacityPoolReporter,
    CapacityProtectedReleaseAcknowledgement,
    CapacityReservationReleaseEvidence,
    CapacityReservationShape,
    CapacityReservationTranche,
    CapacitySubject,
    CapacitySubmissionIntent,
    CapacityTier,
    CapacityWorkerProfile,
)
from loom_capacity_manager.ownership import OwnershipKeyring
from loom_capacity_manager.store import (
    CapacityStoreError,
    IdempotencyConflictError,
    StaleWriterError,
    WriterFence,
    _audit,
    _db_now,
    _lock_authority,
    _write_transaction,
)


class GrantConflictError(CapacityStoreError):
    """One requested grant binding disagrees with current central state."""


class StaleExecutorError(CapacityStoreError):
    """The executor identity or its database-time lease is no longer current."""


class StaleCommandError(CapacityStoreError):
    """An executor command regressed or skipped its durable sequence."""


class ExecutorEquivocationError(CapacityStoreError):
    """One executor command sequence was reused with different canonical input."""


class ExecutorJournalError(CapacityStoreError):
    """The executor's durable local journal regressed or changed at high-water."""


class ProposalExpiredError(CapacityStoreError):
    """The central proposal expired before atomic acceptance."""


class ProposalSupersededError(CapacityStoreError):
    """The central proposal lost its exact allocation or configuration binding."""


class PermitExpiredError(CapacityStoreError):
    """The launch permit expired before its central dry-run transition."""


class LaunchOrderError(CapacityStoreError):
    """A pool executor attempted to pass an earlier globally eligible intent."""


class RateLimitError(CapacityStoreError):
    """At least one durable launch-rate scope lacks a complete token."""


@dataclass(frozen=True, slots=True)
class RegisteredExecutor:
    executor_row_id: UUID
    executor_incarnation: UUID
    lease_expires_at: datetime
    replayed: bool
    executable: Literal[False] = False


@dataclass(frozen=True, slots=True)
class HeartbeatedExecutor:
    executor_row_id: UUID
    heartbeat_sequence: int
    journal_sequence: int
    lease_expires_at: datetime
    replayed: bool
    executable: Literal[False] = False


@dataclass(frozen=True, slots=True)
class ExecutorCheckpoint:
    executor_row_id: UUID
    authority_incarnation: UUID
    writer_epoch: int
    executor_id: str
    executor_incarnation: UUID
    pool_id: str
    pool_generation: int
    command_sequence: int
    journal_sequence: int
    journal_digest: str
    inventory_sequence: int
    lease_expires_at: datetime
    executable: Literal[False] = False


@dataclass(frozen=True, slots=True)
class IngestedExecutorInventory:
    observation_id: UUID
    inventory_digest: str
    inventory_sequence: int
    authenticated_count: int
    quarantined_count: int
    foreign_count: int
    replayed: bool
    executable: Literal[False] = False


@dataclass(frozen=True, slots=True)
class ProposedReservation:
    tranche_id: UUID
    proposal_digest: str
    expires_at: datetime
    replayed: bool
    executable: Literal[False] = False


@dataclass(frozen=True, slots=True)
class AcceptedReservation:
    tranche_id: UUID
    intent_ids: tuple[UUID, ...]
    replayed: bool
    executable: Literal[False] = False


@dataclass(frozen=True, slots=True)
class IntentReady:
    intent_id: UUID
    bootstrap_registration_epoch: int
    replayed: bool
    executable: Literal[False] = False


@dataclass(frozen=True, slots=True)
class IssuedLaunchPermit:
    permit_id: UUID
    permit_digest: str
    expires_at: datetime
    replayed: bool
    executable: Literal[False] = False


@dataclass(frozen=True, slots=True)
class ConsumedLaunchPermit:
    permit_id: UUID
    intent_id: UUID
    replayed: bool
    executable: Literal[False] = False


@dataclass(frozen=True, slots=True)
class ClosingIntent:
    intent_id: UUID
    replayed: bool
    executable: Literal[False] = False


@dataclass(frozen=True, slots=True)
class AcknowledgedProtectedRelease:
    acknowledgement_id: UUID
    shape_instance_id: str
    acknowledgement_digest: str
    replayed: bool
    executable: Literal[False] = False


@dataclass(frozen=True, slots=True)
class ReleasedReservationShapes:
    tranche_id: UUID
    released_shape_ids: tuple[str, ...]
    replayed: bool
    executable: Literal[False] = False


def _payload_digest(value: dict[str, object]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _assert_writer(authority: CapacityAuthorityState, writer: WriterFence) -> None:
    if (
        authority.authority_incarnation != writer.authority_incarnation
        or authority.writer_epoch != writer.writer_epoch
    ):
        raise StaleWriterError("capacity writer fence changed")


class CapacityGrantStore:
    """Persist dry-run Package 3 grants beneath one global writer."""

    def __init__(
        self,
        *,
        executor_lease_seconds: int = 120,
        proposal_ttl_seconds: int = 30,
        permit_ttl_seconds: int = 15,
        pool_observation_freshness_seconds: int = 120,
        ownership_keyring: OwnershipKeyring | None = None,
    ) -> None:
        if type(executor_lease_seconds) is not int or not 1 <= executor_lease_seconds <= 3_600:
            raise ValueError("executor_lease_seconds must be between 1 and 3600")
        if type(proposal_ttl_seconds) is not int or not 1 <= proposal_ttl_seconds <= 600:
            raise ValueError("proposal_ttl_seconds must be between 1 and 600")
        if type(permit_ttl_seconds) is not int or not 1 <= permit_ttl_seconds <= 300:
            raise ValueError("permit_ttl_seconds must be between 1 and 300")
        if (
            type(pool_observation_freshness_seconds) is not int
            or not 1 <= pool_observation_freshness_seconds <= 3_600
        ):
            raise ValueError("pool observation freshness must be between 1 and 3600")
        self._executor_lease = timedelta(seconds=executor_lease_seconds)
        self._proposal_ttl = timedelta(seconds=proposal_ttl_seconds)
        self._permit_ttl = timedelta(seconds=permit_ttl_seconds)
        self._pool_observation_freshness = timedelta(seconds=pool_observation_freshness_seconds)
        self._ownership_keyring = ownership_keyring or OwnershipKeyring()

    async def register_executor(
        self,
        session: AsyncSession,
        writer: WriterFence,
        registration: DryRunExecutorRegistrationV1,
        *,
        actor: str,
        idempotency_key: UUID,
    ) -> RegisteredExecutor:
        if not actor or len(actor.encode("utf-8")) > 256:
            raise ValueError("executor registration actor is invalid")
        digest = canonical_grant_digest(registration)
        async with _write_transaction(session):
            authority = await _lock_authority(session)
            _assert_writer(authority, writer)
            replay = (
                await session.execute(
                    select(CapacityExecutor).where(
                        CapacityExecutor.registration_idempotency_key == idempotency_key
                    )
                )
            ).scalar_one_or_none()
            if replay is not None:
                if replay.registration_digest != digest or replay.registration_actor != actor:
                    raise IdempotencyConflictError(
                        "executor registration idempotency key was reused"
                    )
                return RegisteredExecutor(
                    replay.id,
                    replay.executor_incarnation,
                    replay.lease_expires_at,
                    True,
                )

            configuration_epoch = await self._latest_configuration_epoch(session)
            pool = (
                await session.execute(
                    select(CapacityPool).where(
                        CapacityPool.configuration_epoch == configuration_epoch,
                        CapacityPool.pool_id == registration.pool_id,
                        CapacityPool.pool_generation == registration.pool_generation,
                    )
                )
            ).scalar_one_or_none()
            if pool is None or pool.health != "eligible":
                raise GrantConflictError("executor pool generation is not currently eligible")
            if not self._ownership_keyring.matches(
                registration.signing_key_id,
                registration.signing_key_sha256,
            ):
                raise GrantConflictError("executor ownership signing key is not configured exactly")

            reused_incarnation = (
                await session.execute(
                    select(CapacityExecutor.id)
                    .where(
                        CapacityExecutor.executor_incarnation == registration.executor_incarnation
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if reused_incarnation is not None:
                raise GrantConflictError("executor incarnation was already registered")

            predecessor = (
                await session.execute(
                    select(CapacityExecutor)
                    .where(
                        CapacityExecutor.pool_id == registration.pool_id,
                        CapacityExecutor.state == "dry-run",
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if predecessor is not None:
                predecessor.state = "fenced"
                await session.flush()

            now = await _db_now(session)
            row = CapacityExecutor(
                executor_id=registration.executor_id,
                executor_incarnation=registration.executor_incarnation,
                pool_id=registration.pool_id,
                pool_generation=registration.pool_generation,
                authority_incarnation=writer.authority_incarnation,
                registered_writer_epoch=writer.writer_epoch,
                signing_key_id=registration.signing_key_id,
                signing_key_sha256=registration.signing_key_sha256,
                local_authority_sha256=registration.local_authority_sha256,
                registration_actor=actor,
                registration_idempotency_key=idempotency_key,
                registration_digest=digest,
                state="dry-run",
                command_high_water=0,
                last_command_digest=None,
                heartbeat_high_water=0,
                last_heartbeat_digest=None,
                journal_high_water=0,
                journal_digest=None,
                lease_expires_at=now + self._executor_lease,
                last_heartbeat_at=now,
            )
            session.add(row)
            await session.flush()
            session.add(
                _audit(
                    actor_kind="operator",
                    actor_id=actor,
                    event_kind="capacity_executor_registered_dry_run",
                    object_binding={
                        "executor_id": registration.executor_id,
                        "executor_incarnation": str(registration.executor_incarnation),
                        "pool_id": registration.pool_id,
                    },
                    detail={"registration_digest": digest, "executable": False},
                )
            )
            return RegisteredExecutor(
                row.id,
                row.executor_incarnation,
                row.lease_expires_at,
                False,
            )

    async def executor_checkpoint(
        self,
        session: AsyncSession,
        *,
        authority_incarnation: UUID,
        writer_epoch: int,
        executor_id: str,
        executor_incarnation: UUID,
        pool_id: str,
        pool_generation: int | None = None,
    ) -> ExecutorCheckpoint:
        """Return the exact central journal ancestry an incumbent must retain."""

        row = (
            await session.execute(
                select(CapacityExecutor).where(
                    CapacityExecutor.executor_id == executor_id,
                    CapacityExecutor.executor_incarnation == executor_incarnation,
                    CapacityExecutor.pool_id == pool_id,
                )
            )
        ).scalar_one_or_none()
        if (
            row is None
            or row.state != "dry-run"
            or row.authority_incarnation != authority_incarnation
            or row.registered_writer_epoch != writer_epoch
            or (pool_generation is not None and row.pool_generation != pool_generation)
        ):
            raise StaleExecutorError("executor checkpoint binding is not current")
        return ExecutorCheckpoint(
            executor_row_id=row.id,
            authority_incarnation=row.authority_incarnation,
            writer_epoch=row.registered_writer_epoch,
            executor_id=row.executor_id,
            executor_incarnation=row.executor_incarnation,
            pool_id=row.pool_id,
            pool_generation=row.pool_generation,
            command_sequence=row.command_high_water,
            journal_sequence=row.journal_high_water,
            journal_digest=row.journal_digest or "0" * 64,
            inventory_sequence=row.inventory_high_water,
            lease_expires_at=row.lease_expires_at,
        )

    async def heartbeat_executor(
        self,
        session: AsyncSession,
        heartbeat: DryRunExecutorHeartbeatV1,
    ) -> HeartbeatedExecutor:
        digest = canonical_grant_digest(heartbeat)
        equivocated = False
        journal_error: str | None = None
        result: HeartbeatedExecutor | None = None
        async with _write_transaction(session):
            authority = await _lock_authority(session)
            now = await _db_now(session)
            executor = await self._current_executor(
                session,
                executor_id=heartbeat.executor_id,
                executor_incarnation=heartbeat.executor_incarnation,
                pool_id=heartbeat.pool_id,
                now=now,
                allow_expired=True,
            )
            if (
                heartbeat.authority_incarnation != authority.authority_incarnation
                or heartbeat.writer_epoch != authority.writer_epoch
                or executor.authority_incarnation != authority.authority_incarnation
                or executor.registered_writer_epoch != authority.writer_epoch
                or executor.pool_generation != heartbeat.pool_generation
            ):
                raise StaleExecutorError("executor heartbeat authority binding is stale")
            if heartbeat.heartbeat_sequence < executor.heartbeat_high_water:
                raise StaleCommandError("executor heartbeat sequence regressed")
            if heartbeat.heartbeat_sequence == executor.heartbeat_high_water:
                if executor.last_heartbeat_digest == digest:
                    result = HeartbeatedExecutor(
                        executor.id,
                        executor.heartbeat_high_water,
                        executor.journal_high_water,
                        executor.lease_expires_at,
                        True,
                    )
                else:
                    executor.state = "equivocal"
                    equivocated = True
                    self._audit_equivocation(
                        session,
                        heartbeat.executor_id,
                        heartbeat.heartbeat_sequence,
                    )
            else:
                journal_error = self._journal_advance_error(
                    executor,
                    journal_sequence=heartbeat.journal_sequence,
                    journal_digest=heartbeat.journal_digest,
                    checkpoint_sequence=heartbeat.journal_checkpoint_sequence,
                    checkpoint_digest=heartbeat.journal_checkpoint_digest,
                )
                if journal_error is not None:
                    executor.state = "fenced"
                    session.add(
                        _audit(
                            actor_kind="executor",
                            actor_id=heartbeat.executor_id,
                            event_kind="capacity_executor_journal_fenced",
                            object_binding={
                                "executor_incarnation": str(heartbeat.executor_incarnation),
                                "heartbeat_sequence": heartbeat.heartbeat_sequence,
                            },
                            detail={"reason": journal_error},
                        )
                    )
                else:
                    executor.heartbeat_high_water = heartbeat.heartbeat_sequence
                    executor.last_heartbeat_digest = digest
                    executor.journal_high_water = heartbeat.journal_sequence
                    executor.journal_digest = heartbeat.journal_digest
                    executor.last_heartbeat_at = now
                    executor.lease_expires_at = now + self._executor_lease
                    result = HeartbeatedExecutor(
                        executor.id,
                        executor.heartbeat_high_water,
                        executor.journal_high_water,
                        executor.lease_expires_at,
                        False,
                    )
        if equivocated:
            raise ExecutorEquivocationError("executor heartbeat equivocated at high-water")
        if journal_error is not None:
            raise ExecutorJournalError(journal_error)
        assert result is not None
        return result

    async def ingest_executor_inventory(
        self,
        session: AsyncSession,
        inventory: DryRunExecutorInventoryV1,
    ) -> IngestedExecutorInventory:
        digest = canonical_grant_digest(inventory)
        equivocated = False
        journal_error: str | None = None
        result: IngestedExecutorInventory | None = None
        async with _write_transaction(session):
            authority = await _lock_authority(session)
            now = await _db_now(session)
            executor = await self._current_executor(
                session,
                executor_id=inventory.executor_id,
                executor_incarnation=inventory.executor_incarnation,
                pool_id=inventory.pool_id,
                now=now,
                allow_expired=True,
            )
            if (
                inventory.authority_incarnation != authority.authority_incarnation
                or inventory.writer_epoch != authority.writer_epoch
                or executor.authority_incarnation != authority.authority_incarnation
                or executor.registered_writer_epoch != authority.writer_epoch
                or executor.pool_generation != inventory.pool_generation
            ):
                raise StaleExecutorError("executor inventory authority binding is stale")
            if inventory.inventory_sequence < executor.inventory_high_water:
                prior = (
                    await session.execute(
                        select(CapacityExecutorObservation).where(
                            CapacityExecutorObservation.executor_incarnation
                            == inventory.executor_incarnation,
                            CapacityExecutorObservation.inventory_sequence
                            == inventory.inventory_sequence,
                        )
                    )
                ).scalar_one_or_none()
                if prior is None or prior.inventory_digest != digest:
                    raise StaleCommandError("executor inventory sequence regressed")
                return IngestedExecutorInventory(
                    prior.id,
                    prior.inventory_digest,
                    prior.inventory_sequence,
                    prior.authenticated_count,
                    prior.quarantined_count,
                    prior.foreign_count,
                    True,
                )
            if inventory.inventory_sequence == executor.inventory_high_water:
                if executor.last_inventory_digest == digest:
                    prior = (
                        await session.execute(
                            select(CapacityExecutorObservation).where(
                                CapacityExecutorObservation.executor_incarnation
                                == inventory.executor_incarnation,
                                CapacityExecutorObservation.inventory_sequence
                                == inventory.inventory_sequence,
                            )
                        )
                    ).scalar_one()
                    result = IngestedExecutorInventory(
                        prior.id,
                        prior.inventory_digest,
                        prior.inventory_sequence,
                        prior.authenticated_count,
                        prior.quarantined_count,
                        prior.foreign_count,
                        True,
                    )
                else:
                    executor.state = "equivocal"
                    equivocated = True
                    self._audit_equivocation(
                        session,
                        inventory.executor_id,
                        inventory.inventory_sequence,
                    )
            else:
                journal_error = self._journal_advance_error(
                    executor,
                    journal_sequence=inventory.journal_sequence,
                    journal_digest=inventory.journal_digest,
                    checkpoint_sequence=inventory.journal_checkpoint_sequence,
                    checkpoint_digest=inventory.journal_checkpoint_digest,
                )
                if journal_error is not None:
                    executor.state = "fenced"
                    session.add(
                        _audit(
                            actor_kind="executor",
                            actor_id=inventory.executor_id,
                            event_kind="capacity_executor_inventory_journal_fenced",
                            object_binding={
                                "executor_incarnation": str(inventory.executor_incarnation),
                                "inventory_sequence": inventory.inventory_sequence,
                            },
                            detail={"reason": journal_error},
                        )
                    )
                else:
                    classified_records: list[
                        tuple[
                            ExecutorInventoryRecordV1,
                            Literal["authenticated", "quarantined", "foreign"],
                        ]
                    ] = []
                    for record in inventory.records:
                        classified_records.append(
                            (
                                record,
                                await self._classify_inventory_record(
                                    session,
                                    executor=executor,
                                    record=record,
                                ),
                            )
                        )
                    authenticated_intents = Counter(
                        record.ownership_proof.metadata.intent_id
                        for record, classification in classified_records
                        if classification == "authenticated" and record.ownership_proof is not None
                    )
                    duplicate_intents = {
                        intent_id for intent_id, count in authenticated_intents.items() if count > 1
                    }
                    classifications: list[dict[str, object]] = []
                    authenticated = 0
                    quarantined = 0
                    foreign = 0
                    for record, initial_classification in classified_records:
                        proof = record.ownership_proof
                        classification = initial_classification
                        if (
                            classification == "authenticated"
                            and proof is not None
                            and proof.metadata.intent_id in duplicate_intents
                        ):
                            classification = "quarantined"
                            intent = (
                                await session.execute(
                                    select(CapacitySubmissionIntent)
                                    .where(CapacitySubmissionIntent.id == proof.metadata.intent_id)
                                    .with_for_update()
                                )
                            ).scalar_one()
                            intent.state = "quarantined"
                        if classification != "foreign":
                            classification = await self._upsert_inventory_commitment(
                                session,
                                executor=executor,
                                inventory_sequence=inventory.inventory_sequence,
                                record=record,
                                classification=classification,
                                now=now,
                            )
                        classifications.append(
                            {
                                "physical_identity": record.physical_identity,
                                "classification": classification,
                            }
                        )
                        if classification == "authenticated":
                            authenticated += 1
                        elif classification == "quarantined":
                            quarantined += 1
                        else:
                            foreign += 1
                    await self._quarantine_missing_inventory(
                        session,
                        executor=executor,
                        inventory_sequence=inventory.inventory_sequence,
                        reported_identities={
                            record.physical_identity
                            for record, classification in classified_records
                            if classification != "foreign"
                        },
                        now=now,
                    )
                    observation = CapacityExecutorObservation(
                        executor_row_id=executor.id,
                        executor_incarnation=executor.executor_incarnation,
                        pool_id=executor.pool_id,
                        pool_generation=executor.pool_generation,
                        inventory_sequence=inventory.inventory_sequence,
                        inventory_digest=digest,
                        journal_sequence=inventory.journal_sequence,
                        journal_digest=inventory.journal_digest,
                        authenticated_count=authenticated,
                        quarantined_count=quarantined,
                        foreign_count=foreign,
                        payload=inventory.model_dump(mode="json", exclude_none=False),
                        classification_payload=classifications,
                        validity="valid",
                        executable=False,
                        database_received_at=now,
                    )
                    session.add(observation)
                    executor.inventory_high_water = inventory.inventory_sequence
                    executor.last_inventory_digest = digest
                    executor.journal_high_water = inventory.journal_sequence
                    executor.journal_digest = inventory.journal_digest
                    executor.last_heartbeat_at = now
                    executor.lease_expires_at = now + self._executor_lease
                    await session.flush()
                    result = IngestedExecutorInventory(
                        observation.id,
                        digest,
                        inventory.inventory_sequence,
                        authenticated,
                        quarantined,
                        foreign,
                        False,
                    )
        if equivocated:
            raise ExecutorEquivocationError("executor inventory equivocated at high-water")
        if journal_error is not None:
            raise ExecutorJournalError(journal_error)
        assert result is not None
        return result

    async def propose_reservation(
        self,
        session: AsyncSession,
        writer: WriterFence,
        proposal: DryRunReservationProposalV1,
        *,
        idempotency_key: UUID,
    ) -> ProposedReservation:
        digest = canonical_grant_digest(proposal)
        async with _write_transaction(session):
            authority = await _lock_authority(session)
            _assert_writer(authority, writer)
            replay = (
                await session.execute(
                    select(CapacityReservationTranche).where(
                        CapacityReservationTranche.idempotency_key == idempotency_key
                    )
                )
            ).scalar_one_or_none()
            if replay is not None:
                if replay.proposal_digest != digest:
                    raise IdempotencyConflictError(
                        "reservation idempotency key was reused with different input"
                    )
                return ProposedReservation(
                    replay.id,
                    replay.proposal_digest,
                    replay.expires_at,
                    True,
                )

            if (
                proposal.authority_incarnation != writer.authority_incarnation
                or proposal.writer_epoch != writer.writer_epoch
            ):
                raise StaleWriterError("reservation proposal writer binding is stale")
            now = await _db_now(session)
            executor = await self._current_executor(
                session,
                executor_id=proposal.executor_id,
                executor_incarnation=proposal.executor_incarnation,
                pool_id=proposal.pool_id,
                now=now,
            )
            if executor.pool_generation != proposal.pool_generation:
                raise GrantConflictError("reservation executor pool generation changed")

            latest_configuration = await self._latest_configuration_epoch(session)
            if proposal.configuration_epoch != latest_configuration:
                raise GrantConflictError("reservation configuration epoch is not current")
            await self._assert_pool_launch_eligible(
                session,
                configuration_epoch=latest_configuration,
                pool_id=proposal.pool_id,
                pool_generation=proposal.pool_generation,
                now=now,
            )
            latest_allocation = await self._latest_allocation_epoch(session)
            if proposal.allocation_epoch != latest_allocation.allocation_epoch:
                raise GrantConflictError("reservation allocation epoch is not current")
            if (
                latest_allocation.writer_epoch != writer.writer_epoch
                or latest_allocation.configuration_epoch != proposal.configuration_epoch
                or latest_allocation.status != "shadow"
                or latest_allocation.executable
            ):
                raise GrantConflictError("reservation allocation fence is invalid")

            allocation = (
                await session.execute(
                    select(CapacityAllocation).where(
                        CapacityAllocation.allocation_epoch == proposal.allocation_epoch,
                        CapacityAllocation.subject_id == proposal.subject_id,
                        CapacityAllocation.pool_id == proposal.pool_id,
                    )
                )
            ).scalar_one_or_none()
            if allocation is None:
                raise GrantConflictError("reservation has no exact allocation")
            if (
                allocation.subject_incarnation != proposal.subject_incarnation
                or allocation.deployment_generation != proposal.deployment_generation
                or allocation.executable
            ):
                raise GrantConflictError("reservation allocation binding changed")

            subject = (
                await session.execute(
                    select(CapacitySubject).where(
                        CapacitySubject.configuration_epoch == proposal.configuration_epoch,
                        CapacitySubject.subject_id == proposal.subject_id,
                        CapacitySubject.subject_incarnation == proposal.subject_incarnation,
                    )
                )
            ).scalar_one_or_none()
            if subject is None or subject.lifecycle_state != "active":
                raise GrantConflictError("reservation subject is not active")
            if (
                subject.account_id != proposal.account_id
                or subject.tier_id != proposal.tier_id
                or subject.candidate_generation != proposal.candidate_generation
                or subject.deployment_generation != proposal.deployment_generation
            ):
                raise GrantConflictError("reservation subject binding changed")

            profile = (
                await session.execute(
                    select(CapacityWorkerProfile).where(
                        CapacityWorkerProfile.subject_id == proposal.subject_id,
                        CapacityWorkerProfile.subject_incarnation == proposal.subject_incarnation,
                        CapacityWorkerProfile.deployment_generation
                        == proposal.deployment_generation,
                        CapacityWorkerProfile.pool_id == proposal.pool_id,
                        CapacityWorkerProfile.pool_generation == proposal.pool_generation,
                    )
                )
            ).scalar_one_or_none()
            if profile is None:
                raise GrantConflictError("reservation worker profile is unavailable")
            self._validate_shapes(
                proposal,
                profile,
                allocation,
                latest_allocation.complete_payload,
            )
            await self._close_stale_proposals(
                session,
                proposal=proposal,
                now=now,
            )
            await self._validate_open_shape_counts(session, proposal, allocation)

            row = CapacityReservationTranche(
                id=proposal.tranche_id,
                idempotency_key=idempotency_key,
                authority_incarnation=proposal.authority_incarnation,
                writer_epoch=proposal.writer_epoch,
                configuration_epoch=proposal.configuration_epoch,
                allocation_epoch=proposal.allocation_epoch,
                executor_id=proposal.executor_id,
                executor_incarnation=proposal.executor_incarnation,
                pool_id=proposal.pool_id,
                pool_generation=proposal.pool_generation,
                subject_id=proposal.subject_id,
                subject_incarnation=proposal.subject_incarnation,
                account_id=proposal.account_id,
                tier_id=proposal.tier_id,
                candidate_generation=proposal.candidate_generation,
                deployment_generation=proposal.deployment_generation,
                proposal_digest=digest,
                state="proposed",
                executable=False,
                expires_at=now + self._proposal_ttl,
                accepted_at=None,
                closed_at=None,
                closure_reason=None,
            )
            session.add(row)
            await session.flush()
            for shape in proposal.shapes:
                session.add(
                    CapacityReservationShape(
                        tranche_id=proposal.tranche_id,
                        shape_instance_id=shape.shape_instance_id,
                        intent_id=shape.intent_id,
                        shape_id=shape.shape_id,
                        profile_id=shape.profile_id,
                        profile_generation=shape.profile_generation,
                        profile_digest=shape.profile_digest,
                        concurrency_slots=shape.concurrency_slots,
                        resource_vector=shape.resources.model_dump(mode="json", exclude_none=False),
                        node_ids=list(shape.node_ids),
                        rollout_surge_slots=shape.rollout_surge_slots,
                        old_shape_backing_id=shape.old_shape_backing_id,
                        state="proposed",
                        release_evidence_digest=None,
                        released_at=None,
                    )
                )
            await session.flush()
            session.add(
                _audit(
                    actor_kind="manager",
                    actor_id=str(writer.authority_incarnation),
                    event_kind="capacity_reservation_proposed_dry_run",
                    object_binding={
                        "tranche_id": str(proposal.tranche_id),
                        "allocation_epoch": proposal.allocation_epoch,
                    },
                    detail={
                        "proposal_digest": digest,
                        "shape_count": len(proposal.shapes),
                        "executable": False,
                    },
                )
            )
            return ProposedReservation(row.id, digest, row.expires_at, False)

    async def accept_reservation(
        self,
        session: AsyncSession,
        acceptance: DryRunReservationAcceptanceV1,
    ) -> AcceptedReservation:
        digest = canonical_grant_digest(acceptance)
        equivocated = False
        expired = False
        superseded = False
        result: AcceptedReservation | None = None
        async with _write_transaction(session):
            authority = await _lock_authority(session)
            now = await _db_now(session)
            executor = await self._current_executor(
                session,
                executor_id=acceptance.executor_id,
                executor_incarnation=acceptance.executor_incarnation,
                pool_id=None,
                now=now,
            )
            sequence_state = self._command_sequence_state(
                executor,
                sequence=acceptance.command_sequence,
                digest=digest,
            )
            if sequence_state == "equivocation":
                executor.state = "equivocal"
                session.add(
                    _audit(
                        actor_kind="executor",
                        actor_id=acceptance.executor_id,
                        event_kind="capacity_executor_equivocation",
                        object_binding={
                            "executor_incarnation": str(acceptance.executor_incarnation),
                            "command_sequence": acceptance.command_sequence,
                        },
                        detail={},
                    )
                )
                equivocated = True
            else:
                tranche = (
                    await session.execute(
                        select(CapacityReservationTranche)
                        .where(CapacityReservationTranche.id == acceptance.tranche_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if tranche is None:
                    raise GrantConflictError("reservation tranche is unknown")
                if (
                    tranche.proposal_digest != acceptance.proposal_digest
                    or tranche.executor_id != acceptance.executor_id
                    or tranche.executor_incarnation != acceptance.executor_incarnation
                    or tranche.pool_id != executor.pool_id
                ):
                    raise GrantConflictError("reservation acceptance binding changed")
                if (
                    authority.authority_incarnation != tranche.authority_incarnation
                    or authority.writer_epoch != tranche.writer_epoch
                ):
                    raise StaleWriterError("reservation authority fence changed")
                if sequence_state == "replay":
                    if tranche.state == "closed" and tranche.accepted_at is None:
                        if tranche.closure_reason == "proposal-expired":
                            expired = True
                        elif tranche.closure_reason == "proposal-superseded":
                            superseded = True
                        else:
                            raise GrantConflictError("replayed acceptance has no durable outcome")
                    elif tranche.state not in {"accepted", "closed"}:
                        raise GrantConflictError("replayed acceptance has no durable outcome")
                    else:
                        intent_ids = await self._tranche_intent_ids(session, tranche.id)
                        result = AcceptedReservation(tranche.id, intent_ids, True)
                elif tranche.state != "proposed":
                    if tranche.state == "closed" and tranche.accepted_at is None:
                        executor.command_high_water = acceptance.command_sequence
                        executor.last_command_digest = digest
                        if tranche.closure_reason == "proposal-expired":
                            expired = True
                        elif tranche.closure_reason == "proposal-superseded":
                            superseded = True
                        else:
                            raise GrantConflictError("reservation proposal is no longer open")
                    else:
                        raise GrantConflictError("reservation proposal is no longer open")
                elif tranche.expires_at <= now:
                    tranche.state = "closed"
                    tranche.closed_at = now
                    tranche.closure_reason = "proposal-expired"
                    await self._delete_unaccepted_shapes(session, tranche.id)
                    executor.command_high_water = acceptance.command_sequence
                    executor.last_command_digest = digest
                    expired = True
                else:
                    current_configuration = await self._latest_configuration_epoch(session)
                    current_allocation = await self._latest_allocation_epoch(session)
                    if (
                        tranche.configuration_epoch != current_configuration
                        or tranche.allocation_epoch != current_allocation.allocation_epoch
                    ):
                        tranche.state = "closed"
                        tranche.closed_at = now
                        tranche.closure_reason = "proposal-superseded"
                        await self._delete_unaccepted_shapes(session, tranche.id)
                        executor.command_high_water = acceptance.command_sequence
                        executor.last_command_digest = digest
                        superseded = True
                    else:
                        await self._assert_pool_launch_eligible(
                            session,
                            configuration_epoch=current_configuration,
                            pool_id=tranche.pool_id,
                            pool_generation=tranche.pool_generation,
                            now=now,
                        )
                        shapes = (
                            (
                                await session.execute(
                                    select(CapacityReservationShape)
                                    .where(CapacityReservationShape.tranche_id == tranche.id)
                                    .order_by(CapacityReservationShape.shape_instance_id)
                                    .with_for_update()
                                )
                            )
                            .scalars()
                            .all()
                        )
                        tranche.state = "accepted"
                        tranche.accepted_at = now
                        for shape in shapes:
                            shape.state = "accepted"
                            ownership = self._ownership_metadata(tranche, shape)
                            session.add(
                                CapacitySubmissionIntent(
                                    id=shape.intent_id,
                                    tranche_id=tranche.id,
                                    shape_instance_id=shape.shape_instance_id,
                                    executor_id=tranche.executor_id,
                                    executor_incarnation=tranche.executor_incarnation,
                                    ownership_metadata_sha256=(canonical_grant_digest(ownership)),
                                    state="prepared",
                                    executable=False,
                                    bootstrap_registration_epoch=None,
                                    bootstrap_evidence_sha256=None,
                                    launch_ready_at=None,
                                )
                            )
                        executor.command_high_water = acceptance.command_sequence
                        executor.last_command_digest = digest
                        await session.flush()
                        intent_ids = tuple(shape.intent_id for shape in shapes)
                        session.add(
                            _audit(
                                actor_kind="executor",
                                actor_id=acceptance.executor_id,
                                event_kind="capacity_reservation_accepted_dry_run",
                                object_binding={"tranche_id": str(tranche.id)},
                                detail={
                                    "intent_ids": [str(item) for item in intent_ids],
                                    "executable": False,
                                },
                            )
                        )
                        result = AcceptedReservation(tranche.id, intent_ids, False)
        if equivocated:
            raise ExecutorEquivocationError("executor equivocated at command high-water")
        if expired:
            raise ProposalExpiredError("reservation proposal expired before acceptance")
        if superseded:
            raise ProposalSupersededError("reservation proposal was superseded")
        assert result is not None
        return result

    async def register_bootstrap(
        self,
        session: AsyncSession,
        registration: DryRunBootstrapRegistrationV1,
    ) -> IntentReady:
        digest = canonical_grant_digest(registration)
        equivocated = False
        result: IntentReady | None = None
        async with _write_transaction(session):
            authority = await _lock_authority(session)
            now = await _db_now(session)
            executor = await self._current_executor(
                session,
                executor_id=registration.executor_id,
                executor_incarnation=registration.executor_incarnation,
                pool_id=None,
                now=now,
            )
            sequence_state = self._command_sequence_state(
                executor,
                sequence=registration.command_sequence,
                digest=digest,
            )
            if sequence_state == "equivocation":
                executor.state = "equivocal"
                equivocated = True
                self._audit_equivocation(
                    session, registration.executor_id, registration.command_sequence
                )
            else:
                intent = (
                    await session.execute(
                        select(CapacitySubmissionIntent)
                        .where(CapacitySubmissionIntent.id == registration.intent_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                tranche = (
                    await session.execute(
                        select(CapacityReservationTranche)
                        .where(CapacityReservationTranche.id == registration.tranche_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if intent is None or tranche is None or intent.tranche_id != tranche.id:
                    raise GrantConflictError("bootstrap intent binding is unknown")
                self._assert_executor_tranche(authority, executor, tranche)
                if (
                    intent.executor_id != registration.executor_id
                    or intent.executor_incarnation != registration.executor_incarnation
                ):
                    raise GrantConflictError("bootstrap executor binding changed")
                if sequence_state == "replay":
                    if (
                        intent.bootstrap_registration_epoch
                        != registration.bootstrap_registration_epoch
                        or intent.bootstrap_evidence_sha256
                        != registration.bootstrap_evidence_sha256
                    ):
                        raise GrantConflictError("bootstrap replay has no exact durable outcome")
                    result = IntentReady(
                        intent.id,
                        registration.bootstrap_registration_epoch,
                        True,
                    )
                else:
                    if intent.state != "prepared" or tranche.state != "accepted":
                        raise GrantConflictError("bootstrap intent is not prepared")
                    await self._assert_current_tranche(session, tranche)
                    intent.state = "launch-ready"
                    intent.bootstrap_registration_epoch = registration.bootstrap_registration_epoch
                    intent.bootstrap_evidence_sha256 = registration.bootstrap_evidence_sha256
                    intent.launch_ready_at = now
                    executor.command_high_water = registration.command_sequence
                    executor.last_command_digest = digest
                    session.add(
                        _audit(
                            actor_kind="executor",
                            actor_id=registration.executor_id,
                            event_kind="capacity_intent_launch_ready_dry_run",
                            object_binding={"intent_id": str(intent.id)},
                            detail={
                                "bootstrap_registration_epoch": (
                                    registration.bootstrap_registration_epoch
                                ),
                                "executable": False,
                            },
                        )
                    )
                    result = IntentReady(
                        intent.id,
                        registration.bootstrap_registration_epoch,
                        False,
                    )
        if equivocated:
            raise ExecutorEquivocationError("executor equivocated at command high-water")
        assert result is not None
        return result

    async def begin_intent_close(
        self,
        session: AsyncSession,
        close: DryRunIntentCloseV1,
    ) -> ClosingIntent:
        digest = canonical_grant_digest(close)
        equivocated = False
        result: ClosingIntent | None = None
        async with _write_transaction(session):
            authority = await _lock_authority(session)
            now = await _db_now(session)
            executor = await self._current_executor(
                session,
                executor_id=close.executor_id,
                executor_incarnation=close.executor_incarnation,
                pool_id=None,
                now=now,
            )
            sequence_state = self._command_sequence_state(
                executor,
                sequence=close.command_sequence,
                digest=digest,
            )
            if sequence_state == "equivocation":
                executor.state = "equivocal"
                equivocated = True
                self._audit_equivocation(
                    session,
                    close.executor_id,
                    close.command_sequence,
                )
            else:
                intent = (
                    await session.execute(
                        select(CapacitySubmissionIntent)
                        .where(CapacitySubmissionIntent.id == close.intent_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                tranche = (
                    await session.execute(
                        select(CapacityReservationTranche)
                        .where(CapacityReservationTranche.id == close.tranche_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if intent is None or tranche is None or intent.tranche_id != tranche.id:
                    raise GrantConflictError("closing intent binding is unknown")
                self._assert_executor_tranche(authority, executor, tranche)
                if (
                    intent.executor_id != close.executor_id
                    or intent.executor_incarnation != close.executor_incarnation
                ):
                    raise GrantConflictError("closing intent executor binding changed")
                if sequence_state == "replay":
                    if intent.state not in {"closing", "closed"}:
                        raise GrantConflictError("intent close replay has no durable outcome")
                    result = ClosingIntent(intent.id, True)
                else:
                    if intent.state not in {"prepared", "launch-ready"}:
                        raise GrantConflictError("only an unsubmitted intent may begin close")
                    shape = (
                        await session.execute(
                            select(CapacityReservationShape)
                            .where(CapacityReservationShape.intent_id == intent.id)
                            .with_for_update()
                        )
                    ).scalar_one()
                    await session.execute(
                        update(CapacityLaunchPermit)
                        .where(
                            CapacityLaunchPermit.intent_id == intent.id,
                            CapacityLaunchPermit.state == "current",
                        )
                        .values(state="revoked")
                    )
                    intent.state = "closing"
                    shape.state = "releasing"
                    executor.command_high_water = close.command_sequence
                    executor.last_command_digest = digest
                    session.add(
                        _audit(
                            actor_kind="executor",
                            actor_id=close.executor_id,
                            event_kind="capacity_intent_closing_dry_run",
                            object_binding={"intent_id": str(intent.id)},
                            detail={"executable": False},
                        )
                    )
                    result = ClosingIntent(intent.id, False)
        if equivocated:
            raise ExecutorEquivocationError("executor equivocated at command high-water")
        assert result is not None
        return result

    async def release_shapes(
        self,
        session: AsyncSession,
        release: DryRunPartialReleaseV1,
    ) -> ReleasedReservationShapes:
        digest = canonical_grant_digest(release)
        equivocated = False
        result: ReleasedReservationShapes | None = None
        async with _write_transaction(session):
            authority = await _lock_authority(session)
            now = await _db_now(session)
            executor = await self._current_executor(
                session,
                executor_id=release.executor_id,
                executor_incarnation=release.executor_incarnation,
                pool_id=None,
                now=now,
            )
            if release.command_sequence < executor.command_high_water:
                tranche = (
                    await session.execute(
                        select(CapacityReservationTranche)
                        .where(CapacityReservationTranche.id == release.tranche_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if tranche is None:
                    raise StaleCommandError("delayed release tranche is unknown")
                self._assert_executor_tranche(authority, executor, tranche)
                delayed_released_ids: list[str] = []
                for item in release.releases:
                    existing = (
                        await session.execute(
                            select(CapacityReservationReleaseEvidence).where(
                                CapacityReservationReleaseEvidence.shape_instance_id
                                == item.shape_instance_id
                            )
                        )
                    ).scalar_one_or_none()
                    item_digest = self._release_evidence_digest(release, item)
                    if existing is None or not self._release_evidence_matches(
                        existing,
                        release,
                        item,
                        item_digest,
                    ):
                        raise StaleCommandError("delayed release has no exact durable evidence")
                    delayed_released_ids.append(item.shape_instance_id)
                return ReleasedReservationShapes(
                    tranche.id,
                    tuple(sorted(delayed_released_ids)),
                    True,
                )
            sequence_state = self._command_sequence_state(
                executor,
                sequence=release.command_sequence,
                digest=digest,
            )
            if sequence_state == "equivocation":
                executor.state = "equivocal"
                equivocated = True
                self._audit_equivocation(
                    session,
                    release.executor_id,
                    release.command_sequence,
                )
            else:
                tranche = (
                    await session.execute(
                        select(CapacityReservationTranche)
                        .where(CapacityReservationTranche.id == release.tranche_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if tranche is None:
                    raise GrantConflictError("released reservation tranche is unknown")
                if tranche.state != "accepted" and not (
                    sequence_state == "replay"
                    and tranche.state == "closed"
                    and tranche.closure_reason == "fully-released"
                ):
                    raise GrantConflictError("released reservation tranche is not accepted")
                self._assert_executor_tranche(authority, executor, tranche)
                released_ids: list[str] = []
                for item in release.releases:
                    shape = (
                        await session.execute(
                            select(CapacityReservationShape)
                            .where(
                                CapacityReservationShape.tranche_id == tranche.id,
                                CapacityReservationShape.shape_instance_id
                                == item.shape_instance_id,
                                CapacityReservationShape.intent_id == item.intent_id,
                            )
                            .with_for_update()
                        )
                    ).scalar_one_or_none()
                    intent = (
                        await session.execute(
                            select(CapacitySubmissionIntent)
                            .where(
                                CapacitySubmissionIntent.id == item.intent_id,
                                CapacitySubmissionIntent.tranche_id == tranche.id,
                            )
                            .with_for_update()
                        )
                    ).scalar_one_or_none()
                    if shape is None or intent is None:
                        raise GrantConflictError("release shape binding is unknown")
                    item_digest = self._release_evidence_digest(release, item)
                    existing = (
                        await session.execute(
                            select(CapacityReservationReleaseEvidence).where(
                                CapacityReservationReleaseEvidence.shape_instance_id
                                == item.shape_instance_id
                            )
                        )
                    ).scalar_one_or_none()
                    if existing is not None:
                        if not self._release_evidence_matches(
                            existing,
                            release,
                            item,
                            item_digest,
                        ):
                            raise GrantConflictError("terminal release evidence conflicts")
                        released_ids.append(item.shape_instance_id)
                        continue
                    observation = (
                        await session.execute(
                            select(CapacityExecutorObservation).where(
                                CapacityExecutorObservation.executor_incarnation
                                == release.executor_incarnation,
                                CapacityExecutorObservation.inventory_sequence
                                == item.inventory_sequence,
                                CapacityExecutorObservation.validity == "valid",
                            )
                        )
                    ).scalar_one_or_none()
                    if observation is None:
                        raise GrantConflictError(
                            "release requires an exact complete executor inventory"
                        )
                    if item.protected_registration_epoch <= (
                        intent.bootstrap_registration_epoch or 0
                    ):
                        raise GrantConflictError(
                            "release protection epoch did not advance past bootstrap"
                        )
                    protected_ack = (
                        await session.execute(
                            select(CapacityProtectedReleaseAcknowledgement).where(
                                CapacityProtectedReleaseAcknowledgement.shape_instance_id
                                == item.shape_instance_id
                            )
                        )
                    ).scalar_one_or_none()
                    if (
                        protected_ack is None
                        or protected_ack.tranche_id != tranche.id
                        or protected_ack.intent_id != item.intent_id
                        or protected_ack.bootstrap_registration_epoch
                        != (intent.bootstrap_registration_epoch or 0)
                        or protected_ack.protected_registration_epoch
                        != item.protected_registration_epoch
                        or protected_ack.bootstrap_revoked is not item.bootstrap_revoked
                        or protected_ack.protected_release_sha256 != item.protected_release_sha256
                    ):
                        raise GrantConflictError(
                            "release lacks exact protected-agent acknowledgement"
                        )
                    records = observation.payload.get("records")
                    classifications = observation.classification_payload
                    if not isinstance(records, list):
                        raise GrantConflictError("release inventory payload is invalid")
                    if item.terminal_kind == "unused":
                        if (
                            item.terminal_identity != item.shape_instance_id
                            or item.terminal_evidence_sha256 != observation.inventory_digest
                        ):
                            raise GrantConflictError(
                                "unused release is not bound to its complete inventory"
                            )
                        for record in records:
                            if not isinstance(record, dict):
                                raise GrantConflictError("release inventory record is invalid")
                            proof = record.get("ownership_proof")
                            metadata = proof.get("metadata") if isinstance(proof, dict) else None
                            if isinstance(metadata, dict) and (
                                metadata.get("intent_id") == str(item.intent_id)
                                or metadata.get("shape_instance_id") == item.shape_instance_id
                            ):
                                raise GrantConflictError(
                                    "unused release inventory still contains its shape"
                                )
                    else:
                        record = next(
                            (
                                candidate
                                for candidate in records
                                if isinstance(candidate, dict)
                                and candidate.get("physical_identity") == item.terminal_identity
                            ),
                            None,
                        )
                        classification = next(
                            (
                                candidate.get("classification")
                                for candidate in classifications
                                if candidate.get("physical_identity") == item.terminal_identity
                            ),
                            None,
                        )
                        if (
                            record is None
                            or classification != "authenticated"
                            or record.get("physical_kind") != item.terminal_kind
                            or record.get("state") != "terminal"
                            or record.get("terminal_evidence_sha256")
                            != item.terminal_evidence_sha256
                        ):
                            raise GrantConflictError(
                                "physical release lacks authenticated terminal inventory"
                            )
                    expected_intent_state = (
                        "closing" if item.terminal_kind == "unused" else "terminal"
                    )
                    if intent.state != expected_intent_state or shape.state != "releasing":
                        raise GrantConflictError("released intent was not fenced for close")
                    session.add(
                        CapacityReservationReleaseEvidence(
                            tranche_id=tranche.id,
                            shape_instance_id=item.shape_instance_id,
                            intent_id=item.intent_id,
                            executor_id=release.executor_id,
                            executor_incarnation=release.executor_incarnation,
                            command_sequence=release.command_sequence,
                            inventory_sequence=item.inventory_sequence,
                            terminal_kind=item.terminal_kind,
                            terminal_identity=item.terminal_identity,
                            terminal_evidence_sha256=item.terminal_evidence_sha256,
                            protected_registration_epoch=(item.protected_registration_epoch),
                            bootstrap_revoked=item.bootstrap_revoked,
                            protected_release_sha256=item.protected_release_sha256,
                            evidence_digest=item_digest,
                            received_at=now,
                        )
                    )
                    if item.terminal_kind != "unused":
                        await session.execute(
                            delete(CapacityObservedCommitment).where(
                                CapacityObservedCommitment.kind == "physical",
                                CapacityObservedCommitment.commitment_identity
                                == item.terminal_identity,
                                CapacityObservedCommitment.source_incarnation
                                == release.executor_incarnation,
                            )
                        )
                    shape.state = "released"
                    shape.release_evidence_digest = item_digest
                    shape.released_at = now
                    intent.state = "closed"
                    released_ids.append(item.shape_instance_id)
                await session.flush()
                remaining = (
                    await session.execute(
                        select(func.count())
                        .select_from(CapacityReservationShape)
                        .where(
                            CapacityReservationShape.tranche_id == tranche.id,
                            CapacityReservationShape.state != "released",
                        )
                    )
                ).scalar_one()
                if remaining == 0:
                    tranche.state = "closed"
                    tranche.closed_at = now
                    tranche.closure_reason = "fully-released"
                if sequence_state == "new":
                    executor.command_high_water = release.command_sequence
                    executor.last_command_digest = digest
                    session.add(
                        _audit(
                            actor_kind="executor",
                            actor_id=release.executor_id,
                            event_kind="capacity_reservation_shapes_released_dry_run",
                            object_binding={"tranche_id": str(tranche.id)},
                            detail={
                                "released_shape_ids": sorted(released_ids),
                                "executable": False,
                            },
                        )
                    )
                result = ReleasedReservationShapes(
                    tranche.id,
                    tuple(sorted(released_ids)),
                    sequence_state == "replay",
                )
        if equivocated:
            raise ExecutorEquivocationError("executor equivocated at command high-water")
        assert result is not None
        return result

    async def acknowledge_protected_release(
        self,
        session: AsyncSession,
        acknowledgement: DryRunProtectedReleaseAcknowledgementV1,
        *,
        actor: str,
        idempotency_key: UUID,
    ) -> AcknowledgedProtectedRelease:
        if not actor or len(actor.encode("utf-8")) > 256:
            raise ValueError("protected release actor is invalid")
        digest = canonical_grant_digest(acknowledgement)
        async with _write_transaction(session):
            authority = await _lock_authority(session)
            if (
                authority.authority_incarnation != acknowledgement.authority_incarnation
                or authority.writer_epoch != acknowledgement.writer_epoch
            ):
                raise StaleWriterError("protected release authority fence changed")
            replay = (
                await session.execute(
                    select(CapacityProtectedReleaseAcknowledgement).where(
                        CapacityProtectedReleaseAcknowledgement.idempotency_key == idempotency_key
                    )
                )
            ).scalar_one_or_none()
            if replay is not None:
                if replay.acknowledgement_digest != digest or replay.actor_id != actor:
                    raise IdempotencyConflictError("protected release idempotency key was reused")
                return AcknowledgedProtectedRelease(
                    replay.id,
                    replay.shape_instance_id,
                    replay.acknowledgement_digest,
                    True,
                )

            tranche = (
                await session.execute(
                    select(CapacityReservationTranche)
                    .where(CapacityReservationTranche.id == acknowledgement.tranche_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            shape = (
                await session.execute(
                    select(CapacityReservationShape)
                    .where(
                        CapacityReservationShape.tranche_id == acknowledgement.tranche_id,
                        CapacityReservationShape.shape_instance_id
                        == acknowledgement.shape_instance_id,
                        CapacityReservationShape.intent_id == acknowledgement.intent_id,
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            intent = (
                await session.execute(
                    select(CapacitySubmissionIntent)
                    .where(
                        CapacitySubmissionIntent.id == acknowledgement.intent_id,
                        CapacitySubmissionIntent.tranche_id == acknowledgement.tranche_id,
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            reporter = (
                await session.execute(
                    select(CapacityDemandReporter).where(
                        CapacityDemandReporter.subject_id == acknowledgement.subject_id,
                        CapacityDemandReporter.subject_incarnation
                        == acknowledgement.subject_incarnation,
                        CapacityDemandReporter.reporter_incarnation
                        == acknowledgement.reporter_incarnation,
                    )
                )
            ).scalar_one_or_none()
            if tranche is None or shape is None or intent is None:
                raise GrantConflictError("protected release shape binding is unknown")
            shape_replay = (
                await session.execute(
                    select(CapacityProtectedReleaseAcknowledgement).where(
                        CapacityProtectedReleaseAcknowledgement.shape_instance_id
                        == acknowledgement.shape_instance_id
                    )
                )
            ).scalar_one_or_none()
            if shape_replay is not None:
                if shape_replay.acknowledgement_digest != digest or shape_replay.actor_id != actor:
                    raise GrantConflictError("protected release acknowledgement conflicts")
                return AcknowledgedProtectedRelease(
                    shape_replay.id,
                    shape_replay.shape_instance_id,
                    shape_replay.acknowledgement_digest,
                    True,
                )
            if (
                tranche.authority_incarnation != acknowledgement.authority_incarnation
                or tranche.writer_epoch != acknowledgement.writer_epoch
                or tranche.configuration_epoch != acknowledgement.configuration_epoch
                or tranche.allocation_epoch != acknowledgement.allocation_epoch
                or tranche.subject_id != acknowledgement.subject_id
                or tranche.subject_incarnation != acknowledgement.subject_incarnation
                or tranche.deployment_generation != acknowledgement.deployment_generation
                or tranche.pool_id != acknowledgement.pool_id
                or tranche.pool_generation != acknowledgement.pool_generation
                or reporter is None
                or reporter.state != "current"
                or reporter.deployment_generation != acknowledgement.deployment_generation
            ):
                raise GrantConflictError("protected release authority or subject binding changed")
            if (
                shape.state != "releasing"
                or intent.state not in {"closing", "terminal"}
                or (intent.bootstrap_registration_epoch or 0)
                != acknowledgement.bootstrap_registration_epoch
            ):
                raise GrantConflictError("protected release intent has not been fenced exactly")
            row = CapacityProtectedReleaseAcknowledgement(
                idempotency_key=idempotency_key,
                authority_incarnation=acknowledgement.authority_incarnation,
                writer_epoch=acknowledgement.writer_epoch,
                configuration_epoch=acknowledgement.configuration_epoch,
                allocation_epoch=acknowledgement.allocation_epoch,
                tranche_id=acknowledgement.tranche_id,
                shape_instance_id=acknowledgement.shape_instance_id,
                intent_id=acknowledgement.intent_id,
                subject_id=acknowledgement.subject_id,
                subject_incarnation=acknowledgement.subject_incarnation,
                reporter_incarnation=acknowledgement.reporter_incarnation,
                deployment_generation=acknowledgement.deployment_generation,
                pool_id=acknowledgement.pool_id,
                pool_generation=acknowledgement.pool_generation,
                bootstrap_registration_epoch=(acknowledgement.bootstrap_registration_epoch),
                protected_registration_epoch=(acknowledgement.protected_registration_epoch),
                bootstrap_revoked=acknowledgement.bootstrap_revoked,
                protected_release_sha256=(acknowledgement.protected_release_sha256),
                acknowledgement_digest=digest,
                actor_id=actor,
                executable=False,
            )
            session.add(row)
            await session.flush()
            session.add(
                _audit(
                    actor_kind="subject-agent",
                    actor_id=actor,
                    event_kind="capacity_protected_release_acknowledged_dry_run",
                    object_binding={
                        "tranche_id": str(acknowledgement.tranche_id),
                        "shape_instance_id": acknowledgement.shape_instance_id,
                        "intent_id": str(acknowledgement.intent_id),
                    },
                    detail={
                        "acknowledgement_digest": digest,
                        "executable": False,
                    },
                )
            )
            return AcknowledgedProtectedRelease(
                row.id,
                row.shape_instance_id,
                row.acknowledgement_digest,
                False,
            )

    async def issue_launch_permit(
        self,
        session: AsyncSession,
        writer: WriterFence,
        permit: DryRunLaunchPermitV1,
        *,
        idempotency_key: UUID,
    ) -> IssuedLaunchPermit:
        digest = canonical_grant_digest(permit)
        async with _write_transaction(session):
            authority = await _lock_authority(session)
            _assert_writer(authority, writer)
            replay = (
                await session.execute(
                    select(CapacityLaunchPermit).where(
                        CapacityLaunchPermit.idempotency_key == idempotency_key
                    )
                )
            ).scalar_one_or_none()
            if replay is not None:
                if replay.permit_digest != digest:
                    raise IdempotencyConflictError(
                        "launch permit idempotency key was reused with different input"
                    )
                return IssuedLaunchPermit(
                    replay.id,
                    replay.permit_digest,
                    replay.expires_at,
                    True,
                )

            current_configuration = await self._latest_configuration_epoch(session)
            current_allocation = await self._latest_allocation_epoch(session)
            if (
                permit.configuration_epoch != current_configuration
                or permit.allocation_epoch != current_allocation.allocation_epoch
                or current_allocation.writer_epoch != writer.writer_epoch
            ):
                raise GrantConflictError("launch permit allocation binding is stale")
            intent = (
                await session.execute(
                    select(CapacitySubmissionIntent)
                    .where(CapacitySubmissionIntent.id == permit.intent_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if intent is None or intent.state != "launch-ready":
                raise GrantConflictError("launch permit intent is not ready")
            tranche = (
                await session.execute(
                    select(CapacityReservationTranche)
                    .where(CapacityReservationTranche.id == intent.tranche_id)
                    .with_for_update()
                )
            ).scalar_one()
            self._assert_executor_tranche_from_permit(authority, permit, intent, tranche)
            if tranche.state != "accepted":
                raise GrantConflictError("launch permit tranche is not accepted")
            shape = (
                await session.execute(
                    select(CapacityReservationShape).where(
                        CapacityReservationShape.intent_id == intent.id
                    )
                )
            ).scalar_one()
            expected_rank = next(
                (
                    item["rank"]
                    for item in current_allocation.complete_payload["hypothetical_launch_rank"]
                    if item["subject_id"] == str(tranche.subject_id)
                    and item["pool_id"] == tranche.pool_id
                    and item["shape_instance_id"] == shape.shape_instance_id
                ),
                None,
            )
            if permit.launch_rank != expected_rank:
                raise GrantConflictError("launch permit rank changed")
            now = await _db_now(session)
            await self._current_executor(
                session,
                executor_id=permit.executor_id,
                executor_incarnation=permit.executor_incarnation,
                pool_id=tranche.pool_id,
                now=now,
            )
            await self._assert_pool_launch_eligible(
                session,
                configuration_epoch=current_configuration,
                pool_id=tranche.pool_id,
                pool_generation=tranche.pool_generation,
                now=now,
            )
            current = (
                await session.execute(
                    select(CapacityLaunchPermit)
                    .where(
                        CapacityLaunchPermit.intent_id == intent.id,
                        CapacityLaunchPermit.state == "current",
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if current is not None:
                if permit.permit_epoch <= current.permit_epoch:
                    raise GrantConflictError("launch permit epoch did not advance")
                current.state = "superseded"
                await session.flush()

            await self._ensure_rate_buckets(
                session,
                configuration_epoch=current_configuration,
                authority=authority,
                tranche=tranche,
                now=now,
            )
            row = CapacityLaunchPermit(
                id=permit.permit_id,
                intent_id=permit.intent_id,
                permit_epoch=permit.permit_epoch,
                allocation_epoch=permit.allocation_epoch,
                configuration_epoch=permit.configuration_epoch,
                executor_id=permit.executor_id,
                executor_incarnation=permit.executor_incarnation,
                launch_rank=permit.launch_rank,
                permit_digest=digest,
                idempotency_key=idempotency_key,
                state="current",
                executable=False,
                expires_at=now + self._permit_ttl,
                dry_run_consumed_at=None,
            )
            session.add(row)
            await session.flush()
            session.add(
                _audit(
                    actor_kind="manager",
                    actor_id=str(writer.authority_incarnation),
                    event_kind="capacity_launch_permit_issued_dry_run",
                    object_binding={
                        "permit_id": str(row.id),
                        "intent_id": str(intent.id),
                    },
                    detail={
                        "launch_rank": row.launch_rank,
                        "permit_digest": digest,
                        "executable": False,
                    },
                )
            )
            return IssuedLaunchPermit(row.id, digest, row.expires_at, False)

    async def consume_launch_permit(
        self,
        session: AsyncSession,
        consumption: DryRunPermitConsumptionV1,
    ) -> ConsumedLaunchPermit:
        digest = canonical_grant_digest(consumption)
        equivocated = False
        expired = False
        result: ConsumedLaunchPermit | None = None
        async with _write_transaction(session):
            authority = await _lock_authority(session)
            now = await _db_now(session)
            executor = await self._current_executor(
                session,
                executor_id=consumption.executor_id,
                executor_incarnation=consumption.executor_incarnation,
                pool_id=None,
                now=now,
            )
            sequence_state = self._command_sequence_state(
                executor,
                sequence=consumption.command_sequence,
                digest=digest,
            )
            if sequence_state == "equivocation":
                executor.state = "equivocal"
                equivocated = True
                self._audit_equivocation(
                    session, consumption.executor_id, consumption.command_sequence
                )
            else:
                permit = (
                    await session.execute(
                        select(CapacityLaunchPermit)
                        .where(CapacityLaunchPermit.id == consumption.permit_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if permit is None or (
                    permit.permit_digest != consumption.permit_digest
                    or permit.intent_id != consumption.intent_id
                    or permit.executor_id != consumption.executor_id
                    or permit.executor_incarnation != consumption.executor_incarnation
                ):
                    raise GrantConflictError("launch permit consumption binding changed")
                if sequence_state == "replay":
                    if permit.state == "consumed":
                        result = ConsumedLaunchPermit(
                            permit.id,
                            permit.intent_id,
                            True,
                        )
                    elif permit.state == "revoked" and permit.expires_at <= now:
                        expired = True
                    else:
                        raise GrantConflictError("permit replay has no durable outcome")
                elif permit.state != "current":
                    raise GrantConflictError("launch permit is not current")
                elif permit.expires_at <= now:
                    permit.state = "revoked"
                    executor.command_high_water = consumption.command_sequence
                    executor.last_command_digest = digest
                    expired = True
                else:
                    current_configuration = await self._latest_configuration_epoch(session)
                    current_allocation = await self._latest_allocation_epoch(session)
                    if (
                        permit.configuration_epoch != current_configuration
                        or permit.allocation_epoch != current_allocation.allocation_epoch
                    ):
                        raise GrantConflictError("launch permit was superseded")
                    intent = (
                        await session.execute(
                            select(CapacitySubmissionIntent)
                            .where(CapacitySubmissionIntent.id == permit.intent_id)
                            .with_for_update()
                        )
                    ).scalar_one()
                    tranche = (
                        await session.execute(
                            select(CapacityReservationTranche)
                            .where(CapacityReservationTranche.id == intent.tranche_id)
                            .with_for_update()
                        )
                    ).scalar_one()
                    self._assert_executor_tranche(authority, executor, tranche)
                    if intent.state != "launch-ready":
                        raise GrantConflictError("launch permit intent is not ready")
                    await self._assert_pool_launch_eligible(
                        session,
                        configuration_epoch=current_configuration,
                        pool_id=tranche.pool_id,
                        pool_generation=tranche.pool_generation,
                        now=now,
                    )
                    await self._assert_surge_consumption_fenced(
                        session,
                        intent_id=intent.id,
                    )
                    await session.execute(
                        update(CapacityLaunchPermit)
                        .where(
                            CapacityLaunchPermit.state == "current",
                            CapacityLaunchPermit.expires_at <= now,
                        )
                        .values(state="revoked")
                    )
                    earliest_intent_id = await self._earliest_ready_intent_id(
                        session,
                        allocation=current_allocation,
                        now=now,
                    )
                    if earliest_intent_id != permit.intent_id:
                        raise LaunchOrderError("an earlier global launch is still eligible")
                    await self._assert_pending_limits(
                        session,
                        authority=authority,
                        configuration_epoch=current_configuration,
                        tranche=tranche,
                        intent=intent,
                    )
                    buckets = await self._locked_rate_buckets(
                        session,
                        configuration_epoch=current_configuration,
                        tranche=tranche,
                    )
                    for bucket in buckets:
                        self._refill_bucket(bucket, now)
                    blocked = next(
                        (
                            bucket
                            for bucket in buckets
                            if bucket.available_microtokens < MICROTOKENS_PER_LAUNCH
                        ),
                        None,
                    )
                    if blocked is not None:
                        raise RateLimitError(f"{blocked.scope} launch submission rate is exhausted")
                    for bucket in buckets:
                        bucket.available_microtokens -= MICROTOKENS_PER_LAUNCH
                    permit.state = "consumed"
                    permit.dry_run_consumed_at = now
                    intent.state = "submitting-unknown"
                    executor.command_high_water = consumption.command_sequence
                    executor.last_command_digest = digest
                    session.add(
                        _audit(
                            actor_kind="executor",
                            actor_id=consumption.executor_id,
                            event_kind="capacity_launch_permit_consumed_dry_run",
                            object_binding={
                                "permit_id": str(permit.id),
                                "intent_id": str(intent.id),
                            },
                            detail={"executable": False},
                        )
                    )
                    result = ConsumedLaunchPermit(permit.id, intent.id, False)
        if equivocated:
            raise ExecutorEquivocationError("executor equivocated at command high-water")
        if expired:
            raise PermitExpiredError("launch permit expired before consumption")
        assert result is not None
        return result

    @staticmethod
    async def _latest_configuration_epoch(session: AsyncSession) -> int:
        value = (
            await session.execute(select(func.max(CapacityConfigurationEpoch.configuration_epoch)))
        ).scalar_one()
        if value is None:
            raise GrantConflictError("no active capacity configuration")
        return int(value)

    async def _assert_pool_launch_eligible(
        self,
        session: AsyncSession,
        *,
        configuration_epoch: int,
        pool_id: str,
        pool_generation: int,
        now: datetime,
    ) -> None:
        pool = (
            await session.execute(
                select(CapacityPool).where(
                    CapacityPool.configuration_epoch == configuration_epoch,
                    CapacityPool.pool_id == pool_id,
                    CapacityPool.pool_generation == pool_generation,
                )
            )
        ).scalar_one_or_none()
        reporter = (
            await session.execute(
                select(CapacityPoolReporter).where(
                    CapacityPoolReporter.pool_id == pool_id,
                    CapacityPoolReporter.pool_generation == pool_generation,
                )
            )
        ).scalar_one_or_none()
        if (
            pool is None
            or pool.health != "eligible"
            or reporter is None
            or reporter.state != "current"
            or reporter.high_water <= 0
            or reporter.last_receipt_time is None
            or reporter.last_digest is None
            or now < reporter.last_receipt_time
            or now - reporter.last_receipt_time > self._pool_observation_freshness
        ):
            raise GrantConflictError("pool observation is not launch-eligible")
        observation = (
            await session.execute(
                select(CapacityPoolObservation).where(
                    CapacityPoolObservation.pool_id == pool_id,
                    CapacityPoolObservation.reporter_incarnation == reporter.reporter_incarnation,
                    CapacityPoolObservation.sequence == reporter.high_water,
                )
            )
        ).scalar_one_or_none()
        if observation is None or observation.validity != "valid":
            raise GrantConflictError("pool observation is not launch-eligible")
        try:
            contract = PoolObservationV1.model_validate_json(
                json.dumps(observation.payload, sort_keys=True, separators=(",", ":"))
            )
        except ValueError as exc:
            raise GrantConflictError("pool observation is not launch-eligible") from exc
        if (
            contract.pool_id != pool_id
            or contract.pool_generation != pool_generation
            or contract.reporter_incarnation != reporter.reporter_incarnation
            or contract.sequence != reporter.high_water
            or contract.health != "eligible"
            or observation.database_received_at != reporter.last_receipt_time
            or observation.digest != reporter.last_digest
            or canonical_digest(contract) != observation.digest
        ):
            raise GrantConflictError("pool observation is not launch-eligible")

    @staticmethod
    async def _assert_surge_consumption_fenced(
        session: AsyncSession,
        *,
        intent_id: UUID,
    ) -> None:
        """Keep surge inert until protected lifecycle supplies its exact drain proof."""

        surge_slots = (
            await session.execute(
                select(CapacityReservationShape.rollout_surge_slots).where(
                    CapacityReservationShape.intent_id == intent_id
                )
            )
        ).scalar_one()
        if surge_slots > 0:
            raise GrantConflictError("protected old-worker drain acknowledgement is unavailable")

    @staticmethod
    def _audit_equivocation(
        session: AsyncSession,
        executor_id: str,
        command_sequence: int,
    ) -> None:
        session.add(
            _audit(
                actor_kind="executor",
                actor_id=executor_id,
                event_kind="capacity_executor_equivocation",
                object_binding={"command_sequence": command_sequence},
                detail={},
            )
        )

    @staticmethod
    def _assert_executor_tranche(
        authority: CapacityAuthorityState,
        executor: CapacityExecutor,
        tranche: CapacityReservationTranche,
    ) -> None:
        if (
            authority.authority_incarnation != tranche.authority_incarnation
            or authority.writer_epoch != tranche.writer_epoch
            or executor.executor_id != tranche.executor_id
            or executor.executor_incarnation != tranche.executor_incarnation
            or executor.pool_id != tranche.pool_id
            or executor.pool_generation != tranche.pool_generation
        ):
            raise GrantConflictError("executor reservation fence changed")

    @staticmethod
    def _assert_executor_tranche_from_permit(
        authority: CapacityAuthorityState,
        permit: DryRunLaunchPermitV1,
        intent: CapacitySubmissionIntent,
        tranche: CapacityReservationTranche,
    ) -> None:
        if (
            authority.authority_incarnation != tranche.authority_incarnation
            or authority.writer_epoch != tranche.writer_epoch
            or permit.executor_id != intent.executor_id
            or permit.executor_incarnation != intent.executor_incarnation
            or permit.executor_id != tranche.executor_id
            or permit.executor_incarnation != tranche.executor_incarnation
            or permit.allocation_epoch != tranche.allocation_epoch
            or permit.configuration_epoch != tranche.configuration_epoch
        ):
            raise GrantConflictError("launch permit executor fence changed")

    async def _classify_inventory_record(
        self,
        session: AsyncSession,
        *,
        executor: CapacityExecutor,
        record: ExecutorInventoryRecordV1,
    ) -> Literal["authenticated", "quarantined", "foreign"]:
        if record.authority_scope == "foreign":
            return "foreign"
        proof = record.ownership_proof
        if (
            proof is None
            or proof.signing_key_id != executor.signing_key_id
            or not self._ownership_keyring.verify(
                proof,
                expected_public_key_sha256=executor.signing_key_sha256,
            )
        ):
            return "quarantined"
        intent = (
            await session.execute(
                select(CapacitySubmissionIntent).where(
                    CapacitySubmissionIntent.id == proof.metadata.intent_id
                )
            )
        ).scalar_one_or_none()
        tranche = (
            await session.execute(
                select(CapacityReservationTranche).where(
                    CapacityReservationTranche.id == proof.metadata.tranche_id
                )
            )
        ).scalar_one_or_none()
        shape = (
            await session.execute(
                select(CapacityReservationShape).where(
                    CapacityReservationShape.tranche_id == proof.metadata.tranche_id,
                    CapacityReservationShape.intent_id == proof.metadata.intent_id,
                    CapacityReservationShape.shape_instance_id == proof.metadata.shape_instance_id,
                )
            )
        ).scalar_one_or_none()
        if (
            intent is None
            or tranche is None
            or shape is None
            or intent.tranche_id != tranche.id
            or intent.executor_id != executor.executor_id
            or intent.executor_incarnation != executor.executor_incarnation
            or intent.ownership_metadata_sha256 != canonical_grant_digest(proof.metadata)
            or self._ownership_metadata(tranche, shape) != proof.metadata
        ):
            return "quarantined"
        if (
            record.resources != proof.metadata.resources
            or record.node_ids != proof.metadata.node_ids
        ):
            if intent.state not in {"terminal", "closed"}:
                intent.state = "quarantined"
            return "quarantined"
        permitted_states = {
            "pending": {"submitting-unknown", "bound"},
            "active": {"submitting-unknown", "bound", "observed"},
            "draining": {"bound", "observed"},
            "terminal": {
                "submitting-unknown",
                "bound",
                "observed",
                "quarantined",
                "terminal",
            },
        }
        if record.state == "unknown" or intent.state not in permitted_states[record.state]:
            if intent.state not in {"terminal", "closed"}:
                intent.state = "quarantined"
            return "quarantined"
        return "authenticated"

    @staticmethod
    async def _quarantine_missing_inventory(
        session: AsyncSession,
        *,
        executor: CapacityExecutor,
        inventory_sequence: int,
        reported_identities: set[str],
        now: datetime,
    ) -> None:
        existing_rows = (
            (
                await session.execute(
                    select(CapacityObservedCommitment)
                    .where(
                        CapacityObservedCommitment.kind == "physical",
                        CapacityObservedCommitment.source_incarnation
                        == executor.executor_incarnation,
                    )
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        for row in existing_rows:
            if row.commitment_identity in reported_identities:
                continue
            observed = row.binding_payload.get("observed_contract")
            if not isinstance(observed, dict):
                row.state = "quarantined"
                continue
            row.state = "quarantined"
            row.binding_payload = {
                **row.binding_payload,
                "observed_contract": {**observed, "state": "quarantined"},
                "missing_inventory_sequence": inventory_sequence,
            }
            row.last_reporter_high_water = inventory_sequence
            row.last_receipt_time = now
            reservation_identity = observed.get("reservation_identity")
            if observed.get("ownership_state") != "authenticated" or not isinstance(
                reservation_identity, str
            ):
                continue
            try:
                intent_id = UUID(reservation_identity)
            except ValueError:
                continue
            intent = (
                await session.execute(
                    select(CapacitySubmissionIntent)
                    .where(CapacitySubmissionIntent.id == intent_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if intent is not None and intent.state in {
                "submitting-unknown",
                "bound",
                "observed",
            }:
                intent.state = "quarantined"

    @staticmethod
    async def _upsert_inventory_commitment(
        session: AsyncSession,
        *,
        executor: CapacityExecutor,
        inventory_sequence: int,
        record: ExecutorInventoryRecordV1,
        classification: Literal["authenticated", "quarantined"],
        now: datetime,
    ) -> Literal["authenticated", "quarantined"]:
        proof = record.ownership_proof
        authenticated_intent: CapacitySubmissionIntent | None = None
        authenticated_shape: CapacityReservationShape | None = None
        if classification == "authenticated":
            assert proof is not None
            metadata = proof.metadata
            authenticated_intent = (
                await session.execute(
                    select(CapacitySubmissionIntent)
                    .where(CapacitySubmissionIntent.id == metadata.intent_id)
                    .with_for_update()
                )
            ).scalar_one()
            authenticated_shape = (
                await session.execute(
                    select(CapacityReservationShape)
                    .where(CapacityReservationShape.intent_id == metadata.intent_id)
                    .with_for_update()
                )
            ).scalar_one()
            state_by_inventory = {
                "pending": "pending",
                "active": "live",
                "draining": "draining",
                "terminal": "unknown",
                "unknown": "unknown",
            }
            observed = ObservedCommitmentV1(
                kind="physical",
                commitment_id=record.physical_identity,
                physical_identity=record.physical_identity,
                reservation_identity=str(metadata.intent_id),
                ownership_state="authenticated",
                subject_id=metadata.subject_id,
                subject_incarnation=metadata.subject_incarnation,
                deployment_generation=metadata.deployment_generation,
                pool_id=metadata.pool_id,
                pool_generation=metadata.pool_generation,
                profile_id=metadata.profile_id,
                profile_generation=metadata.profile_generation,
                profile_digest=metadata.profile_digest,
                shape_id=metadata.shape_id,
                resources=record.resources,
                state=cast(Any, state_by_inventory[record.state]),
                node_ids=record.node_ids,
            )
        else:
            observed = ObservedCommitmentV1(
                kind="physical",
                commitment_id=record.physical_identity,
                physical_identity=record.physical_identity,
                reservation_identity=None,
                ownership_state="unverified",
                subject_id=None,
                subject_incarnation=None,
                deployment_generation=None,
                pool_id=executor.pool_id,
                pool_generation=executor.pool_generation,
                profile_id=None,
                profile_generation=None,
                profile_digest=None,
                shape_id=None,
                resources=record.resources,
                state="quarantined",
                node_ids=record.node_ids,
            )
        payload = observed.model_dump(mode="json", exclude_none=False)
        existing = (
            await session.execute(
                select(CapacityObservedCommitment)
                .where(
                    CapacityObservedCommitment.kind == "physical",
                    CapacityObservedCommitment.commitment_identity == record.physical_identity,
                    CapacityObservedCommitment.source_incarnation == executor.executor_incarnation,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        binding_payload = {
            "observed_contract": payload,
            "inventory_record": record.model_dump(mode="json", exclude_none=False),
            "classification": classification,
        }
        if existing is None:
            CapacityGrantStore._advance_authenticated_inventory_state(
                intent=authenticated_intent,
                shape=authenticated_shape,
                record=record,
            )
            session.add(
                CapacityObservedCommitment(
                    kind="physical",
                    commitment_identity=observed.commitment_id,
                    source_incarnation=executor.executor_incarnation,
                    subject_id=observed.subject_id,
                    subject_incarnation=observed.subject_incarnation,
                    pool_id=observed.pool_id,
                    pool_generation=observed.pool_generation,
                    deployment_generation=observed.deployment_generation,
                    profile_id=observed.profile_id,
                    profile_generation=observed.profile_generation,
                    profile_digest=observed.profile_digest,
                    shape_id=observed.shape_id,
                    attempt_id=None,
                    concurrency_slots=None,
                    binding_payload=binding_payload,
                    resource_vector=observed.resources.model_dump(mode="json", exclude_none=False),
                    state=observed.state,
                    first_reporter_high_water=inventory_sequence,
                    last_reporter_high_water=inventory_sequence,
                    first_receipt_time=now,
                    last_receipt_time=now,
                )
            )
            return classification
        prior = existing.binding_payload.get("observed_contract")
        prior_inventory = existing.binding_payload.get("inventory_record")
        prior_binding = dict(prior) if isinstance(prior, dict) else None
        current_binding = dict(payload)
        if prior_binding is not None:
            prior_binding.pop("state", None)
        current_binding.pop("state", None)
        prior_reservation_identity = (
            prior.get("reservation_identity") if isinstance(prior, dict) else None
        )
        authenticated_reservation_conflict = (
            isinstance(prior, dict)
            and prior.get("ownership_state") == "authenticated"
            and observed.ownership_state == "authenticated"
            and prior_reservation_identity != observed.reservation_identity
        )
        physical_kind_conflict = (
            not isinstance(prior_inventory, dict)
            or prior_inventory.get("physical_kind") != record.physical_kind
        )
        authority_scope_conflict = (
            not isinstance(prior_inventory, dict)
            or prior_inventory.get("authority_scope") != record.authority_scope
        )
        terminal_evidence_conflict = (
            isinstance(prior_inventory, dict)
            and prior_inventory.get("state") == "terminal"
            and record.state == "terminal"
            and prior_inventory.get("terminal_evidence_sha256")
            != record.terminal_evidence_sha256
        )
        immutable_inventory_conflict = (
            physical_kind_conflict
            or authority_scope_conflict
            or terminal_evidence_conflict
        )
        if (
            prior_binding == current_binding
            and not immutable_inventory_conflict
            and "conflicting_contract" not in existing.binding_payload
        ):
            CapacityGrantStore._advance_authenticated_inventory_state(
                intent=authenticated_intent,
                shape=authenticated_shape,
                record=record,
            )
            existing.state = observed.state
            existing.binding_payload = binding_payload
            existing.last_reporter_high_water = inventory_sequence
            existing.last_receipt_time = now
            return classification
        await CapacityGrantStore._quarantine_authenticated_binding(
            session,
            prior if isinstance(prior, dict) else None,
        )
        await CapacityGrantStore._quarantine_authenticated_binding(session, payload)
        existing.state = "quarantined"
        existing.binding_payload = {
            **existing.binding_payload,
            "conflicting_contract": payload,
            "terminal_release_blocked": bool(
                existing.binding_payload.get("terminal_release_blocked")
                or authenticated_reservation_conflict
                or physical_kind_conflict
                or authority_scope_conflict
                or terminal_evidence_conflict
            ),
        }
        conflict_identity = (
            f"{record.physical_identity}-conflict-"
            f"{canonical_digest_excluding(observed, 'state')[:12]}"
        )
        conflict = (
            await session.execute(
                select(CapacityObservedCommitment.id).where(
                    CapacityObservedCommitment.kind == "physical",
                    CapacityObservedCommitment.commitment_identity == conflict_identity,
                    CapacityObservedCommitment.source_incarnation == executor.executor_incarnation,
                )
            )
        ).scalar_one_or_none()
        if conflict is None:
            conflict_contract = observed.model_copy(
                update={
                    "commitment_id": conflict_identity,
                    "state": "quarantined",
                }
            )
            session.add(
                CapacityObservedCommitment(
                    kind="physical",
                    commitment_identity=conflict_identity,
                    source_incarnation=executor.executor_incarnation,
                    subject_id=conflict_contract.subject_id,
                    subject_incarnation=conflict_contract.subject_incarnation,
                    pool_id=conflict_contract.pool_id,
                    pool_generation=conflict_contract.pool_generation,
                    deployment_generation=conflict_contract.deployment_generation,
                    profile_id=conflict_contract.profile_id,
                    profile_generation=conflict_contract.profile_generation,
                    profile_digest=conflict_contract.profile_digest,
                    shape_id=conflict_contract.shape_id,
                    attempt_id=None,
                    concurrency_slots=None,
                    binding_payload={
                        "observed_contract": conflict_contract.model_dump(
                            mode="json", exclude_none=False
                        ),
                        "conflicts_with": record.physical_identity,
                    },
                    resource_vector=conflict_contract.resources.model_dump(
                        mode="json", exclude_none=False
                    ),
                    state="quarantined",
                    first_reporter_high_water=inventory_sequence,
                    last_reporter_high_water=inventory_sequence,
                    first_receipt_time=now,
                    last_receipt_time=now,
                )
            )
        if (
            classification == "authenticated"
            and record.state == "terminal"
            and not existing.binding_payload["terminal_release_blocked"]
        ):
            CapacityGrantStore._advance_authenticated_inventory_state(
                intent=authenticated_intent,
                shape=authenticated_shape,
                record=record,
            )
            return "authenticated"
        return "quarantined"

    @staticmethod
    def _advance_authenticated_inventory_state(
        *,
        intent: CapacitySubmissionIntent | None,
        shape: CapacityReservationShape | None,
        record: ExecutorInventoryRecordV1,
    ) -> None:
        """Advance only after a physical identity's immutable binding agrees."""

        if intent is None or shape is None:
            return
        if record.state == "terminal":
            intent.state = "terminal"
            shape.state = "releasing"
        elif record.state == "pending":
            intent.state = "bound"
        else:
            intent.state = "observed"

    @staticmethod
    async def _quarantine_authenticated_binding(
        session: AsyncSession,
        observed: dict[str, Any] | None,
    ) -> None:
        if observed is None or observed.get("ownership_state") != "authenticated":
            return
        reservation_identity = observed.get("reservation_identity")
        if not isinstance(reservation_identity, str):
            return
        try:
            intent_id = UUID(reservation_identity)
        except ValueError:
            return
        intent = (
            await session.execute(
                select(CapacitySubmissionIntent)
                .where(CapacitySubmissionIntent.id == intent_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if intent is not None and intent.state != "closed":
            intent.state = "quarantined"

    @staticmethod
    def _release_evidence_digest(
        release: DryRunPartialReleaseV1,
        item: ReleasedShapeV1,
    ) -> str:
        return _payload_digest(
            {
                "tranche_id": str(release.tranche_id),
                "shape_instance_id": item.shape_instance_id,
                "intent_id": str(item.intent_id),
                "executor_id": release.executor_id,
                "executor_incarnation": str(release.executor_incarnation),
                "command_sequence": release.command_sequence,
                "inventory_sequence": item.inventory_sequence,
                "terminal_kind": item.terminal_kind,
                "terminal_identity": item.terminal_identity,
                "terminal_evidence_sha256": item.terminal_evidence_sha256,
                "protected_registration_epoch": item.protected_registration_epoch,
                "bootstrap_revoked": item.bootstrap_revoked,
                "protected_release_sha256": item.protected_release_sha256,
                "executable": False,
            }
        )

    @staticmethod
    def _release_evidence_matches(
        existing: CapacityReservationReleaseEvidence,
        release: DryRunPartialReleaseV1,
        item: ReleasedShapeV1,
        item_digest: str,
    ) -> bool:
        return (
            existing.tranche_id == release.tranche_id
            and existing.intent_id == item.intent_id
            and existing.executor_id == release.executor_id
            and existing.executor_incarnation == release.executor_incarnation
            and existing.command_sequence == release.command_sequence
            and existing.inventory_sequence == item.inventory_sequence
            and existing.terminal_kind == item.terminal_kind
            and existing.terminal_identity == item.terminal_identity
            and existing.terminal_evidence_sha256 == item.terminal_evidence_sha256
            and existing.protected_registration_epoch == item.protected_registration_epoch
            and existing.bootstrap_revoked is item.bootstrap_revoked
            and existing.protected_release_sha256 == item.protected_release_sha256
            and existing.evidence_digest == item_digest
        )

    @staticmethod
    def _ownership_metadata(
        tranche: CapacityReservationTranche,
        shape: CapacityReservationShape,
    ) -> OwnershipMetadataV1:
        return OwnershipMetadataV1(
            authority_incarnation=tranche.authority_incarnation,
            writer_epoch=tranche.writer_epoch,
            configuration_epoch=tranche.configuration_epoch,
            allocation_epoch=tranche.allocation_epoch,
            tranche_id=tranche.id,
            intent_id=shape.intent_id,
            shape_instance_id=shape.shape_instance_id,
            subject_id=tranche.subject_id,
            subject_incarnation=tranche.subject_incarnation,
            account_id=tranche.account_id,
            tier_id=cast(
                Literal["production", "staging", "development"],
                tranche.tier_id,
            ),
            candidate_generation=tranche.candidate_generation,
            deployment_generation=tranche.deployment_generation,
            pool_id=tranche.pool_id,
            pool_generation=tranche.pool_generation,
            shape_id=shape.shape_id,
            profile_id=shape.profile_id,
            profile_generation=shape.profile_generation,
            profile_digest=shape.profile_digest,
            concurrency_slots=shape.concurrency_slots,
            resources=ResourceVectorV1.model_validate(shape.resource_vector),
            node_ids=tuple(shape.node_ids),
            executor_id=tranche.executor_id,
            executor_incarnation=tranche.executor_incarnation,
        )

    async def _assert_current_tranche(
        self,
        session: AsyncSession,
        tranche: CapacityReservationTranche,
    ) -> None:
        if tranche.configuration_epoch != await self._latest_configuration_epoch(session):
            raise GrantConflictError("reservation configuration was superseded")
        allocation = await self._latest_allocation_epoch(session)
        if tranche.allocation_epoch != allocation.allocation_epoch:
            raise GrantConflictError("reservation allocation was superseded")

    @staticmethod
    async def _assert_pending_limits(
        session: AsyncSession,
        *,
        authority: CapacityAuthorityState,
        configuration_epoch: int,
        tranche: CapacityReservationTranche,
        intent: CapacitySubmissionIntent,
    ) -> None:
        tier = (
            await session.execute(
                select(CapacityTier)
                .where(
                    CapacityTier.configuration_epoch == configuration_epoch,
                    CapacityTier.tier_id == tranche.tier_id,
                )
                .with_for_update()
            )
        ).scalar_one()
        account = (
            await session.execute(
                select(CapacityAccountPolicy)
                .where(
                    CapacityAccountPolicy.configuration_epoch == configuration_epoch,
                    CapacityAccountPolicy.account_id == tranche.account_id,
                )
                .with_for_update()
            )
        ).scalar_one()
        subject = (
            await session.execute(
                select(CapacitySubject)
                .where(
                    CapacitySubject.configuration_epoch == configuration_epoch,
                    CapacitySubject.subject_id == tranche.subject_id,
                    CapacitySubject.subject_incarnation == tranche.subject_incarnation,
                )
                .with_for_update()
            )
        ).scalar_one()
        pool = (
            await session.execute(
                select(CapacityPool)
                .where(
                    CapacityPool.configuration_epoch == configuration_epoch,
                    CapacityPool.pool_id == tranche.pool_id,
                    CapacityPool.pool_generation == tranche.pool_generation,
                )
                .with_for_update()
            )
        ).scalar_one()
        target_shape = (
            await session.execute(
                select(CapacityReservationShape).where(
                    CapacityReservationShape.intent_id == intent.id
                )
            )
        ).scalar_one()
        totals: dict[str, list[int]] = {
            scope: [0, 0] for scope in ("global", "tier", "account", "subject", "pool")
        }

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
                ("pool", pool_id == tranche.pool_id),
                ("tier", tier_id == tranche.tier_id),
                ("account", account_id == tranche.account_id),
                ("subject", subject_id == tranche.subject_id),
            ):
                if applies:
                    totals[scope][0] += slots
                    totals[scope][1] += 1

        submitted = (
            await session.execute(
                select(
                    CapacitySubmissionIntent.id,
                    CapacityReservationShape.concurrency_slots,
                    CapacityReservationTranche.subject_id,
                    CapacityReservationTranche.account_id,
                    CapacityReservationTranche.tier_id,
                    CapacityReservationTranche.pool_id,
                )
                .join(
                    CapacityReservationShape,
                    CapacityReservationShape.intent_id == CapacitySubmissionIntent.id,
                )
                .join(
                    CapacityReservationTranche,
                    CapacityReservationTranche.id == CapacitySubmissionIntent.tranche_id,
                )
                .where(CapacitySubmissionIntent.state == "submitting-unknown")
            )
        ).all()
        submitted_ids = {str(row.id) for row in submitted}
        for row in submitted:
            add(
                subject_id=row.subject_id,
                account_id=row.account_id,
                tier_id=row.tier_id,
                pool_id=row.pool_id,
                slots=row.concurrency_slots,
            )

        subject_rows = (
            await session.execute(
                select(CapacitySubject).where(
                    CapacitySubject.configuration_epoch == configuration_epoch
                )
            )
        ).scalars()
        subject_scopes = {
            (row.subject_id, row.subject_incarnation): (row.account_id, row.tier_id)
            for row in subject_rows
        }
        physical_rows = (
            (
                await session.execute(
                    select(CapacityObservedCommitment).where(
                        CapacityObservedCommitment.kind == "physical",
                    )
                )
            )
            .scalars()
            .all()
        )
        for commitment in physical_rows:
            observed = commitment.binding_payload.get("observed_contract")
            inventory_record = commitment.binding_payload.get("inventory_record")
            if not (
                commitment.state == "pending"
                or (isinstance(observed, dict) and observed.get("state") == "pending")
                or (
                    isinstance(inventory_record, dict)
                    and inventory_record.get("state") == "pending"
                )
            ):
                continue
            reservation_identity = (
                observed.get("reservation_identity") if isinstance(observed, dict) else None
            )
            if reservation_identity in submitted_ids:
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
                slots=int(commitment.resource_vector["slots"]),
            )

        add(
            subject_id=tranche.subject_id,
            account_id=tranche.account_id,
            tier_id=tranche.tier_id,
            pool_id=tranche.pool_id,
            slots=target_shape.concurrency_slots,
        )
        limits = {
            "global": (
                authority.global_pending_slot_ceiling,
                authority.global_pending_job_ceiling,
            ),
            "tier": (tier.max_pending_slots, tier.max_pending_jobs),
            "account": (account.max_pending_slots, account.max_pending_jobs),
            "subject": (subject.max_pending_slots, subject.max_pending_jobs),
            "pool": (pool.max_pending_slots, pool.max_pending_jobs),
        }
        for scope, (slot_limit, job_limit) in limits.items():
            slots, jobs = totals[scope]
            if slots > slot_limit or jobs > job_limit:
                raise GrantConflictError(f"{scope} pending limit changed")

    @staticmethod
    async def _ensure_rate_buckets(
        session: AsyncSession,
        *,
        configuration_epoch: int,
        authority: CapacityAuthorityState,
        tranche: CapacityReservationTranche,
        now: datetime,
    ) -> None:
        account = (
            await session.execute(
                select(CapacityAccountPolicy).where(
                    CapacityAccountPolicy.configuration_epoch == configuration_epoch,
                    CapacityAccountPolicy.account_id == tranche.account_id,
                )
            )
        ).scalar_one()
        subject = (
            await session.execute(
                select(CapacitySubject).where(
                    CapacitySubject.configuration_epoch == configuration_epoch,
                    CapacitySubject.subject_id == tranche.subject_id,
                    CapacitySubject.subject_incarnation == tranche.subject_incarnation,
                )
            )
        ).scalar_one()
        pool = (
            await session.execute(
                select(CapacityPool).where(
                    CapacityPool.configuration_epoch == configuration_epoch,
                    CapacityPool.pool_id == tranche.pool_id,
                    CapacityPool.pool_generation == tranche.pool_generation,
                )
            )
        ).scalar_one()
        scopes = (
            ("global", "fleet", authority.global_submission_rate_ceiling),
            ("account", tranche.account_id, account.submission_rate_per_minute),
            ("subject", str(tranche.subject_id), subject.submission_rate_per_minute),
            ("pool", tranche.pool_id, pool.submission_rate_per_minute),
        )
        for scope, identity, rate in scopes:
            existing = (
                await session.execute(
                    select(CapacityLaunchRateBucket).where(
                        CapacityLaunchRateBucket.configuration_epoch == configuration_epoch,
                        CapacityLaunchRateBucket.scope == scope,
                        CapacityLaunchRateBucket.scope_identity == identity,
                    )
                )
            ).scalar_one_or_none()
            if existing is not None:
                if existing.rate_per_minute != rate:
                    raise GrantConflictError("durable launch rate disagrees with configuration")
                continue
            capacity = rate * MICROTOKENS_PER_LAUNCH
            session.add(
                CapacityLaunchRateBucket(
                    configuration_epoch=configuration_epoch,
                    scope=scope,
                    scope_identity=identity,
                    rate_per_minute=rate,
                    capacity_microtokens=capacity,
                    available_microtokens=capacity,
                    refill_remainder=0,
                    last_refill_at=now,
                    state="dry-run",
                )
            )
        await session.flush()

    @staticmethod
    async def _earliest_ready_intent_id(
        session: AsyncSession,
        *,
        allocation: CapacityAllocationEpoch,
        now: datetime,
    ) -> UUID:
        ready = (
            await session.execute(
                select(
                    CapacitySubmissionIntent.id,
                    CapacityReservationShape.shape_instance_id,
                )
                .join(
                    CapacityReservationShape,
                    CapacityReservationShape.intent_id == CapacitySubmissionIntent.id,
                )
                .join(
                    CapacityReservationTranche,
                    CapacityReservationTranche.id == CapacitySubmissionIntent.tranche_id,
                )
                .join(
                    CapacityLaunchPermit,
                    CapacityLaunchPermit.intent_id == CapacitySubmissionIntent.id,
                )
                .where(
                    CapacitySubmissionIntent.state == "launch-ready",
                    CapacityReservationTranche.allocation_epoch == allocation.allocation_epoch,
                    CapacityReservationTranche.state == "accepted",
                    CapacityLaunchPermit.state == "current",
                    CapacityLaunchPermit.allocation_epoch == allocation.allocation_epoch,
                    CapacityLaunchPermit.configuration_epoch
                    == allocation.configuration_epoch,
                    CapacityLaunchPermit.expires_at > now,
                )
                .with_for_update()
            )
        ).all()
        if not ready:
            raise LaunchOrderError("no globally eligible launch-ready intent exists")
        rank_by_shape = {
            item["shape_instance_id"]: item["rank"]
            for item in allocation.complete_payload["hypothetical_launch_rank"]
        }
        if any(shape_id not in rank_by_shape for _, shape_id in ready):
            raise GrantConflictError("launch-ready intent is absent from allocation order")
        return cast(
            UUID,
            min(
                ready,
                key=lambda item: (rank_by_shape[item.shape_instance_id], item.id),
            ).id,
        )

    @staticmethod
    async def _locked_rate_buckets(
        session: AsyncSession,
        *,
        configuration_epoch: int,
        tranche: CapacityReservationTranche,
    ) -> tuple[CapacityLaunchRateBucket, ...]:
        identities = {
            ("global", "fleet"),
            ("account", tranche.account_id),
            ("subject", str(tranche.subject_id)),
            ("pool", tranche.pool_id),
        }
        rows = tuple(
            (
                await session.execute(
                    select(CapacityLaunchRateBucket)
                    .where(
                        CapacityLaunchRateBucket.configuration_epoch == configuration_epoch,
                        tuple_(
                            CapacityLaunchRateBucket.scope,
                            CapacityLaunchRateBucket.scope_identity,
                        ).in_(tuple(sorted(identities))),
                    )
                    .order_by(
                        CapacityLaunchRateBucket.scope,
                        CapacityLaunchRateBucket.scope_identity,
                    )
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        if len(rows) != 4 or {(row.scope, row.scope_identity) for row in rows} != identities:
            raise GrantConflictError("durable launch rate bucket set is incomplete")
        return rows

    @staticmethod
    def _refill_bucket(bucket: CapacityLaunchRateBucket, now: datetime) -> None:
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

    @staticmethod
    async def _latest_allocation_epoch(session: AsyncSession) -> CapacityAllocationEpoch:
        row = (
            (
                await session.execute(
                    select(CapacityAllocationEpoch)
                    .where(CapacityAllocationEpoch.status == "shadow")
                    .order_by(CapacityAllocationEpoch.allocation_epoch.desc())
                )
            )
            .scalars()
            .first()
        )
        if row is None:
            raise GrantConflictError("no committed shadow allocation")
        return row

    @staticmethod
    async def _current_executor(
        session: AsyncSession,
        *,
        executor_id: str,
        executor_incarnation: UUID,
        pool_id: str | None,
        now: datetime,
        allow_expired: bool = False,
    ) -> CapacityExecutor:
        conditions = [
            CapacityExecutor.executor_id == executor_id,
            CapacityExecutor.executor_incarnation == executor_incarnation,
        ]
        if pool_id is not None:
            conditions.append(CapacityExecutor.pool_id == pool_id)
        executor = (
            await session.execute(select(CapacityExecutor).where(*conditions).with_for_update())
        ).scalar_one_or_none()
        if (
            executor is None
            or executor.state != "dry-run"
            or (not allow_expired and executor.lease_expires_at <= now)
        ):
            raise StaleExecutorError("executor identity or lease is not current")
        return executor

    @staticmethod
    def _journal_advance_error(
        executor: CapacityExecutor,
        *,
        journal_sequence: int,
        journal_digest: str,
        checkpoint_sequence: int,
        checkpoint_digest: str,
    ) -> str | None:
        central_sequence = executor.journal_high_water
        central_digest = executor.journal_digest or "0" * 64
        if journal_sequence < central_sequence:
            return "executor journal sequence regressed"
        if journal_sequence < executor.command_high_water:
            return "executor journal is behind central command state"
        if journal_sequence == central_sequence:
            if journal_digest != central_digest:
                return "executor journal digest changed at high-water"
            return None
        if checkpoint_sequence != central_sequence or checkpoint_digest != central_digest:
            return "executor journal does not cover the central checkpoint"
        return None

    @staticmethod
    def _command_sequence_state(
        executor: CapacityExecutor,
        *,
        sequence: int,
        digest: str,
    ) -> Literal["new", "replay", "equivocation"]:
        if sequence < executor.command_high_water:
            raise StaleCommandError("executor command sequence regressed")
        if sequence == executor.command_high_water:
            if executor.last_command_digest == digest:
                return "replay"
            return "equivocation"
        if sequence != executor.command_high_water + 1:
            raise StaleCommandError("executor command sequence has a gap")
        return "new"

    @staticmethod
    def _validate_shapes(
        proposal: DryRunReservationProposalV1,
        profile: CapacityWorkerProfile,
        allocation: CapacityAllocation,
        epoch_payload: dict[str, object],
    ) -> None:
        catalog = {item["shape_id"]: item for item in profile.shape_catalog}
        ranked_payload = cast(
            list[dict[str, Any]],
            epoch_payload.get("hypothetical_launch_rank", []),
        )
        allocation_payload = cast(
            list[dict[str, Any]],
            epoch_payload.get("allocations", []),
        )
        witness_payload = cast(
            list[dict[str, Any]],
            epoch_payload.get("pool_witnesses", []),
        )
        ranks = {
            (
                item["subject_id"],
                item["pool_id"],
                item["shape_instance_id"],
            )
            for item in ranked_payload
        }
        surge = {
            item["new_shape_instance_id"]: item
            for candidate in allocation_payload
            if candidate["subject_id"] == str(proposal.subject_id)
            and candidate["pool_id"] == proposal.pool_id
            for item in candidate.get("surge_pairings", [])
        }
        placement_nodes = {
            (witness["pool_id"], placement["instance_id"]): tuple(placement["node_ids"])
            for witness in witness_payload
            for placement in witness.get("placements", [])
        }
        for shape in proposal.shapes:
            expected = catalog.get(shape.shape_id)
            if expected is None:
                raise GrantConflictError("reservation shape is outside the worker profile")
            if (
                shape.profile_id != expected["shape_id"]
                or shape.profile_generation != profile.profile_generation
                or shape.profile_digest != profile.profile_digest
                or shape.concurrency_slots != expected["concurrency_slots"]
                or shape.resources.model_dump(mode="json", exclude_none=False)
                != expected["total_resources"]
            ):
                raise GrantConflictError("reservation shape resource binding changed")
            if (
                str(proposal.subject_id),
                proposal.pool_id,
                shape.shape_instance_id,
            ) not in ranks:
                raise GrantConflictError("reservation shape is absent from launch order")
            if placement_nodes.get((proposal.pool_id, shape.shape_instance_id)) != shape.node_ids:
                raise GrantConflictError("reservation shape topology binding changed")
            pairing = surge.get(shape.shape_instance_id)
            if pairing is None:
                if shape.rollout_surge_slots or shape.old_shape_backing_id is not None:
                    raise GrantConflictError("reservation shape has unallocated surge backing")
            elif (
                shape.rollout_surge_slots != pairing["backed_slots"]
                or shape.old_shape_backing_id != pairing["old_commitment_id"]
            ):
                raise GrantConflictError("reservation surge backing changed")

    @staticmethod
    async def _delete_unaccepted_shapes(
        session: AsyncSession,
        tranche_id: UUID,
    ) -> None:
        await session.execute(
            delete(CapacityReservationShape).where(
                CapacityReservationShape.tranche_id == tranche_id,
                CapacityReservationShape.state == "proposed",
            )
        )

    @staticmethod
    async def _close_stale_proposals(
        session: AsyncSession,
        *,
        proposal: DryRunReservationProposalV1,
        now: datetime,
    ) -> None:
        stale = (
            (
                await session.execute(
                    select(CapacityReservationTranche)
                    .where(
                        CapacityReservationTranche.subject_id == proposal.subject_id,
                        CapacityReservationTranche.subject_incarnation
                        == proposal.subject_incarnation,
                        CapacityReservationTranche.pool_id == proposal.pool_id,
                        CapacityReservationTranche.state == "proposed",
                        or_(
                            CapacityReservationTranche.configuration_epoch
                            != proposal.configuration_epoch,
                            CapacityReservationTranche.allocation_epoch
                            != proposal.allocation_epoch,
                            CapacityReservationTranche.expires_at <= now,
                        ),
                    )
                    .order_by(CapacityReservationTranche.id)
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        if not stale:
            return
        stale_ids = tuple(row.id for row in stale)
        for row in stale:
            row.state = "closed"
            row.closed_at = now
            row.closure_reason = (
                "proposal-expired" if row.expires_at <= now else "proposal-superseded"
            )
            session.add(
                _audit(
                    actor_kind="manager",
                    actor_id=str(proposal.authority_incarnation),
                    event_kind="capacity_reservation_proposal_closed_dry_run",
                    object_binding={"tranche_id": str(row.id)},
                    detail={
                        "closure_reason": row.closure_reason,
                        "replacement_allocation_epoch": proposal.allocation_epoch,
                        "executable": False,
                    },
                )
            )
        await session.execute(
            delete(CapacityReservationShape).where(
                CapacityReservationShape.tranche_id.in_(stale_ids),
                CapacityReservationShape.state == "proposed",
            )
        )

    @staticmethod
    async def _validate_open_shape_counts(
        session: AsyncSession,
        proposal: DryRunReservationProposalV1,
        allocation: CapacityAllocation,
    ) -> None:
        allowed = {item["shape_id"]: item["count"] for item in allocation.desired_shapes}
        existing = Counter(
            (
                await session.execute(
                    select(CapacityReservationShape.shape_id)
                    .join(
                        CapacityReservationTranche,
                        CapacityReservationTranche.id == CapacityReservationShape.tranche_id,
                    )
                    .where(
                        CapacityReservationTranche.subject_id == proposal.subject_id,
                        CapacityReservationTranche.subject_incarnation
                        == proposal.subject_incarnation,
                        CapacityReservationTranche.pool_id == proposal.pool_id,
                        CapacityReservationTranche.state.in_(("proposed", "accepted")),
                        CapacityReservationShape.state != "released",
                    )
                )
            )
            .scalars()
            .all()
        )
        requested = Counter(shape.shape_id for shape in proposal.shapes)
        for shape_id, count in requested.items():
            if count + existing[shape_id] > allowed.get(shape_id, 0):
                raise GrantConflictError("reservation exceeds allocated shape count")

    @staticmethod
    async def _tranche_intent_ids(
        session: AsyncSession,
        tranche_id: UUID,
    ) -> tuple[UUID, ...]:
        return tuple(
            (
                await session.execute(
                    select(CapacitySubmissionIntent.id)
                    .where(CapacitySubmissionIntent.tranche_id == tranche_id)
                    .order_by(CapacitySubmissionIntent.shape_instance_id)
                )
            )
            .scalars()
            .all()
        )


__all__ = [
    "AcceptedReservation",
    "AcknowledgedProtectedRelease",
    "CapacityGrantStore",
    "ClosingIntent",
    "ConsumedLaunchPermit",
    "ExecutorCheckpoint",
    "ExecutorEquivocationError",
    "ExecutorJournalError",
    "GrantConflictError",
    "HeartbeatedExecutor",
    "IdempotencyConflictError",
    "IngestedExecutorInventory",
    "IntentReady",
    "IssuedLaunchPermit",
    "LaunchOrderError",
    "PermitExpiredError",
    "ProposalExpiredError",
    "ProposalSupersededError",
    "ProposedReservation",
    "RateLimitError",
    "RegisteredExecutor",
    "ReleasedReservationShapes",
    "StaleCommandError",
    "StaleExecutorError",
]
