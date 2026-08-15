"""Durable execution-epoch preparation and activation boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import delete, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableExecutorRegistrationV2,
    ExecutionActivationV2,
    ExecutionContextV2,
    ExecutionPreparationPolicyV2,
    ExecutionPreparationV2,
    LegacyWriterFenceV2,
    PoolControllerAuthorityV2,
    PreparedExecutorBindingV2,
    SubjectExecutionAcknowledgementV2,
    canonical_executable_digest,
)
from loom_capacity_manager.grant_contracts import DryRunExecutorRegistrationV1
from loom_capacity_manager.grant_store import CapacityGrantStore
from loom_capacity_manager.models import (
    Base,
    CapacityAuthorityState,
    CapacityCandidate,
    CapacityExecutionEpoch,
    CapacityExecutionExecutor,
)
from loom_capacity_manager.ownership import OwnershipKeyring, public_key_fingerprint
from loom_capacity_manager.store import (
    AuthorityRecoveryError,
    CapacityManagementStore,
    ExecutionConflictError,
    ExecutionPreparationDisabledError,
    IdempotencyConflictError,
    StaleWriterError,
    WriterFence,
)
from tests.capacity_fixtures import (
    AUTHORITY_ID,
    configuration_activation,
    fleet_manifest,
    subject_configuration,
)

_EXECUTOR_INCARNATIONS = {
    "gb10": UUID("00000000-0000-4000-8000-000000000711"),
    "oldlab": UUID("00000000-0000-4000-8000-000000000712"),
}
_EXECUTOR_KEYS = {
    "gb10": Ed25519PrivateKey.from_private_bytes(b"\x11" * 32),
    "oldlab": Ed25519PrivateKey.from_private_bytes(b"\x12" * 32),
}
_CONTROLLER_DIGESTS = {"gb10": "c" * 64, "oldlab": "d" * 64}
_TRUSTED_RELEASE = "e" * 64


@dataclass(frozen=True, slots=True)
class _PreparedFixture:
    store: CapacityManagementStore
    writer: WriterFence
    request: ExecutionPreparationV2


def _source_candidate() -> CandidateBindingV2:
    return CandidateBindingV2(
        algorithm="source-sha256",
        identity="1" * 64,
        publication_sha256="2" * 64,
    )


def _protected_candidate() -> CandidateBindingV2:
    return CandidateBindingV2(
        algorithm="git-sha1",
        identity="1" * 40,
        publication_sha256="2" * 64,
    )


def _acknowledgement(
    candidate: CandidateBindingV2 | None = None,
) -> SubjectExecutionAcknowledgementV2:
    subject = subject_configuration(fleet_manifest())
    return SubjectExecutionAcknowledgementV2(
        subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation,
        configuration_generation=subject.configuration_generation,
        deployment_generation=subject.deployment_generation,
        candidate=_source_candidate() if candidate is None else candidate,
        reporter_incarnation=subject.demand_reporter_incarnation,
        protected_admission_sha256="3" * 64,
        legacy_writer_high_water=0,
        acknowledgement_sha256="4" * 64,
    )


def _executor_binding(pool_id: str) -> PreparedExecutorBindingV2:
    private_key = _EXECUTOR_KEYS[pool_id]
    return PreparedExecutorBindingV2(
        pool_id=pool_id,
        pool_generation=1,
        executor_id=f"{pool_id}-executor",
        executor_incarnation=_EXECUTOR_INCARNATIONS[pool_id],
        signing_key_sha256=public_key_fingerprint(private_key.public_key()),
        local_authority_sha256=("a" if pool_id == "gb10" else "b") * 64,
        controller_authority_sha256=_CONTROLLER_DIGESTS[pool_id],
    )


def _policy(
    candidate: CandidateBindingV2 | None = None,
) -> ExecutionPreparationPolicyV2:
    return ExecutionPreparationPolicyV2(
        trusted_fleet_release_sha256=_TRUSTED_RELEASE,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
        executors=tuple(_executor_binding(pool_id) for pool_id in ("gb10", "oldlab")),
        subject_acknowledgements=(_acknowledgement(candidate),),
        rollback_evidence_sha256="6" * 64,
        controller_authorities=tuple(
            PoolControllerAuthorityV2(
                pool_id=pool_id,
                controller_authority_sha256=_CONTROLLER_DIGESTS[pool_id],
            )
            for pool_id in ("gb10", "oldlab")
        ),
        legacy_writer_fences=(
            LegacyWriterFenceV2(
                writer_id="global-dev-supervisor",
                writer_kind="allocation",
                scope_kind="global",
                scope_id="development",
                high_water=9,
                freeze_evidence_sha256="5" * 64,
                state="frozen",
            ),
        ),
    )


async def _setup(
    session: AsyncSession,
    *,
    execution_policy: ExecutionPreparationPolicyV2 | None = None,
    candidate: CandidateBindingV2 | None = None,
) -> _PreparedFixture:
    for table in reversed(Base.metadata.sorted_tables):
        if table.name != CapacityAuthorityState.__tablename__:
            await session.execute(delete(table))
    await session.execute(
        update(CapacityAuthorityState)
        .where(CapacityAuthorityState.singleton_id == 1)
        .values(
            authority_incarnation=AUTHORITY_ID,
            writer_epoch=0,
            recovery_state="shadow",
            increase_freeze=True,
            increase_freeze_reason="initial_shadow_freeze",
            executable_new_capacity_ceiling=0,
            execution_epoch=0,
            execution_state="shadow",
            execution_manifest_sha256=None,
            global_pending_slot_ceiling=0,
            global_pending_job_ceiling=0,
            global_submission_rate_ceiling=0,
        )
    )

    store = CapacityManagementStore(execution_policy=execution_policy)
    fleet = fleet_manifest()
    subject = subject_configuration(fleet)
    fleet_proposal = await store.propose_fleet_configuration(
        session,
        fleet,
        actor="fleet-operator",
        idempotency_key=UUID(int=701),
    )
    subject_proposal = await store.propose_subject_configuration(
        session,
        subject,
        actor="environment-state",
        idempotency_key=UUID(int=702),
    )
    active = await store.activate_configuration(
        session,
        configuration_activation(
            fleet=fleet_proposal,
            subjects=(subject_proposal,),
        ),
        actor="fleet-operator",
        idempotency_key=UUID(int=703),
    )
    acknowledgement = _acknowledgement(candidate)
    session.add(
        CapacityCandidate(
            subject_id=acknowledgement.subject_id,
            subject_incarnation=acknowledgement.subject_incarnation,
            candidate_generation=subject.candidate_generation,
            candidate_digest=(
                acknowledgement.candidate.identity
                if acknowledgement.candidate.algorithm == "source-sha256"
                else "a" * 64
            ),
            candidate_identity_algorithm=acknowledgement.candidate.algorithm,
            candidate_identity=acknowledgement.candidate.identity,
            source_payload={"publication_sha256": acknowledgement.candidate.publication_sha256},
            artifact_payload={},
            architecture_payload={},
            launcher_payload={},
            attestation_payload={},
            protocol_payload={},
        )
    )
    await session.flush()
    writer = await store.register_writer(session, AUTHORITY_ID, expected_epoch=0)

    grant_store = CapacityGrantStore(
        ownership_keyring=OwnershipKeyring(
            {
                f"{pool_id}-key": private_key.public_key()
                for pool_id, private_key in _EXECUTOR_KEYS.items()
            }
        )
    )
    executors: list[PreparedExecutorBindingV2] = []
    for index, pool_id in enumerate(("gb10", "oldlab"), start=1):
        public_key = _EXECUTOR_KEYS[pool_id].public_key()
        fingerprint = public_key_fingerprint(public_key)
        registration = DryRunExecutorRegistrationV1(
            executor_id=f"{pool_id}-executor",
            executor_incarnation=_EXECUTOR_INCARNATIONS[pool_id],
            pool_id=pool_id,
            pool_generation=1,
            signing_key_id=f"{pool_id}-key",
            signing_key_sha256=fingerprint,
            local_authority_sha256=("a" if pool_id == "gb10" else "b") * 64,
        )
        await grant_store.register_executor(
            session,
            writer,
            registration,
            actor="executor-installer",
            idempotency_key=UUID(int=710 + index),
        )
        executors.append(
            PreparedExecutorBindingV2(
                pool_id=pool_id,
                pool_generation=1,
                executor_id=registration.executor_id,
                executor_incarnation=registration.executor_incarnation,
                signing_key_sha256=registration.signing_key_sha256,
                local_authority_sha256=registration.local_authority_sha256,
                controller_authority_sha256=_CONTROLLER_DIGESTS[pool_id],
            )
        )

    request = ExecutionPreparationV2(
        authority_incarnation=AUTHORITY_ID,
        expected_writer_epoch=writer.writer_epoch,
        configuration_epoch=active.configuration_epoch,
        fleet_generation=fleet.fleet_generation,
        fleet_digest=fleet_proposal.digest,
        trusted_fleet_release_sha256=_TRUSTED_RELEASE,
        requested_ceiling=1,
        requested_rate_per_minute=1,
        executors=tuple(executors),
        subject_acknowledgements=(acknowledgement,),
        legacy_writer_fences=(
            LegacyWriterFenceV2(
                writer_id="global-dev-supervisor",
                writer_kind="allocation",
                scope_kind="global",
                scope_id="development",
                high_water=9,
                freeze_evidence_sha256="5" * 64,
                state="frozen",
            ),
        ),
        rollback_evidence_sha256="6" * 64,
    )
    return _PreparedFixture(store=store, writer=writer, request=request)


async def _register_execution_executors(
    session: AsyncSession,
    fixture: _PreparedFixture,
    prepared: ExecutionContextV2,
) -> None:
    for index, binding in enumerate(fixture.request.executors, start=1):
        await fixture.store.register_execution_executor(
            session,
            ExecutableExecutorRegistrationV2(
                execution=prepared,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                pool_id=binding.pool_id,
                pool_generation=binding.pool_generation,
                signing_key_id=f"{binding.pool_id}-key",
                signing_key_sha256=binding.signing_key_sha256,
                local_authority_sha256=binding.local_authority_sha256,
                controller_authority_sha256=binding.controller_authority_sha256,
            ),
            actor="executor-installer",
            idempotency_key=UUID(int=740 + index),
        )


async def _activate_fixture(
    capacity_session: AsyncSession,
    *,
    policy: ExecutionPreparationPolicyV2 | None = None,
) -> tuple[
    _PreparedFixture,
    ExecutionContextV2,
    ExecutionActivationV2,
    ExecutionContextV2,
]:
    effective_policy = _policy() if policy is None else policy
    fixture = await _setup(capacity_session, execution_policy=effective_policy)
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=780),
    )
    await _register_execution_executors(capacity_session, fixture, prepared)
    activation = ExecutionActivationV2(
        authority_incarnation=AUTHORITY_ID,
        expected_writer_epoch=fixture.writer.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
    )
    active = await fixture.store.activate_execution_epoch(
        capacity_session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=781),
    )
    return fixture, prepared, activation, active


async def test_execution_preparation_is_disabled_without_owner_policy(
    capacity_session: AsyncSession,
) -> None:
    """A caller-supplied manifest alone must never create executable authority."""

    fixture = await _setup(capacity_session)
    with pytest.raises(ExecutionPreparationDisabledError):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            fixture.request,
            actor="activation-operator",
            idempotency_key=UUID(int=720),
        )


async def test_prepare_rejects_executor_pool_generation_outside_active_fleet(
    capacity_session: AsyncSession,
) -> None:
    """Owner policy cannot prepare an executor against a stale pool generation."""

    policy = _policy()
    stale_executors = tuple(
        binding.model_copy(update={"pool_generation": 2}) if binding.pool_id == "gb10" else binding
        for binding in policy.executors
    )
    stale_policy = policy.model_copy(update={"executors": stale_executors})
    fixture = await _setup(capacity_session, execution_policy=stale_policy)
    stale_request = fixture.request.model_copy(update={"executors": stale_executors})

    with pytest.raises(ExecutionConflictError, match="executor pool generation"):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            stale_request,
            actor="activation-operator",
            idempotency_key=UUID(int=719),
        )


async def test_prepare_is_exact_replay_and_keeps_the_ceiling_zero(
    capacity_session: AsyncSession,
) -> None:
    """Preparation must persist immutable evidence without enabling capacity."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    first = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=721),
    )
    replay = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=721),
    )

    assert replay == first
    assert first.execution_state == "prepared"
    assert first.executable_new_capacity_ceiling == 0
    assert first.execution_manifest_sha256 == canonical_executable_digest(fixture.request)
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert (authority.execution_state, authority.executable_new_capacity_ceiling) == (
        "prepared",
        0,
    )

    with pytest.raises(DBAPIError, match="execution epoch immutable evidence changed"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutionEpoch).values(fleet_digest="f" * 64)
            )
    row = (await capacity_session.execute(select(CapacityExecutionEpoch))).scalar_one()
    with pytest.raises(DBAPIError, match="must be inserted prepared"):
        async with capacity_session.begin_nested():
            capacity_session.add(
                CapacityExecutionEpoch(
                    execution_epoch=row.execution_epoch + 1,
                    authority_incarnation=row.authority_incarnation,
                    prepared_writer_epoch=row.prepared_writer_epoch,
                    current_writer_epoch=row.current_writer_epoch,
                    configuration_epoch=row.configuration_epoch,
                    fleet_generation=row.fleet_generation,
                    fleet_digest=row.fleet_digest,
                    execution_manifest_sha256="9" * 64,
                    manifest_payload=row.manifest_payload,
                    trusted_fleet_release_sha256=row.trusted_fleet_release_sha256,
                    oldlab_executor_id=row.oldlab_executor_id,
                    oldlab_executor_incarnation=row.oldlab_executor_incarnation,
                    oldlab_pool_id=row.oldlab_pool_id,
                    oldlab_pool_generation=row.oldlab_pool_generation,
                    oldlab_signing_key_sha256=row.oldlab_signing_key_sha256,
                    oldlab_local_authority_sha256=row.oldlab_local_authority_sha256,
                    oldlab_controller_authority_sha256=(row.oldlab_controller_authority_sha256),
                    gb10_executor_id=row.gb10_executor_id,
                    gb10_executor_incarnation=row.gb10_executor_incarnation,
                    gb10_pool_id=row.gb10_pool_id,
                    gb10_pool_generation=row.gb10_pool_generation,
                    gb10_signing_key_sha256=row.gb10_signing_key_sha256,
                    gb10_local_authority_sha256=row.gb10_local_authority_sha256,
                    gb10_controller_authority_sha256=row.gb10_controller_authority_sha256,
                    environment_acknowledgements_sha256=(row.environment_acknowledgements_sha256),
                    legacy_writer_manifest_sha256=row.legacy_writer_manifest_sha256,
                    rollback_evidence_sha256=row.rollback_evidence_sha256,
                    requested_ceiling=1,
                    effective_ceiling=1,
                    requested_rate_per_minute=1,
                    effective_rate_per_minute=1,
                    state="active",
                    actor="forged-operator",
                    idempotency_key=UUID(int=727),
                    request_digest="9" * 64,
                    activation_actor="forged-operator",
                    activation_idempotency_key=UUID(int=728),
                    activation_request_digest="8" * 64,
                    activated_at=datetime.now(UTC),
                )
            )
            await capacity_session.flush()
    changed = fixture.request.model_copy(update={"requested_ceiling": 2})
    with pytest.raises(IdempotencyConflictError):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            changed,
            actor="activation-operator",
            idempotency_key=UUID(int=721),
        )


async def test_prepare_replay_revalidates_current_candidate_provenance(
    capacity_session: AsyncSession,
) -> None:
    """An idempotent replay cannot bypass candidate drift discovered later."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=782),
    )
    await capacity_session.execute(update(CapacityCandidate).values(candidate_identity="9" * 64))

    with pytest.raises(ExecutionConflictError, match="candidate provenance"):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            fixture.request,
            actor="activation-operator",
            idempotency_key=UUID(int=782),
        )


async def test_prepare_and_executor_registration_replays_stop_after_activation(
    capacity_session: AsyncSession,
) -> None:
    """Prepared-only operations cannot replay into positive active authority."""

    fixture, prepared, _, _ = await _activate_fixture(capacity_session)

    with pytest.raises(ExecutionConflictError, match="only while prepared"):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            fixture.request,
            actor="activation-operator",
            idempotency_key=UUID(int=780),
        )

    binding = fixture.request.executors[0]
    with pytest.raises(ExecutionConflictError, match="only while prepared"):
        await fixture.store.register_execution_executor(
            capacity_session,
            ExecutableExecutorRegistrationV2(
                execution=prepared,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                pool_id=binding.pool_id,
                pool_generation=binding.pool_generation,
                signing_key_id=f"{binding.pool_id}-key",
                signing_key_sha256=binding.signing_key_sha256,
                local_authority_sha256=binding.local_authority_sha256,
                controller_authority_sha256=binding.controller_authority_sha256,
            ),
            actor="executor-installer",
            idempotency_key=UUID(int=741),
        )


async def test_prepare_rejects_incomplete_subject_or_legacy_inventory(
    capacity_session: AsyncSession,
) -> None:
    """Omitting a subject or configured writer must not produce a prepared epoch."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    acknowledgement = fixture.request.subject_acknowledgements[0]
    for changed in (
        fixture.request.model_copy(update={"subject_acknowledgements": ()}),
        fixture.request.model_copy(update={"legacy_writer_fences": ()}),
        fixture.request.model_copy(
            update={
                "subject_acknowledgements": (
                    acknowledgement.model_copy(update={"acknowledgement_sha256": "9" * 64}),
                )
            }
        ),
        fixture.request.model_copy(update={"rollback_evidence_sha256": "9" * 64}),
        fixture.request.model_copy(update={"requested_ceiling": 2}),
    ):
        with pytest.raises(ExecutionConflictError):
            await fixture.store.prepare_execution_epoch(
                capacity_session,
                changed,
                actor="activation-operator",
                idempotency_key=UUID(int=722),
            )


async def test_activation_rechecks_freeze_and_exact_evidence(
    capacity_session: AsyncSession,
) -> None:
    """Changing a prepared fence must block the active authority transition."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=723),
    )
    activation = ExecutionActivationV2(
        authority_incarnation=AUTHORITY_ID,
        expected_writer_epoch=fixture.writer.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
    )
    await _register_execution_executors(capacity_session, fixture, prepared)
    await capacity_session.execute(
        update(CapacityAuthorityState).values(
            increase_freeze=False,
            increase_freeze_reason=None,
        )
    )
    with pytest.raises(ExecutionConflictError, match="increase freeze"):
        await fixture.store.activate_execution_epoch(
            capacity_session,
            activation,
            actor="activation-operator",
            idempotency_key=UUID(int=724),
        )


async def test_execution_rechecks_durable_candidate_provenance(
    capacity_session: AsyncSession,
) -> None:
    """A policy-matching acknowledgement cannot override durable candidate state."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    await capacity_session.execute(update(CapacityCandidate).values(candidate_identity="9" * 64))
    with pytest.raises(ExecutionConflictError, match="candidate provenance"):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            fixture.request,
            actor="activation-operator",
            idempotency_key=UUID(int=733),
        )

    await capacity_session.execute(update(CapacityCandidate).values(candidate_identity="1" * 64))
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=734),
    )
    await _register_execution_executors(capacity_session, fixture, prepared)
    activation = ExecutionActivationV2(
        authority_incarnation=AUTHORITY_ID,
        expected_writer_epoch=fixture.writer.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
    )
    await capacity_session.execute(
        update(CapacityCandidate).values(source_payload={"publication_sha256": "8" * 64})
    )
    with pytest.raises(ExecutionConflictError, match="candidate provenance"):
        await fixture.store.activate_execution_epoch(
            capacity_session,
            activation,
            actor="activation-operator",
            idempotency_key=UUID(int=735),
        )
    await capacity_session.execute(
        update(CapacityCandidate).values(source_payload={"publication_sha256": "2" * 64})
    )
    await fixture.store.activate_execution_epoch(
        capacity_session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=738),
    )
    await capacity_session.execute(update(CapacityCandidate).values(candidate_identity="7" * 64))
    with pytest.raises(AuthorityRecoveryError, match="owner policy"):
        await fixture.store.execution_authority(capacity_session)


async def test_execution_accepts_exact_protected_git_candidate(
    capacity_session: AsyncSession,
) -> None:
    """A protected release retains its exact 40-character Git identity."""

    candidate = _protected_candidate()
    fixture = await _setup(
        capacity_session,
        execution_policy=_policy(candidate),
        candidate=candidate,
    )

    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=739),
    )

    assert prepared.execution_state == "prepared"


async def test_execution_rejects_stale_protected_git_candidate(
    capacity_session: AsyncSession,
) -> None:
    """A protected acknowledgement cannot substitute another exact Git commit."""

    candidate = _protected_candidate()
    fixture = await _setup(
        capacity_session,
        execution_policy=_policy(candidate),
        candidate=candidate,
    )
    await capacity_session.execute(update(CapacityCandidate).values(candidate_identity="9" * 40))

    with pytest.raises(ExecutionConflictError, match="candidate provenance"):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            fixture.request,
            actor="activation-operator",
            idempotency_key=UUID(int=740),
        )


async def test_writer_restart_retires_a_stale_preparation(
    capacity_session: AsyncSession,
) -> None:
    """A restarted writer must not strand a prepared authority on an old fence."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=729),
    )
    await _register_execution_executors(capacity_session, fixture, prepared)

    successor = await fixture.store.register_writer(
        capacity_session,
        AUTHORITY_ID,
        expected_epoch=fixture.writer.writer_epoch,
    )

    assert successor.writer_epoch == fixture.writer.writer_epoch + 1
    assert await fixture.store.execution_authority(capacity_session) is None
    row = (
        await capacity_session.execute(
            select(CapacityExecutionEpoch).where(
                CapacityExecutionEpoch.execution_epoch == prepared.execution_epoch
            )
        )
    ).scalar_one()
    assert row.state == "retired"
    assert row.retired_at is not None

    with pytest.raises(ExecutionConflictError, match="only while prepared"):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            fixture.request,
            actor="activation-operator",
            idempotency_key=UUID(int=729),
        )

    binding = fixture.request.executors[0]
    with pytest.raises(ExecutionConflictError, match="only while prepared"):
        await fixture.store.register_execution_executor(
            capacity_session,
            ExecutableExecutorRegistrationV2(
                execution=prepared,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                pool_id=binding.pool_id,
                pool_generation=binding.pool_generation,
                signing_key_id=f"{binding.pool_id}-key",
                signing_key_sha256=binding.signing_key_sha256,
                local_authority_sha256=binding.local_authority_sha256,
                controller_authority_sha256=binding.controller_authority_sha256,
            ),
            actor="executor-installer",
            idempotency_key=UUID(int=741),
        )


async def test_active_writer_restart_clamps_to_drain_only_and_refences(
    capacity_session: AsyncSession,
) -> None:
    """A replacement writer must atomically remove stale scale-up authority."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=736),
    )
    await _register_execution_executors(capacity_session, fixture, prepared)
    await fixture.store.activate_execution_epoch(
        capacity_session,
        ExecutionActivationV2(
            authority_incarnation=AUTHORITY_ID,
            expected_writer_epoch=fixture.writer.writer_epoch,
            execution_epoch=prepared.execution_epoch,
            execution_manifest_sha256=prepared.execution_manifest_sha256,
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
        ),
        actor="activation-operator",
        idempotency_key=UUID(int=737),
    )

    successor = await fixture.store.register_writer(
        capacity_session,
        AUTHORITY_ID,
        expected_epoch=fixture.writer.writer_epoch,
    )

    authority = await fixture.store.execution_authority(capacity_session)
    assert authority is not None
    assert authority.execution_state == "drain-only"
    assert authority.executable_new_capacity_ceiling == 0
    assert authority.executable_new_capacity_rate_per_minute == 0
    assert authority.writer_epoch == successor.writer_epoch
    row = (await capacity_session.execute(select(CapacityExecutionEpoch))).scalar_one()
    assert row.state == "drain-only"
    assert row.current_writer_epoch == successor.writer_epoch
    with pytest.raises(StaleWriterError):
        await fixture.store.load_allocation_input(capacity_session, fixture.writer)

    replacement = await fixture.store.register_writer(
        capacity_session,
        AUTHORITY_ID,
        expected_epoch=successor.writer_epoch,
    )
    authority = await fixture.store.execution_authority(capacity_session)
    assert authority is not None
    assert authority.execution_state == "drain-only"
    assert authority.writer_epoch == replacement.writer_epoch
    await capacity_session.refresh(row)
    assert row.current_writer_epoch == replacement.writer_epoch
    with pytest.raises(StaleWriterError):
        await fixture.store.load_allocation_input(capacity_session, successor)


async def test_dry_run_executor_registration_is_not_executable_provenance(
    capacity_session: AsyncSession,
) -> None:
    """A v1 rehearsal registration must never authorize a v2 activation."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=730),
    )
    activation = ExecutionActivationV2(
        authority_incarnation=AUTHORITY_ID,
        expected_writer_epoch=fixture.writer.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
    )

    with pytest.raises(DBAPIError, match="executable executor evidence"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutionEpoch).values(
                    state="active",
                    effective_ceiling=1,
                    effective_rate_per_minute=1,
                    activation_actor="forged-operator",
                    activation_idempotency_key=UUID(int=732),
                    activation_request_digest="9" * 64,
                    activated_at=datetime.now(UTC),
                )
            )

    with pytest.raises(ExecutionConflictError, match="executable executor"):
        await fixture.store.activate_execution_epoch(
            capacity_session,
            activation,
            actor="activation-operator",
            idempotency_key=UUID(int=731),
        )


async def test_activation_is_atomic_and_exact_replay(
    capacity_session: AsyncSession,
) -> None:
    """Activation must update the epoch and singleton as one exact transaction."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=725),
    )
    activation = ExecutionActivationV2(
        authority_incarnation=AUTHORITY_ID,
        expected_writer_epoch=fixture.writer.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
    )
    await _register_execution_executors(capacity_session, fixture, prepared)
    with pytest.raises(DBAPIError, match="append-only"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutionExecutor).values(local_authority_sha256="9" * 64)
            )
    first = await fixture.store.activate_execution_epoch(
        capacity_session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=726),
    )
    replay = await fixture.store.activate_execution_epoch(
        capacity_session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=726),
    )

    assert replay == first
    assert first.execution_state == "active"
    assert first.executable_new_capacity_ceiling == 1
    authority = await fixture.store.execution_authority(capacity_session)
    assert authority == first
    row = (await capacity_session.execute(select(CapacityExecutionEpoch))).scalar_one()
    assert row.state == "active"
    assert row.effective_ceiling == 1
    singleton = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert singleton.increase_freeze is False

    with pytest.raises(DBAPIError, match="without transition"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutionEpoch).values(activation_actor="forged-operator")
            )

    changed = activation.model_copy(update={"executable_new_capacity_ceiling": 2})
    with pytest.raises(IdempotencyConflictError):
        await fixture.store.activate_execution_epoch(
            capacity_session,
            changed,
            actor="activation-operator",
            idempotency_key=UUID(int=726),
        )

    with pytest.raises(AuthorityRecoveryError, match="owner policy"):
        await CapacityManagementStore().execution_authority(capacity_session)


async def test_activation_replay_revalidates_policy_and_current_writer_fence(
    capacity_session: AsyncSession,
) -> None:
    """An old activation idempotency key cannot survive policy or writer drift."""

    fixture, _, activation, _ = await _activate_fixture(capacity_session)
    drifted_policy = _policy().model_copy(update={"rollback_evidence_sha256": "7" * 64})
    drifted_store = CapacityManagementStore(execution_policy=drifted_policy)
    with pytest.raises(ExecutionConflictError, match="owner policy"):
        await drifted_store.activate_execution_epoch(
            capacity_session,
            activation,
            actor="activation-operator",
            idempotency_key=UUID(int=781),
        )

    await fixture.store.register_writer(
        capacity_session,
        AUTHORITY_ID,
        expected_epoch=fixture.writer.writer_epoch,
    )
    with pytest.raises(ExecutionConflictError, match="exact active"):
        await fixture.store.activate_execution_epoch(
            capacity_session,
            activation,
            actor="activation-operator",
            idempotency_key=UUID(int=781),
        )


async def test_database_activation_rejects_executor_authority_hash_drift(
    capacity_session: AsyncSession,
) -> None:
    """Matching names cannot substitute for signed executor authority bindings."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=783),
    )
    for index, binding in enumerate(fixture.request.executors, start=1):
        capacity_session.add(
            CapacityExecutionExecutor(
                execution_epoch=prepared.execution_epoch,
                execution_manifest_sha256=prepared.execution_manifest_sha256,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                pool_id=binding.pool_id,
                pool_generation=binding.pool_generation,
                signing_key_id=f"{binding.pool_id}-key",
                signing_key_sha256=str(index) * 64,
                local_authority_sha256=str(index + 2) * 64,
                controller_authority_sha256=str(index + 4) * 64,
                actor="forged-installer",
                idempotency_key=UUID(int=783 + index),
                registration_digest=str(index + 6) * 64,
                registration_payload={},
            )
        )
    await capacity_session.flush()

    with pytest.raises(DBAPIError, match="executable executor evidence"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutionEpoch).values(
                    state="active",
                    effective_ceiling=1,
                    effective_rate_per_minute=1,
                    activation_actor="forged-operator",
                    activation_idempotency_key=UUID(int=786),
                    activation_request_digest="9" * 64,
                    activated_at=datetime.now(UTC),
                )
            )


async def test_executor_registration_rejects_a_second_key_for_the_same_pool(
    capacity_session: AsyncSession,
) -> None:
    """A duplicate pool registration must be a domain conflict, not a raw DB error."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=787),
    )
    await _register_execution_executors(capacity_session, fixture, prepared)
    binding = fixture.request.executors[0]

    with pytest.raises(ExecutionConflictError, match="already registered"):
        await fixture.store.register_execution_executor(
            capacity_session,
            ExecutableExecutorRegistrationV2(
                execution=prepared,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                pool_id=binding.pool_id,
                pool_generation=binding.pool_generation,
                signing_key_id=f"{binding.pool_id}-key",
                signing_key_sha256=binding.signing_key_sha256,
                local_authority_sha256=binding.local_authority_sha256,
                controller_authority_sha256=binding.controller_authority_sha256,
            ),
            actor="executor-installer",
            idempotency_key=UUID(int=788),
        )


async def test_database_rejects_executor_bound_to_another_manifest(
    capacity_session: AsyncSession,
) -> None:
    """A mismatched manifest cannot permanently occupy an epoch's pool slot."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=789),
    )
    binding = fixture.request.executors[0]

    with pytest.raises(DBAPIError, match="epoch_manifest"):
        async with capacity_session.begin_nested():
            capacity_session.add(
                CapacityExecutionExecutor(
                    execution_epoch=prepared.execution_epoch,
                    execution_manifest_sha256="9" * 64,
                    executor_id=binding.executor_id,
                    executor_incarnation=binding.executor_incarnation,
                    pool_id=binding.pool_id,
                    pool_generation=binding.pool_generation,
                    signing_key_id=f"{binding.pool_id}-key",
                    signing_key_sha256=binding.signing_key_sha256,
                    local_authority_sha256=binding.local_authority_sha256,
                    controller_authority_sha256=binding.controller_authority_sha256,
                    actor="forged-installer",
                    idempotency_key=UUID(int=790),
                    registration_digest="8" * 64,
                    registration_payload={},
                )
            )
            await capacity_session.flush()


async def test_database_activation_ignores_temporary_executor_table_shadow(
    capacity_session: AsyncSession,
) -> None:
    """Session-local relations cannot replace durable executor evidence."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=791),
    )
    fake_rows = [
        {
            "execution_epoch": prepared.execution_epoch,
            "execution_manifest_sha256": prepared.execution_manifest_sha256,
            "pool_id": binding.pool_id,
            "executor_id": binding.executor_id,
            "executor_incarnation": binding.executor_incarnation,
            "pool_generation": binding.pool_generation,
            "signing_key_sha256": binding.signing_key_sha256,
            "local_authority_sha256": binding.local_authority_sha256,
            "controller_authority_sha256": binding.controller_authority_sha256,
        }
        for binding in fixture.request.executors
    ]

    with pytest.raises(DBAPIError, match="executable executor evidence"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                text(
                    "CREATE TEMPORARY TABLE capacity_execution_executors ("
                    "execution_epoch bigint NOT NULL, "
                    "execution_manifest_sha256 text NOT NULL, "
                    "pool_id text NOT NULL, executor_id text NOT NULL, "
                    "executor_incarnation uuid NOT NULL, pool_generation bigint NOT NULL, "
                    "signing_key_sha256 text NOT NULL, local_authority_sha256 text NOT NULL, "
                    "controller_authority_sha256 text NOT NULL) ON COMMIT DROP"
                )
            )
            await capacity_session.execute(
                text(
                    "INSERT INTO capacity_execution_executors ("
                    "execution_epoch, execution_manifest_sha256, pool_id, executor_id, "
                    "executor_incarnation, pool_generation, signing_key_sha256, "
                    "local_authority_sha256, controller_authority_sha256) VALUES ("
                    ":execution_epoch, :execution_manifest_sha256, :pool_id, :executor_id, "
                    ":executor_incarnation, :pool_generation, :signing_key_sha256, "
                    ":local_authority_sha256, :controller_authority_sha256)"
                ),
                fake_rows,
            )
            now = datetime.now(UTC)
            await capacity_session.execute(
                update(CapacityExecutionEpoch).values(
                    state="active",
                    effective_ceiling=1,
                    effective_rate_per_minute=1,
                    activation_actor="forged-operator",
                    activation_idempotency_key=UUID(int=792),
                    activation_request_digest="9" * 64,
                    activated_at=now,
                )
            )
            await capacity_session.execute(
                update(CapacityAuthorityState).values(
                    execution_state="active",
                    executable_new_capacity_ceiling=1,
                )
            )


async def test_recovery_rejects_corrupted_executor_authority_hashes(
    capacity_session: AsyncSession,
) -> None:
    """Recovery must verify more than the presence of both pool names."""

    fixture, _, _, _ = await _activate_fixture(capacity_session)
    await capacity_session.execute(
        text("ALTER TABLE capacity_execution_executors DISABLE TRIGGER USER")
    )
    try:
        await capacity_session.execute(
            update(CapacityExecutionExecutor)
            .where(CapacityExecutionExecutor.pool_id == "gb10")
            .values(controller_authority_sha256="9" * 64)
        )
    finally:
        await capacity_session.execute(
            text("ALTER TABLE capacity_execution_executors ENABLE TRIGGER USER")
        )

    with pytest.raises(AuthorityRecoveryError, match="executor binding"):
        await fixture.store.execution_authority(capacity_session)


async def test_database_rejects_detaching_an_active_authority_to_shadow(
    capacity_session: AsyncSession,
) -> None:
    """Nullable shadow bindings cannot bypass the execution transition graph."""

    await _activate_fixture(capacity_session)
    with pytest.raises(DBAPIError, match="authority execution transition"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityAuthorityState).values(
                    execution_epoch=0,
                    execution_state="shadow",
                    execution_manifest_sha256=None,
                    executable_new_capacity_ceiling=0,
                )
            )
