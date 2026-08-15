"""Shared durable execution-authority builders for capacity integration tests."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import delete, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableExecutorRegistrationV2,
    ExecutionContextV2,
    ExecutionPreparationPolicyV2,
    ExecutionPreparationV2,
    LegacyWriterFenceV2,
    PoolControllerAuthorityV2,
    PreparedExecutorBindingV2,
    SubjectExecutionAcknowledgementV2,
)
from loom_capacity_manager.grant_contracts import DryRunExecutorRegistrationV1
from loom_capacity_manager.grant_store import CapacityGrantStore
from loom_capacity_manager.models import (
    Base,
    CapacityAuthorityState,
    CapacityCandidate,
)
from loom_capacity_manager.ownership import OwnershipKeyring, public_key_fingerprint
from loom_capacity_manager.store import CapacityManagementStore, WriterFence
from tests.capacity_fixtures import (
    AUTHORITY_ID,
    configuration_activation,
    fleet_manifest,
    subject_configuration,
)

EXECUTOR_INCARNATIONS = {
    "gb10": UUID("00000000-0000-4000-8000-000000000711"),
    "oldlab": UUID("00000000-0000-4000-8000-000000000712"),
}
EXECUTOR_KEYS = {
    "gb10": Ed25519PrivateKey.from_private_bytes(b"\x11" * 32),
    "oldlab": Ed25519PrivateKey.from_private_bytes(b"\x12" * 32),
}
CONTROLLER_DIGESTS = {"gb10": "c" * 64, "oldlab": "d" * 64}
TRUSTED_RELEASE = "e" * 64


@dataclass(frozen=True, slots=True)
class PreparedExecutionFixture:
    store: CapacityManagementStore
    writer: WriterFence
    request: ExecutionPreparationV2


def source_candidate() -> CandidateBindingV2:
    return CandidateBindingV2(
        algorithm="source-sha256",
        identity="1" * 64,
        publication_sha256="2" * 64,
    )


def protected_candidate() -> CandidateBindingV2:
    return CandidateBindingV2(
        algorithm="git-sha1",
        identity="1" * 40,
        publication_sha256="2" * 64,
    )


def execution_acknowledgement(
    candidate: CandidateBindingV2 | None = None,
) -> SubjectExecutionAcknowledgementV2:
    subject = subject_configuration(fleet_manifest())
    return SubjectExecutionAcknowledgementV2(
        subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation,
        configuration_generation=subject.configuration_generation,
        deployment_generation=subject.deployment_generation,
        candidate=source_candidate() if candidate is None else candidate,
        reporter_incarnation=subject.demand_reporter_incarnation,
        protected_admission_sha256="3" * 64,
        legacy_writer_high_water=0,
        acknowledgement_sha256="4" * 64,
    )


def executor_binding(pool_id: str) -> PreparedExecutorBindingV2:
    private_key = EXECUTOR_KEYS[pool_id]
    return PreparedExecutorBindingV2(
        pool_id=pool_id,
        pool_generation=1,
        executor_id=f"{pool_id}-executor",
        executor_incarnation=EXECUTOR_INCARNATIONS[pool_id],
        signing_key_sha256=public_key_fingerprint(private_key.public_key()),
        local_authority_sha256=("a" if pool_id == "gb10" else "b") * 64,
        controller_authority_sha256=CONTROLLER_DIGESTS[pool_id],
    )


def execution_policy(
    candidate: CandidateBindingV2 | None = None,
) -> ExecutionPreparationPolicyV2:
    return ExecutionPreparationPolicyV2(
        trusted_fleet_release_sha256=TRUSTED_RELEASE,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
        executors=tuple(executor_binding(pool_id) for pool_id in ("gb10", "oldlab")),
        subject_acknowledgements=(execution_acknowledgement(candidate),),
        rollback_evidence_sha256="6" * 64,
        controller_authorities=tuple(
            PoolControllerAuthorityV2(
                pool_id=pool_id,
                controller_authority_sha256=CONTROLLER_DIGESTS[pool_id],
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


async def setup_execution(
    session: AsyncSession,
    *,
    execution_policy: ExecutionPreparationPolicyV2 | None = None,
    candidate: CandidateBindingV2 | None = None,
) -> PreparedExecutionFixture:
    await session.execute(
        text("ALTER TABLE capacity_execution_executors DISABLE TRIGGER USER")
    )
    await session.execute(
        text("ALTER TABLE capacity_execution_epochs DISABLE TRIGGER USER")
    )
    await session.execute(
        text(
            "ALTER TABLE capacity_authority_state DISABLE TRIGGER "
            "capacity_authority_execution_transition_guard"
        )
    )
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
    for table in reversed(Base.metadata.sorted_tables):
        if table.name != CapacityAuthorityState.__tablename__:
            await session.execute(delete(table))
    await session.execute(
        text(
            "ALTER TABLE capacity_authority_state ENABLE TRIGGER "
            "capacity_authority_execution_transition_guard"
        )
    )
    await session.execute(
        text("ALTER TABLE capacity_execution_epochs ENABLE TRIGGER USER")
    )
    await session.execute(
        text("ALTER TABLE capacity_execution_executors ENABLE TRIGGER USER")
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
    acknowledgement = execution_acknowledgement(candidate)
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
                for pool_id, private_key in EXECUTOR_KEYS.items()
            }
        )
    )
    executors: list[PreparedExecutorBindingV2] = []
    for index, pool_id in enumerate(("gb10", "oldlab"), start=1):
        public_key = EXECUTOR_KEYS[pool_id].public_key()
        fingerprint = public_key_fingerprint(public_key)
        registration = DryRunExecutorRegistrationV1(
            executor_id=f"{pool_id}-executor",
            executor_incarnation=EXECUTOR_INCARNATIONS[pool_id],
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
                controller_authority_sha256=CONTROLLER_DIGESTS[pool_id],
            )
        )

    request = ExecutionPreparationV2(
        authority_incarnation=AUTHORITY_ID,
        expected_writer_epoch=writer.writer_epoch,
        configuration_epoch=active.configuration_epoch,
        fleet_generation=fleet.fleet_generation,
        fleet_digest=fleet_proposal.digest,
        trusted_fleet_release_sha256=TRUSTED_RELEASE,
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
    return PreparedExecutionFixture(store=store, writer=writer, request=request)


async def register_execution_executors(
    session: AsyncSession,
    fixture: PreparedExecutionFixture,
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


__all__ = [
    "CONTROLLER_DIGESTS",
    "EXECUTOR_INCARNATIONS",
    "EXECUTOR_KEYS",
    "TRUSTED_RELEASE",
    "PreparedExecutionFixture",
    "execution_acknowledgement",
    "execution_policy",
    "executor_binding",
    "protected_candidate",
    "register_execution_executors",
    "setup_execution",
    "source_candidate",
]
