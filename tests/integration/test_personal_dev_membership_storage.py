"""PostgreSQL proof for mode migration and lease-fenced membership persistence."""

import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import CheckConstraint, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    DevInstance,
    DevLifecycleOperation,
    PersonalDevCandidate,
    Team,
    User,
)
from loom.personal_dev_environment import (
    PersonalDevAccessBinding,
    PersonalDevEnvironmentApplyRequest,
    PersonalDevEnvironmentDestroyRequest,
)
from loom.personal_dev_environment_store import (
    PersonalDevEnvironmentOperationFencedError,
    SqlAlchemyPersonalDevEnvironmentAuthority,
)
from loom.personal_dev_membership_checkpoint import PersonalDevMembershipEnvelopeV1
from loom_capacity_manager.contracts import canonical_digest
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    SubjectExecutionAcknowledgementV2,
)
from loom_capacity_manager.membership_contracts import (
    PersonalApplicationMembershipMutationV1,
    PersonalMembershipCheckpointV1,
)
from loom_capacity_manager.membership_outcomes import (
    PersonalMembershipOperationCommittedV1,
    parse_membership_operation_outcome,
)
from loom_capacity_manager.membership_subject_status import (
    PersonalMembershipReleasePendingV1,
    PersonalMembershipReleaseVerifiedV1,
    parse_membership_release_observation,
)
from tests.capacity_fixtures import development_projection
from tests.unit.test_capacity_manager_executable_allocator import execution_authority_fixture
from tests.unit.test_personal_dev_membership_checkpoint import membership_response
from tests.unit.test_personal_dev_membership_client import _outcome_payload
from tests.unit.test_personal_dev_membership_subject_client import (
    _response as membership_release_response,
)


@pytest.mark.parametrize("historical_destroy_receipt", (False, True))
@pytest.mark.asyncio
async def test_shadow_ready_migrates_only_after_exact_membership_receipt(
    isolated_migration_postgres_url: str,
    historical_destroy_receipt: bool,
) -> None:
    # This test commits lifecycle rows across independent sessions. Keep those
    # durable receipts out of the suite's shared database and cleanup fixtures.
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    owner_id, team_id, candidate_id = uuid4(), uuid4(), uuid4()
    subject_id, incarnation, previous_operation_id = uuid4(), uuid4(), uuid4()
    reporter = uuid4()
    now = datetime.now(UTC)
    candidate_sha = "a" * 64
    environment_name = (
        "membership-history" if historical_destroy_receipt else "membership-storage"
    )
    try:
        async with sessions() as session:
            session.add(Team(id=team_id, name=f"membership-{team_id}"))
            session.add(
                User(
                    id=owner_id,
                    email=f"{owner_id}@example.test",
                    username=f"member-{owner_id}",
                    username_normalized=f"member-{owner_id}",
                    status="active",
                )
            )
            await session.commit()
            session.add(
                PersonalDevCandidate(
                    id=candidate_id,
                    owner_user_id=owner_id,
                    owner_team_id=team_id,
                    candidate_sha=candidate_sha,
                    source_sha256="b" * 64,
                    archive_sha256="c" * 64,
                    build_contract_sha256="d" * 64,
                    source_commit="e" * 40,
                    dirty=False,
                    manifest_json={"schema_version": 1},
                    object_bucket="artifacts",
                    object_key=(
                        f"personal-dev/sources/{team_id}/{owner_id}/{candidate_sha}/"
                        f"{'c' * 64}.tar"
                    ),
                    source_generation_id=candidate_id,
                    archive_size_bytes=1024,
                    status="ready",
                    image_manifest_digest="sha256:" + "1" * 64,
                    publication_json={"schema_version": 1},
                    publication_sha256="2" * 64,
                    artifact_state="retained",
                    ready_at=now,
                    created_at=now,
                    updated_at=now,
                )
            )
            await session.commit()
            session.add(
                DevInstance(
                    name=environment_name,
                    subject_id=subject_id,
                    subject_incarnation=incarnation,
                    owner_user_id=owner_id,
                    owner_team_id=team_id,
                    min_slots=0,
                    max_slots=2,
                    status="ready",
                    deployment_generation=1,
                    candidate_id=candidate_id,
                    candidate_sha=candidate_sha,
                    capacity_namespace=f"loom-dev-{environment_name}",
                    capacity_database=f"loom_dev_{environment_name.replace('-', '_')}",
                    operation_epoch=1,
                    operation_id=previous_operation_id,
                    operation_step="complete",
                    accepted_capacity_mode="shadow-v1",
                    capacity_configuration_epoch=5,
                    capacity_configuration_sha256="3" * 64,
                    capacity_reporter_incarnation=reporter,
                    capacity_reporter_token_sha256="4" * 64,
                    local_activation_sha256="5" * 64,
                    protected_admission_sha256="6" * 64,
                    capacity_agent_installation_sha256="7" * 64,
                    capacity_supported_pool_ids=["gb10", "oldlab"],
                    capacity_supported_architectures=["arm64", "x86_64"],
                    ready_at=now,
                    created_at=now,
                    updated_at=now,
                )
            )
            await session.commit()
            with pytest.raises(DBAPIError):
                await session.execute(
                    text(
                        "UPDATE dev_instances SET accepted_capacity_mode = 'membership-v1', "
                        "capacity_configuration_epoch = NULL, "
                        "capacity_configuration_sha256 = NULL WHERE name = :name"
                    ),
                    {"name": environment_name},
                )
            await session.rollback()
            with pytest.raises(DBAPIError):
                await session.execute(
                    text(
                        "UPDATE dev_instances SET accepted_capacity_mode = 'membership-v1', "
                        "accepted_capacity_membership_checkpoint = "
                        "jsonb_build_object('namespace_id', "
                        "'00000000-0000-0000-0000-000000000991'), "
                        "capacity_configuration_epoch = NULL, "
                        "capacity_configuration_sha256 = NULL WHERE name = :name"
                    ),
                    {"name": environment_name},
                )
            await session.rollback()

        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(session)
            idempotency_key = uuid4()
            reservation = await authority.apply(
                PersonalDevEnvironmentApplyRequest(
                    name=environment_name,
                    owner_user_id=owner_id,
                    owner_team_id=team_id,
                    candidate_id=candidate_id,
                    candidate_sha=candidate_sha,
                    min_slots=0,
                    max_slots=2,
                    expected_operation_epoch=1,
                    idempotency_key=idempotency_key,
                ),
                access_binding=PersonalDevAccessBinding(
                    auth_kind="bearer", credential_hash=b"x" * 32
                ),
                capacity_mode="membership-v1",
                now=now,
            )
            assert reservation.operation.kind == "capacity"
            assert reservation.operation.capacity_mode == "membership-v1"
            assert reservation.environment.accepted_capacity_mode == "shadow-v1"
            assert reservation.environment.capacity_configuration_epoch == 5
            with pytest.raises(DBAPIError):
                await session.execute(
                    text(
                        "UPDATE dev_lifecycle_operations SET "
                        "checkpoint = 'capacity_projection_pending', "
                        "capacity_membership_envelope = '{}'::jsonb WHERE id = :operation_id"
                    ),
                    {"operation_id": reservation.operation.id},
                )
            await session.rollback()

            claim = await authority.claim_next_reconciliation(
                reconciler_id="membership-reconciler",
                now=now,
                lease_seconds=60,
            )
            assert claim is not None
            execution = execution_authority_fixture()
            projection = development_projection(
                expected_configuration_epoch=execution.configuration_epoch,
                operation_kind="capacity",
                operation_id=reservation.operation.id,
                operation_epoch=reservation.operation.operation_epoch,
                environment_name=reservation.environment.name,
                subject_id=subject_id,
                subject_incarnation=incarnation,
                owner_id=owner_id,
                min_slots=0,
                max_slots=2,
                candidate_generation=1,
                deployment_generation=1,
                configuration_generation=reservation.operation.operation_epoch,
                demand_reporter_incarnation=reporter,
            ).model_copy(
                update={
                    "candidate_sha256": candidate_sha,
                    "candidate_publication_sha256": "2" * 64,
                    "demand_reporter_token_sha256": "4" * 64,
                    "local_activation_sha256": "5" * 64,
                    "protected_admission_sha256": "6" * 64,
                    "capacity_agent_installation_sha256": "7" * 64,
                }
            )
            acknowledgement = SubjectExecutionAcknowledgementV2(
                subject_id=subject_id,
                subject_incarnation=incarnation,
                configuration_generation=reservation.operation.operation_epoch,
                deployment_generation=1,
                candidate=CandidateBindingV2(
                    algorithm="source-sha256",
                    identity=candidate_sha,
                    publication_sha256="2" * 64,
                ),
                reporter_incarnation=reporter,
                protected_admission_sha256="6" * 64,
                legacy_writer_high_water=0,
                acknowledgement_sha256="8" * 64,
            )
            checkpoint = PersonalMembershipCheckpointV1(
                execution=execution,
                namespace_id=UUID(int=991),
                revision=0,
                head_sha256="0" * 64,
            )
            request = PersonalApplicationMembershipMutationV1(
                execution=execution,
                namespace_id=checkpoint.namespace_id,
                expected_revision=0,
                projection=projection,
                acknowledgement=acknowledgement,
            )
            envelope = PersonalDevMembershipEnvelopeV1(
                management_principal_id="personal-membership-manager",
                idempotency_key=idempotency_key,
                expected_checkpoint=checkpoint,
                request=request,
                request_sha256=canonical_digest(request),
                observation={
                    "operation_id": reservation.operation.id,
                    "operation_epoch": reservation.operation.operation_epoch,
                    "attempt_id": claim.attempt.id,
                    "observation_lease_epoch": claim.attempt.lease_epoch,
                    "observed_at": now,
                    "execution": execution,
                    "local_activation_sha256": "5" * 64,
                    "capacity_agent_installation_sha256": "7" * 64,
                    "acknowledgement": acknowledgement,
                },
            )
            pending = await authority.prepare_capacity_membership(
                operation_id=reservation.operation.id,
                operation_epoch=reservation.operation.operation_epoch,
                attempt_id=claim.attempt.id,
                reconciler_id="membership-reconciler",
                lease_epoch=claim.attempt.lease_epoch,
                envelope=envelope,
                now=now,
            )
            assert pending.environment.accepted_capacity_mode == "shadow-v1"
            assert pending.environment.capacity_configuration_epoch == 5
            with pytest.raises(DBAPIError):
                await session.execute(
                    text(
                        "UPDATE dev_lifecycle_operations SET "
                        "checkpoint = 'capacity_projected' WHERE id = :operation_id"
                    ),
                    {"operation_id": reservation.operation.id},
                )
            await session.rollback()

            takeover = await authority.claim_next_reconciliation(
                reconciler_id="takeover-reconciler",
                now=now + timedelta(seconds=1),
                lease_seconds=60,
            )
            assert takeover is not None
            assert takeover.attempt.lease_epoch > claim.attempt.lease_epoch
            assert (
                takeover.operation.capacity_membership_envelope.observation.observation_lease_epoch
                == claim.attempt.lease_epoch
            )
            refreshed_checkpoint = checkpoint.model_copy(
                update={"revision": 2, "head_sha256": "9" * 64}
            )
            refreshed = await authority.refresh_capacity_membership(
                operation_id=reservation.operation.id,
                operation_epoch=reservation.operation.operation_epoch,
                attempt_id=takeover.attempt.id,
                reconciler_id="takeover-reconciler",
                lease_epoch=takeover.attempt.lease_epoch,
                checkpoint=refreshed_checkpoint,
                now=now + timedelta(seconds=1),
            )
            assert refreshed.operation.capacity_membership_envelope.observation == (
                envelope.observation
            )
            receipt_claim = await authority.claim_next_reconciliation(
                reconciler_id="receipt-reconciler",
                now=now + timedelta(seconds=2),
                lease_seconds=60,
            )
            assert receipt_claim is not None
            refreshed_request = receipt_claim.operation.capacity_membership_envelope.request
            response = membership_response(
                refreshed_request,
                key=idempotency_key,
                previous_head="9" * 64,
            )
            with pytest.raises(PersonalDevEnvironmentOperationFencedError):
                await authority.record_capacity_membership(
                    operation_id=reservation.operation.id,
                    operation_epoch=reservation.operation.operation_epoch,
                    attempt_id=claim.attempt.id,
                    reconciler_id="membership-reconciler",
                    lease_epoch=claim.attempt.lease_epoch,
                    response=response,
                    now=now + timedelta(seconds=1),
                )
            accepted = await authority.record_capacity_membership(
                operation_id=reservation.operation.id,
                operation_epoch=reservation.operation.operation_epoch,
                attempt_id=receipt_claim.attempt.id,
                reconciler_id="receipt-reconciler",
                lease_epoch=receipt_claim.attempt.lease_epoch,
                response=response,
                now=now + timedelta(seconds=2),
            )
            assert accepted.operation.state == "succeeded"
            assert accepted.environment.accepted_capacity_mode == "membership-v1"
            assert accepted.environment.capacity_configuration_epoch is None
            assert accepted.environment.accepted_capacity_membership_checkpoint == (
                response.checkpoint
            )

            with pytest.raises(DBAPIError):
                await session.execute(
                    text(
                        "UPDATE dev_instances SET capacity_configuration_epoch = 9, "
                        "capacity_configuration_sha256 = :digest WHERE name = :name"
                    ),
                    {"digest": "9" * 64, "name": environment_name},
                )
            await session.rollback()
            stored = (
                await session.execute(
                    select(DevInstance).where(DevInstance.name == environment_name)
                )
            ).scalar_one()
            assert stored.accepted_capacity_mode == "membership-v1"

            destroy_key = uuid4()
            destroy = await authority.destroy(
                PersonalDevEnvironmentDestroyRequest(
                    name=environment_name,
                    owner_user_id=owner_id,
                    owner_team_id=team_id,
                    expected_operation_epoch=2,
                    idempotency_key=destroy_key,
                ),
                access_binding=PersonalDevAccessBinding(
                    auth_kind="bearer", credential_hash=b"x" * 32
                ),
                capacity_mode="membership-v1",
                now=now + timedelta(seconds=3),
            )
            destroy_claim = await authority.claim_next_reconciliation(
                reconciler_id="destroy-reconciler",
                now=now + timedelta(seconds=3),
                lease_seconds=60,
            )
            assert destroy_claim is not None
            destroy_projection = refreshed_request.projection.model_copy(
                update={
                    "operation_kind": "destroy",
                    "operation_id": destroy.operation.id,
                    "operation_epoch": destroy.operation.operation_epoch,
                    "configuration_generation": destroy.operation.operation_epoch,
                    "min_slots": 0,
                    "max_slots": 0,
                }
            )
            destroy_acknowledgement = acknowledgement.model_copy(
                update={"configuration_generation": destroy.operation.operation_epoch}
            )
            destroy_request = PersonalApplicationMembershipMutationV1(
                execution=execution,
                namespace_id=response.checkpoint.namespace_id,
                expected_revision=response.checkpoint.revision,
                projection=destroy_projection,
                acknowledgement=destroy_acknowledgement,
            )
            destroy_envelope = PersonalDevMembershipEnvelopeV1(
                management_principal_id="personal-membership-manager",
                idempotency_key=destroy_key,
                expected_checkpoint=response.checkpoint,
                request=destroy_request,
                request_sha256=canonical_digest(destroy_request),
                observation={
                    "operation_id": destroy.operation.id,
                    "operation_epoch": destroy.operation.operation_epoch,
                    "attempt_id": destroy_claim.attempt.id,
                    "observation_lease_epoch": destroy_claim.attempt.lease_epoch,
                    "observed_at": now + timedelta(seconds=3),
                    "execution": execution,
                    "local_activation_sha256": "5" * 64,
                    "capacity_agent_installation_sha256": "7" * 64,
                    "acknowledgement": destroy_acknowledgement,
                },
            )
            await authority.prepare_capacity_membership(
                operation_id=destroy.operation.id,
                operation_epoch=destroy.operation.operation_epoch,
                attempt_id=destroy_claim.attempt.id,
                reconciler_id="destroy-reconciler",
                lease_epoch=destroy_claim.attempt.lease_epoch,
                envelope=destroy_envelope,
                now=now + timedelta(seconds=3),
            )
            destroy_receipt_claim = await authority.claim_next_reconciliation(
                reconciler_id="destroy-receipt-reconciler",
                now=now + timedelta(seconds=4),
                lease_seconds=60,
            )
            assert destroy_receipt_claim is not None
            destroy_response = membership_response(
                destroy_request,
                key=destroy_key,
                previous_head=response.checkpoint.head_sha256,
            )
            if historical_destroy_receipt:
                _, outcome_payload = _outcome_payload(destroy_envelope, "committed")
                outcome_payload["receipt"] = destroy_response.model_dump(mode="json")
                historical_outcome = parse_membership_operation_outcome(
                    json.dumps(outcome_payload)
                )
                assert isinstance(
                    historical_outcome, PersonalMembershipOperationCommittedV1
                )
                cleanup = await authority.record_capacity_membership_outcome(
                    operation_id=destroy.operation.id,
                    operation_epoch=destroy.operation.operation_epoch,
                    attempt_id=destroy_receipt_claim.attempt.id,
                    reconciler_id="destroy-receipt-reconciler",
                    lease_epoch=destroy_receipt_claim.attempt.lease_epoch,
                    outcome=historical_outcome,
                    now=now + timedelta(seconds=4),
                )
            else:
                cleanup = await authority.record_capacity_membership(
                    operation_id=destroy.operation.id,
                    operation_epoch=destroy.operation.operation_epoch,
                    attempt_id=destroy_receipt_claim.attempt.id,
                    reconciler_id="destroy-receipt-reconciler",
                    lease_epoch=destroy_receipt_claim.attempt.lease_epoch,
                    response=destroy_response,
                    now=now + timedelta(seconds=4),
                )
            assert cleanup.operation.state == "running"
            release_source_checkpoint = (
                "membership_outcome_resolved"
                if historical_destroy_receipt
                else "cleanup_pending"
            )
            assert cleanup.operation.checkpoint == release_source_checkpoint
            assert cleanup.environment.status == "deleting"
            cleanup_claim = await authority.claim_next_reconciliation(
                reconciler_id="cleanup-reconciler",
                now=now + timedelta(seconds=5),
                lease_seconds=60,
            )
            assert cleanup_claim is not None
            with pytest.raises(PersonalDevEnvironmentOperationFencedError):
                await authority.advance_destroy_checkpoint(
                    operation_id=destroy.operation.id,
                    operation_epoch=destroy.operation.operation_epoch,
                    attempt_id=cleanup_claim.attempt.id,
                    reconciler_id="cleanup-reconciler",
                    lease_epoch=cleanup_claim.attempt.lease_epoch,
                    expected_checkpoint=release_source_checkpoint,
                    checkpoint="local_authority_sealed",
                    now=now + timedelta(seconds=5),
                )
            saved_envelope = cleanup.operation.capacity_membership_envelope
            assert saved_envelope is not None
            release_payload_envelope = saved_envelope
            if saved_envelope.result is None:
                assert isinstance(
                    saved_envelope.historical_outcome,
                    PersonalMembershipOperationCommittedV1,
                )
                release_payload_envelope = saved_envelope.model_copy(
                    update={
                        "result": saved_envelope.historical_outcome.receipt,
                        "historical_outcome": None,
                    }
                )
            _, pending_payload = membership_release_response(
                release_payload_envelope, "pending"
            )
            pending_release = parse_membership_release_observation(
                json.dumps(pending_payload)
            )
            assert isinstance(pending_release, PersonalMembershipReleasePendingV1)
            with pytest.raises(TypeError):
                await authority.record_capacity_membership_release(
                    operation_id=destroy.operation.id,
                    operation_epoch=destroy.operation.operation_epoch,
                    attempt_id=cleanup_claim.attempt.id,
                    reconciler_id="cleanup-reconciler",
                    lease_epoch=cleanup_claim.attempt.lease_epoch,
                    release=pending_release,
                    now=now + timedelta(seconds=5),
                )

            _, verified_payload = membership_release_response(
                release_payload_envelope, "verified"
            )
            verified_release = parse_membership_release_observation(
                json.dumps(verified_payload)
            )
            assert isinstance(verified_release, PersonalMembershipReleaseVerifiedV1)
            wrong_receipt = destroy_response.model_copy(
                update={
                    "checkpoint": destroy_response.checkpoint.model_copy(
                        update={"namespace_id": UUID(int=992)}
                    )
                }
            )
            wrong_release = verified_release.model_copy(
                update={"membership_receipt": wrong_receipt}
            )
            with pytest.raises(PersonalDevEnvironmentOperationFencedError):
                await authority.record_capacity_membership_release(
                    operation_id=destroy.operation.id,
                    operation_epoch=destroy.operation.operation_epoch,
                    attempt_id=cleanup_claim.attempt.id,
                    reconciler_id="cleanup-reconciler",
                    lease_epoch=cleanup_claim.attempt.lease_epoch,
                    release=wrong_release,
                    now=now + timedelta(seconds=5),
                )

            release_claim = await authority.claim_next_reconciliation(
                reconciler_id="release-reconciler",
                now=now + timedelta(seconds=66),
                lease_seconds=60,
            )
            assert release_claim is not None
            assert release_claim.operation.id == destroy.operation.id
            with pytest.raises(PersonalDevEnvironmentOperationFencedError):
                await authority.record_capacity_membership_release(
                    operation_id=destroy.operation.id,
                    operation_epoch=destroy.operation.operation_epoch,
                    attempt_id=cleanup_claim.attempt.id,
                    reconciler_id="cleanup-reconciler",
                    lease_epoch=cleanup_claim.attempt.lease_epoch,
                    release=verified_release,
                    now=now + timedelta(seconds=66),
                )
            released = await authority.record_capacity_membership_release(
                operation_id=destroy.operation.id,
                operation_epoch=destroy.operation.operation_epoch,
                attempt_id=release_claim.attempt.id,
                reconciler_id="release-reconciler",
                lease_epoch=release_claim.attempt.lease_epoch,
                release=verified_release,
                now=now + timedelta(seconds=66),
            )
            assert released.operation.checkpoint == "release_verified"
            assert released.operation.capacity_membership_envelope is not None
            assert released.operation.capacity_membership_envelope.release == verified_release
            if historical_destroy_receipt:
                assert released.operation.capacity_membership_envelope.result is None
                assert (
                    released.operation.capacity_membership_envelope.historical_outcome
                    == historical_outcome
                )
            else:
                assert released.operation.capacity_membership_envelope.result == destroy_response
            with pytest.raises(DBAPIError):
                await session.execute(
                    text(
                        "UPDATE dev_lifecycle_operations SET "
                        "capacity_membership_envelope = jsonb_set("
                        "capacity_membership_envelope, '{release}', 'null'::jsonb) "
                        "WHERE id = :operation_id"
                    ),
                    {"operation_id": destroy.operation.id},
                )
            await session.rollback()
            for required_field in ("release", "historical_outcome"):
                with pytest.raises(DBAPIError):
                    await session.execute(
                        text(
                            "UPDATE dev_lifecycle_operations SET "
                            "capacity_membership_envelope = "
                            "capacity_membership_envelope - CAST(:field AS text) "
                            "WHERE id = :operation_id"
                        ),
                        {
                            "field": required_field,
                            "operation_id": destroy.operation.id,
                        },
                    )
                await session.rollback()
            if historical_destroy_receipt:
                with pytest.raises(DBAPIError):
                    await session.execute(
                        text(
                            "UPDATE dev_lifecycle_operations SET "
                            "capacity_membership_envelope = jsonb_set("
                            "capacity_membership_envelope, "
                            "'{historical_outcome,outcome}', "
                            "'\"terminal-not-committed\"'::jsonb) "
                            "WHERE id = :operation_id"
                        ),
                        {"operation_id": destroy.operation.id},
                    )
                await session.rollback()

            seal_claim = await authority.claim_next_reconciliation(
                reconciler_id="seal-reconciler",
                now=now + timedelta(seconds=67),
                lease_seconds=60,
            )
            assert seal_claim is not None
            sealed = await authority.advance_destroy_checkpoint(
                operation_id=destroy.operation.id,
                operation_epoch=destroy.operation.operation_epoch,
                attempt_id=seal_claim.attempt.id,
                reconciler_id="seal-reconciler",
                lease_epoch=seal_claim.attempt.lease_epoch,
                expected_checkpoint="release_verified",
                checkpoint="local_authority_sealed",
                now=now + timedelta(seconds=67),
            )
            assert sealed.operation.checkpoint == "local_authority_sealed"
            current = sealed
            for seconds, expected_checkpoint, checkpoint in (
                (68, "local_authority_sealed", "namespace_deleted"),
                (69, "namespace_deleted", "database_deleted"),
                (70, "database_deleted", "buckets_deleted"),
                (71, "buckets_deleted", "tenant_deleted"),
                (72, "tenant_deleted", "complete"),
            ):
                cleanup_step_claim = await authority.claim_next_reconciliation(
                    reconciler_id=f"cleanup-{seconds}",
                    now=now + timedelta(seconds=seconds),
                    lease_seconds=60,
                )
                assert cleanup_step_claim is not None
                assert cleanup_step_claim.operation.id == destroy.operation.id
                current = await authority.advance_destroy_checkpoint(
                    operation_id=destroy.operation.id,
                    operation_epoch=destroy.operation.operation_epoch,
                    attempt_id=cleanup_step_claim.attempt.id,
                    reconciler_id=f"cleanup-{seconds}",
                    lease_epoch=cleanup_step_claim.attempt.lease_epoch,
                    expected_checkpoint=expected_checkpoint,
                    checkpoint=checkpoint,
                    now=now + timedelta(seconds=seconds),
                )
            assert current.operation.state == "succeeded"
            assert current.operation.checkpoint == "complete"
            assert current.environment.status == "deleted"
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    ("checkpoint", "envelope"),
    [
        pytest.param("candidate_build", None, id="pre-membership-success"),
        pytest.param(
            "membership_outcome_resolved",
            '{"result":null,"historical_outcome":{}}',
            id="historical-outcome-without-current-result",
        ),
    ],
)
@pytest.mark.asyncio
async def test_orm_membership_completion_check_requires_current_result(
    postgres_url: str,
    checkpoint: str,
    envelope: str | None,
) -> None:
    completion_check = next(
        constraint
        for constraint in DevLifecycleOperation.__table__.constraints
        if isinstance(constraint, CheckConstraint)
        and constraint.name == "dev_lifecycle_operations_capacity_completion_check"
    )
    engine = create_async_engine(postgres_url)
    try:
        async with engine.connect() as connection:
            await connection.execute(
                text(
                    "CREATE TEMPORARY TABLE orm_membership_completion_probe ("
                    "capacity_mode text, checkpoint text, kind text, state text, "
                    "readiness_evidence_sha256 text, "
                    "activation_acknowledgement_sha256 text, "
                    "local_activation_sha256 text, "
                    "capacity_expected_configuration_epoch bigint, "
                    "capacity_projection_request_sha256 text, "
                    "capacity_configuration_epoch bigint, "
                    "capacity_configuration_sha256 text, "
                    "capacity_reporter_incarnation uuid, "
                    "capacity_reporter_token_sha256 text, "
                    "protected_admission_sha256 text, "
                    "capacity_agent_installation_sha256 text, "
                    "capacity_supported_pool_ids jsonb, "
                    "capacity_supported_architectures jsonb, "
                    "capacity_membership_envelope jsonb, "
                    f"CHECK ({completion_check.sqltext}))"
                )
            )
            await connection.commit()
            with pytest.raises(DBAPIError):
                await connection.execute(
                    text(
                        "INSERT INTO orm_membership_completion_probe "
                        "(capacity_mode, checkpoint, kind, state, "
                        "capacity_membership_envelope) VALUES "
                        "('membership-v1', :checkpoint, 'capacity', 'succeeded', "
                        "CAST(:envelope AS jsonb))"
                    ),
                    {"checkpoint": checkpoint, "envelope": envelope},
                )
            await connection.rollback()
    finally:
        await engine.dispose()
