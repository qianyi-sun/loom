from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timedelta
from uuid import UUID, uuid4

import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import (
    TaskImageBuildContainmentAttestation,
    TaskImageBuildGrant,
    TaskImageBuildGrantEvent,
    TaskImageBuildProjection,
    TaskImageBuildProjectionEvent,
    TaskImageBuildSessionGeneration,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImageMaterializationOperationEvent,
    TaskImagePublicationCandidate,
    TaskImagePublicationEvidence,
    TaskImageRegistryCredentialGeneration,
)
from loom_task_image_authority.contracts import (
    TaskImageAttachmentProofV1,
    TaskImageBuildSessionV2,
    TaskImageGuardPrincipalV1,
    TaskImagePublicationCandidateRequestV1,
    TaskImageRegistryCredentialRequestV1,
    TaskImageSessionRenewalV1,
    canonical_public_binding_sha256,
)
from loom_task_image_authority.materializations import (
    TaskImageSessionMaterializationAuthorizationError,
    TaskImageSessionMaterializationConflictError,
    claim_session_materialization,
    heartbeat_session_materialization,
)
from loom_task_image_authority.registry_credentials import (
    TaskImageRegistryCredentialUnavailableError,
    issue_session_registry_credential,
    record_session_publication_candidate,
)
from loom_task_image_authority.registry_token import DistributionRegistryTokenIssuer
from loom_task_image_authority.store import (
    TaskImageBuildSessionAuthorization,
    authorize_task_image_build_session,
    renew_task_image_build_session,
)
from tests.integration.test_task_image_authority_materializations import (
    CLAIM_ID,
    _active_authorization,
    _attempt,
    _queued_materialization,
)
from tests.integration.test_task_image_projection_store import (
    GRANT_ID,
    NEXT_SESSION_ID,
    NOW,
    RENEWAL_ID,
    _attestation,
    _MemorySecretStore,
)

REQUEST_ID = UUID("11111111-2222-4222-8222-111111111111")
CREDENTIAL_ID = UUID("22222222-3333-4333-8333-222222222222")
NEXT_CREDENTIAL_ID = UUID("33333333-4444-4444-8444-333333333333")
HEARTBEAT_ID = UUID("44444444-5555-4555-8555-444444444444")
CANDIDATE_OPERATION_ID = UUID("55555555-6666-4666-8666-555555555555")
CANDIDATE_ID = UUID("66666666-7777-4777-8777-666666666666")


@pytest.fixture
async def registry_authority_session(
    postgres_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        async with factory() as session:
            await session.execute(delete(TaskImagePublicationCandidate))
            await session.execute(delete(TaskImageRegistryCredentialGeneration))
            await session.execute(delete(TaskImageMaterializationOperationEvent))
            await session.execute(delete(TaskImagePublicationEvidence))
            await session.execute(delete(TaskImageMaterializationAttempt))
            await session.execute(delete(TaskImageMaterialization))
            await session.execute(delete(TaskImageBuildSessionGeneration))
            await session.execute(delete(TaskImageBuildContainmentAttestation))
            await session.execute(delete(TaskImageBuildProjectionEvent))
            await session.execute(delete(TaskImageBuildProjection))
            await session.execute(delete(TaskImageBuildGrantEvent))
            await session.execute(delete(TaskImageBuildGrant))
            await session.commit()
        await engine.dispose()


@pytest.fixture(scope="module")
def registry_issuer() -> DistributionRegistryTokenIssuer:
    return DistributionRegistryTokenIssuer(
        private_key=rsa.generate_private_key(public_exponent=65537, key_size=3072),
        registry_origin="https://registry.example:5443",
        service="registry.example",
        issuer="loom-task-image-authority",
    )


async def _claimed_attempt(
    session: AsyncSession,
) -> tuple[
    TaskImageBuildSessionAuthorization,
    TaskImageGuardPrincipalV1,
    TaskImageAttachmentProofV1,
    TaskImageBuildSessionV2,
    _MemorySecretStore,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
]:
    authorization, principal, proof, build_session, secrets = await _active_authorization(
        session
    )
    await _queued_materialization(session)
    claimed = await claim_session_materialization(
        session,
        authorization=authorization,
        claim_id=CLAIM_ID,
        now=NOW + timedelta(seconds=10),
        lease_seconds=300,
    )
    assert claimed is not None
    row, _plan = claimed
    return authorization, principal, proof, build_session, secrets, row, await _attempt(session)


def _credential_request(
    authorization: TaskImageBuildSessionAuthorization,
    build_session: TaskImageBuildSessionV2,
    row: TaskImageMaterialization,
    attempt: TaskImageMaterializationAttempt,
    **changes: object,
) -> TaskImageRegistryCredentialRequestV1:
    values: dict[str, object] = {
        "request_id": REQUEST_ID,
        "grant_id": authorization.grant_id,
        "session_id": authorization.session_id,
        "session_generation": authorization.session_generation,
        "session_token": build_session.session_token,
        "materialization_id": row.id,
        "attempt_id": attempt.id,
        "lease_epoch": row.lease_epoch,
        "component": "task",
        "predecessor_credential_id": None,
        "predecessor_generation": None,
    }
    values.update(changes)
    return TaskImageRegistryCredentialRequestV1.model_validate(values)


def _candidate_request(
    authorization: TaskImageBuildSessionAuthorization,
    build_session: TaskImageBuildSessionV2,
    row: TaskImageMaterialization,
    attempt: TaskImageMaterializationAttempt,
    *,
    credential_id: UUID = CREDENTIAL_ID,
    credential_generation: int = 1,
    operation_id: UUID = CANDIDATE_OPERATION_ID,
    **changes: object,
) -> TaskImagePublicationCandidateRequestV1:
    values: dict[str, object] = {
        "operation_id": operation_id,
        "grant_id": authorization.grant_id,
        "session_id": authorization.session_id,
        "session_generation": authorization.session_generation,
        "session_token": build_session.session_token,
        "materialization_id": row.id,
        "attempt_id": attempt.id,
        "lease_epoch": row.lease_epoch,
        "credential_id": credential_id,
        "credential_generation": credential_generation,
        "component": "task",
        "manifest_digest": "sha256:" + "a" * 64,
        "manifest_size": 512,
        "oci_file_sha256": "b" * 64,
        "oci_file_size": 4096,
        "platform": "linux/arm64",
    }
    values.update(changes)
    return TaskImagePublicationCandidateRequestV1.model_validate(values)


async def _issue_first(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    build_session: TaskImageBuildSessionV2,
    secrets: _MemorySecretStore,
    row: TaskImageMaterialization,
    attempt: TaskImageMaterializationAttempt,
    issuer: DistributionRegistryTokenIssuer,
    request_id: UUID = REQUEST_ID,
    credential_id_factory: Callable[[], UUID] = lambda: CREDENTIAL_ID,
    now: datetime = NOW + timedelta(seconds=11),
):
    request = _credential_request(
        authorization,
        build_session,
        row,
        attempt,
        request_id=request_id,
    )
    response = await issue_session_registry_credential(
        session,
        authorization=authorization,
        request=request,
        now=now,
        issuer=issuer,
        secret_store=secrets,
        credential_id_factory=credential_id_factory,
    )
    return request, response


async def test_first_credential_is_exact_persisted_without_raw_token_and_replayed(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer: DistributionRegistryTokenIssuer,
) -> None:
    async with registry_authority_session() as session:
        authorization, _principal, _proof, build_session, secrets, row, attempt = (
            await _claimed_attempt(session)
        )
        request, credential = await _issue_first(
            session,
            authorization=authorization,
            build_session=build_session,
            secrets=secrets,
            row=row,
            attempt=attempt,
            issuer=registry_issuer,
        )
        replay = await issue_session_registry_credential(
            session,
            authorization=authorization,
            request=request,
            now=NOW + timedelta(seconds=12),
            issuer=registry_issuer,
            secret_store=secrets,
            credential_id_factory=uuid4,
        )

        assert replay == credential
        assert credential.credential_id == CREDENTIAL_ID
        assert credential.generation == 1
        assert credential.expires_at == NOW + timedelta(seconds=40)
        assert credential.repository == (
            f"loom-task-image-attempts/arm64/{attempt.id}/task"
        )
        stored = (
            await session.scalars(select(TaskImageRegistryCredentialGeneration))
        ).one()
        assert stored.request_sha256 == canonical_public_binding_sha256(request)
        assert stored.response_public_json == credential.public_binding()
        assert stored.response_sha256 == canonical_public_binding_sha256(credential)
        assert stored.token_hash == hashlib.sha256(
            credential.bearer_token.encode("ascii")
        ).digest()
        assert credential.bearer_token not in json.dumps(stored.__dict__, default=str)
        assert secrets.put_count == 3
        assert await session.scalar(
            select(func.count(TaskImageRegistryCredentialGeneration.credential_id))
        ) == 1


async def test_credential_replay_conflicts_on_changed_body_and_rejects_expiry(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer: DistributionRegistryTokenIssuer,
) -> None:
    async with registry_authority_session() as session:
        authorization, _principal, _proof, build_session, secrets, row, attempt = (
            await _claimed_attempt(session)
        )
        request, _credential = await _issue_first(
            session,
            authorization=authorization,
            build_session=build_session,
            secrets=secrets,
            row=row,
            attempt=attempt,
            issuer=registry_issuer,
        )

        with pytest.raises(TaskImageSessionMaterializationConflictError):
            await issue_session_registry_credential(
                session,
                authorization=authorization,
                request=request.model_copy(update={"component": "sidecar:missing"}),
                now=NOW + timedelta(seconds=12),
                issuer=registry_issuer,
                secret_store=secrets,
                credential_id_factory=uuid4,
            )
        with pytest.raises(TaskImageSessionMaterializationAuthorizationError):
            await issue_session_registry_credential(
                session,
                authorization=authorization,
                request=request,
                now=NOW + timedelta(seconds=40),
                issuer=registry_issuer,
                secret_store=secrets,
                credential_id_factory=uuid4,
            )


async def test_renewal_requires_new_session_attestation_and_post_issue_heartbeat(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer: DistributionRegistryTokenIssuer,
) -> None:
    async with registry_authority_session() as session:
        authorization, principal, proof, build_session, secrets, row, attempt = (
            await _claimed_attempt(session)
        )
        _request, first = await _issue_first(
            session,
            authorization=authorization,
            build_session=build_session,
            secrets=secrets,
            row=row,
            attempt=attempt,
            issuer=registry_issuer,
        )
        same_session_renewal = _credential_request(
            authorization,
            build_session,
            row,
            attempt,
            request_id=uuid4(),
            predecessor_credential_id=first.credential_id,
            predecessor_generation=first.generation,
        )
        with pytest.raises(TaskImageSessionMaterializationAuthorizationError):
            await issue_session_registry_credential(
                session,
                authorization=authorization,
                request=same_session_renewal,
                now=NOW + timedelta(seconds=12),
                issuer=registry_issuer,
                secret_store=secrets,
                credential_id_factory=lambda: NEXT_CREDENTIAL_ID,
            )

        next_attestation = _attestation(proof, generation=2)
        renewed_session = await renew_task_image_build_session(
            session,
            principal=principal,
            request=TaskImageSessionRenewalV1(
                renewal_id=RENEWAL_ID,
                grant_id=GRANT_ID,
                session_id=build_session.session_id,
                session_generation=build_session.generation,
                session_token=build_session.session_token,
                attestation=next_attestation,
                observed_at=next_attestation.issued_at,
            ),
            now=NOW + timedelta(seconds=13),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "C" * 64,
            session_id_factory=lambda: NEXT_SESSION_ID,
        )
        renewed_authorization = await authorize_task_image_build_session(
            session,
            grant_id=GRANT_ID,
            session_id=renewed_session.session_id,
            session_generation=renewed_session.generation,
            raw_session_token=renewed_session.session_token,
            now=NOW + timedelta(seconds=14),
        )
        renewal_request = _credential_request(
            renewed_authorization,
            renewed_session,
            row,
            attempt,
            request_id=uuid4(),
            predecessor_credential_id=first.credential_id,
            predecessor_generation=first.generation,
        )
        with pytest.raises(TaskImageSessionMaterializationAuthorizationError):
            await issue_session_registry_credential(
                session,
                authorization=renewed_authorization,
                request=renewal_request,
                now=NOW + timedelta(seconds=14),
                issuer=registry_issuer,
                secret_store=secrets,
                credential_id_factory=lambda: NEXT_CREDENTIAL_ID,
            )

        await heartbeat_session_materialization(
            session,
            authorization=renewed_authorization,
            materialization_id=row.id,
            attempt_id=attempt.id,
            lease_epoch=row.lease_epoch,
            operation_id=HEARTBEAT_ID,
            now=NOW + timedelta(seconds=15),
        )
        renewed = await issue_session_registry_credential(
            session,
            authorization=renewed_authorization,
            request=renewal_request,
            now=NOW + timedelta(seconds=16),
            issuer=registry_issuer,
            secret_store=secrets,
            credential_id_factory=lambda: NEXT_CREDENTIAL_ID,
        )

        assert renewed.generation == 2
        assert renewed.predecessor_credential_id == first.credential_id
        assert renewed.lease_heartbeat_operation_id == HEARTBEAT_ID
        assert renewed.session_id == renewed_session.session_id
        assert renewed.attestation_generation == 2
        assert renewed.expires_at == NOW + timedelta(seconds=52)


async def test_candidate_is_exactly_replayable_and_cannot_change_readiness(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer: DistributionRegistryTokenIssuer,
) -> None:
    async with registry_authority_session() as session:
        authorization, _principal, _proof, build_session, secrets, row, attempt = (
            await _claimed_attempt(session)
        )
        _request, credential = await _issue_first(
            session,
            authorization=authorization,
            build_session=build_session,
            secrets=secrets,
            row=row,
            attempt=attempt,
            issuer=registry_issuer,
        )
        request = _candidate_request(
            authorization,
            build_session,
            row,
            attempt,
            credential_id=credential.credential_id,
            credential_generation=credential.generation,
        )
        before = (
            row.state,
            row.attempt_count,
            row.registry_images,
            row.registry_image_history,
            row.ready_at,
            row.finished_at,
            row.failure_reason,
            row.failure_message,
        )
        candidate = await record_session_publication_candidate(
            session,
            authorization=authorization,
            request=request,
            now=NOW + timedelta(seconds=12),
            candidate_id_factory=lambda: CANDIDATE_ID,
        )
        replay = await record_session_publication_candidate(
            session,
            authorization=authorization,
            request=request,
            now=NOW + timedelta(seconds=13),
            candidate_id_factory=uuid4,
        )
        after = (
            row.state,
            row.attempt_count,
            row.registry_images,
            row.registry_image_history,
            row.ready_at,
            row.finished_at,
            row.failure_reason,
            row.failure_message,
        )

        assert replay == candidate
        assert before == after
        assert candidate.candidate_id == CANDIDATE_ID
        assert candidate.repository == credential.repository
        assert await session.scalar(
            select(func.count(TaskImagePublicationCandidate.candidate_id))
        ) == 1

        with pytest.raises(TaskImageSessionMaterializationConflictError):
            await record_session_publication_candidate(
                session,
                authorization=authorization,
                request=request.model_copy(update={"manifest_size": 513}),
                now=NOW + timedelta(seconds=14),
                candidate_id_factory=uuid4,
            )
        with pytest.raises(TaskImageSessionMaterializationConflictError):
            await record_session_publication_candidate(
                session,
                authorization=authorization,
                request=_candidate_request(
                    authorization,
                    build_session,
                    row,
                    attempt,
                    credential_id=credential.credential_id,
                    credential_generation=credential.generation,
                    operation_id=uuid4(),
                ),
                now=NOW + timedelta(seconds=14),
                candidate_id_factory=uuid4,
            )


async def test_concurrent_exact_credential_requests_create_one_generation(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer: DistributionRegistryTokenIssuer,
) -> None:
    async with registry_authority_session() as setup_session:
        authorization, _principal, _proof, build_session, secrets, row, attempt = (
            await _claimed_attempt(setup_session)
        )
        request = _credential_request(
            authorization,
            build_session,
            row,
            attempt,
        )
        await setup_session.commit()

    async def issue(factory_value: UUID):
        async with registry_authority_session() as session:
            response = await issue_session_registry_credential(
                session,
                authorization=authorization,
                request=request,
                now=NOW + timedelta(seconds=11),
                issuer=registry_issuer,
                secret_store=secrets,
                credential_id_factory=lambda: factory_value,
            )
            await session.commit()
            return response

    try:
        first, second = await asyncio.gather(
            issue(CREDENTIAL_ID),
            issue(NEXT_CREDENTIAL_ID),
        )
        assert first == second
        assert first.credential_id in {CREDENTIAL_ID, NEXT_CREDENTIAL_ID}
        async with registry_authority_session() as session:
            assert await session.scalar(
                select(func.count(TaskImageRegistryCredentialGeneration.credential_id))
            ) == 1
    finally:
        # This test alone commits setup so independent connections can contend.
        # Truncating the complete FK cycle restores the otherwise rollback-only fixture.
        async with registry_authority_session() as cleanup_session:
            await cleanup_session.execute(
                text(
                    "TRUNCATE TABLE "
                    "task_image_publication_candidates, "
                    "task_image_registry_credentials, "
                    "task_image_materialization_operation_events, "
                    "task_image_publication_evidence, "
                    "task_image_materialization_attempts, "
                    "task_image_materializations, "
                    "task_image_build_session_generations, "
                    "task_image_build_containment_attestations, "
                    "task_image_build_projection_events, "
                    "task_image_build_projections, "
                    "task_image_build_grant_events, "
                    "task_image_build_grants CASCADE"
                )
            )
            await cleanup_session.commit()


async def test_credential_replay_rejects_persisted_public_scalar_drift(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer: DistributionRegistryTokenIssuer,
) -> None:
    async with registry_authority_session() as session:
        authorization, _principal, _proof, build_session, secrets, row, attempt = (
            await _claimed_attempt(session)
        )
        request, _credential = await _issue_first(
            session,
            authorization=authorization,
            build_session=build_session,
            secrets=secrets,
            row=row,
            attempt=attempt,
            issuer=registry_issuer,
        )
        stored = (
            await session.scalars(select(TaskImageRegistryCredentialGeneration))
        ).one()
        stored.registry_service = "other-registry.example"

        with session.no_autoflush:
            with pytest.raises(TaskImageRegistryCredentialUnavailableError):
                await issue_session_registry_credential(
                    session,
                    authorization=authorization,
                    request=request,
                    now=NOW + timedelta(seconds=12),
                    issuer=registry_issuer,
                    secret_store=secrets,
                    credential_id_factory=uuid4,
                )


async def test_signer_and_secret_store_failures_leave_no_credential_row(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer: DistributionRegistryTokenIssuer,
) -> None:
    async with registry_authority_session() as session:
        authorization, _principal, _proof, build_session, secrets, row, attempt = (
            await _claimed_attempt(session)
        )
        request = _credential_request(authorization, build_session, row, attempt)

        class FailingIssuer:
            def issue(self, **_kwargs: object) -> object:
                raise RuntimeError("synthetic signer failure with secret material")

        with pytest.raises(RuntimeError, match="synthetic signer failure"):
            await issue_session_registry_credential(
                session,
                authorization=authorization,
                request=request,
                now=NOW + timedelta(seconds=11),
                issuer=FailingIssuer(),  # type: ignore[arg-type]
                secret_store=secrets,
                credential_id_factory=lambda: CREDENTIAL_ID,
            )
        assert await session.scalar(
            select(func.count(TaskImageRegistryCredentialGeneration.credential_id))
        ) == 0

        failing_store = _MemorySecretStore(fail_put=True)
        with pytest.raises(RuntimeError, match="synthetic secret-store failure"):
            await issue_session_registry_credential(
                session,
                authorization=authorization,
                request=request,
                now=NOW + timedelta(seconds=11),
                issuer=registry_issuer,
                secret_store=failing_store,
                credential_id_factory=lambda: CREDENTIAL_ID,
            )
        assert await session.scalar(
            select(func.count(TaskImageRegistryCredentialGeneration.credential_id))
        ) == 0


async def test_candidate_replay_rejects_persisted_public_scalar_drift(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer: DistributionRegistryTokenIssuer,
) -> None:
    async with registry_authority_session() as session:
        authorization, _principal, _proof, build_session, secrets, row, attempt = (
            await _claimed_attempt(session)
        )
        _request, credential = await _issue_first(
            session,
            authorization=authorization,
            build_session=build_session,
            secrets=secrets,
            row=row,
            attempt=attempt,
            issuer=registry_issuer,
        )
        request = _candidate_request(
            authorization,
            build_session,
            row,
            attempt,
            credential_id=credential.credential_id,
            credential_generation=credential.generation,
        )
        await record_session_publication_candidate(
            session,
            authorization=authorization,
            request=request,
            now=NOW + timedelta(seconds=12),
            candidate_id_factory=lambda: CANDIDATE_ID,
        )
        stored = (await session.scalars(select(TaskImagePublicationCandidate))).one()
        stored.manifest_size += 1

        with session.no_autoflush:
            with pytest.raises(TaskImageSessionMaterializationConflictError):
                await record_session_publication_candidate(
                    session,
                    authorization=authorization,
                    request=request,
                    now=NOW + timedelta(seconds=13),
                    candidate_id_factory=uuid4,
                )


@pytest.mark.parametrize(
    "changes",
    [
        {"credential_id": NEXT_CREDENTIAL_ID},
        {"credential_generation": 2},
        {"component": "sidecar:missing"},
        {"platform": "linux/amd64"},
    ],
)
async def test_candidate_rejects_cross_binding_without_changing_materialization(
    registry_authority_session: async_sessionmaker[AsyncSession],
    registry_issuer: DistributionRegistryTokenIssuer,
    changes: dict[str, object],
) -> None:
    async with registry_authority_session() as session:
        authorization, _principal, _proof, build_session, secrets, row, attempt = (
            await _claimed_attempt(session)
        )
        _request, credential = await _issue_first(
            session,
            authorization=authorization,
            build_session=build_session,
            secrets=secrets,
            row=row,
            attempt=attempt,
            issuer=registry_issuer,
        )
        request_changes: dict[str, object] = {
            "credential_id": credential.credential_id,
            "credential_generation": credential.generation,
        }
        request_changes.update(changes)
        request = _candidate_request(
            authorization,
            build_session,
            row,
            attempt,
            **request_changes,
        )
        before = (row.state, row.registry_images, row.ready_at, row.finished_at)

        with pytest.raises(TaskImageSessionMaterializationAuthorizationError):
            await record_session_publication_candidate(
                session,
                authorization=authorization,
                request=request,
                now=NOW + timedelta(seconds=12),
                candidate_id_factory=lambda: CANDIDATE_ID,
            )

        assert (row.state, row.registry_images, row.ready_at, row.finished_at) == before
        assert await session.scalar(
            select(func.count(TaskImagePublicationCandidate.candidate_id))
        ) == 0
