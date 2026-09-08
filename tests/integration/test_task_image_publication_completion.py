from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib
from dataclasses import replace
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import select, text, update
from sqlalchemy.exc import IntegrityError

from loom.db.schema import (
    TaskImageBuildContainmentAttestation,
    TaskImageBuildGrant,
    TaskImageBuildProjection,
    TaskImageBuildSessionGeneration,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImagePublicationCandidate,
    TaskImagePublicationEnvelope,
    TaskImagePublicationJob,
    TaskImagePublicationKey,
    TaskImagePublicationState,
    TaskImageRegistryCredentialGeneration,
)
from loom_task_image_authority.contracts import (
    TaskImagePublicationCandidateRequestV2,
    TaskImageSessionRenewalV1,
)
from loom_task_image_authority.materializations import (
    claim_session_materialization,
    heartbeat_session_materialization,
)
from loom_task_image_authority.publication_contracts import (
    PublicationEnvelope,
    PublicationUnsignedInput,
    canonical_publication_bytes,
)
from loom_task_image_authority.publication_receipts import canonical_receipt_bytes
from loom_task_image_authority.publication_signing import (
    DistributedKeysetSnapshot,
    PublicationKeyRecord,
    PublicationState,
    VerifiedPublication,
    prepare_publication_statement,
)
from loom_task_image_authority.publication_store import (
    claim_publication_job,
    submit_publication_job,
)
from loom_task_image_authority.registry_credentials import (
    issue_session_registry_credential,
    record_session_publication_candidate_v2,
)
from loom_task_image_authority.store import (
    authorize_task_image_build_session,
    renew_task_image_build_session,
)
from tests.integration.test_task_image_authority_materializations import (
    _active_authorization,
    _queued_materialization,
)
from tests.integration.test_task_image_projection_store import _attestation
from tests.integration.test_task_image_publication_jobs import _blocked
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import (
    NOW,
    _candidate_request,
    _credential_request,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)
from tests.unit.test_task_image_publication_contracts import unsigned_payload


def completion():
    name = "loom_task_image_authority.publication_completion"
    assert importlib.util.find_spec(name) is not None, "atomic publication completion missing"
    return importlib.import_module(name)


async def _queued_job(
    session,
    issuer,
    *,
    names=("task",),
    root=None,
    lease_seconds=300,
    context=None,
    lifetime_seconds=1800,
):
    authorization, principal, proof, build_session, secrets = await _active_authorization(session)
    if context is not None:
        context.update(
            authorization=authorization,
            principal=principal,
            proof=proof,
            build_session=build_session,
            secrets=secrets,
        )
    row = await _queued_materialization(session)
    config = dict(row.task_config)
    environment = dict(config["environment"])
    if "task" not in names:
        environment.pop("dockerfile")
    environment["sidecars"] = [
        {"name": name.removeprefix("sidecar:"), "dockerfile": "environment/Dockerfile"}
        for name in names
        if name != "task"
    ]
    config["environment"] = environment
    row.task_config = config
    await session.flush()
    claimed = await claim_session_materialization(
        session,
        authorization=authorization,
        claim_id=uuid4(),
        now=NOW + timedelta(seconds=10),
        lease_seconds=lease_seconds,
    )
    assert claimed is not None
    attempt = (await session.scalars(select(TaskImageMaterializationAttempt))).one()
    for name in names:
        credential = await issue_session_registry_credential(
            session,
            authorization=authorization,
            request=_credential_request(
                authorization, build_session, row, attempt, component=name, request_id=uuid4()
            ),
            now=NOW + timedelta(seconds=11),
            issuer=issuer,
            secret_store=secrets,
            credential_id_factory=uuid4,
        )
        legacy = _candidate_request(
            authorization,
            build_session,
            row,
            attempt,
            credential_id=credential.credential_id,
            credential_generation=1,
            operation_id=uuid4(),
            component=name,
            manifest_digest=root.digest if root else "sha256:" + "a" * 64,
            manifest_size=root.size if root else 512,
        )
        request = TaskImagePublicationCandidateRequestV2.model_validate(
            dict(
                legacy.model_dump(),
                schema_version=2,
                base_resolution={
                    "schema": "loom.task-image-base-resolution/v1",
                    "solve_ref": "solve-1",
                    "platform": "linux/arm64",
                    "output_digest": legacy.manifest_digest,
                    "observed_base_digests": [],
                },
            )
        )
        await record_session_publication_candidate_v2(
            session,
            authorization=authorization,
            request=request,
            now=NOW + timedelta(seconds=12),
            candidate_id_factory=uuid4,
        )
    return await submit_publication_job(
        session,
        authorization=authorization,
        operation_id=uuid4(),
        materialization_id=row.id,
        attempt_id=attempt.id,
        lease_epoch=row.lease_epoch,
        registry_origin=issuer.registry_origin,
        lifetime_seconds=lifetime_seconds,
        clock=lambda: NOW + timedelta(seconds=14),
    )


async def _signed_job(session, issuer, *, worker_lease_seconds=60, binding_changes=None, **options):
    job = await _queued_job(session, issuer, **options)
    owner = uuid4()
    job = await claim_publication_job(
        session,
        operation_id=UUID(job.operation_id),
        owner_id=owner,
        clock=lambda: NOW + timedelta(seconds=14),
        lease_seconds=worker_lease_seconds,
    )
    private = Ed25519PrivateKey.generate()
    key = PublicationKeyRecord("publication-1", private.public_key().public_bytes_raw(), NOW)
    session.add(
        TaskImagePublicationKey(
            key_id=key.key_id,
            public_key=key.public_key,
            activated_at=key.activated_at,
            status="active",
        )
    )
    await session.execute(update(TaskImagePublicationState).values(keyset_version=1))
    await session.flush()
    distribution = DistributedKeysetSnapshot(1, 0, (key.key_id,), NOW, NOW + timedelta(minutes=10))
    publications = tuple(
        _sign(job, component, private, key, distribution, binding_changes)
        for component in job.snapshot.components
    )
    return job, owner, publications, distribution


def _sign(job, component, private, key, distribution, binding_changes=None):
    values = job.snapshot.model_dump(
        mode="json", by_alias=True, exclude={"components", "builder_id"}
    )
    values.update(
        schema="loom.task-image-publication/v1",
        component=component.candidate.component,
        repository=component.candidate.repository,
        root=component.root.model_dump(),
        manifest=component.root.model_dump(),
        config=unsigned_payload()["config"],
        layers=(),
        observed_base_digests=component.candidate.base_resolution.observed_base_digests,
    )
    values.update(binding_changes or {})
    unsigned = PublicationUnsignedInput.model_validate(values)
    statement = prepare_publication_statement(
        unsigned,
        key=key,
        state=PublicationState(0, 1),
        distribution=distribution,
        signer_now=NOW + timedelta(seconds=14),
    )
    canonical = canonical_publication_bytes(statement)
    envelope = PublicationEnvelope(
        canonical_statement=canonical.decode(),
        statement_sha256=hashlib.sha256(canonical).hexdigest(),
        key_id=key.key_id,
        algorithm="Ed25519",
        signature=base64.urlsafe_b64encode(
            private.sign(b"loom-task-image-publication-v1\x00" + canonical)
        )
        .rstrip(b"=")
        .decode(),
    )
    return VerifiedPublication(envelope, statement)


async def _complete(session, values, *, instant=NOW + timedelta(seconds=14), clock=None):
    job, owner, publications, distribution = values
    return await completion().complete_publication_job(
        session,
        job=job,
        owner_id=owner,
        generation=job.worker_generation,
        publications=publications,
        distribution=distribution,
        clock=clock or (lambda: instant),
    )


async def _untouched(factory):
    async with factory() as session:
        row = (await session.scalars(select(TaskImageMaterialization))).one()
        assert row.state == "claimed" and row.ready_at is None and not row.registry_images
        assert row.attempt_count == 0
        assert not list(await session.scalars(select(TaskImagePublicationEnvelope)))
        job = (await session.scalars(select(TaskImagePublicationJob))).one()
        assert job.state == "running" and job.canonical_receipt is None


async def test_completion_uses_renewed_materialization_lease(
    registry_authority_session, registry_issuer
):
    async with registry_authority_session() as session:
        values = await _signed_job(session, registry_issuer, lease_seconds=5)
        row = (await session.scalars(select(TaskImageMaterialization))).one()
        row.lease_expires_at = NOW + timedelta(seconds=100)
        await session.commit()
        receipt = await _complete(session, values, instant=NOW + timedelta(seconds=16))
        await session.commit()
        assert receipt.component_count == 1


@pytest.mark.parametrize(
    "names", [("task", "sidecar:cache", "sidecar:db"), ("sidecar:cache", "sidecar:db")]
)
async def test_complete_component_set_and_sidecar_only(
    registry_authority_session, registry_issuer, names
):
    async with registry_authority_session() as session:
        values = await _signed_job(session, registry_issuer, names=names)
        await session.commit()
        receipt = await _complete(session, values)
        await session.commit()
        row = (await session.scalars(select(TaskImageMaterialization))).one()
        assert set(row.registry_images) == set(names)
        assert receipt.component_count == len(names)
        assert (
            await completion().replay_completed_publication(
                session, operation_id=values[0].operation_id
            )
            == receipt
        )


@pytest.mark.parametrize("stage", ["envelopes", "ready"])
async def test_post_flush_expiry_rolls_back_everything(
    registry_authority_session, registry_issuer, stage
):
    async with registry_authority_session() as session:
        values = await _signed_job(
            session, registry_issuer, names=("task", "sidecar:db"), worker_lease_seconds=5
        )
        await session.execute(
            text("""CREATE FUNCTION completion_test_wait() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN PERFORM pg_advisory_xact_lock(4421); RETURN NEW; END $$""")
        )
        table, operation = (
            ("task_image_publication_envelopes", "INSERT")
            if stage == "envelopes"
            else ("task_image_materializations", "UPDATE")
        )
        await session.execute(
            text(
                f"CREATE TRIGGER completion_test_wait AFTER {operation} ON {table} FOR EACH ROW EXECUTE FUNCTION completion_test_wait()"
            )
        )
        await session.commit()
    now = NOW + timedelta(seconds=14)
    async with registry_authority_session() as blocker, registry_authority_session() as worker:
        await blocker.execute(text("SELECT pg_advisory_xact_lock(4421)"))
        pid = await worker.scalar(text("SELECT pg_backend_pid()"))
        task = asyncio.create_task(_complete(worker, values, clock=lambda: now))
        try:
            await _blocked(blocker, pid, task)
            now = values[0].lease.expires_at
            await blocker.rollback()
            with pytest.raises(RuntimeError, match=r"expired|fence lost"):
                await task
        finally:
            await blocker.rollback()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await worker.rollback()
    await _untouched(registry_authority_session)


@pytest.mark.parametrize(
    "model",
    [
        TaskImagePublicationState,
        TaskImagePublicationKey,
        TaskImageBuildGrant,
        TaskImageBuildProjection,
        TaskImageBuildSessionGeneration,
        TaskImageBuildContainmentAttestation,
        TaskImageMaterialization,
        TaskImageMaterializationAttempt,
        TaskImageRegistryCredentialGeneration,
        TaskImagePublicationCandidate,
        TaskImagePublicationJob,
    ],
)
async def test_completion_expiry_after_real_lock_wait(
    registry_authority_session, registry_issuer, model
):
    async with registry_authority_session() as setup:
        values = await _signed_job(setup, registry_issuer, worker_lease_seconds=5)
        await setup.commit()
    now = NOW + timedelta(seconds=14)
    async with registry_authority_session() as blocker, registry_authority_session() as worker:
        await blocker.scalar(select(model).with_for_update())
        pid = await worker.scalar(text("SELECT pg_backend_pid()"))
        task = asyncio.create_task(_complete(worker, values, clock=lambda: now))
        try:
            await _blocked(blocker, pid, task)
            now = values[0].lease.expires_at
            await blocker.rollback()
            with pytest.raises(RuntimeError, match=r"expired|fence lost"):
                await task
        finally:
            await blocker.rollback()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await worker.rollback()
    await _untouched(registry_authority_session)


@pytest.mark.parametrize(
    "mutation", ["signature", "statement", "snapshot", "key", "set", "distribution"]
)
async def test_rejects_forged_verified_publication_and_bindings(
    registry_authority_session, registry_issuer, mutation
):
    async with registry_authority_session() as session:
        job, owner, publications, distribution = await _signed_job(session, registry_issuer)
        await session.commit()
        original = publications[0]
        if mutation == "signature":
            publications = (
                replace(
                    original, envelope=original.envelope.model_copy(update={"signature": "A" * 86})
                ),
            )
        elif mutation == "statement":
            publications = (
                replace(
                    original, statement=original.statement.model_copy(update={"task_id": "changed"})
                ),
            )
        elif mutation == "snapshot":
            job = job.model_copy(update={"snapshot_sha256": "e" * 64})
        elif mutation == "key":
            publications = (
                replace(
                    original, envelope=original.envelope.model_copy(update={"key_id": "other"})
                ),
            )
        elif mutation == "set":
            publications = ()
        else:
            distribution = replace(distribution, keyset_version=2)
        with pytest.raises(RuntimeError):
            await _complete(session, (job, owner, publications, distribution))
        await session.rollback()
    await _untouched(registry_authority_session)


@pytest.mark.parametrize(
    "change",
    [
        "retired",
        "revoked_key",
        "version",
        "revoked_grant",
        "revoked_projection",
        "lost_lease",
        "takeover",
    ],
)
async def test_committed_authority_change_during_completion_lock_wait(
    registry_authority_session, registry_issuer, change
):
    async with registry_authority_session() as session:
        values = await _signed_job(session, registry_issuer)
        await session.commit()
    model = (
        TaskImagePublicationState
        if change in ("retired", "revoked_key", "version")
        else TaskImageBuildGrant
        if change in ("revoked_grant", "revoked_projection")
        else TaskImageMaterialization
        if change == "lost_lease"
        else TaskImagePublicationJob
    )
    async with registry_authority_session() as blocker, registry_authority_session() as worker:
        await blocker.scalar(select(model).with_for_update())
        pid = await worker.scalar(text("SELECT pg_backend_pid()"))
        task = asyncio.create_task(_complete(worker, values, instant=NOW + timedelta(seconds=15)))
        try:
            await _blocked(blocker, pid, task)
            if change == "retired":
                await blocker.execute(
                    update(TaskImagePublicationKey).values(
                        status="verify_only", retired_at=NOW + timedelta(seconds=15)
                    )
                )
            elif change == "revoked_key":
                await blocker.execute(
                    update(TaskImagePublicationKey).values(
                        status="revoked", revoked_at=NOW + timedelta(seconds=15)
                    )
                )
            elif change == "version":
                await blocker.execute(update(TaskImagePublicationState).values(keyset_version=2))
            elif change == "revoked_grant":
                await blocker.execute(
                    update(TaskImageBuildGrant).values(
                        state="revoked",
                        released_at=None,
                        revoked_at=NOW + timedelta(seconds=15),
                        revoke_reason="test_revocation",
                    )
                )
            elif change == "revoked_projection":
                await blocker.execute(
                    update(TaskImageBuildProjection).values(
                        state="revoked",
                        revoked_at=NOW + timedelta(seconds=15),
                        revoke_reason="test_revocation",
                    )
                )
            elif change == "lost_lease":
                await blocker.execute(update(TaskImageMaterialization).values(lease_epoch=2))
            else:
                await blocker.execute(
                    update(TaskImagePublicationJob).values(worker_id=uuid4(), worker_generation=2)
                )
            await blocker.commit()
            with pytest.raises(RuntimeError):
                await task
        finally:
            await blocker.rollback()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await worker.rollback()
    await _untouched(registry_authority_session)


async def test_partial_envelope_insert_failure_is_atomic(
    registry_authority_session, registry_issuer
):
    async with registry_authority_session() as session:
        values = await _signed_job(session, registry_issuer, names=("task", "sidecar:db"))
        await session.execute(
            text("""CREATE FUNCTION completion_test_partial() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.component = 'sidecar:db' THEN
                    IF NOT EXISTS (SELECT 1 FROM task_image_publication_envelopes WHERE component = 'task') THEN
                        RAISE EXCEPTION 'first component not inserted';
                    END IF;
                    RAISE EXCEPTION 'second component insert failed' USING ERRCODE = '23514';
                END IF;
                RETURN NEW;
            END $$""")
        )
        await session.execute(
            text(
                "CREATE TRIGGER completion_test_partial BEFORE INSERT ON task_image_publication_envelopes FOR EACH ROW EXECUTE FUNCTION completion_test_partial()"
            )
        )
        await session.commit()
        with pytest.raises(IntegrityError, match="second component insert failed"):
            await _complete(session, values)
        await session.rollback()
    await _untouched(registry_authority_session)


async def test_two_completing_workers_and_nonlocking_historical_receipt(
    registry_authority_session, registry_issuer
):
    async with registry_authority_session() as session:
        values = await _signed_job(session, registry_issuer)
        await session.commit()
    async with registry_authority_session() as first, registry_authority_session() as second:
        receipt = await _complete(first, values)
        pid = await second.scalar(text("SELECT pg_backend_pid()"))
        # A later owner's claim response may have been lost; exact history wins.
        contender = (values[0], uuid4(), values[2], values[3])
        task = asyncio.create_task(_complete(second, contender))
        try:
            await _blocked(first, pid, task)
            await first.commit()
            assert await task == receipt
            await second.commit()
        finally:
            await first.rollback()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    async with registry_authority_session() as retirement:
        await retirement.execute(
            update(TaskImagePublicationKey).values(
                status="revoked", revoked_at=NOW + timedelta(seconds=16)
            )
        )
        await retirement.execute(update(TaskImagePublicationState).values(keyset_version=2))
        await retirement.commit()
    async with registry_authority_session() as blocker, registry_authority_session() as history:
        await blocker.scalar(select(TaskImagePublicationState).with_for_update())
        await blocker.scalar(select(TaskImagePublicationKey).with_for_update())
        # Models auth's grant-first order: historical lookup must not lock state/key/job.
        await history.scalar(select(TaskImageBuildGrant).with_for_update())
        async with asyncio.timeout(2):
            assert (
                await completion().replay_completed_publication(
                    history, operation_id=values[0].operation_id
                )
                == receipt
            )


async def test_atomic_completion_preserves_exact_microseconds_and_historical_replay(
    registry_authority_session, registry_issuer
):
    c = completion()
    async with registry_authority_session() as session:
        job, owner, publications, distribution = await _signed_job(session, registry_issuer)
        await session.commit()
        instant = NOW + timedelta(seconds=14, microseconds=999999)
        receipt = await c.complete_publication_job(
            session,
            job=job,
            owner_id=owner,
            generation=1,
            publications=publications,
            distribution=distribution,
            clock=lambda: instant,
        )
        await session.commit()
        row = await session.get(TaskImageMaterialization, job.snapshot.materialization_id)
        assert row.state == "ready" and row.ready_at == instant
        assert row.claimed_by is None and row.lease_expires_at is None
        assert len(row.registry_images) == 1
        assert receipt.completed_at == "2026-09-02T14:00:14Z"
        assert len(list(await session.scalars(select(TaskImagePublicationEnvelope)))) == 1
        assert (
            await c.replay_completed_publication(session, operation_id=job.operation_id) == receipt
        )


async def test_valid_successor_and_heartbeat_during_lock_wait_complete_past_original_lease(
    registry_authority_session, registry_issuer
):
    context = {}
    async with registry_authority_session() as session:
        values = await _signed_job(session, registry_issuer, lease_seconds=5, context=context)
        await session.commit()
    job = values[0]
    async with registry_authority_session() as blocker, registry_authority_session() as worker:
        await blocker.scalar(select(TaskImageBuildGrant).with_for_update())
        pid = await worker.scalar(text("SELECT pg_backend_pid()"))
        task = asyncio.create_task(_complete(worker, values, instant=NOW + timedelta(seconds=16)))
        try:
            await _blocked(blocker, pid, task)
            original = context["build_session"]
            attestation = _attestation(context["proof"], generation=2)
            successor = await renew_task_image_build_session(
                blocker,
                principal=context["principal"],
                request=TaskImageSessionRenewalV1(
                    renewal_id=uuid4(),
                    grant_id=UUID(job.snapshot.grant_id),
                    session_id=original.session_id,
                    session_generation=1,
                    session_token=original.session_token,
                    attestation=attestation,
                    observed_at=attestation.issued_at,
                ),
                now=NOW + timedelta(seconds=14),
                secret_store=context["secrets"],
                session_token_factory=lambda: "loom_tibs_" + "C" * 64,
                session_id_factory=uuid4,
            )
            current = await authorize_task_image_build_session(
                blocker,
                grant_id=UUID(job.snapshot.grant_id),
                session_id=successor.session_id,
                session_generation=2,
                raw_session_token=successor.session_token,
                now=NOW + timedelta(seconds=14),
            )
            assert current.attestation_sha256 != job.snapshot.containment_attestation_sha256
            await heartbeat_session_materialization(
                blocker,
                authorization=current,
                materialization_id=UUID(job.snapshot.materialization_id),
                attempt_id=UUID(job.snapshot.attempt_id),
                lease_epoch=job.snapshot.lease_epoch,
                operation_id=uuid4(),
                now=NOW + timedelta(seconds=14.5),
            )
            await blocker.commit()
            receipt = await task
            await worker.commit()
            assert receipt.snapshot_sha256 == job.snapshot_sha256
        finally:
            await blocker.rollback()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("change", ["receipt", "signature", "candidate"])
async def test_historical_replay_revalidates_immutable_evidence(
    registry_authority_session, registry_issuer, change
):
    async with registry_authority_session() as session:
        values = await _signed_job(session, registry_issuer)
        await session.commit()
        receipt = await _complete(session, values)
        await session.commit()
        # Fault injection only in the disposable DB: ordinary writes remain trigger-fenced.
        table = {
            "receipt": "task_image_publication_jobs",
            "signature": "task_image_publication_envelopes",
            "candidate": "task_image_publication_candidates",
        }[change]
        await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
        if change == "receipt":
            encoded = canonical_receipt_bytes(
                receipt.model_copy(update={"publication_set_sha256": "e" * 64})
            )
            await session.execute(
                update(TaskImagePublicationJob).values(
                    canonical_receipt=encoded, receipt_sha256=hashlib.sha256(encoded).hexdigest()
                )
            )
        elif change == "signature":
            await session.execute(update(TaskImagePublicationEnvelope).values(signature="A" * 86))
        else:
            # The set's IDs remain unchanged; identity-only hashing must not hide evidence drift.
            await session.execute(
                update(TaskImagePublicationCandidate).values(oci_file_sha256="e" * 64)
            )
        await session.commit()
        with pytest.raises((RuntimeError, ValueError)):
            await completion().replay_completed_publication(
                session, operation_id=values[0].operation_id
            )


async def test_completed_receipt_constraints_and_terminal_retention(
    registry_authority_session, registry_issuer
):
    async with registry_authority_session() as session:
        values = await _signed_job(session, registry_issuer)
        await session.commit()
        with pytest.raises(IntegrityError):
            await session.execute(
                update(TaskImagePublicationJob).values(
                    state="completed", worker_id=None, worker_expires_at=None
                )
            )
        await session.rollback()
        receipt = await _complete(session, values)
        await session.commit()
        for changes in (
            {"completed_at": NOW + timedelta(seconds=15)},
            {"canonical_receipt": b"{}"},
            {"receipt_sha256": "e" * 64},
            {"state": "queued"},
        ):
            with pytest.raises(IntegrityError):
                await session.execute(update(TaskImagePublicationJob).values(**changes))
            await session.rollback()
        assert (
            await completion().replay_completed_publication(
                session, operation_id=values[0].operation_id
            )
            == receipt
        )

        # Later materialization history cannot rewrite the immutable job timestamp.
        row = (await session.scalars(select(TaskImageMaterialization))).one()
        row.ready_at = NOW + timedelta(days=1)
        await session.commit()
        assert (
            await completion().replay_completed_publication(
                session, operation_id=values[0].operation_id
            )
            == receipt
        )


async def test_valid_signature_for_different_frozen_input_is_not_authority(
    registry_authority_session, registry_issuer
):
    async with registry_authority_session() as session:
        values = await _signed_job(
            session, registry_issuer, binding_changes={"task_id": "different-task"}
        )
        await session.commit()
        with pytest.raises(RuntimeError, match=r"binding invalid|frozen input"):
            await _complete(session, values)
        await session.rollback()
    await _untouched(registry_authority_session)


@pytest.mark.parametrize("authority", ["grant", "attestation", "materialization", "deadline"])
async def test_completion_rechecks_each_expiry_after_state_lock_wait(
    registry_authority_session, registry_issuer, authority
):
    context = {}
    options = (
        {"lease_seconds": 6}
        if authority == "materialization"
        else {"lifetime_seconds": 2}
        if authority == "deadline"
        else {}
    )
    async with registry_authority_session() as setup:
        values = await _signed_job(setup, registry_issuer, context=context, **options)
        await setup.commit()
    expiry = (
        context["authorization"].grant_expires_at
        if authority == "grant"
        else context["authorization"].attestation_expires_at
        if authority == "attestation"
        else NOW + timedelta(seconds=16)
        if authority == "materialization"
        else values[0].deadline
    )
    now = NOW + timedelta(seconds=14)
    async with registry_authority_session() as blocker, registry_authority_session() as worker:
        await blocker.scalar(select(TaskImagePublicationState).with_for_update())
        pid = await worker.scalar(text("SELECT pg_backend_pid()"))
        task = asyncio.create_task(_complete(worker, values, clock=lambda: now))
        try:
            await _blocked(blocker, pid, task)
            now = expiry
            await blocker.rollback()
            with pytest.raises(RuntimeError):
                await task
        finally:
            await blocker.rollback()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await worker.rollback()
    await _untouched(registry_authority_session)
