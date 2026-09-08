from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
from datetime import timedelta
from uuid import uuid4

import pytest
import rfc8785
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import (
    TaskImageBuildGrant,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImagePublicationCandidate,
    TaskImagePublicationJob,
    TaskImageRegistryCredentialGeneration,
)
from loom_task_image_authority.contracts import (
    TaskImagePublicationCandidateRequestV2,
    TaskImageSessionRenewalV1,
)
from loom_task_image_authority.store import (
    authorize_task_image_build_session,
    renew_task_image_build_session,
)
from tests.integration.test_task_image_candidate_v2 import _prepared, _record
from tests.integration.test_task_image_projection_store import (
    NEXT_SESSION_ID,
    RENEWAL_ID,
    _attestation,
)
from tests.integration.test_task_image_registry_credential_migration import _config
from tests.integration.test_task_image_registry_credentials import (
    NOW,
    _candidate_request,
    _claimed_attempt,
    _issue_first,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)


@pytest.fixture
async def registry_authority_session(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


def store():
    name = "loom_task_image_authority.publication_store"
    assert importlib.util.find_spec(name) is not None, "publication store missing"
    return importlib.import_module(name)


async def _submit(session, issuer, **changes):
    s = store()
    authorization, row, request, _ = await _prepared(session, issuer)
    await _record(session, authorization, request)
    values = dict(
        authorization=authorization,
        operation_id=uuid4(),
        materialization_id=row.id,
        attempt_id=request.attempt_id,
        lease_epoch=request.lease_epoch,
        registry_origin="https://registry.example:5443",
        clock=lambda: NOW + timedelta(seconds=14),
    )
    values.update(changes)
    return await s.submit_publication_job(session, **values), values


async def test_submit_replay_claim_and_fencing(registry_authority_session, registry_issuer):
    s = store()
    async with registry_authority_session() as session:
        job, values = await _submit(session, registry_issuer)
        assert job.state == "queued"
        replay = await s.submit_publication_job(session, **values)
        assert replay == job
        owner = uuid4()
        running = await s.claim_publication_job(
            session, operation_id=values["operation_id"], owner_id=owner, clock=values["clock"]
        )
        assert running.lease.generation == 1
        with pytest.raises(s.PublicationJobOwnershipError):
            await s.claim_publication_job(
                session,
                operation_id=values["operation_id"],
                owner_id=uuid4(),
                clock=values["clock"],
            )
        expiry = running.lease.expires_at
        with pytest.raises(s.PublicationJobOwnershipError):
            await s.renew_publication_job(
                session,
                operation_id=values["operation_id"],
                owner_id=owner,
                generation=1,
                clock=lambda: expiry,
            )
        takeover = await s.claim_publication_job(
            session, operation_id=values["operation_id"], owner_id=uuid4(), clock=lambda: expiry
        )
        assert takeover.lease.generation == 2
        with pytest.raises(s.PublicationJobOwnershipError):
            await s.release_publication_job(
                session,
                operation_id=values["operation_id"],
                owner_id=owner,
                generation=1,
                clock=lambda: expiry,
            )
        assert (
            await s.read_publication_job(session, operation_id=values["operation_id"])
        ).snapshot == job.snapshot


async def _blocked(blocker, pid, task):
    async with asyncio.timeout(5):
        while not await blocker.scalar(
            text("SELECT cardinality(pg_blocking_pids(:pid)) > 0"), {"pid": pid}
        ):
            if task.done():
                await task
                pytest.fail("operation did not wait for the required row lock")
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("conflict", [False, True])
async def test_concurrent_submit_database_uniqueness(
    registry_authority_session, registry_issuer, conflict
):
    s = store()
    async with registry_authority_session() as setup:
        authorization, row, request, _ = await _prepared(setup, registry_issuer)
        await _record(setup, authorization, request)
        values = dict(
            authorization=authorization,
            operation_id=uuid4(),
            materialization_id=row.id,
            attempt_id=request.attempt_id,
            lease_epoch=request.lease_epoch,
            registry_origin="https://registry.example:5443",
            clock=lambda: NOW + timedelta(seconds=14),
        )
        await setup.commit()
    async with registry_authority_session() as first, registry_authority_session() as second:
        job = await s.submit_publication_job(first, **values)
        pid = await second.scalar(text("SELECT pg_backend_pid()"))
        other = dict(values, operation_id=uuid4()) if conflict else values
        task = asyncio.create_task(s.submit_publication_job(second, **other))
        try:
            await _blocked(first, pid, task)
            await first.commit()
            if conflict:
                with pytest.raises(s.PublicationJobConflictError):
                    await task
            else:
                assert await task == job
                await second.commit()
        finally:
            await first.rollback()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    async with registry_authority_session() as session:
        assert len(list(await session.scalars(select(TaskImagePublicationJob)))) == 1


@pytest.mark.parametrize("advance", [False, True])
async def test_simultaneous_claim_resamples_after_job_lock(
    registry_authority_session, registry_issuer, advance
):
    s = store()
    async with registry_authority_session() as setup:
        _, values = await _submit(setup, registry_issuer)
        await setup.commit()
    now = NOW + timedelta(seconds=14)
    async with registry_authority_session() as first, registry_authority_session() as second:
        running = await s.claim_publication_job(
            first, operation_id=values["operation_id"], owner_id=uuid4(), clock=lambda: now
        )
        pid = await second.scalar(text("SELECT pg_backend_pid()"))
        task = asyncio.create_task(
            s.claim_publication_job(
                second, operation_id=values["operation_id"], owner_id=uuid4(), clock=lambda: now
            )
        )
        try:
            await _blocked(first, pid, task)
            if advance:
                now = running.lease.expires_at
            await first.commit()
            if advance:
                assert (await task).lease.generation == 2
            else:
                with pytest.raises(s.PublicationJobOwnershipError):
                    await task
        finally:
            await first.rollback()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize(
    "model",
    [
        TaskImageMaterialization,
        TaskImageMaterializationAttempt,
        TaskImageRegistryCredentialGeneration,
        TaskImagePublicationCandidate,
        TaskImagePublicationJob,
    ],
)
async def test_submit_expiry_after_remaining_lock_wait(
    registry_authority_session, registry_issuer, model
):
    s = store()
    async with registry_authority_session() as setup:
        job, values = await _submit(setup, registry_issuer)
        await setup.commit()
    now = NOW + timedelta(seconds=14)
    values["clock"] = lambda: now
    async with registry_authority_session() as blocker, registry_authority_session() as worker:
        await blocker.scalar(select(model).with_for_update())
        pid = await worker.scalar(text("SELECT pg_backend_pid()"))
        task = asyncio.create_task(s.submit_publication_job(worker, **values))
        try:
            await _blocked(blocker, pid, task)
            now = values["authorization"].attestation_expires_at + timedelta(microseconds=1)
            await blocker.rollback()
            with pytest.raises(s.PublicationJobAuthorizationError):
                await task
        finally:
            await blocker.rollback()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    async with registry_authority_session() as session:
        stored = await s.read_publication_job(session, operation_id=values["operation_id"])
        assert stored == job
        row = await session.get(TaskImageMaterialization, values["materialization_id"])
        assert row.ready_at is None and not row.registry_images


async def test_retry_backoff_safe_failure_and_immutable_deadline(
    registry_authority_session, registry_issuer
):
    s = store()
    async with registry_authority_session() as session:
        job, values = await _submit(session, registry_issuer)
        now = NOW + timedelta(seconds=14)
        owner = uuid4()
        args = dict(operation_id=values["operation_id"], owner_id=owner, clock=lambda: now)
        running = await s.claim_publication_job(session, **args, lease_seconds=300)
        renewed = await s.renew_publication_job(session, **args, generation=1, lease_seconds=300)
        assert renewed.deadline == job.deadline
        released = await s.release_publication_job(session, **args, generation=1)
        assert released.available_at == now + timedelta(seconds=5)
        with pytest.raises(s.PublicationJobOwnershipError):
            await s.claim_publication_job(session, **args)
        now = released.available_at
        next_job = await s.claim_publication_job(session, **args)
        assert next_job.lease.generation == 2
        for stale in (dict(owner_id=uuid4(), generation=2), dict(owner_id=owner, generation=1)):
            for method in (
                s.renew_publication_job,
                s.release_publication_job,
                s.fail_publication_job,
            ):
                extra = {"failure_code": "integrity"} if method == s.fail_publication_job else {}
                with pytest.raises(s.PublicationJobOwnershipError):
                    await method(
                        session,
                        operation_id=values["operation_id"],
                        clock=lambda: now,
                        **stale,
                        **extra,
                    )
        with pytest.raises(ValueError):
            await s.fail_publication_job(
                session, **args, generation=2, failure_code="untrusted registry body"
            )
        failed = await s.fail_publication_job(
            session, **args, generation=2, failure_code="integrity"
        )
        assert failed.failure_code == "integrity"
        assert (await s.submit_publication_job(session, **values)).state == "failed"
        with pytest.raises(s.PublicationJobOwnershipError):
            await s.claim_publication_job(session, **args)
        assert running.snapshot == failed.snapshot == job.snapshot


@pytest.mark.parametrize("method", ["claim", "renew", "release"])
async def test_exact_total_deadline_revokes_ownership(
    registry_authority_session, registry_issuer, method
):
    s = store()
    async with registry_authority_session() as session:
        job, values = await _submit(session, registry_issuer)
        owner = uuid4()
        await s.claim_publication_job(
            session, operation_id=values["operation_id"], owner_id=owner, clock=values["clock"]
        )
        args = dict(
            operation_id=values["operation_id"],
            owner_id=owner if method != "claim" else uuid4(),
            clock=lambda: job.deadline,
        )
        if method != "claim":
            args["generation"] = 1
        with pytest.raises(s.PublicationJobOwnershipError):
            await getattr(s, method + "_publication_job")(session, **args)


@pytest.mark.parametrize(
    "model,field,value",
    [
        (TaskImageMaterialization, "task_checksum", "e" * 64),
        (TaskImageMaterializationAttempt, "claim_plan_sha256", "e" * 64),
        (TaskImageRegistryCredentialGeneration, "response_sha256", "e" * 64),
        (TaskImagePublicationCandidate, "manifest_size", 999),
        (TaskImagePublicationJob, "snapshot_sha256", "e" * 64),
    ],
)
async def test_suppressed_autoflush_never_discards_dirty_authority(
    registry_authority_session, registry_issuer, model, field, value
):
    s = store()
    async with registry_authority_session() as session:
        _, values = await _submit(session, registry_issuer)
        row = await session.scalar(select(model))
        setattr(row, field, value)
        with session.no_autoflush, pytest.raises(s.PublicationJobConflictError):
            await s.submit_publication_job(session, **values)
        assert getattr(row, field) == value


@pytest.mark.parametrize(
    "corruption",
    [
        "plan_task",
        "plan_checksum",
        "plan_arch",
        "candidate_hash",
        "candidate_evidence",
        "credential_origin",
        "credential_schema",
        "missing_candidate",
        "v1",
    ],
)
async def test_replay_revalidates_frozen_semantics(
    registry_authority_session, registry_issuer, corruption
):
    s = store()
    async with registry_authority_session() as session:
        job, values = await _submit(session, registry_issuer)
        await session.commit()
        if corruption.startswith("plan_"):
            attempt = await session.scalar(select(TaskImageMaterializationAttempt))
            payload = json.loads(json.dumps(attempt.claim_plan_json))
            if corruption == "plan_task":
                payload["task_id"] = "other-task"
            elif corruption == "plan_checksum":
                payload["task_checksum"] = "e" * 64
            else:
                payload["cpu_arch"], payload["platform"] = "x86_64", "linux/amd64"
            attempt.claim_plan_json = payload
            attempt.claim_plan_sha256 = hashlib.sha256(rfc8785.dumps(payload)).hexdigest()
        elif corruption in ("credential_origin", "credential_schema"):
            credential = await session.scalar(select(TaskImageRegistryCredentialGeneration))
            if corruption == "credential_origin":
                credential.registry_origin = "https://evil.example"
            else:
                payload = dict(credential.response_public_json, schema_version=2)
                credential.response_public_json = payload
                credential.response_sha256 = hashlib.sha256(rfc8785.dumps(payload)).hexdigest()
        else:
            candidate = await session.scalar(select(TaskImagePublicationCandidate))
            if corruption == "missing_candidate":
                await session.delete(candidate)
            elif corruption == "candidate_hash":
                candidate.response_sha256 = "e" * 64
            else:
                payload = json.loads(json.dumps(candidate.response_json))
                if corruption == "candidate_evidence":
                    payload["base_resolution"]["solve_ref"] = "different"
                else:
                    payload["schema_version"] = "loom.task-image-publication-candidate.v1"
                    del payload["base_resolution"]
                candidate.response_json = payload
                candidate.response_sha256 = hashlib.sha256(rfc8785.dumps(payload)).hexdigest()
        await session.flush()
        with pytest.raises((s.PublicationJobConflictError, ValueError, RuntimeError)):
            await s.submit_publication_job(session, **values)
        assert (
            await session.scalar(select(TaskImagePublicationJob))
        ).canonical_snapshot == s.canonical_snapshot_bytes(job.snapshot)
        row = await session.get(TaskImageMaterialization, values["materialization_id"])
        assert not row.registry_images and row.ready_at is None


async def test_successor_session_replay_preserves_initial_containment(
    registry_authority_session, registry_issuer
):
    s = store()
    async with registry_authority_session() as session:
        (
            authorization,
            principal,
            proof,
            build_session,
            secrets,
            row,
            attempt,
        ) = await _claimed_attempt(session)
        _, credential = await _issue_first(
            session,
            authorization=authorization,
            build_session=build_session,
            secrets=secrets,
            row=row,
            attempt=attempt,
            issuer=registry_issuer,
        )
        legacy = _candidate_request(
            authorization,
            build_session,
            row,
            attempt,
            credential_id=credential.credential_id,
            credential_generation=1,
        )
        request = TaskImagePublicationCandidateRequestV2.model_validate(
            dict(
                legacy.model_dump(),
                schema_version=2,
                base_resolution={
                    "schema": "loom.task-image-base-resolution/v1",
                    "solve_ref": "original-solve",
                    "platform": "linux/arm64",
                    "output_digest": legacy.manifest_digest,
                    "observed_base_digests": [],
                },
            )
        )
        await _record(session, authorization, request)
        values = dict(
            authorization=authorization,
            operation_id=uuid4(),
            materialization_id=row.id,
            attempt_id=attempt.id,
            lease_epoch=attempt.lease_epoch,
            registry_origin="https://registry.example:5443",
            clock=lambda: NOW + timedelta(seconds=12),
        )
        original = await s.submit_publication_job(session, **values)
        await session.commit()
        attestation = _attestation(proof, generation=2)
        successor = await renew_task_image_build_session(
            session,
            principal=principal,
            request=TaskImageSessionRenewalV1(
                renewal_id=RENEWAL_ID,
                grant_id=authorization.grant_id,
                session_id=build_session.session_id,
                session_generation=1,
                session_token=build_session.session_token,
                attestation=attestation,
                observed_at=attestation.issued_at,
            ),
            now=NOW + timedelta(seconds=13),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "C" * 64,
            session_id_factory=lambda: NEXT_SESSION_ID,
        )
        await session.commit()
        current = await authorize_task_image_build_session(
            session,
            grant_id=authorization.grant_id,
            session_id=successor.session_id,
            session_generation=2,
            raw_session_token=successor.session_token,
            now=NOW + timedelta(seconds=14),
        )
        assert current.attestation_sha256 != original.snapshot.containment_attestation_sha256
        with pytest.raises(s.PublicationJobAuthorizationError):
            await s.submit_publication_job(
                session, **dict(values, clock=lambda: NOW + timedelta(seconds=14))
            )
        replay = await s.submit_publication_job(
            session,
            **dict(values, authorization=current, clock=lambda: NOW + timedelta(seconds=14)),
        )
        assert replay == original
        assert replay.snapshot.original_claim_session_id == str(build_session.session_id)


@pytest.mark.parametrize(
    "model,values",
    [
        (TaskImageMaterialization, {"task_checksum": "e" * 64}),
        (TaskImageMaterializationAttempt, {"claim_plan_sha256": "e" * 64}),
        (TaskImageRegistryCredentialGeneration, {"response_sha256": "e" * 64}),
        (TaskImagePublicationCandidate, {"response_sha256": "e" * 64}),
    ],
)
async def test_clean_preloaded_rows_refresh_under_lock(
    registry_authority_session, registry_issuer, model, values
):
    s = store()
    async with registry_authority_session() as session:
        _, args = await _submit(session, registry_issuer)
        await session.commit()
    async with registry_authority_session() as worker, registry_authority_session() as writer:
        cached = await worker.scalar(select(model))
        assert cached is not None
        await writer.execute(update(model).values(**values))
        await writer.commit()
        with pytest.raises((s.PublicationJobConflictError, RuntimeError)):
            await s.submit_publication_job(worker, **args)


@pytest.mark.parametrize("bad", [True, 0, -1, float("inf"), float("nan"), 301])
async def test_worker_duration_validation(registry_authority_session, registry_issuer, bad):
    s = store()
    async with registry_authority_session() as session:
        _, values = await _submit(session, registry_issuer)
        args = dict(operation_id=values["operation_id"], owner_id=uuid4(), clock=values["clock"])
        for method, keyword in (
            (s.claim_publication_job, "lease_seconds"),
            (s.renew_publication_job, "lease_seconds"),
            (s.release_publication_job, "retry_delay_seconds"),
        ):
            extra = {} if method == s.claim_publication_job else {"generation": 1}
            with pytest.raises(ValueError):
                await method(session, **args, **extra, **{keyword: bad})


async def test_generation_exhaustion_and_nonboolean_fence(
    registry_authority_session, registry_issuer
):
    s = store()
    async with registry_authority_session() as session:
        _, values = await _submit(session, registry_issuer)
        owner = uuid4()
        args = dict(operation_id=values["operation_id"], owner_id=owner, clock=values["clock"])
        await s.claim_publication_job(session, **args)
        with pytest.raises(ValueError):
            await s.renew_publication_job(session, **args, generation=True)
        await s.release_publication_job(session, **args, generation=1)
        await session.execute(
            update(TaskImagePublicationJob).values(worker_generation=9007199254740991)
        )
        args["clock"] = lambda: NOW + timedelta(seconds=20)
        with pytest.raises(s.PublicationJobOwnershipError):
            await s.claim_publication_job(session, **args)


async def test_migration_job_immutability_constraints_and_used_downgrade(
    registry_authority_session, registry_issuer, isolated_migration_postgres_url
):
    from alembic import command
    from sqlalchemy import create_engine, inspect
    from sqlalchemy.exc import DBAPIError

    async with registry_authority_session() as session:
        await _submit(session, registry_issuer)
        await session.commit()
    engine = create_engine(isolated_migration_postgres_url)
    try:
        inspector = inspect(engine)
        assert {
            column["name"] for column in inspector.get_columns("task_image_publication_jobs")
        } == set(TaskImagePublicationJob.__table__.columns.keys())
        fks = inspector.get_foreign_keys("task_image_publication_jobs")
        assert len(fks) == 1 and fks[0]["options"]["ondelete"] == "RESTRICT"
        assert len(fks[0]["constrained_columns"]) == 6
        for statement in (
            "UPDATE task_image_publication_jobs SET deadline = deadline + interval '1 second'",
            "UPDATE task_image_publication_jobs SET operation_id = gen_random_uuid()",
            "UPDATE task_image_publication_jobs SET canonical_snapshot = '{}'::bytea",
            "UPDATE task_image_publication_jobs SET worker_generation = -1",
            "UPDATE task_image_publication_jobs SET state = 'running'",
            "UPDATE task_image_publication_jobs SET state = 'failed'",
            "UPDATE task_image_publication_jobs SET state = 'failed', failure_code = 'registry body'",
            "DELETE FROM task_image_publication_jobs",
            "DELETE FROM task_image_materialization_attempts",
        ):
            with pytest.raises(DBAPIError), engine.begin() as connection:
                connection.execute(text(statement))
        with pytest.raises(DBAPIError, match="publication authority cannot be discarded"):
            command.downgrade(_config(isolated_migration_postgres_url), "0132")
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM task_image_publication_jobs")) == 1
            assert connection.scalar(text("SELECT count(*) FROM task_image_publication_keys")) == 0
            assert (
                connection.scalar(text("SELECT count(*) FROM task_image_publication_envelopes"))
                == 0
            )
    finally:
        engine.dispose()


@pytest.mark.parametrize("corruption", ["digest", "binding", "duplicate", "unknown"])
async def test_every_durable_job_read_rejects_snapshot_corruption(
    registry_authority_session, registry_issuer, corruption
):
    s = store()
    async with registry_authority_session() as session:
        _, args = await _submit(session, registry_issuer)
        stored = await session.scalar(select(TaskImagePublicationJob))
        encoded = stored.canonical_snapshot
        if corruption == "binding":
            payload = json.loads(encoded)
            payload["task_id"] = "different-task"
            payload["materialization_id"] = str(uuid4())
            encoded = rfc8785.dumps(payload)
        elif corruption == "duplicate":
            encoded = encoded.replace(b'"lease_epoch":1', b'"lease_epoch":1,"lease_epoch":1', 1)
        elif corruption == "unknown":
            encoded = encoded[:-1] + b',"unknown":true}'
        # Isolated corruption injection only: production triggers correctly reject
        # these rewrites. Digest corruption additionally bypasses its CHECK.
        await session.execute(
            text(
                "ALTER TABLE task_image_publication_jobs DISABLE TRIGGER task_image_publication_jobs_preserve"
            )
        )
        if corruption == "digest":
            await session.execute(
                text(
                    "ALTER TABLE task_image_publication_jobs DROP CONSTRAINT task_image_publication_jobs_snapshot_check"
                )
            )
        await session.execute(
            update(TaskImagePublicationJob).values(
                canonical_snapshot=encoded,
                snapshot_sha256="e" * 64
                if corruption == "digest"
                else hashlib.sha256(encoded).hexdigest(),
            )
        )
        await session.execute(
            text(
                "ALTER TABLE task_image_publication_jobs ENABLE TRIGGER task_image_publication_jobs_preserve"
            )
        )
        for method in (s.read_publication_job, s.claim_publication_job):
            extra = (
                {}
                if method == s.read_publication_job
                else {"owner_id": uuid4(), "clock": args["clock"]}
            )
            with pytest.raises(s.PublicationJobConflictError):
                await method(session, operation_id=args["operation_id"], **extra)


async def test_renewal_transaction_never_waits_on_grant(
    registry_authority_session, registry_issuer
):
    s = store()
    async with registry_authority_session() as setup:
        job, values = await _submit(setup, registry_issuer)
        owner = uuid4()
        await s.claim_publication_job(
            setup, operation_id=values["operation_id"], owner_id=owner, clock=values["clock"]
        )
        await setup.commit()
    async with registry_authority_session() as blocker, registry_authority_session() as worker:
        await blocker.scalar(select(TaskImageBuildGrant).with_for_update())
        # A reversed grant acquisition deadlocks against blocker and times out.
        renewed = await asyncio.wait_for(
            s.renew_publication_job(
                worker,
                operation_id=values["operation_id"],
                owner_id=owner,
                generation=1,
                clock=lambda: NOW + timedelta(seconds=20),
            ),
            timeout=3,
        )
        assert renewed.lease.expires_at == NOW + timedelta(seconds=80)
        assert renewed.snapshot == job.snapshot


@pytest.mark.parametrize("lifetime", [30, 7200])
async def test_total_deadline_and_worker_expiry_are_capped(
    registry_authority_session, registry_issuer, lifetime
):
    s = store()
    async with registry_authority_session() as session:
        job, values = await _submit(session, registry_issuer, lifetime_seconds=lifetime)
        expected = min(
            job.created_at + timedelta(seconds=lifetime), values["authorization"].grant_expires_at
        )
        assert job.deadline == expected
        running = await s.claim_publication_job(
            session, operation_id=values["operation_id"], owner_id=uuid4(), clock=values["clock"]
        )
        assert running.lease.expires_at == min(job.created_at + timedelta(seconds=60), expected)
        for invalid in (True, 0, -1, float("inf"), float("nan"), 7201):
            with pytest.raises(ValueError):
                await s.submit_publication_job(session, **dict(values, lifetime_seconds=invalid))
