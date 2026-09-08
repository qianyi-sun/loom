from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import event, select, text, update

from loom.db.schema import (
    TaskImageBuildGrant,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImagePublicationEnvelope,
    TaskImagePublicationJob,
    TaskImagePublicationKey,
    TaskImagePublicationState,
)
from loom_task_image_authority.contracts import TaskImagePublicationCandidateRequestV2
from loom_task_image_authority.materializations import claim_session_materialization
from loom_task_image_authority.publication_contracts import (
    PublicationEnvelope,
    PublicationUnsignedInput,
    canonical_publication_bytes,
)
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
from tests.integration.test_task_image_authority_materializations import (
    _active_authorization,
    _queued_materialization,
)
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


async def _queued_job(session, issuer, *, names=("task",), root=None, lease_seconds=300):
    authorization, _, _, build_session, secrets = await _active_authorization(session)
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
        clock=lambda: NOW + timedelta(seconds=14),
    )


async def _signed_job(session, issuer, **options):
    job = await _queued_job(session, issuer, **options)
    owner = uuid4()
    job = await claim_publication_job(
        session,
        operation_id=UUID(job.operation_id),
        owner_id=owner,
        clock=lambda: NOW + timedelta(seconds=14),
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
        _sign(job, component, private, key, distribution) for component in job.snapshot.components
    )
    return job, owner, publications, distribution


def _sign(job, component, private, key, distribution):
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
        values = await _signed_job(session, registry_issuer, names=("task", "sidecar:db"))
        await session.commit()
        now = NOW + timedelta(seconds=14)

        def after_flush(sync_session, context):
            nonlocal now
            rows = sync_session.new if stage == "envelopes" else sync_session.dirty
            if any(
                isinstance(
                    row,
                    TaskImagePublicationEnvelope
                    if stage == "envelopes"
                    else TaskImageMaterialization,
                )
                for row in rows
            ):
                now = values[0].lease.expires_at

        event.listen(session.sync_session, "after_flush", after_flush)
        with pytest.raises(RuntimeError, match=r"expired|fence lost"):
            await _complete(session, values, clock=lambda: now)
        await session.rollback()
    await _untouched(registry_authority_session)


@pytest.mark.parametrize(
    "model",
    [
        TaskImagePublicationState,
        TaskImagePublicationKey,
        TaskImageBuildGrant,
        TaskImageMaterialization,
        TaskImageMaterializationAttempt,
        TaskImagePublicationJob,
    ],
)
async def test_completion_expiry_after_real_lock_wait(
    registry_authority_session, registry_issuer, model
):
    async with registry_authority_session() as setup:
        values = await _signed_job(setup, registry_issuer)
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
        # Subsequent materialization history must not overwrite the immutable job timestamp.
        row.ready_at = NOW + timedelta(days=1)
        await session.commit()
        assert (
            await c.replay_completed_publication(session, operation_id=job.operation_id) == receipt
        )
