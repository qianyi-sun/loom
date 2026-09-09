from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import update

from loom.db.schema import (
    TaskImageMaterialization,
    TaskImagePublicationKey,
    TaskImagePublicationState,
)
from loom_control_plane.task_image_materializations import retry_task_image_materialization
from loom_task_image_authority.publication_signing import (
    DistributedKeysetSnapshot,
    PublicationKeyRecord,
    PublicationState,
)
from loom_task_image_authority.publication_store import claim_publication_job
from tests.integration import test_task_image_publication_completion as fixtures
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import NOW
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)
from tests.integration.test_task_image_retirement_store import observe


@pytest.mark.parametrize("names", [("task", "sidecar:db"), ("sidecar:db",)])
async def test_retirement_clears_complete_multicomponent_map(
    registry_authority_session,
    registry_issuer,
    names,
):
    factory = registry_authority_session
    async with factory() as session:
        receipt = await fixtures._complete(
            session, await fixtures._signed_job(session, registry_issuer, names=names)
        )
        await session.commit()
    attempt_id = UUID(receipt.attempt_id)
    instant = NOW + timedelta(hours=1)
    assert (await observe(factory, attempt_id, instant)).status == "observing"
    assert (await observe(factory, attempt_id, instant + timedelta(days=7))).status == "retired"
    async with factory() as session:
        row = await session.get(TaskImageMaterialization, UUID(receipt.materialization_id))
        assert row.state == "retired" and not row.registry_images
        assert (
            await fixtures.completion().replay_completed_publication(
                session, operation_id=receipt.operation_id
            )
            == receipt
        )


async def test_retiring_old_attempt_preserves_actual_newer_rootless_ready_owner(
    registry_authority_session,
    registry_issuer,
    monkeypatch,
):
    factory = registry_authority_session
    context = {}
    async with factory() as session:
        first = await fixtures._complete(
            session, await fixtures._signed_job(session, registry_issuer, context=context)
        )
        await session.commit()
    attempt_id, row_id = UUID(first.attempt_id), UUID(first.materialization_id)
    instant = NOW + timedelta(hours=1)
    await observe(factory, attempt_id, instant)

    async def existing_authority(session):
        return tuple(
            context[name]
            for name in ("authorization", "principal", "proof", "build_session", "secrets")
        )

    async def existing_materialization(session):
        return await session.get(TaskImageMaterialization, row_id)

    monkeypatch.setattr(fixtures, "_active_authorization", existing_authority)
    monkeypatch.setattr(fixtures, "_queued_materialization", existing_materialization)
    async with factory() as session:
        await retry_task_image_materialization(session, materialization_id=row_id)
        await session.commit()
    async with factory() as session:
        queued = await fixtures._queued_job(session, registry_issuer)
        owner = uuid4()
        job = await claim_publication_job(
            session,
            operation_id=UUID(queued.operation_id),
            owner_id=owner,
            clock=lambda: NOW + timedelta(seconds=14),
        )
        private = Ed25519PrivateKey.generate()
        key = PublicationKeyRecord("publication-2", private.public_key().public_bytes_raw(), NOW)
        session.add(
            TaskImagePublicationKey(
                key_id=key.key_id, public_key=key.public_key, activated_at=NOW, status="active"
            )
        )
        await session.execute(update(TaskImagePublicationState).values(keyset_version=2))
        await session.flush()
        distribution = DistributedKeysetSnapshot(
            2, 0, (key.key_id,), NOW, NOW + timedelta(minutes=10)
        )
        publications = tuple(
            fixtures._sign(job, item, private, key, distribution, state=PublicationState(0, 2))
            for item in job.snapshot.components
        )
        second = await fixtures._complete(session, (job, owner, publications, distribution))
        await session.commit()
        row = await session.get(TaskImageMaterialization, row_id)
        before = (
            row.state,
            row.ready_publication_operation_id,
            row.ready_at,
            row.registry_images,
            row.registry_image_history,
        )
    assert second.attempt_id != first.attempt_id
    assert (await observe(factory, attempt_id, instant + timedelta(days=7))).status == "retired"
    async with factory() as session:
        row = await session.get(TaskImageMaterialization, row_id)
        assert (
            row.state,
            row.ready_publication_operation_id,
            row.ready_at,
            row.registry_images,
            row.registry_image_history,
        ) == before
        for receipt in (first, second):
            assert (
                await fixtures.completion().replay_completed_publication(
                    session, operation_id=receipt.operation_id
                )
                == receipt
            )


async def test_running_job_pins_after_worker_lease_until_total_deadline(
    registry_authority_session,
    registry_issuer,
):
    factory = registry_authority_session
    async with factory() as session:
        job, _, _, _ = await fixtures._signed_job(session, registry_issuer)
        await session.commit()
    attempt_id = UUID(job.snapshot.attempt_id)
    assert job.lease.expires_at < job.deadline - timedelta(seconds=1)
    assert (await observe(factory, attempt_id, job.deadline - timedelta(seconds=1))).pins == (
        "publication_job",
    )
    assert (await observe(factory, attempt_id, job.deadline)).status == "observing"
