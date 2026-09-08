from __future__ import annotations

import base64
import hashlib
import importlib
from datetime import timedelta
from uuid import uuid4

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import select, update

from loom.db.schema import (
    TaskImageMaterialization,
    TaskImagePublicationEnvelope,
    TaskImagePublicationKey,
    TaskImagePublicationState,
)
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
from loom_task_image_authority.publication_store import claim_publication_job
from tests.integration.test_task_image_publication_jobs import _submit
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import NOW
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)
from tests.unit.test_task_image_publication_contracts import unsigned_payload


def completion():
    name = "loom_task_image_authority.publication_completion"
    assert importlib.util.find_spec(name) is not None, "atomic publication completion missing"
    return importlib.import_module(name)


async def _signed_job(session, issuer):
    job, args = await _submit(session, issuer)
    owner = uuid4()
    job = await claim_publication_job(
        session, operation_id=args["operation_id"], owner_id=owner, clock=args["clock"]
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
    component = job.snapshot.components[0]
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
    return job, owner, (VerifiedPublication(envelope, statement),), distribution


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
