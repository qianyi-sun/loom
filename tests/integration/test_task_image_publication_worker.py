from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import UUID

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import select, update

from loom.db.schema import (
    TaskImageBuildGrant,
    TaskImageMaterialization,
    TaskImagePublicationEnvelope,
    TaskImagePublicationJob,
    TaskImagePublicationKey,
    TaskImagePublicationState,
)
from loom_task_image_authority.publication_contracts import (
    PUBLICATION_DOMAIN,
    PublicationEnvelope,
    canonical_publication_bytes,
    decode_unsigned_input,
)
from loom_task_image_authority.publication_signing import (
    DistributedKeysetSnapshot,
    PublicationKeyRecord,
    PublicationState,
    prepare_publication_statement,
)
from tests.integration.test_task_image_publication_completion import _queued_job
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import NOW
from tests.unit.test_task_image_oci_verification import _graph
from tests.unit.test_task_image_registry_reader import _issuer, _Response
from tests.unit.test_task_image_registry_reader import tls_registry as tls_registry
from tests.unit.test_task_image_registry_reader import token_key as token_key


def worker_module():
    name = "loom_task_image_authority.publication_worker"
    assert importlib.util.find_spec(name) is not None, "renewable publication worker missing"
    return importlib.import_module(name)


class Distribution:
    def __init__(self, value):
        self.value = value

    async def snapshot(self, *, state, key):
        return self.value


class Signer:
    def __init__(self, private, key, distribution, clock):
        self.private, self.key, self.distribution, self.clock = private, key, distribution, clock
        self.entered = asyncio.Event()
        self.proceed = asyncio.Event()
        self.proceed.set()
        self.closed = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.calls = 0

    @asynccontextmanager
    async def open(self):
        try:
            yield self
        finally:
            self.closed.set()

    async def sign_publication(self, canonical_unsigned_input, *, maximum_reply_bytes):
        self.calls += 1
        self.entered.set()
        try:
            await self.proceed.wait()
            statement = prepare_publication_statement(
                decode_unsigned_input(canonical_unsigned_input),
                key=self.key,
                state=PublicationState(0, 1),
                distribution=self.distribution,
                signer_now=self.clock().replace(microsecond=0),
            )
            canonical = canonical_publication_bytes(statement)
            reply = canonical_publication_bytes(
                PublicationEnvelope(
                    canonical_statement=canonical.decode(),
                    statement_sha256=hashlib.sha256(canonical).hexdigest(),
                    key_id=self.key.key_id,
                    algorithm="Ed25519",
                    signature=base64.urlsafe_b64encode(
                        self.private.sign(PUBLICATION_DOMAIN + canonical)
                    )
                    .rstrip(b"=")
                    .decode(),
                )
            )
            assert len(reply) <= maximum_reply_bytes
            return reply
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


async def _prepared(factory, registry, token_key, *, names=("task",), delay=0):
    reader, root, _ = _graph(arch="arm64")
    issuer = _issuer(registry, token_key)
    private = Ed25519PrivateKey.generate()
    key = PublicationKeyRecord("publication-1", private.public_key().public_bytes_raw(), NOW)
    distribution = DistributedKeysetSnapshot(1, 0, (key.key_id,), NOW, NOW + timedelta(minutes=10))
    async with factory() as session:
        job = await _queued_job(session, issuer, names=names, root=root)
        session.add(
            TaskImagePublicationKey(
                key_id=key.key_id,
                public_key=key.public_key,
                activated_at=key.activated_at,
                status="active",
            )
        )
        await session.execute(update(TaskImagePublicationState).values(keyset_version=1))
        await session.commit()
    for component in job.snapshot.components:
        for digest, payload in reader.objects.items():
            kind = "manifest" if digest == root.digest else "blob"
            registry.routes[f"/v2/{component.candidate.repository}/{kind}s/{digest}"] = _Response(
                headers=[
                    (
                        "Content-Type",
                        root.media_type if kind == "manifest" else "application/octet-stream",
                    ),
                    ("Docker-Content-Digest", digest),
                ],
                chunks=(payload[: len(payload) // 2], payload[len(payload) // 2 :]),
                chunk_delay=delay,
            )
    started = time.monotonic()

    def clock():
        return NOW + timedelta(seconds=14 + time.monotonic() - started)

    signer = Signer(private, key, distribution, clock)
    return job, issuer, signer, Distribution(distribution), clock


def _worker(factory, registry, values, **limits):
    w = worker_module()
    _, issuer, signer, distribution, clock = values
    return w.PublicationWorker(
        sessions=factory,
        token_issuer=issuer,
        ca_file=registry.ca_file,
        key_id=signer.key.key_id,
        signer_factory=signer.open,
        distribution=distribution,
        clock=clock,
        limits=w.PublicationWorkerLimits(**limits),
    )


async def _renewed_unlocked(factory, initial):
    async with asyncio.timeout(5):
        while True:
            async with factory() as session:
                job = (await session.scalars(select(TaskImagePublicationJob))).one()
                expiry = job.worker_expires_at
                if expiry is not None and expiry > initial:
                    for model in (
                        TaskImagePublicationState,
                        TaskImagePublicationKey,
                        TaskImageBuildGrant,
                        TaskImageMaterialization,
                        TaskImagePublicationJob,
                    ):
                        await session.scalar(select(model).with_for_update(nowait=True))
                    return expiry
            await asyncio.sleep(0.01)


async def test_real_streaming_and_signing_renew_without_authority_locks(
    registry_authority_session, tls_registry, token_key
):
    values = await _prepared(
        registry_authority_session,
        tls_registry,
        token_key,
        names=("task", "sidecar:cache"),
        delay=0.08,
    )
    job, _, signer, _, _ = values
    signer.proceed.clear()
    worker = _worker(
        registry_authority_session,
        tls_registry,
        values,
        lease_seconds=0.6,
        renewal_interval_seconds=0.08,
    )
    task = asyncio.create_task(worker.run(UUID(job.operation_id)))
    try:
        await asyncio.wait_for(tls_registry.first_chunk_sent.wait(), 5)
        first = await _renewed_unlocked(registry_authority_session, NOW + timedelta(seconds=14.7))
        await asyncio.wait_for(signer.entered.wait(), 5)
        await _renewed_unlocked(registry_authority_session, first)
        signer.proceed.set()
        receipt = await asyncio.wait_for(task, 5)
        assert receipt.component_count == 2 and signer.calls == 2 and signer.closed.is_set()
        assert len(tls_registry.requests) == 6
        # Re-running an exact completed operation is historical confirmation, no I/O.
        assert await worker.run(UUID(job.operation_id)) == receipt
        assert signer.calls == 2 and len(tls_registry.requests) == 6
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("during", ["stream", "signer"])
async def test_worker_cancellation_closes_io_and_joins_renewal(
    registry_authority_session, tls_registry, token_key, during
):
    values = await _prepared(registry_authority_session, tls_registry, token_key)
    job, _, signer, _, _ = values
    signer.proceed.clear()
    if during == "stream":
        next(iter(tls_registry.routes.values())).wait_for_peer_close_before_response = True
    worker = _worker(
        registry_authority_session,
        tls_registry,
        values,
        lease_seconds=0.6,
        renewal_interval_seconds=0.08,
    )
    task = asyncio.create_task(worker.run(UUID(job.operation_id)))
    try:
        await asyncio.wait_for(
            (tls_registry.request_received if during == "stream" else signer.entered).wait(), 5
        )
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        if during == "stream":
            await asyncio.wait_for(tls_registry.peer_closed.wait(), 5)
        else:
            assert signer.cancelled.is_set() and signer.closed.is_set()
        async with registry_authority_session() as session:
            stored = (await session.scalars(select(TaskImagePublicationJob))).one()
            assert stored.state == "queued" and stored.worker_id is None
            row = (await session.scalars(select(TaskImageMaterialization))).one()
            assert row.ready_at is None and not row.registry_images and row.attempt_count == 0
            assert not list(await session.scalars(select(TaskImagePublicationEnvelope)))
        assert not [
            task for task in asyncio.all_tasks() if task.get_name().startswith("publication-")
        ]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
