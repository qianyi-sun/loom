from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import select, text, update
from sqlalchemy.exc import OperationalError

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
from loom_task_image_authority.publication_store import (
    claim_publication_job,
    submit_publication_job,
)
from tests.integration.test_task_image_publication_completion import _queued_job
from tests.integration.test_task_image_publication_jobs import _independent_publication_chain
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


async def _prepared(
    factory, registry, token_key, *, names=("task",), delay=0, lifetime_seconds=1800
):
    reader, root, _ = _graph(arch="arm64")
    issuer = _issuer(registry, token_key)
    private = Ed25519PrivateKey.generate()
    key = PublicationKeyRecord("publication-1", private.public_key().public_bytes_raw(), NOW)
    distribution = DistributedKeysetSnapshot(1, 0, (key.key_id,), NOW, NOW + timedelta(minutes=10))
    async with factory() as session:
        job = await _queued_job(
            session, issuer, names=names, root=root, lifetime_seconds=lifetime_seconds
        )
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


@pytest.mark.parametrize("startup_delay_seconds", [0, 1])
@pytest.mark.parametrize("expired_before_claim", [False, True])
async def test_total_deadline_has_durable_terminal_disposition(
    registry_authority_session, tls_registry, token_key, expired_before_claim, startup_delay_seconds
):
    values = await _prepared(
        registry_authority_session, tls_registry, token_key, lifetime_seconds=0.5
    )
    job = values[0]
    # Explicitly simulate setup finishing after the half-second job budget.
    # This advances the trusted test clock without guessing at machine speed.
    original_clock = values[4]
    values = (*values[:4], lambda: original_clock() + timedelta(seconds=startup_delay_seconds))
    if expired_before_claim:
        values = (*values[:4], lambda: job.deadline)
    else:
        next(
            response for path, response in tls_registry.routes.items() if "/manifests/" in path
        ).wait_for_peer_close_before_response = True
    worker = _worker(
        registry_authority_session,
        tls_registry,
        values,
        lease_seconds=0.4,
        renewal_interval_seconds=0.08,
    )
    with pytest.raises((RuntimeError, TimeoutError)):
        await worker.run(UUID(job.operation_id))
    async with registry_authority_session() as session:
        stored = await session.get(TaskImagePublicationJob, UUID(job.operation_id))
        assert stored.state == "failed" and stored.failure_code == "deadline"
        assert stored.worker_id is None and stored.worker_expires_at is None
        row = (await session.scalars(select(TaskImageMaterialization))).one()
        assert not row.registry_images and row.ready_at is None and row.attempt_count == 0
        assert not list(await session.scalars(select(TaskImagePublicationEnvelope)))
    # Expiry is allowed before a connection ever opens. The real-time test
    # establishes terminal disposition; the event-gated test below proves I/O
    # cleanup only after observing actual network admission.
    if expired_before_claim or startup_delay_seconds:
        assert not tls_registry.requests


async def test_observed_registry_io_closes_when_renewal_detects_total_deadline(
    registry_authority_session, tls_registry, token_key
):
    values = await _prepared(registry_authority_session, tls_registry, token_key)
    job, _, signer, _, _ = values
    now = NOW + timedelta(seconds=14)
    values = (*values[:4], lambda: now)
    signer.clock = values[4]
    target, response = next(
        (path, response) for path, response in tls_registry.routes.items() if "/manifests/" in path
    )
    response.wait_for_peer_close_before_response = True
    worker = _worker(
        registry_authority_session, tls_registry, values,
        lease_seconds=60, renewal_interval_seconds=0.05,
    )
    task = asyncio.create_task(worker.run(UUID(job.operation_id)))
    try:
        await asyncio.wait_for(tls_registry.request_received.wait(), 5)
        assert [request.target for request in tls_registry.requests] == [target]
        assert not tls_registry.peer_closed.is_set()
        # Move the trusted job clock only after network admission. Renewal
        # detects expiry and cancels work; this does not fast-forward the
        # monotonic outer timer. The separate real-time deadline cases remain above.
        now = job.deadline
        completed, _ = await asyncio.wait((task,), timeout=5)
        assert task in completed, "worker did not detect expiry without harness cancellation"
        with pytest.raises((RuntimeError, TimeoutError)):
            await task
        await asyncio.wait_for(tls_registry.peer_closed.wait(), 5)
        assert signer.closed.is_set() and signer.calls == 0
        assert not [
            pending for pending in asyncio.all_tasks()
            if pending.get_name().startswith("publication-")
        ]
        async with registry_authority_session() as session:
            stored = await session.get(TaskImagePublicationJob, UUID(job.operation_id))
            assert stored.state == "failed" and stored.failure_code == "deadline"
            assert stored.worker_id is None and stored.worker_expires_at is None
            row = (await session.scalars(select(TaskImageMaterialization))).one()
            assert not row.registry_images and row.ready_at is None and row.attempt_count == 0
            assert not list(await session.scalars(select(TaskImagePublicationEnvelope)))
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize(
    "sqlstate,retryable", [("08006", True), ("40001", True), ("40P01", True), ("42601", False)]
)
async def test_database_errors_are_narrowly_classified(
    registry_authority_session, tls_registry, token_key, sqlstate, retryable
):
    values = await _prepared(registry_authority_session, tls_registry, token_key)

    class DatabaseFailureError(Exception):
        pass

    original = DatabaseFailureError("generated transport failure")
    original.sqlstate = sqlstate
    failure = OperationalError("SELECT", {}, original)

    class FailingDistribution:
        async def snapshot(self, *, state, key):
            raise failure

    values = (*values[:3], FailingDistribution(), values[4])
    worker = _worker(registry_authority_session, tls_registry, values)
    with pytest.raises(OperationalError):
        await worker.run(UUID(values[0].operation_id))
    async with registry_authority_session() as session:
        stored = (await session.scalars(select(TaskImagePublicationJob))).one()
        assert stored.state == ("queued" if retryable else "failed")
        assert not list(await session.scalars(select(TaskImagePublicationEnvelope)))


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
        next(
            response for path, response in tls_registry.routes.items() if "/manifests/" in path
        ).wait_for_peer_close_before_response = True
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


@pytest.mark.parametrize("failure", ["retryable_registry", "bad_digest", "signer_timeout"])
async def test_worker_failures_preserve_task_budget(
    registry_authority_session, tls_registry, token_key, failure
):
    values = await _prepared(registry_authority_session, tls_registry, token_key)
    response = next(
        response for path, response in tls_registry.routes.items() if "/manifests/" in path
    )
    if failure == "retryable_registry":
        response.status = 401
    elif failure == "bad_digest":
        response.chunks = (b"X" * sum(map(len, response.chunks)),)
    else:
        values[2].proceed.clear()
    worker = _worker(registry_authority_session, tls_registry, values, signer_timeout_seconds=0.1)
    with pytest.raises((RuntimeError, ValueError, TimeoutError)):
        await worker.run(UUID(values[0].operation_id))
    async with registry_authority_session() as session:
        job = (await session.scalars(select(TaskImagePublicationJob))).one()
        assert job.state == ("failed" if failure == "bad_digest" else "queued")
        if failure == "bad_digest":
            assert job.failure_code == "integrity"
        else:
            assert job.available_at > values[4]()
        row = (await session.scalars(select(TaskImageMaterialization))).one()
        assert row.attempt_count == 0 and row.ready_at is None and not row.registry_images
        assert not list(await session.scalars(select(TaskImagePublicationEnvelope)))
    assert values[2].closed.is_set()


@pytest.mark.parametrize("during", ["stream", "signer"])
async def test_worker_takeover_cancels_io_without_releasing_successor(
    registry_authority_session, tls_registry, token_key, during
):
    values = await _prepared(registry_authority_session, tls_registry, token_key)
    now = NOW + timedelta(seconds=14)
    values = (*values[:4], lambda: now)
    values[2].clock = values[4]
    values[2].proceed.clear()
    if during == "stream":
        next(
            response for path, response in tls_registry.routes.items() if "/manifests/" in path
        ).wait_for_peer_close_before_response = True
    worker = _worker(
        registry_authority_session,
        tls_registry,
        values,
        lease_seconds=0.6,
        renewal_interval_seconds=0.08,
    )
    task = asyncio.create_task(worker.run(UUID(values[0].operation_id)))
    try:
        await asyncio.wait_for(
            (tls_registry.request_received if during == "stream" else values[2].entered).wait(), 5
        )
        successor = uuid4()
        async with registry_authority_session() as session:
            job = (await session.scalars(select(TaskImagePublicationJob))).one()
            now = job.worker_expires_at
            await claim_publication_job(
                session,
                operation_id=UUID(values[0].operation_id),
                owner_id=successor,
                clock=lambda: now,
            )
            await session.commit()
        with pytest.raises(RuntimeError, match="fence lost"):
            await asyncio.wait_for(task, 5)
        async with registry_authority_session() as session:
            job = (await session.scalars(select(TaskImagePublicationJob))).one()
            assert (
                job.state == "running" and job.worker_id == successor and job.worker_generation == 2
            )
            assert not list(await session.scalars(select(TaskImagePublicationEnvelope)))
        if during == "stream":
            await asyncio.wait_for(tls_registry.peer_closed.wait(), 5)
        else:
            assert values[2].cancelled.is_set() and values[2].closed.is_set()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_service_admission_precedes_second_durable_claim(
    registry_authority_session, tls_registry, token_key
):
    values = await _prepared(registry_authority_session, tls_registry, token_key)
    operation = uuid4()
    async with registry_authority_session() as session:
        arguments, _ = await _independent_publication_chain(
            session, values[1], operation_id=operation, job_id="812345"
        )
        arguments["registry_origin"] = tls_registry.origin
        await submit_publication_job(session, **arguments)
        await session.commit()
    values[2].proceed.clear()
    worker = _worker(registry_authority_session, tls_registry, values, maximum_jobs=1)
    first = asyncio.create_task(worker.run(UUID(values[0].operation_id)))
    waiting = asyncio.Event()

    async def second_run():
        waiting.set()
        return await worker.run(operation)

    second = None
    try:
        await asyncio.wait_for(values[2].entered.wait(), 5)
        second = asyncio.create_task(second_run())
        await waiting.wait()
        async with registry_authority_session() as session:
            row = await session.get(TaskImagePublicationJob, operation)
            assert row.state == "queued" and row.worker_generation == 0
        assert not second.done()
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
        values[2].proceed.set()
        assert (await asyncio.wait_for(first, 5)).component_count == 1
    finally:
        first.cancel()
        if second is not None:
            second.cancel()
        await asyncio.gather(first, *((second,) if second else ()), return_exceptions=True)


async def _wait_blocked_pids(session, blocker_pid):
    async with asyncio.timeout(5):
        while True:
            # pg_stat_activity snapshots otherwise hide newly opened renewal sessions.
            await session.execute(text("SELECT pg_stat_clear_snapshot()"))
            pids = list(
                await session.scalars(
                    text(
                        "SELECT pid FROM pg_stat_activity WHERE :blocker = ANY(pg_blocking_pids(pid)) AND datname=current_database()"
                    ),
                    {"blocker": blocker_pid},
                )
            )
            if pids:
                return pids
            await asyncio.sleep(0.01)


async def test_renewal_database_disconnect_cancels_stream_and_retries(
    registry_authority_session, tls_registry, token_key
):
    values = await _prepared(registry_authority_session, tls_registry, token_key)
    next(
        response for path, response in tls_registry.routes.items() if "/manifests/" in path
    ).wait_for_peer_close_before_response = True
    worker = _worker(
        registry_authority_session,
        tls_registry,
        values,
        lease_seconds=2,
        renewal_interval_seconds=0.1,
    )
    task = asyncio.create_task(worker.run(UUID(values[0].operation_id)))
    try:
        await asyncio.wait_for(tls_registry.request_received.wait(), 5)
        async with registry_authority_session() as blocker:
            await blocker.scalar(select(TaskImagePublicationJob).with_for_update())
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            waiting = await _wait_blocked_pids(blocker, pid)
            assert len(waiting) == 1
            assert await blocker.scalar(
                text("SELECT pg_terminate_backend(:pid)"), {"pid": waiting[0]}
            )
            await blocker.rollback()
        with pytest.raises(OperationalError):
            await asyncio.wait_for(task, 5)
        await asyncio.wait_for(tls_registry.peer_closed.wait(), 5)
        async with registry_authority_session() as session:
            row = (await session.scalars(select(TaskImagePublicationJob))).one()
            assert row.state == "queued" and row.worker_id is None
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_completion_wins_over_late_renewal_only_with_exact_receipt(
    registry_authority_session, tls_registry, token_key
):
    values = await _prepared(registry_authority_session, tls_registry, token_key)
    async with registry_authority_session() as session:
        await session.execute(
            text("""CREATE FUNCTION worker_completion_wait() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.state = 'ready' THEN PERFORM pg_advisory_xact_lock(4422); END IF; RETURN NEW; END $$""")
        )
        await session.execute(
            text(
                "CREATE TRIGGER worker_completion_wait AFTER UPDATE ON task_image_materializations FOR EACH ROW EXECUTE FUNCTION worker_completion_wait()"
            )
        )
        await session.commit()
    worker = _worker(
        registry_authority_session,
        tls_registry,
        values,
        lease_seconds=2,
        renewal_interval_seconds=0.1,
    )
    async with registry_authority_session() as blocker:
        await blocker.execute(text("SELECT pg_advisory_xact_lock(4422)"))
        pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        task = asyncio.create_task(worker.run(UUID(values[0].operation_id)))
        try:
            completing = await _wait_blocked_pids(blocker, pid)
            assert len(completing) == 1
            await _wait_blocked_pids(blocker, completing[0])
            await blocker.rollback()
            receipt = await asyncio.wait_for(task, 5)
            assert await worker.run(UUID(values[0].operation_id)) == receipt
            assert values[2].calls == 1
        finally:
            await blocker.rollback()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_blocked_renewal_closes_stream_at_lease_expiry_not_database_timeout(
    registry_authority_session, tls_registry, token_key
):
    values = await _prepared(registry_authority_session, tls_registry, token_key)
    next(
        response for path, response in tls_registry.routes.items() if "/manifests/" in path
    ).wait_for_peer_close_before_response = True
    worker = _worker(
        registry_authority_session,
        tls_registry,
        values,
        lease_seconds=0.5,
        renewal_interval_seconds=0.08,
        database_timeout_seconds=5,
    )
    task = asyncio.create_task(worker.run(UUID(values[0].operation_id)))
    try:
        await asyncio.wait_for(tls_registry.request_received.wait(), 5)
        async with registry_authority_session() as blocker:
            await blocker.scalar(select(TaskImagePublicationJob).with_for_update())
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            await _wait_blocked_pids(blocker, pid)
            # The lease is .5 s, not the 5 s DB timeout. Wait for actual socket closure.
            await asyncio.wait_for(tls_registry.peer_closed.wait(), 1.5)
            await blocker.rollback()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(task, 5)
        assert values[2].closed.is_set()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_deadline_crossed_while_retry_cleanup_waits_is_terminal(
    registry_authority_session, tls_registry, token_key
):
    values = await _prepared(registry_authority_session, tls_registry, token_key)
    entered, proceed = asyncio.Event(), asyncio.Event()

    class FailingDistribution:
        async def snapshot(self, *, state, key):
            entered.set()
            await proceed.wait()
            raise ConnectionError("distribution unavailable")

    now = NOW + timedelta(seconds=14)
    values = (*values[:3], FailingDistribution(), lambda: now)
    worker = _worker(registry_authority_session, tls_registry, values)
    task = asyncio.create_task(worker.run(UUID(values[0].operation_id)))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        async with registry_authority_session() as blocker:
            await blocker.scalar(select(TaskImagePublicationJob).with_for_update())
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            proceed.set()
            await _wait_blocked_pids(blocker, pid)
            now = values[0].deadline
            await blocker.rollback()
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(task, 5)
        async with registry_authority_session() as session:
            row = (await session.scalars(select(TaskImagePublicationJob))).one()
            assert row.state == "failed" and row.failure_code == "deadline"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
