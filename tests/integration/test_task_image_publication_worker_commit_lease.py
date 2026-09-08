"""Real deferred-commit barriers for allocation-independent publication leases."""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from uuid import UUID

import pytest
from sqlalchemy import select, text

from loom.db.schema import (
    TaskImageMaterialization,
    TaskImagePublicationEnvelope,
    TaskImagePublicationJob,
)
from loom_task_image_authority.publication_jobs import PublicationJobOwnershipError
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_publication_worker import (
    _prepared,
    _wait_blocked_pids,
    _worker,
)
from tests.integration.test_task_image_registry_credentials import NOW
from tests.unit.test_task_image_registry_reader import tls_registry as tls_registry
from tests.unit.test_task_image_registry_reader import token_key as token_key


@pytest.mark.parametrize("expired_at_commit", [True, False])
async def test_claim_commit_consumed_lease_bounds_work_before_renewal_sleep(
    registry_authority_session, tls_registry, token_key, expired_at_commit
):
    values = await _prepared(registry_authority_session, tls_registry, token_key)
    next(
        response for path, response in tls_registry.routes.items() if "/manifests/" in path
    ).wait_for_peer_close_before_response = True
    base, started = NOW + timedelta(seconds=14), time.monotonic()
    last_sample = [base]

    def clock():
        last_sample[0] = base + timedelta(seconds=time.monotonic() - started)
        return last_sample[0]

    values = (*values[:4], clock)
    async with registry_authority_session() as session:
        await session.execute(
            text("""CREATE FUNCTION publication_claim_commit_wait() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN
            PERFORM pg_advisory_xact_lock(987123); RETURN NEW; END $$""")
        )
        await session.execute(
            text("""CREATE CONSTRAINT TRIGGER publication_claim_commit_wait
            AFTER UPDATE ON task_image_publication_jobs DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW WHEN (OLD.state='queued' AND NEW.state='running')
            EXECUTE FUNCTION publication_claim_commit_wait()""")
        )
        await session.commit()
    worker = _worker(
        registry_authority_session,
        tls_registry,
        values,
        lease_seconds=4,
        renewal_interval_seconds=1.5,
        database_timeout_seconds=10,
    )
    task = None
    try:
        async with registry_authority_session() as blocker:
            await blocker.execute(text("SELECT pg_advisory_xact_lock(987123)"))
            blocker_pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            task = asyncio.create_task(worker.run(UUID(values[0].operation_id)))
            blocked = await _wait_blocked_pids(blocker, blocker_pid)
            query = await blocker.scalar(
                text("SELECT query FROM pg_stat_activity WHERE pid=:pid"), {"pid": blocked[0]}
            )
            assert query.strip().upper() == "COMMIT"
            # Simulate clock passage while a genuine deferred PostgreSQL commit
            # owns the new lease. Do not mock claim, transaction, renewal or I/O.
            claimed_lease_expiry = last_sample[0] + timedelta(seconds=4)
            base = claimed_lease_expiry + timedelta(seconds=0.2 if expired_at_commit else -0.05)
            started = time.monotonic()
            await blocker.rollback()
        if expired_at_commit:
            done, _ = await asyncio.wait({task}, timeout=0.8)
            assert task in done, "claim commit left work running beyond its remaining lease"
            with pytest.raises(PublicationJobOwnershipError):
                await task
            assert not tls_registry.requests
            assert not values[2].entered.is_set()
        else:
            # Recover remaining live authority promptly instead of unnecessarily
            # discarding valid work. The previous fixed sleep cannot renew in
            # this window; acceptance requires a genuinely committed new lease.
            async with asyncio.timeout(0.8):
                while True:
                    async with registry_authority_session() as observer:
                        expiry = await observer.scalar(
                            select(TaskImagePublicationJob.worker_expires_at)
                        )
                    if expiry is not None and expiry > claimed_lease_expiry:
                        break
                    await asyncio.sleep(0.01)
            assert tls_registry.request_received.is_set()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert tls_registry.peer_closed.is_set()
        async with registry_authority_session() as session:
            assert not list(await session.scalars(select(TaskImagePublicationEnvelope)))
            row = (await session.scalars(select(TaskImageMaterialization))).one()
            assert row.ready_at is None and row.registry_images == {}
    finally:
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
