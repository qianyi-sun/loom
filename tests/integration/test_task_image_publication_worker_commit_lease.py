"""Real deferred-commit barriers for allocation-independent publication leases."""

from __future__ import annotations

import asyncio
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
@pytest.mark.parametrize("commit_stage", ["claim", "renewal"])
async def test_claim_commit_consumed_lease_bounds_work_before_renewal_sleep(
    registry_authority_session,
    tls_registry,
    token_key,
    monkeypatch,
    expired_at_commit,
    commit_stage,
):
    values = await _prepared(registry_authority_session, tls_registry, token_key)
    next(
        response for path, response in tls_registry.routes.items() if "/manifests/" in path
    ).wait_for_peer_close_before_response = True
    now = NOW + timedelta(seconds=14)
    commit_released = False
    post_commit_sleeps = []
    real_sleep = asyncio.sleep

    def clock():
        return now

    async def renewal_sleep(delay, result=None):
        nonlocal now
        if asyncio.current_task().get_name() != "publication-renewal":
            return await real_sleep(delay, result)
        if commit_released:
            post_commit_sleeps.append(delay)
            if len(post_commit_sleeps) > 1:
                # The recovered transaction has committed. Keep its next renewal
                # asleep until the observer verifies the row and cancels work.
                await asyncio.Event().wait()
            assert delay == 4, "renewal must use half the remaining committed lease"
        else:
            assert delay == 15
            # Establish actual in-flight I/O before testing renewal-COMMIT loss;
            # accelerating the scheduler must not race the TLS setup itself.
            async with asyncio.timeout(5):
                await tls_registry.request_received.wait()
        now += timedelta(seconds=delay)
        return await real_sleep(0, result)

    # Control only this worker's renewal scheduling and injected authority clock.
    # Claim, deferred COMMIT, renewal, timeout contexts and TLS remain real. This
    # asserts the requested sleep directly instead of requiring DB/scheduler work
    # to finish inside a 50 ms wall-clock lease on a shared CI runner.
    monkeypatch.setattr(asyncio, "sleep", renewal_sleep)

    values = (*values[:4], clock)
    old_state = "queued" if commit_stage == "claim" else "running"
    async with registry_authority_session() as session:
        await session.execute(
            text("""CREATE FUNCTION publication_claim_commit_wait() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN
            PERFORM pg_advisory_xact_lock(987123); RETURN NEW; END $$""")
        )
        await session.execute(
            text(f"""CREATE CONSTRAINT TRIGGER publication_claim_commit_wait
            AFTER UPDATE ON task_image_publication_jobs DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW WHEN (OLD.state='{old_state}' AND NEW.state='running')
            EXECUTE FUNCTION publication_claim_commit_wait()""")
        )
        await session.commit()
    worker = _worker(
        registry_authority_session,
        tls_registry,
        values,
        lease_seconds=60,
        renewal_interval_seconds=15,
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
            claimed_lease_expiry = now + timedelta(seconds=60)
            now = claimed_lease_expiry + timedelta(seconds=0.2 if expired_at_commit else -8)
            if not expired_at_commit:
                # A real release delay must not choose the simulated authority
                # branch. This deterministically broke the former 50 ms test.
                await blocker.execute(text("SELECT pg_sleep(0.1)"))
            commit_released = True
            await blocker.rollback()
        if expired_at_commit:
            done, _ = await asyncio.wait({task}, timeout=5)
            assert task in done, "claim commit left work running beyond its remaining lease"
            with pytest.raises(PublicationJobOwnershipError):
                await task
            if commit_stage == "claim":
                assert not tls_registry.requests
            else:
                assert len(tls_registry.requests) == 1
                assert tls_registry.peer_closed.is_set()
            assert not values[2].entered.is_set()
            assert not post_commit_sleeps
        else:
            # Acceptance requires a genuinely committed new lease, not merely
            # entering renewal or requesting the correct sleep duration.
            async with asyncio.timeout(5):
                while True:
                    if post_commit_sleeps:
                        assert post_commit_sleeps[0] == 4
                    if task.done():
                        await task
                    async with registry_authority_session() as observer:
                        expiry = await observer.scalar(
                            select(TaskImagePublicationJob.worker_expires_at)
                        )
                    if (
                        expiry is not None
                        and expiry > claimed_lease_expiry
                        and tls_registry.request_received.is_set()
                    ):
                        break
                    await asyncio.sleep(0.01)
            assert tls_registry.request_received.is_set()
            assert post_commit_sleeps[0] == 4
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
