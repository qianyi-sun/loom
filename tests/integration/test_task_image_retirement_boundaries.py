from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import UUID

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ServiceExecutionLease,
    TaskImageAttemptRetention,
    TaskImageMaterialization,
    TaskImagePublicationJob,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.execution_contract import VerifierTopology
from loom_control_plane.service_execution import (
    enqueue_execution_transition,
    record_execution_event,
)
from loom_control_plane.task_image_materializations import retry_task_image_materialization
from loom_task_image_authority.publication_store import claim_publication_job
from tests.integration.test_service_execution_leases import (
    _requirements,
    _reserve,
    _runtime_contract,
    _seed_ready_trial,
)
from tests.integration.test_task_image_publication_completion import _complete, _signed_job
from tests.integration.test_task_image_publication_jobs import _submit
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import NOW, _issue_first
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)
from tests.integration.test_task_image_retirement_snapshot import ORIGIN, _setup
from tests.integration.test_task_image_retirement_store import observe, store


async def _observe_positive_semantics(factory, attempt_id, instant):
    # An idle-aborted transaction has not observed or retired anything. This
    # positive pin-semantics test may restart the entire supported operation;
    # timeout/rollback/cancellation tests below continue to call observe directly.
    for attempt in range(3):
        try:
            return await observe(factory, attempt_id, instant)
        except DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) != "25P03" or attempt == 2:
                raise
    raise AssertionError("positive observation retry loop exhausted without outcome")


async def test_positive_observation_retries_one_real_idle_abort(
    registry_authority_session, registry_issuer, monkeypatch,
):
    """A retry restarts preparation; the protected timeout remains unchanged."""
    factory = registry_authority_session
    _, attempt, _ = await _setup(factory, registry_issuer)
    module = store()
    original = module.revalidate_retirement_inventory
    calls = []

    async def expire_once(session, *, prepared):
        calls.append(prepared.inventory.attempt_id)
        if len(calls) == 1:
            backend = await session.scalar(text("SELECT pg_backend_pid()"))
            assert await session.scalar(text("SHOW idle_in_transaction_session_timeout")) == "1s"
            async with factory.kw["bind"].connect() as probe:
                await probe.execution_options(isolation_level="AUTOCOMMIT")
                async with asyncio.timeout(3):
                    while await probe.scalar(text("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=:pid)"), {"pid": backend}):
                        await asyncio.sleep(0.02)
        await original(session, prepared=prepared)

    monkeypatch.setattr(module, "revalidate_retirement_inventory", expire_once)
    # Resolve dynamically so the missing positive-only adapter is the regression.
    import tests.integration.test_task_image_retirement_boundaries as boundaries

    result = await boundaries._observe_positive_semantics(factory, attempt.id, NOW + timedelta(seconds=12))
    assert result.status == "pinned" and result.pins == ("build_lease",)
    assert calls == [attempt.id, attempt.id]
    assert result.unreferenced_since is None and result.retired_at is None


@pytest.mark.parametrize("idle_abort", [True, False])
async def test_positive_observation_retry_is_bounded_and_sqlstate_specific(monkeypatch, idle_abort):
    from psycopg.errors import IdleInTransactionSessionTimeout, QueryCanceled

    import tests.integration.test_task_image_retirement_boundaries as boundaries

    original = IdleInTransactionSessionTimeout("injected idle abort") if idle_abort else QueryCanceled("not retryable here")
    error = DBAPIError("probe", {}, original)
    calls = []
    factory, attempt, instant = object(), UUID(int=1), NOW

    async def unavailable(*args):
        calls.append(args)
        raise error

    monkeypatch.setattr(boundaries, "observe", unavailable)
    with pytest.raises(DBAPIError) as caught:
        await boundaries._observe_positive_semantics(factory, attempt, instant)
    assert caught.value is error
    assert calls == [(factory, attempt, instant)] * (3 if idle_abort else 1)


@pytest.mark.parametrize("complete", [False, True])
async def test_nonterminal_and_terminal_execution_pins_require_positive_cleanup(
    registry_authority_session,
    registry_issuer,
    complete,
):
    factory = registry_authority_session
    async with factory() as session:
        values = await _signed_job(session, registry_issuer)
        receipt = await _complete(session, values)
        trial_id, target = await _seed_ready_trial(session, now=NOW)
        session.add(
            TrialTaskImageMaterialization(
                trial_id=trial_id,
                materialization_id=UUID(receipt.materialization_id),
            )
        )
        await session.commit()
    attempt_id = UUID(receipt.attempt_id)
    instant = NOW + timedelta(days=1)
    assert (await _observe_positive_semantics(factory, attempt_id, instant)).pins == ("nonterminal_trial",)
    async with factory() as session:
        lease = await _reserve(session, trial_id=trial_id, target=target, now=NOW)
        trial = await session.get(Trial, trial_id)
        trial.state = "succeeded"
        trial.result = {"reward": 1.0}
        await session.commit()
    assert (await _observe_positive_semantics(factory, attempt_id, instant)).pins == ("execution_lease",)
    async with factory() as session:
        # Desired deletion and elapsed runtime deadline are not positive cleanup.
        await enqueue_execution_transition(
            session,
            lease_id=lease.id,
            expected_generation=1,
            desired_state="delete_pending",
            now=NOW + timedelta(seconds=1),
        )
        await session.commit()
    assert (await _observe_positive_semantics(factory, attempt_id, instant)).pins == ("execution_lease",)
    async with factory() as session:
        if complete:
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=2,
                ordinal=1,
                event_kind="deleted",
                payload={"resource_release": "complete"},
                observed_at=NOW + timedelta(seconds=2),
            )
        else:
            # A partially populated deleted row is database-valid but immutable;
            # it must remain pinned, not be mistaken for successful cleanup.
            current = await session.get(ServiceExecutionLease, lease.id)
            current.desired_state = "deleted"
            current.deleted_at = NOW + timedelta(seconds=2)
        await session.commit()
    result = await _observe_positive_semantics(factory, attempt_id, instant)
    if complete:
        assert result.status == "observing" and result.unreferenced_since == instant
    else:
        assert result.pins == ("execution_lease",) and result.unreferenced_since is None


async def test_old_completed_retirement_preserves_newer_lease_and_legacy_history(
    registry_authority_session,
    registry_issuer,
):
    factory = registry_authority_session
    async with factory() as session:
        values = await _signed_job(session, registry_issuer)
        receipt = await _complete(session, values)
        await session.commit()
    attempt_id, row_id = UUID(receipt.attempt_id), UUID(receipt.materialization_id)
    instant = NOW + timedelta(hours=1)
    await observe(factory, attempt_id, instant)
    async with factory() as session:
        row = await retry_task_image_materialization(session, materialization_id=row_id)
        row.state, row.claimed_by = "claimed", "later-builder"
        row.lease_epoch += 1
        row.lease_expires_at = instant + timedelta(days=10)
        row.registry_image_history = [{"component": "task", "registry_image": "legacy-evidence"}]
        await session.commit()
        expected = (
            row.state,
            row.claimed_by,
            row.lease_epoch,
            row.lease_expires_at,
            row.registry_image_history,
        )
    assert (await observe(factory, attempt_id, instant + timedelta(days=7))).status == "retired"
    async with factory() as session:
        row = await session.get(TaskImageMaterialization, row_id)
        assert (
            row.state,
            row.claimed_by,
            row.lease_epoch,
            row.lease_expires_at,
            row.registry_image_history,
        ) == expected


async def test_busy_publication_job_does_not_hold_catalog_while_waiting(
    registry_authority_session,
    registry_issuer,
):
    factory = registry_authority_session
    async with factory() as session:
        job, _ = await _submit(session, registry_issuer)
        await session.commit()
    async with factory() as blocker:
        await blocker.execute(select(TaskImagePublicationJob).with_for_update())
        with pytest.raises(DBAPIError) as error:
            await observe(factory, UUID(job.snapshot.attempt_id), NOW + timedelta(days=2))
        assert error.value.orig.sqlstate == "55P03"
        async with factory() as probe:
            await probe.execute(text("LOCK TABLE public.tasks IN ROW EXCLUSIVE MODE NOWAIT"))


@pytest.mark.parametrize("change", ["credential", "job"])
async def test_change_after_preparation_aborts_instead_of_retiring_stale_evidence(
    registry_authority_session,
    registry_issuer,
    monkeypatch,
    change,
):
    from uuid import uuid4

    factory = registry_authority_session
    module = store()
    if change == "credential":
        _, attempt, options = await _setup(factory, registry_issuer)
        attempt_id = attempt.id
    else:
        async with factory() as session:
            job, _ = await _submit(session, registry_issuer)
            await session.commit()
        attempt_id = UUID(job.snapshot.attempt_id)
    original = module._prepare_publication

    async def change_after(*args):
        result = await original(*args)
        async with factory() as writer:
            if change == "credential":
                await _issue_first(writer, **options)
            else:
                await claim_publication_job(
                    writer,
                    operation_id=UUID(job.operation_id),
                    owner_id=uuid4(),
                    clock=lambda: NOW + timedelta(seconds=15),
                )
            await writer.commit()
        return result

    monkeypatch.setattr(module, "_prepare_publication", change_after)
    with pytest.raises(module.RetirementInventoryChangedError):
        await observe(factory, attempt_id, NOW + timedelta(days=2))
    async with factory() as probe:
        await probe.execute(text("LOCK TABLE public.tasks IN ROW EXCLUSIVE MODE NOWAIT"))
        assert await probe.get(TaskImageAttemptRetention, attempt_id) is None


@pytest.mark.parametrize("boundary", ["before_flush", "after_flush"])
async def test_cancellation_rolls_back_retirement_and_ready_clear(
    registry_authority_session,
    registry_issuer,
    monkeypatch,
    boundary,
):
    factory = registry_authority_session
    async with factory() as session:
        receipt = await _complete(session, await _signed_job(session, registry_issuer))
        await session.commit()
    attempt_id = UUID(receipt.attempt_id)
    instant = NOW + timedelta(hours=1)
    await observe(factory, attempt_id, instant)
    reached = asyncio.Event()
    original = AsyncSession.flush

    async def pause(self, *args, **kwargs):
        retiring = any(
            isinstance(item, TaskImageAttemptRetention) and item.retired_at
            for item in (*self.new, *self.dirty)
        )
        if not retiring:
            return await original(self, *args, **kwargs)
        if boundary == "after_flush":
            await original(self, *args, **kwargs)
        reached.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(AsyncSession, "flush", pause)
    task = asyncio.create_task(observe(factory, attempt_id, instant + timedelta(days=7)))
    try:
        async with asyncio.timeout(5):
            await reached.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    async with factory() as session:
        await session.execute(text("LOCK TABLE public.tasks IN ROW EXCLUSIVE MODE NOWAIT"))
        marker = await session.get(TaskImageAttemptRetention, attempt_id)
        assert marker.retired_at is None and marker.observed_at == instant
        row = await session.get(TaskImageMaterialization, UUID(receipt.materialization_id))
        assert row.state == "ready" and row.registry_images
        assert row.ready_publication_operation_id == UUID(receipt.operation_id)


async def test_statement_timeout_rolls_back_and_releases_locks(
    registry_authority_session,
    registry_issuer,
):
    factory = registry_authority_session
    _, attempt, _ = await _setup(factory, registry_issuer)
    async with factory() as session:
        # Disposable migrated database only. Exercise the real server timeout,
        # not a mocked exception: no guessed delay determines a race winner.
        await session.execute(
            text("""
            CREATE FUNCTION test_slow_retirement() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN PERFORM pg_sleep(2); RETURN NEW; END $$;
            CREATE TRIGGER test_slow_retirement BEFORE INSERT ON task_image_attempt_retention
            FOR EACH ROW EXECUTE FUNCTION test_slow_retirement();
        """)
        )
        await session.commit()
    with pytest.raises(DBAPIError) as error:
        await observe(factory, attempt.id, NOW + timedelta(hours=1))
    assert error.value.orig.sqlstate == "57014"
    async with factory() as probe:
        await probe.execute(text("LOCK TABLE public.tasks IN ROW EXCLUSIVE MODE NOWAIT"))
        assert await probe.get(TaskImageAttemptRetention, attempt.id) is None


async def test_post_flush_clock_regression_rolls_back(
    registry_authority_session,
    registry_issuer,
):
    factory = registry_authority_session
    _, attempt, _ = await _setup(factory, registry_issuer)
    instant = NOW + timedelta(hours=1)
    times = iter((instant, instant, instant - timedelta(seconds=1)))
    with pytest.raises(ValueError, match="clock"):
        await store().observe_or_retire_attempt(
            factory.kw["bind"],
            attempt_id=attempt.id,
            registry_origin=ORIGIN,
            clock=lambda: next(times),
        )
    async with factory() as probe:
        await probe.execute(text("LOCK TABLE public.tasks IN ROW EXCLUSIVE MODE NOWAIT"))
        assert await probe.get(TaskImageAttemptRetention, attempt.id) is None


async def test_total_transaction_deadline_cancels_inflight_sql(
    registry_authority_session,
    registry_issuer,
    monkeypatch,
):
    factory = registry_authority_session
    module = store()
    _, attempt, _ = await _setup(factory, registry_issuer)
    entered = []

    async def slow_query(session, *args, **kwargs):
        entered.append(True)
        await session.execute(text("SELECT pg_sleep(2)"))
        pytest.fail("total transaction deadline did not interrupt SQL")

    monkeypatch.setattr(module, "_pins", slow_query)
    monkeypatch.setattr(module, "_TRANSACTION_SECONDS", 0.5)
    with pytest.raises(TimeoutError):
        await observe(factory, attempt.id, NOW + timedelta(hours=1))
    assert entered == [True]
    async with factory() as probe:
        await probe.execute(text("LOCK TABLE public.tasks IN ROW EXCLUSIVE MODE NOWAIT"))
        assert await probe.get(TaskImageAttemptRetention, attempt.id) is None


async def test_terminal_verifier_pins_after_parent_execution_is_cleaned(
    registry_authority_session,
    registry_issuer,
):
    factory = registry_authority_session
    async with factory() as session:
        receipt = await _complete(session, await _signed_job(session, registry_issuer))
        trial_id, target = await _seed_ready_trial(session, now=NOW)
        session.add(
            TrialTaskImageMaterialization(
                trial_id=trial_id,
                materialization_id=UUID(receipt.materialization_id),
            )
        )
        parent = await _reserve(
            session,
            trial_id=trial_id,
            target=target,
            now=NOW,
            requirements=_requirements(verifier_topology=VerifierTopology.SEPARATE_EXECUTION),
            runtime_contract=_runtime_contract(verifier_execution="separate_execution", now=NOW),
        )
        await record_execution_event(
            session,
            lease_id=parent.id,
            generation=1,
            ordinal=1,
            event_kind="kubernetes_observed",
            payload={"normalized_state": "succeeded"},
            observed_at=NOW + timedelta(seconds=1),
        )
        trial = await session.get(Trial, trial_id)
        trial.state = "failed"
        await session.commit()
    async with factory() as session:
        verifier = await _reserve(
            session,
            trial_id=trial_id,
            target=target,
            now=NOW + timedelta(seconds=2),
            requirements=_requirements(verifier_topology=VerifierTopology.SEPARATE_EXECUTION),
            runtime_contract=_runtime_contract(
                execution_role="verifier", verifier_execution="skipped", now=NOW
            ),
            parent_lease_id=parent.id,
        )
        await session.commit()
    for lease in (parent, verifier):
        async with factory() as session:
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="delete_pending",
                now=NOW + timedelta(seconds=3),
            )
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=2,
                ordinal=2 if lease is parent else 1,
                event_kind="deleted",
                payload={"resource_release": "complete"},
                observed_at=NOW + timedelta(seconds=4),
            )
            await session.commit()
            current = await session.get(ServiceExecutionLease, lease.id)
            assert current.deleted_at is not None and current.cleanup_state == "complete"
        result = await observe(factory, UUID(receipt.attempt_id), NOW + timedelta(days=1))
        if lease is parent:
            assert result.pins == ("execution_lease",)
        else:
            assert result.status == "observing"
