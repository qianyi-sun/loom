from __future__ import annotations

import asyncio
import hashlib
import importlib
from datetime import timedelta
from uuid import UUID

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError

from loom.db.schema import Task, TaskImageAttemptRetention, TaskImageMaterialization
from loom_task_image_authority.materializations import TaskImageSessionMaterializationConflictError
from tests.integration.test_task_image_publication_completion import (
    _complete,
    _signed_job,
    completion,
)
from tests.integration.test_task_image_publication_jobs import _submit
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import NOW, _issue_first
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)
from tests.integration.test_task_image_retirement_snapshot import ORIGIN, _setup


def store():
    name = "loom_task_image_authority.retirement_store"
    assert importlib.util.find_spec(name) is not None, "owned retirement transaction missing"
    return importlib.import_module(name)


async def observe(factory, attempt_id, instant, **kwargs):
    return await store().observe_or_retire_attempt(
        factory.kw["bind"],
        attempt_id=attempt_id,
        registry_origin=ORIGIN,
        clock=lambda: instant,
        **kwargs,
    )


@pytest.mark.parametrize("issued", [False, True])
async def test_abandoned_observation_grace_retirement_and_permanent_replay(
    registry_authority_session,
    registry_issuer,
    issued,
):
    factory = registry_authority_session
    _, attempt, options = await _setup(factory, registry_issuer, issued=issued)
    instant = NOW + timedelta(hours=1)
    first = await observe(factory, attempt.id, instant)
    assert first.status == "observing" and first.unreferenced_since == instant
    assert (
        await observe(factory, attempt.id, instant + timedelta(hours=24, microseconds=-1))
    ).status == "observing"
    retired = await observe(factory, attempt.id, instant + timedelta(hours=24))
    assert retired.status == "retired" and retired.retired_at == instant + timedelta(hours=24)
    async with factory() as session:
        row = await session.get(TaskImageAttemptRetention, attempt.id)
        frozen = (row.observed_at, row.canonical_inventory, row.inventory_sha256)
        assert hashlib.sha256(row.canonical_inventory).hexdigest() == row.inventory_sha256
    assert await observe(factory, attempt.id, NOW) == retired
    async with factory() as session:
        row = await session.get(TaskImageAttemptRetention, attempt.id)
        assert (row.observed_at, row.canonical_inventory, row.inventory_sha256) == frozen
        with pytest.raises(TaskImageSessionMaterializationConflictError, match="retired"):
            await _issue_first(session, **options)


async def test_live_build_pin_resets_observed_grace(registry_authority_session, registry_issuer):
    factory = registry_authority_session
    row, attempt, _ = await _setup(factory, registry_issuer)
    first = await observe(factory, attempt.id, NOW + timedelta(seconds=12))
    assert first.status == "pinned" and first.pins == ("build_lease",)
    instant = NOW + timedelta(hours=1)
    await observe(factory, attempt.id, instant)
    async with factory() as session:
        current = await session.get(TaskImageMaterialization, row.id)
        current.lease_expires_at = instant + timedelta(hours=2)
        await session.commit()
    pinned = await observe(factory, attempt.id, instant + timedelta(hours=1))
    assert pinned.status == "pinned" and pinned.unreferenced_since is None
    fresh = await observe(factory, attempt.id, instant + timedelta(hours=24))
    assert fresh.status == "observing" and fresh.unreferenced_since == instant + timedelta(hours=24)


async def test_job_total_deadline_pins_after_builder_lease_expires(
    registry_authority_session,
    registry_issuer,
):
    factory = registry_authority_session
    async with factory() as session:
        job, _ = await _submit(session, registry_issuer)
        await session.commit()
    attempt_id = UUID(job.snapshot.attempt_id)
    pinned = await observe(factory, attempt_id, job.deadline - timedelta(seconds=1))
    assert pinned.status == "pinned" and pinned.pins == ("publication_job",)
    assert (await observe(factory, attempt_id, job.deadline)).status == "observing"


@pytest.mark.parametrize("catalog_checksum", [None, "matching", "prefixed", "different"])
async def test_completed_grace_catalog_pin_and_atomic_ready_clear(
    registry_authority_session,
    registry_issuer,
    catalog_checksum,
):
    factory = registry_authority_session
    async with factory() as session:
        values = await _signed_job(session, registry_issuer)
        receipt = await _complete(session, values)
        row = await session.get(TaskImageMaterialization, UUID(receipt.materialization_id))
        if catalog_checksum:
            checksum = "e" * 64 if catalog_checksum == "different" else row.task_checksum
            if catalog_checksum == "prefixed":
                checksum = "sha256:" + checksum
            session.add(Task(id=row.task_id, checksum=checksum, config=row.task_config))
        await session.commit()
    attempt_id = UUID(receipt.attempt_id)
    instant = NOW + timedelta(hours=1)
    first = await observe(factory, attempt_id, instant)
    if catalog_checksum in ("matching", "prefixed"):
        assert first.status == "pinned" and first.pins == ("current_task",)
        assert (await observe(factory, attempt_id, instant + timedelta(days=8))).status == "pinned"
        return
    assert first.status == "observing"
    assert (
        await observe(factory, attempt_id, instant + timedelta(hours=168, microseconds=-1))
    ).status == "observing"
    assert (await observe(factory, attempt_id, instant + timedelta(hours=168))).status == "retired"
    async with factory() as session:
        current = await session.get(TaskImageMaterialization, row.id)
        assert current.state == "retired" and current.registry_images == {}
        assert current.ready_at is None and current.ready_publication_operation_id is None
        assert (
            await completion().replay_completed_publication(
                session, operation_id=receipt.operation_id
            )
            == receipt
        )


@pytest.mark.parametrize(
    "table",
    [
        "tasks",
        "task_image_materializations",
        "task_image_materialization_attempts",
        "task_image_attempt_retention",
    ],
)
async def test_busy_fence_fails_promptly_and_releases_catalog(
    registry_authority_session,
    registry_issuer,
    table,
):
    factory = registry_authority_session
    _, attempt, _ = await _setup(factory, registry_issuer)
    await observe(factory, attempt.id, NOW + timedelta(hours=1))
    async with factory() as blocker:
        if table == "tasks":
            await blocker.execute(text("LOCK TABLE public.tasks IN ROW EXCLUSIVE MODE"))
        else:
            await blocker.execute(text(f"SELECT 1 FROM {table} FOR UPDATE"))
        async with asyncio.timeout(3):
            with pytest.raises(DBAPIError) as error:
                await observe(factory, attempt.id, NOW + timedelta(days=2))
        assert error.value.orig.sqlstate == "55P03"
        async with factory() as probe:
            await probe.execute(text("LOCK TABLE public.tasks IN ROW EXCLUSIVE MODE NOWAIT"))
    async with factory() as session:
        assert (await session.get(TaskImageAttemptRetention, attempt.id)).retired_at is None


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "AUTOCOMMIT"])
async def test_writer_owns_actual_read_committed_transaction(
    registry_authority_session,
    registry_issuer,
    isolation,
):
    factory = registry_authority_session
    _, attempt, _ = await _setup(factory, registry_issuer)
    engine = factory.kw["bind"].execution_options(isolation_level=isolation)
    observed = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        if "LOCK TABLE public.tasks IN SHARE MODE NOWAIT" in statement:
            observed.append(
                tuple(
                    connection.exec_driver_sql(
                        "SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')"
                    ).one()
                )
            )

    event.listen(engine.sync_engine, "before_cursor_execute", capture)
    try:
        await store().observe_or_retire_attempt(
            engine,
            attempt_id=attempt.id,
            registry_origin=ORIGIN,
            clock=lambda: NOW + timedelta(hours=1),
        )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
    assert observed == [("read committed", "off")]
    async with factory() as session:
        assert await session.get(TaskImageAttemptRetention, attempt.id) is not None


async def test_clock_regression_rolls_back_observation(registry_authority_session, registry_issuer):
    factory = registry_authority_session
    _, attempt, _ = await _setup(factory, registry_issuer)
    instant = NOW + timedelta(hours=1)
    await observe(factory, attempt.id, instant)
    with pytest.raises(ValueError, match="clock"):
        await observe(factory, attempt.id, instant - timedelta(seconds=1))
    async with factory() as session:
        marker = await session.get(TaskImageAttemptRetention, attempt.id)
        assert marker.observed_at == instant and marker.retired_at is None
