"""Inventory preparation/recheck is not reference eligibility or deletion authority."""

import importlib
from dataclasses import FrozenInstanceError
from uuid import uuid4

import pytest
from sqlalchemy import event, func, select, text

from loom.db.schema import (
    TaskImageAttemptRetention,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
)
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import (
    NOW,
    _claimed_attempt,
    _issue_first,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)

ORIGIN = "https://registry.example:5443"


def snapshot_module():
    name = "loom_task_image_authority.retirement_snapshot"
    assert importlib.util.find_spec(name) is not None, "retirement snapshot preparation missing"
    return importlib.import_module(name)


async def _setup(factory, issuer, *, issued=False):
    async with factory() as session:
        auth, _, _, build_session, secrets, row, attempt = await _claimed_attempt(session)
        options = dict(
            authorization=auth, build_session=build_session, secrets=secrets,
            row=row, attempt=attempt, issuer=issuer,
        )
        if issued:
            await _issue_first(session, **options)
        await session.commit()
        return row, attempt, options


async def _lock(session, prepared):
    # Models the caller's REQUIRED shared fence; this helper is deliberately not
    # an actual retirement transaction and does not prove reference safety.
    await session.execute(text("LOCK TABLE public.tasks IN SHARE MODE NOWAIT"))
    await session.scalar(
        select(TaskImageMaterialization)
        .where(TaskImageMaterialization.id == prepared.inventory.materialization_id)
        .with_for_update(nowait=True)
    )
    await session.scalar(
        select(TaskImageMaterializationAttempt)
        .where(TaskImageMaterializationAttempt.id == prepared.inventory.attempt_id)
        .with_for_update(nowait=True)
    )


@pytest.mark.parametrize("issued", [False, True])
async def test_preparation_validates_detached_inventory_without_catalog_lock(
    registry_authority_session, registry_issuer, monkeypatch, issued
):
    module = snapshot_module()
    factory = registry_authority_session
    engine = factory.kw["bind"]
    row, attempt, _ = await _setup(factory, registry_issuer, issued=issued)
    original = module.derive_attempt_repository_inventory
    calls = []

    def validate(**values):
        # The owned preparation session must be closed before bulk validation.
        assert engine.pool.checkedout() == 0
        calls.append(True)
        return original(**values)

    monkeypatch.setattr(module, "derive_attempt_repository_inventory", validate)
    prepared = await module.prepare_attempt_retirement_inventory(
        engine, attempt_id=attempt.id, registry_origin=ORIGIN,
    )
    assert calls == [True]
    assert prepared.credential_count == int(issued)
    assert prepared.inventory.materialization_id == row.id
    assert len(prepared.inventory.repositories) == int(issued)
    with pytest.raises(FrozenInstanceError):
        prepared.credential_count = 999

    async with factory() as session:
        await _lock(session, prepared)
        await module.revalidate_retirement_inventory(session, prepared=prepared)
        assert calls == [True], "locked recheck repeats bulk credential validation"
        assert await session.scalar(select(func.count()).select_from(TaskImageAttemptRetention)) == 0


async def test_preparation_ignores_unflushed_caller_state_and_busy_catalog(
    registry_authority_session, registry_issuer
):
    module = snapshot_module()
    factory = registry_authority_session
    row, attempt, _ = await _setup(factory, registry_issuer)
    async with factory() as writer:
        # Preparation must neither ask for the tasks barrier nor reuse/flush a
        # caller's unit of work. Its read-only snapshot can coexist with this.
        await writer.execute(text("LOCK TABLE public.tasks IN ROW EXCLUSIVE MODE"))
        cached = await writer.get(TaskImageMaterialization, row.id)
        cached.task_checksum = "e" * 64
        prepared = await module.prepare_attempt_retirement_inventory(
            factory.kw["bind"], attempt_id=attempt.id, registry_origin=ORIGIN,
        )
        assert cached in writer.dirty
        await writer.rollback()
    async with factory() as session:
        await _lock(session, prepared)
        await module.revalidate_retirement_inventory(session, prepared=prepared)


@pytest.mark.parametrize("isolation_level", ["REPEATABLE READ", "AUTOCOMMIT"])
async def test_preparation_owns_read_committed_read_only_transaction(
    registry_authority_session, registry_issuer, isolation_level
):
    module = snapshot_module()
    factory = registry_authority_session
    _, attempt, _ = await _setup(factory, registry_issuer)
    engine = factory.kw["bind"].execution_options(isolation_level=isolation_level)
    observed = []

    def observe(connection, cursor, statement, parameters, context, executemany):
        if "pg_catalog.pg_trigger" in statement:
            observed.append(tuple(connection.exec_driver_sql(
                "SELECT current_setting('transaction_isolation'), "
                "current_setting('transaction_read_only')"
            ).one()))

    event.listen(engine.sync_engine, "before_cursor_execute", observe)
    try:
        await module.prepare_attempt_retirement_inventory(
            engine, attempt_id=attempt.id, registry_origin=ORIGIN,
        )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", observe)
    assert observed == [("read committed", "on")]


async def test_locked_recheck_refuses_pending_retirement_without_flushing(
    registry_authority_session, registry_issuer
):
    module = snapshot_module()
    factory = registry_authority_session
    _, attempt, _ = await _setup(factory, registry_issuer)
    prepared = await module.prepare_attempt_retirement_inventory(
        factory.kw["bind"], attempt_id=attempt.id, registry_origin=ORIGIN,
    )
    async with factory() as session:
        await _lock(session, prepared)
        marker = TaskImageAttemptRetention(attempt_id=attempt.id, observed_at=NOW)
        session.add(marker)
        with pytest.raises(module.RetirementInventoryUnavailableError, match="unflushed"):
            await module.revalidate_retirement_inventory(session, prepared=prepared)
        assert marker in session.new


async def test_locked_recheck_rejects_credential_appended_after_preparation(
    registry_authority_session, registry_issuer
):
    module = snapshot_module()
    factory = registry_authority_session
    _, attempt, options = await _setup(factory, registry_issuer)
    prepared = await module.prepare_attempt_retirement_inventory(
        factory.kw["bind"], attempt_id=attempt.id, registry_origin=ORIGIN,
    )
    async with factory() as writer:
        await _issue_first(writer, **options)
        await writer.commit()
    queries = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        if "count(" in statement and "task_image_registry_credentials" in statement:
            queries.append((statement, parameters))

    engine = factory.kw["bind"]
    event.listen(engine.sync_engine, "before_cursor_execute", capture)
    try:
        async with factory() as session:
            await _lock(session, prepared)
            with pytest.raises(module.RetirementInventoryChangedError):
                await module.revalidate_retirement_inventory(session, prepared=prepared)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture)
    assert len(queries) == 1
    assert "LIMIT" in queries[0][0]
    assert prepared.credential_count + 1 in queries[0][1].values()


@pytest.mark.parametrize("changed", ["parent", "plan_without_hash_change", "attempt"])
async def test_locked_recheck_rejects_changed_identity_not_only_plan_hash(
    registry_authority_session, registry_issuer, changed
):
    module = snapshot_module()
    factory = registry_authority_session
    row, attempt, _ = await _setup(factory, registry_issuer)
    prepared = await module.prepare_attempt_retirement_inventory(
        factory.kw["bind"], attempt_id=attempt.id, registry_origin=ORIGIN,
    )
    async with factory() as writer:
        if changed == "parent":
            current = await writer.get(TaskImageMaterialization, row.id)
            current.task_id += "-changed"
        else:
            current = await writer.get(TaskImageMaterializationAttempt, attempt.id)
            if changed == "plan_without_hash_change":
                current.claim_plan_json = {**current.claim_plan_json, "bundle_prefix": "changed/"}
            else:
                current.claim_id = uuid4()
        await writer.commit()
    async with factory() as session:
        await _lock(session, prepared)
        with pytest.raises(module.RetirementInventoryChangedError):
            await module.revalidate_retirement_inventory(session, prepared=prepared)


async def test_preparation_rejects_overflow_before_bulk_validation(
    registry_authority_session, registry_issuer, monkeypatch
):
    module = snapshot_module()
    factory = registry_authority_session
    _, attempt, _ = await _setup(factory, registry_issuer, issued=True)
    monkeypatch.setattr(module, "MAX_RETIREMENT_CREDENTIALS", 0)

    def must_not_validate(**values):
        pytest.fail("overflow snapshot reached bulk validation")

    monkeypatch.setattr(module, "derive_attempt_repository_inventory", must_not_validate)
    with pytest.raises(module.RetirementInventoryUnavailableError):
        await module.prepare_attempt_retirement_inventory(
            factory.kw["bind"], attempt_id=attempt.id, registry_origin=ORIGIN,
        )


@pytest.mark.parametrize("phase", ["prepare", "recheck"])
async def test_inventory_requires_active_credential_immutability_guard(
    registry_authority_session, registry_issuer, phase
):
    module = snapshot_module()
    factory = registry_authority_session
    _, attempt, _ = await _setup(factory, registry_issuer)
    prepared = await module.prepare_attempt_retirement_inventory(
        factory.kw["bind"], attempt_id=attempt.id, registry_origin=ORIGIN,
    )
    # DB-owner fault injection in this disposable migrated database only.
    async with factory() as writer:
        await writer.execute(text(
            "ALTER TABLE public.task_image_registry_credentials DISABLE TRIGGER "
            "task_image_registry_credentials_preserve"
        ))
        await writer.commit()
    with pytest.raises(module.RetirementInventoryUnavailableError):
        if phase == "prepare":
            await module.prepare_attempt_retirement_inventory(
                factory.kw["bind"], attempt_id=attempt.id, registry_origin=ORIGIN,
            )
        else:
            async with factory() as session:
                await _lock(session, prepared)
                await module.revalidate_retirement_inventory(session, prepared=prepared)
