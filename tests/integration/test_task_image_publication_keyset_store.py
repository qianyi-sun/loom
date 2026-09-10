"""Durable signed distribution using disposable PostgreSQL, not fleet acceptance."""

from __future__ import annotations

import asyncio
import importlib
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import TaskImagePublicationKey, TaskImagePublicationState
from loom_task_image_authority.publication_signing import PublicationState
from tests.unit.test_task_image_publication_keyset import _b64, _sign, fixture
from tests.unit.test_task_image_publication_signing import NOW


def module():
    name = "loom_task_image_authority.publication_keyset_store"
    assert importlib.util.find_spec(name) is not None, "durable keyset distribution is missing"
    return importlib.import_module(name)


@pytest.fixture
async def database(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        yield engine, async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


async def prepared(database):
    m = module()
    private, root, payload, *_ = fixture()
    key = m.PublicationVerificationKey.model_validate(payload["keys"][0]).record()
    _, sessions = database
    async with sessions.begin() as session:
        session.add(TaskImagePublicationKey(**vars(key)))
    async with sessions.begin() as session:
        plan = await m.prepare_keyset(session, trust_root=root)
    payload.update(keyset_version=1, revocation_epoch=0)
    return m, private, root, payload, plan, key


async def publish(database):
    m, private, root, payload, plan, key = await prepared(database)
    wire = _sign(payload, private)
    async with database[1].begin() as session:
        result = await m.finalize_keyset(session, preparation=plan, wire=wire, trust_root=root, clock=lambda: NOW)
    return m, private, root, payload, plan, key, wire, result


async def test_publish_replay_read_and_original_distribution_expiry(database):
    m, _, root, payload, plan, key, wire, result = await publish(database)
    assert result.wire == wire
    assert result.state == PublicationState(keyset_version=1)
    async with database[1].begin() as session:
        assert await m.finalize_keyset(session, preparation=plan, wire=wire, trust_root=root, clock=lambda: NOW) == result
        assert await m.read_keyset(session, trust_root=root, expected_state=result.state, clock=lambda: NOW) == result
    adapter = m.DatabasePublicationDistribution(database[0], trust_root=root, clock=lambda: NOW + timedelta(minutes=1))
    snapshot = await adapter.snapshot(state=result.state, key=key)
    assert snapshot.expires_at.strftime("%Y-%m-%dT%H:%M:%SZ") == payload["expires_at"]
    assert snapshot.issued_at.strftime("%Y-%m-%dT%H:%M:%SZ") == payload["issued_at"]
    assert snapshot.key_ids == (key.key_id,)
    assert await adapter.envelope(state=result.state) == wire


@pytest.mark.parametrize("change", ["insert", "retire", "revoke", "version"])
async def test_finalize_fences_changed_authority(database, change):
    m, private, root, payload, plan, key = await prepared(database)
    async with database[1].begin() as session:
        if change == "insert":
            session.add(TaskImagePublicationKey(**vars(replace(key, key_id="second", public_key=bytes(range(32))))))
        elif change == "version":
            await session.execute(text("UPDATE task_image_publication_state SET keyset_version=1"))
        else:
            field, status = ("retired_at", "verify_only") if change == "retire" else ("revoked_at", "revoked")
            await session.execute(text(f"UPDATE task_image_publication_keys SET status=:status, {field}=:now"), {"status": status, "now": NOW})
    with pytest.raises(ValueError):
        async with database[1].begin() as session:
            await m.finalize_keyset(session, preparation=plan, wire=_sign(payload, private), trust_root=root, clock=lambda: NOW)
    async with database[1]() as session:
        assert await session.scalar(text("SELECT count(*) FROM task_image_publication_keysets")) == 0


@pytest.mark.parametrize("change", ["key", "environment", "epoch", "version", "missing", "root", "expired"])
async def test_finalize_rejects_invalid_signed_input(database, change):
    m, private, root, payload, plan, _ = await prepared(database)
    if change == "key":
        payload["keys"][0]["public_key"] = _b64(bytes(range(32)))
    elif change == "environment":
        payload["environment"] = "staging"
    elif change in {"epoch", "version"}:
        payload["revocation_epoch" if change == "epoch" else "keyset_version"] += 1
    elif change == "missing":
        payload["keys"] = []
    elif change == "root":
        root = replace(root, public_key=bytes(range(32)))
    clock = lambda: NOW + timedelta(minutes=5) if change == "expired" else NOW
    with pytest.raises(ValueError):
        async with database[1].begin() as session:
            await m.finalize_keyset(session, preparation=plan, wire=_sign(payload, private), trust_root=root, clock=clock)
    async with database[1]() as session:
        assert await session.scalar(select(TaskImagePublicationState.keyset_version)) == 0


@pytest.mark.parametrize("replay", [False, True])
async def test_waiting_for_state_cannot_return_expired_authority(database, replay):
    m, private, root, payload, plan, _ = await prepared(database)
    wire = _sign(payload, private)
    if replay:
        async with database[1].begin() as session:
            await m.finalize_keyset(session, preparation=plan, wire=wire, trust_root=root, clock=lambda: NOW)
    now = NOW
    async with database[1].begin() as blocker, database[1]() as waiter:
        await blocker.execute(select(TaskImagePublicationState).with_for_update())
        task = asyncio.create_task(m.finalize_keyset(waiter, preparation=plan, wire=wire, trust_root=root, clock=lambda: now))
        await asyncio.sleep(0.1)
        assert not task.done()
        now += timedelta(minutes=5)
        await blocker.commit()
        with pytest.raises(ValueError):
            await asyncio.wait_for(task, 3)
        await waiter.rollback()


@pytest.mark.parametrize("isolation", ["AUTOCOMMIT", "REPEATABLE READ", "SERIALIZABLE"])
async def test_unsafe_transactions_reject_before_pending_flush(database, isolation):
    m, _, root, _, _, key = await prepared(database)
    unsafe = async_sessionmaker(database[0].execution_options(isolation_level=isolation))
    async with unsafe() as session:
        pending = TaskImagePublicationKey(**vars(replace(key, key_id="pending")))
        session.add(pending)
        with pytest.raises(ValueError, match="READ COMMITTED|pending"):
            await m.prepare_keyset(session, trust_root=root)
        assert pending in session.new
        await session.rollback()
    async with unsafe() as session:
        with pytest.raises(ValueError, match="READ COMMITTED"):
            await m.prepare_keyset(session, trust_root=root)


async def test_read_checks_full_key_snapshot_and_selected_key_bytes(database):
    m, _, root, _, _, key, _, result = await publish(database)
    adapter = m.DatabasePublicationDistribution(database[0], trust_root=root, clock=lambda: NOW)
    with pytest.raises(ValueError):
        await adapter.snapshot(state=result.state, key=replace(key, public_key=bytes(range(32))))
    async with database[1].begin() as session:
        session.add(TaskImagePublicationKey(**vars(replace(key, key_id="new", public_key=bytes(range(32))))))
    with pytest.raises(ValueError):
        await adapter.snapshot(state=result.state, key=key)


async def test_signed_refresh_increments_version_without_rewriting_audit(database):
    m, private, root, payload, old_plan, key, wire, old = await publish(database)
    async with database[1].begin() as session:
        plan = await m.prepare_keyset(session, trust_root=root)
    payload.update(keyset_version=2)
    fresh_wire = _sign(payload, private)
    async with database[1].begin() as session:
        fresh = await m.finalize_keyset(session, preparation=plan, wire=fresh_wire, trust_root=root, clock=lambda: NOW)
    assert fresh.state.keyset_version == 2
    async with database[1].begin() as session:
        with pytest.raises(ValueError):
            await m.finalize_keyset(session, preparation=old_plan, wire=wire, trust_root=root, clock=lambda: NOW)
    async with database[1]() as session:
        assert await session.scalar(text("SELECT count(*) FROM task_image_publication_keysets")) == 2
        assert await session.scalar(text("SELECT canonical_envelope FROM task_image_publication_keysets WHERE keyset_version=1")) == wire


async def test_bounded_full_keyset_never_silently_omits_historical_keys(database):
    m, _, root, _, _, key = await prepared(database)
    async with database[1].begin() as session:
        session.add_all(TaskImagePublicationKey(**vars(replace(key, key_id=f"key-{n:03}", public_key=n.to_bytes(32, "big")))) for n in range(128))
    async with database[1].begin() as session:
        with pytest.raises(ValueError, match="128|bound"):
            await m.prepare_keyset(session, trust_root=root)
