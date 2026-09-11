"""Durable signed distribution using disposable PostgreSQL, not fleet acceptance."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
from dataclasses import replace
from datetime import timedelta

import pytest
import rfc8785
from alembic import command
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import TaskImagePublicationKey, TaskImagePublicationState
from loom_task_image_authority.publication_signing import PublicationState
from tests.integration.test_task_image_registry_credential_migration import _config
from tests.unit.test_task_image_publication_keyset import _b64, _sign, fixture
from tests.unit.test_task_image_publication_signing import NOW, setup_signing


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


async def wait_locked(observer, pid):
    async with asyncio.timeout(3):
        while not await observer.scalar(text("SELECT cardinality(pg_blocking_pids(:pid)) > 0"), {"pid": pid}):
            await asyncio.sleep(0.01)


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
    def clock():
        return NOW + timedelta(minutes=5) if change == "expired" else NOW
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
        pid = await waiter.scalar(text("SELECT pg_backend_pid()"))
        await blocker.execute(select(TaskImagePublicationState).with_for_update())
        task = asyncio.create_task(m.finalize_keyset(waiter, preparation=plan, wire=wire, trust_root=root, clock=lambda: now))
        await wait_locked(blocker, pid)
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
        with pytest.raises(ValueError, match=r"READ COMMITTED|pending"):
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
    m, private, root, payload, old_plan, _key, wire, _old = await publish(database)
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
        with pytest.raises(ValueError, match=r"128|bound"):
            await m.prepare_keyset(session, trust_root=root)


async def test_key_order_is_canonical_not_database_locale_order(database):
    m, private, root, payload, _, key = await prepared(database)
    async with database[1].begin() as session:
        session.add_all(TaskImagePublicationKey(**vars(replace(key, key_id=name, public_key=Ed25519PrivateKey.generate().public_key().public_bytes_raw()))) for name in ("a-1", "a.1", "a_1", "a0"))
    async with database[1].begin() as session:
        plan = await m.prepare_keyset(session, trust_root=root)
    assert tuple(member.key_id for member in plan.keys) == tuple(sorted(member.key_id for member in plan.keys))
    payload.update(keys=[member.model_dump(mode="json", exclude_none=True) for member in plan.keys])
    async with database[1].begin() as session:
        retained = await m.finalize_keyset(session, preparation=plan, wire=_sign(payload, private), trust_root=root, clock=lambda: NOW)
    async with database[1].begin() as session:
        assert await m.read_keyset(session, trust_root=root, expected_state=retained.state, clock=lambda: NOW) == retained


@pytest.mark.parametrize("isolation", ["AUTOCOMMIT", "REPEATABLE READ"])
async def test_adapter_owns_checked_out_connection_isolation(database, isolation):
    m, _, root, _, _, key, _, result = await publish(database)
    adapter = m.DatabasePublicationDistribution(database[0].execution_options(isolation_level=isolation), trust_root=root, clock=lambda: NOW)
    assert (await adapter.snapshot(state=result.state, key=key)).keyset_version == 1


async def test_adapter_configures_independent_server_timeouts(database, monkeypatch):
    m, _, root, _, _, key, _, result = await publish(database)
    read = m.read_keyset

    async def inspect_bounds(session, **kwargs):
        assert await session.scalar(text("SHOW statement_timeout")) == "5s"
        assert await session.scalar(text("SHOW idle_in_transaction_session_timeout")) == "5s"
        return await read(session, **kwargs)

    monkeypatch.setattr(m, "read_keyset", inspect_bounds)
    adapter = m.DatabasePublicationDistribution(database[0], trust_root=root, clock=lambda: NOW, timeout_seconds=5)
    assert (await adapter.snapshot(state=result.state, key=key)).keyset_version == 1


async def test_adapter_cancellation_releases_owned_connection_and_does_not_change_state(database, monkeypatch):
    m, _, root, _, _, key, _, result = await publish(database)
    read = m.read_keyset
    pid = asyncio.Future()

    async def identify(session, **kwargs):
        pid.set_result(await session.scalar(text("SELECT pg_backend_pid()")))
        return await read(session, **kwargs)

    monkeypatch.setattr(m, "read_keyset", identify)
    adapter = m.DatabasePublicationDistribution(database[0], trust_root=root, clock=lambda: NOW)
    async with database[1].begin() as blocker:
        await blocker.execute(select(TaskImagePublicationState).with_for_update())
        task = asyncio.create_task(adapter.snapshot(state=result.state, key=key))
        await wait_locked(blocker, await asyncio.wait_for(pid, 3))
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 3)
    monkeypatch.setattr(m, "read_keyset", read)
    assert (await adapter.snapshot(state=result.state, key=key)).keyset_version == 1


async def test_expiry_during_flush_rolls_back_entire_publication(database, monkeypatch):
    m, private, root, payload, plan, _ = await prepared(database)
    now = NOW
    with pytest.raises(ValueError):
        async with database[1].begin() as session:
            flush = session.flush

            async def slow_flush(*args, **kwargs):
                nonlocal now
                await flush(*args, **kwargs)
                now += timedelta(minutes=5)

            monkeypatch.setattr(session, "flush", slow_flush)
            await m.finalize_keyset(session, preparation=plan, wire=_sign(payload, private), trust_root=root, clock=lambda: now)
    async with database[1]() as session:
        assert await session.scalar(select(TaskImagePublicationState.keyset_version)) == 0
        for table in ("task_image_publication_keysets", "task_image_publication_keyset_members"):
            assert await session.scalar(text(f"SELECT count(*) FROM {table}")) == 0


async def test_key_insert_serializes_and_waiting_prepare_sees_committed_phantom(database):
    m, _, root, _, _, key = await prepared(database)
    async with database[1].begin() as writer, database[1]() as reader:
        pid = await reader.scalar(text("SELECT pg_backend_pid()"))
        writer.add(TaskImagePublicationKey(**vars(replace(key, key_id="new", public_key=Ed25519PrivateKey.generate().public_key().public_bytes_raw()))))
        await writer.flush()  # Real statement trigger holds unchanged singleton.
        task = asyncio.create_task(m.prepare_keyset(reader, trust_root=root))
        await wait_locked(writer, pid)
        await writer.commit()
        plan = await asyncio.wait_for(task, 3)
        assert {member.key_id for member in plan.keys} == {key.key_id, "new"}
        await reader.rollback()


@pytest.mark.parametrize("identical", [True, False])
async def test_concurrent_finalizers_replay_only_exact_winner(database, identical):
    m, private, root, payload, plan, _ = await prepared(database)
    wire = _sign(payload, private)
    if not identical:
        payload["expires_at"] = (NOW + timedelta(minutes=4)).strftime("%Y-%m-%dT%H:%M:%SZ")
    other = _sign(payload, private)
    async with database[1].begin() as winner, database[1]() as loser:
        pid = await loser.scalar(text("SELECT pg_backend_pid()"))
        result = await m.finalize_keyset(winner, preparation=plan, wire=wire, trust_root=root, clock=lambda: NOW)
        task = asyncio.create_task(m.finalize_keyset(loser, preparation=plan, wire=other, trust_root=root, clock=lambda: NOW))
        await wait_locked(winner, pid)
        await winner.commit()
        if identical:
            assert await asyncio.wait_for(task, 3) == result
        else:
            with pytest.raises(ValueError, match="replay conflicts"):
                await asyncio.wait_for(task, 3)
        await loser.rollback()


async def test_cached_state_is_refreshed_and_pending_changes_are_preserved(database):
    m, _, root, _, _, _, _, result = await publish(database)
    async with database[1]() as cached:
        row = await cached.get(TaskImagePublicationState, 1)
        async with database[1].begin() as writer:
            await writer.execute(text("UPDATE task_image_publication_state SET revocation_epoch=1"))
        with pytest.raises(ValueError, match="stale"):
            await m.read_keyset(cached, trust_root=root, expected_state=result.state, clock=lambda: NOW)
        assert row.revocation_epoch == 1
        row.keyset_version = 2
        with pytest.raises(ValueError, match="pending"):
            await m.prepare_keyset(cached, trust_root=root)
        assert row in cached.dirty
        await cached.rollback()


@pytest.mark.parametrize("method", ["snapshot", "envelope"])
async def test_adapter_rejects_expired_artifact_without_refreshing(database, method):
    m, _, root, _, _, key, _, result = await publish(database)
    adapter = m.DatabasePublicationDistribution(database[0], trust_root=root, clock=lambda: NOW + timedelta(minutes=5))
    with pytest.raises(ValueError):
        if method == "snapshot":
            await adapter.snapshot(state=result.state, key=key)
        else:
            await adapter.envelope(state=result.state)


@pytest.mark.parametrize("field", ["root_sha256", "keyset_sha256", "environment", "issued_at", "members"])
async def test_read_refuses_corrupt_metadata_or_membership(database, field):
    m, _, root, _, _, _, _, result = await publish(database)
    async with database[1].begin() as corrupt:
        table = "task_image_publication_keyset_members" if field == "members" else "task_image_publication_keysets"
        # Disposable superuser fixture simulates persisted corruption; runtime has
        # no trigger-disabling API and never repairs bytes in place.
        await corrupt.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
        if field == "members":
            await corrupt.execute(text(f"DELETE FROM {table}"))
        else:
            value = "issued_at - interval '1 second'" if field == "issued_at" else "'staging'" if field == "environment" else "repeat('e',64)"
            await corrupt.execute(text(f"UPDATE {table} SET {field}={value}"))
        await corrupt.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))
    async with database[1].begin() as session:
        with pytest.raises(ValueError):
            await m.read_keyset(session, trust_root=root, expected_state=result.state, clock=lambda: NOW)


async def test_published_audit_is_immutable_and_downgrade_refuses(database, isolated_migration_postgres_url):
    await publish(database)
    for table in ("task_image_publication_keysets", "task_image_publication_keyset_members"):
        for sql in (f"DELETE FROM {table}", f"UPDATE {table} SET keyset_version=keyset_version", f"TRUNCATE {table} CASCADE"):
            async with database[1]() as session:
                with pytest.raises(DBAPIError, match="immutable"):
                    await session.execute(text(sql))
                await session.rollback()
    with pytest.raises(DBAPIError, match="keyset audit cannot be discarded"):
        command.downgrade(_config(isolated_migration_postgres_url), "0137")
    async with database[1]() as session:
        assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0141"
        assert await session.scalar(select(TaskImagePublicationState.keyset_version)) == 1


def test_empty_migration_roundtrip_and_schema_parity(isolated_migration_postgres_url):
    from loom.db.schema import Base

    engine = create_engine(isolated_migration_postgres_url)
    config = _config(isolated_migration_postgres_url)
    tables = ("task_image_publication_keysets", "task_image_publication_keyset_members")
    try:
        command.downgrade(config, "0137")
        assert not set(tables) & set(inspect(engine).get_table_names())
        command.upgrade(config, "head")
        inspector = inspect(engine)
        for table in tables:
            model = Base.metadata.tables[table]
            assert {col["name"]: col["nullable"] for col in inspector.get_columns(table)} == {col.name: col.nullable for col in model.columns}
            assert {item["name"] for item in inspector.get_check_constraints(table)} == {item.name for item in model.constraints if item.__class__.__name__ == "CheckConstraint"}
            assert all(item["options"]["ondelete"] == "RESTRICT" for item in inspector.get_foreign_keys(table))
        assert {item["referred_table"] for item in inspector.get_foreign_keys(tables[1])} == {tables[0], "task_image_publication_keys"}
    finally:
        engine.dispose()


@pytest.mark.parametrize("target", ["task_image_publication_state", "task_image_publication_keys"])
@pytest.mark.parametrize("direction", ["upgrade", "downgrade"])
def test_migration_refuses_busy_parents_without_waiting(isolated_migration_postgres_url, target, direction):
    config = _config(isolated_migration_postgres_url)
    if direction == "upgrade":
        command.downgrade(config, "0137")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as holder:
            holder.execute(text(f"LOCK TABLE {target} IN ACCESS SHARE MODE"))
            with pytest.raises(DBAPIError, match="could not obtain lock"):
                if direction == "upgrade":
                    command.upgrade(config, "head")
                else:
                    command.downgrade(config, "0137")
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT version_num FROM alembic_version")) == ("0137" if direction == "upgrade" else "0141")
    finally:
        engine.dispose()


async def test_real_signed_distribution_composes_with_publication_signing_without_locks(database):
    m = module()
    contracts, signing, publication_private, key, _, _, unsigned, _ = setup_signing()
    execution_private, root, payload, *_ = fixture()
    async with database[1].begin() as session:
        session.add(TaskImagePublicationKey(**vars(key)))
    async with database[1].begin() as session:
        plan = await m.prepare_keyset(session, trust_root=root)
    payload.update(keyset_version=1, revocation_epoch=0, keys=[member.model_dump(mode="json", exclude_none=True) for member in plan.keys])
    keyset_wire = _sign(payload, execution_private)
    async with database[1].begin() as session:
        retained = await m.finalize_keyset(session, preparation=plan, wire=keyset_wire, trust_root=root, clock=lambda: NOW)
    adapter = m.DatabasePublicationDistribution(database[0], trust_root=root, clock=lambda: NOW)
    distributed = await adapter.snapshot(state=retained.state, key=key)

    class Signer:
        async def sign_publication(self, request, *, maximum_reply_bytes):
            # Real independent transaction proves distribution held no lock
            # over this test signer invocation. Production signer is external.
            async with database[1].begin() as probe:
                await probe.execute(select(TaskImagePublicationState).with_for_update(nowait=True))
            statement = signing.prepare_publication_statement(
                contracts.decode_unsigned_input(request), key=key, state=retained.state,
                distribution=distributed, signer_now=NOW,
            )
            canonical = contracts.canonical_publication_bytes(statement)
            wire = rfc8785.dumps(dict(
                canonical_statement=canonical.decode(), statement_sha256=hashlib.sha256(canonical).hexdigest(),
                key_id=key.key_id, algorithm="Ed25519",
                signature=_b64(publication_private.sign(contracts.PUBLICATION_DOMAIN + canonical)),
            ))
            assert len(wire) <= maximum_reply_bytes
            return wire

    result = await signing.request_publication_signature(
        Signer(), unsigned, key=key, state=retained.state, distribution=distributed,
        clock=lambda: NOW, timeout_seconds=2,
    )
    assert result.statement.unsigned_input() == unsigned
    assert result.statement.distributed_keyset_version == 1
