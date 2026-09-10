"""Signer-owned database policy, never an authority-injected trusted snapshot."""

from __future__ import annotations

import asyncio
import importlib
import json
import secrets
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
import rfc8785
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import TaskImagePublicationKey
from loom_task_image_authority.publication_keyset import verify_publication_keyset
from loom_task_image_authority.publication_keyset_store import finalize_keyset, prepare_keyset
from loom_task_image_authority.publication_signing import (
    PublicationState,
    verify_historical_publication,
)
from tests.unit.test_task_image_publication_keyset import fixture
from tests.unit.test_task_image_publication_signing import NOW, setup_signing


@pytest.fixture
async def database(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        yield engine, async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


def module():
    name = "loom_task_image_signer.policy"
    try:
        found = importlib.util.find_spec(name)
    except ModuleNotFoundError:
        found = None
    assert found is not None, "dedicated signer policy is missing"
    return importlib.import_module(name)


class Provider:
    def __init__(self, private):
        self.private = private
        self.public_key = private.public_key().public_bytes_raw()
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.release.set()
        self.preimages = []

    async def sign(self, preimage):
        self.preimages.append(preimage)
        self.entered.set()
        await self.release.wait()
        return self.private.sign(preimage)


async def setup(database, *, clock=lambda: NOW, timeout=3.0):
    m = module()
    execution, root, *_ = fixture()
    c, _, private, key, _, _, unsigned, _ = setup_signing()
    async with database[1].begin() as session:
        session.add(TaskImagePublicationKey(**vars(key)))
    async with database[1].begin() as session:
        preparation = await prepare_keyset(session, trust_root=root)
    publication_provider, execution_provider = Provider(private), Provider(execution)
    selection = m.PublicationSelection.from_unsigned(unsigned)
    policy = m.SignerPolicy(
        database[0], trust_root=root, publication_key_id=key.key_id,
        publication_provider=publication_provider, execution_provider=execution_provider,
        selections=(selection,), clock=clock, timeout_seconds=timeout, keyset_lifetime_seconds=300,
    )
    request = rfc8785.dumps(dict(
        schema="loom.task-image-keyset-signing-request/v1", environment=root.environment,
        previous_keyset_version=0, proposed_keyset_version=1, revocation_epoch=0,
        keys=[member.model_dump(mode="json", exclude_none=True) for member in preparation.keys],
    ))
    return policy, request, preparation, root, key, unsigned, c, publication_provider, execution_provider


async def commit_keyset(database, setup_result):
    policy, request, plan, root, *_ = setup_result
    wire = await policy.sign_keyset(request)
    async with database[1].begin() as session:
        await finalize_keyset(session, preparation=plan, wire=wire, trust_root=root, clock=lambda: NOW)
    return wire


async def test_bootstrap_requires_no_old_artifact_but_publication_requires_commit(database):
    result = await setup(database)
    policy, request, plan, root, key, unsigned, c, publication_provider, execution_provider = result
    with pytest.raises(ValueError):
        await policy.sign_publication(c.canonical_publication_bytes(unsigned))
    assert not publication_provider.preimages
    wire = await policy.sign_keyset(request)
    checked = verify_publication_keyset(wire, trust_root=root, expected_state=plan.proposed_state, now=NOW)
    assert checked.keyset.issued_at == NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
    assert checked.keyset.expires_at == (NOW + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    with pytest.raises(ValueError):
        await policy.sign_publication(c.canonical_publication_bytes(unsigned))
    async with database[1].begin() as session:
        await finalize_keyset(session, preparation=plan, wire=wire, trust_root=root, clock=lambda: NOW)
    reply = await policy.sign_publication(c.canonical_publication_bytes(unsigned))
    publication = verify_historical_publication(reply, key=key)
    assert publication.statement.unsigned_input() == unsigned
    assert publication.statement.distributed_keyset_version == 1
    assert publication.statement.issued_at == NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
    assert execution_provider.preimages[0].startswith(b"loom-task-image-publication-keyset-v1\x00")
    assert publication_provider.preimages[0].startswith(b"loom-task-image-publication-v1\x00")


@pytest.mark.parametrize("operation", ["keyset", "publication"])
@pytest.mark.parametrize("change", ["insert", "retire", "revoke", "advance"])
async def test_signing_releases_database_locks_and_fences_authority_change(database, operation, change):
    result = await setup(database)
    policy, request, _, _, key, unsigned, c, publication_provider, execution_provider = result
    if operation == "publication":
        await commit_keyset(database, result)
    provider = publication_provider if operation == "publication" else execution_provider
    provider.entered.clear()
    provider.release.clear()
    task = asyncio.create_task(policy.sign_publication(c.canonical_publication_bytes(unsigned)) if operation == "publication" else policy.sign_keyset(request))
    try:
        await asyncio.wait_for(provider.entered.wait(), 2)
        async with database[1].begin() as session:
            await session.execute(text("SET LOCAL lock_timeout='500ms'"))
            # Must acquire both locks while provider I/O is held in another task.
            await session.execute(text("SELECT * FROM task_image_publication_state FOR UPDATE NOWAIT"))
            await session.execute(text("SELECT * FROM task_image_publication_keys FOR UPDATE NOWAIT"))
            if change == "insert":
                session.add(TaskImagePublicationKey(**vars(replace(key, key_id="second", public_key=Ed25519PrivateKey.generate().public_key().public_bytes_raw()))))
            elif change == "advance":
                await session.execute(text("UPDATE task_image_publication_state SET keyset_version=keyset_version+1"))
            else:
                column, status = ("retired_at", "verify_only") if change == "retire" else ("revoked_at", "revoked")
                await session.execute(text(f"UPDATE task_image_publication_keys SET status=:status, {column}=:now"), {"status": status, "now": NOW})
        provider.release.set()
        with pytest.raises(ValueError):
            await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("field,value", [
    ("environment", "staging"), ("pool_id", "wrong-pool"),
    ("registry_origin", "https://other.example"), ("build_policy_sha256", "b" * 64),
    ("builder_release_sha256", "c" * 64), ("supervisor_executable_sha256", "d" * 64),
    ("slurm_cluster_id", "other"),
])
async def test_publication_rejects_unconfigured_selection_before_provider(database, field, value):
    result = await setup(database)
    policy, _, _, _, _, unsigned, c, publication_provider, _ = result
    await commit_keyset(database, result)
    changed = unsigned.model_copy(update={field: value})
    with pytest.raises(ValueError):
        await policy.sign_publication(c.canonical_publication_bytes(changed))
    assert not publication_provider.preimages


async def test_new_allocation_attestation_uses_same_stable_release_selection(database):
    result = await setup(database)
    policy, _, _, _, key, unsigned, c, provider, _ = result
    await commit_keyset(database, result)
    for digest in ("8" * 64, "e" * 64):
        changed = unsigned.model_copy(update={"containment_attestation_sha256": digest})
        wire = await policy.sign_publication(c.canonical_publication_bytes(changed))
        statement = verify_historical_publication(wire, key=key).statement
        assert statement.containment_attestation_sha256 == digest
    assert len(provider.preimages) == 2


@pytest.mark.parametrize("operation", ["keyset", "publication"])
async def test_provider_deadline_cancels_and_leaves_database_usable(database, operation):
    result = await setup(database, timeout=0.2)
    policy, request, _, _, _, unsigned, c, publication_provider, execution_provider = result
    if operation == "publication":
        await commit_keyset(database, result)
    provider = publication_provider if operation == "publication" else execution_provider
    provider.release.clear()
    with pytest.raises(TimeoutError):
        await (policy.sign_publication(c.canonical_publication_bytes(unsigned)) if operation == "publication" else policy.sign_keyset(request))
    async with database[1].begin() as session:
        await session.execute(text("SELECT * FROM task_image_publication_state FOR UPDATE NOWAIT"))
        assert await session.scalar(text("SELECT count(*) FROM task_image_publication_keys")) == 1


@pytest.mark.parametrize("operation", ["keyset", "publication"])
async def test_signer_rechecks_own_clock_after_provider(database, operation):
    now = NOW
    result = await setup(database, clock=lambda: now)
    policy, request, _, _, _, unsigned, c, publication_provider, execution_provider = result
    if operation == "publication":
        await commit_keyset(database, result)
    provider = publication_provider if operation == "publication" else execution_provider
    provider.entered.clear()
    provider.release.clear()
    task = asyncio.create_task(policy.sign_publication(c.canonical_publication_bytes(unsigned)) if operation == "publication" else policy.sign_keyset(request))
    try:
        await asyncio.wait_for(provider.entered.wait(), 2)
        now += timedelta(minutes=5)
        provider.release.set()
        with pytest.raises(ValueError):
            await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_real_minimal_login_can_lock_and_sign_but_cannot_mutate_authority(database):
    result = await setup(database)
    _, request, _, root, key, unsigned, c, publication_provider, execution_provider = result
    await commit_keyset(database, result)
    role, password = "signer_" + uuid4().hex, secrets.token_hex(24)
    # Only the test-container administrator provisions this disposable role.
    async with database[0].begin() as connection:
        await connection.execute(text(f"CREATE ROLE {role} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '{password}'"))
        await connection.execute(text(f"GRANT USAGE ON SCHEMA public TO {role}"))
        await connection.execute(text(f"GRANT SELECT ON task_image_publication_state, task_image_publication_keys, task_image_publication_keysets, task_image_publication_keyset_members TO {role}"))
        await connection.execute(text(f"GRANT UPDATE(singleton_id) ON task_image_publication_state TO {role}"))
        await connection.execute(text(f"GRANT UPDATE(key_id) ON task_image_publication_keys TO {role}"))
    engine = create_async_engine(database[0].url.set(username=role, password=password))
    try:
        m = module()
        policy = m.SignerPolicy(
            engine, trust_root=root, publication_key_id=key.key_id,
            publication_provider=publication_provider, execution_provider=execution_provider,
            selections=(m.PublicationSelection.from_unsigned(unsigned),), clock=lambda: NOW,
        )
        reply = await policy.sign_publication(c.canonical_publication_bytes(unsigned))
        assert verify_historical_publication(reply, key=key).statement.unsigned_input() == unsigned
        refresh = json.loads(request)
        refresh.update(previous_keyset_version=1, proposed_keyset_version=2)
        signed = await policy.sign_keyset(rfc8785.dumps(refresh))
        assert verify_publication_keyset(signed, trust_root=root, expected_state=PublicationState(keyset_version=2), now=NOW)
        async with engine.begin() as connection:
            assert await connection.scalar(text("SELECT current_user = session_user")) is True
            assert await connection.scalar(text("SELECT count(*) FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=current_user)")) == 0
            assert await connection.scalar(text("SELECT has_schema_privilege(current_user, 'public', 'CREATE')")) is False
            await connection.execute(text("UPDATE task_image_publication_state SET singleton_id=singleton_id"))
            await connection.execute(text("UPDATE task_image_publication_keys SET key_id=key_id"))
        refused = [
            "UPDATE task_image_publication_state SET singleton_id=2",
            "UPDATE task_image_publication_state SET keyset_version=keyset_version+1",
            "UPDATE task_image_publication_state SET revocation_epoch=revocation_epoch+1",
            "UPDATE task_image_publication_keys SET key_id='substitution'",
            "UPDATE task_image_publication_keys SET public_key=decode(repeat('00',32),'hex')",
            "UPDATE task_image_publication_keys SET status='revoked', revoked_at=now()",
            "DELETE FROM task_image_publication_keys",
            "TRUNCATE task_image_publication_state",
            "UPDATE task_image_publication_keysets SET expires_at=expires_at + interval '1 hour'",
            "DELETE FROM task_image_publication_keyset_members",
            "INSERT INTO task_image_publication_keysets SELECT * FROM task_image_publication_keysets",
            "ALTER TABLE task_image_publication_keys DISABLE TRIGGER ALL",
            "SET session_replication_role='replica'",
            "CREATE TABLE public.signer_escalation(id int)",
            "SET ROLE postgres",
        ]
        for sql in refused:
            with pytest.raises(DBAPIError):
                async with engine.begin() as connection:
                    await connection.execute(text(sql))
    finally:
        await engine.dispose()
        async with database[0].begin() as connection:
            # Explicit disposable role; drops its grants, not any product tables.
            await connection.execute(text(f"DROP OWNED BY {role}"))
            await connection.execute(text(f"DROP ROLE {role}"))


@pytest.mark.parametrize("change", ["environment", "state", "key-bytes", "missing-key"])
async def test_keyset_request_must_equal_independent_preparation(database, change):
    policy, request, _, _, _, _, _, _, provider = await setup(database)
    data = json.loads(request)
    if change == "environment":
        data["environment"] = "staging"
    elif change == "state":
        data.update(previous_keyset_version=1, proposed_keyset_version=2)
    elif change == "key-bytes":
        data["keys"][0]["public_key"] = "A" * 43
    else:
        data["keys"][0]["key_id"] = "missing"
    with pytest.raises(ValueError):
        await policy.sign_keyset(rfc8785.dumps(data))
    assert not provider.preimages


@pytest.mark.parametrize("bad", ["short", "wrong-key", "wrong-domain"])
@pytest.mark.parametrize("operation", ["keyset", "publication"])
async def test_provider_result_is_independently_authenticated_before_return(database, bad, operation):
    result = await setup(database)
    policy, request, _, _, _, unsigned, c, publication, execution = result
    if operation == "publication":
        await commit_keyset(database, result)
    provider = publication if operation == "publication" else execution
    async def invalid(preimage):
        if bad == "short":
            return b"x"
        if bad == "wrong-key":
            return Ed25519PrivateKey.generate().sign(preimage)
        return provider.private.sign(b"wrong-domain" + preimage)
    provider.sign = invalid
    with pytest.raises(ValueError):
        await (policy.sign_publication(c.canonical_publication_bytes(unsigned)) if operation == "publication" else policy.sign_keyset(request))


async def test_real_tls_policy_database_bootstrap_commit_and_publication_chain(database, tmp_path):
    from loom_task_image_authority.publication_transport import (
        HTTPSKeysetSigner,
        HTTPSPublicationSigner,
    )
    from tests.unit.test_task_image_signer_server import service

    result = await setup(database)
    policy, request, plan, root, key, unsigned, c, _, _ = result
    async with service(tmp_path, operations=policy) as (_, _, identities):
        async with HTTPSKeysetSigner(**identities["keyset"]) as client:
            wire = await client.sign_keyset(request, maximum_reply_bytes=131072)
        checked = verify_publication_keyset(wire, trust_root=root, expected_state=plan.proposed_state, now=NOW)
        assert checked.keyset.keys == plan.keys
        async with HTTPSPublicationSigner(**identities["publication"]) as client:
            with pytest.raises(ValueError):
                await client.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=131072)
            async with database[1].begin() as session:
                await finalize_keyset(session, preparation=plan, wire=wire, trust_root=root, clock=lambda: NOW)
            reply = await client.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=131072)
        assert verify_historical_publication(reply, key=key).statement.unsigned_input() == unsigned
