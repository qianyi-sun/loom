"""Dedicated execution signer reads committed preparation with read-only grants."""

import asyncio
import hashlib
from uuid import UUID

import pytest
import rfc8785
from sqlalchemy import select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from loom.db.schema import TaskImageExecutionGrant, TaskImagePublicationKey
from loom_task_image_authority import execution_store as store
from loom_task_image_authority.publication_contracts import (
    decode_publication_envelope,
    decode_publication_statement,
)
from loom_task_image_signer.policy import PublicationSelection, SignerPolicy
from tests.integration.test_task_image_execution_store import ready, start_request
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)
from tests.integration.test_task_image_signer_policy import Provider
from tests.integration.test_trial_legacy_claim_identity import _TOKEN_HASH


async def setup(factory, issuer, tmp_path, monkeypatch):
    assert hasattr(store, "prepare_execution_signing_request"), "committed execution signing request missing"
    claim, root, private, now, _ = await ready(factory, issuer, tmp_path, monkeypatch)
    common = dict(claim=claim, worker_token_hash=_TOKEN_HASH, trust_root=root,
                  purpose="production", shadow_campaign_id=None, clock=lambda: now)
    async with factory.begin() as session:
        request = await store.prepare_execution_signing_request(session, **common)
        key = (await session.scalars(select(TaskImagePublicationKey))).one()
        url = session.bind.url
    unsigned = decode_publication_statement(decode_publication_envelope(request.publications[0].encode()).canonical_statement.encode()).unsigned_input()
    # Publication signing is not used by this test. The fixed public identity
    # must nevertheless match the retained publication key configuration.
    class PublicOnly:
        public_key = key.public_key

        async def sign(self, preimage):
            raise AssertionError("execution must not use publication private authority")

    provider = Provider(private)
    engine = create_async_engine(url)
    policy = SignerPolicy(engine, trust_root=root, publication_key_id=key.key_id,
                          publication_provider=PublicOnly(), execution_provider=provider,
                          selections=(PublicationSelection.from_unsigned(unsigned),), clock=lambda: now)
    return request, policy, provider, common, engine


async def test_dedicated_signer_reply_finalizes_and_consumes_retained_authority(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch,
):
    factory = registry_authority_session
    request, policy, provider, common, engine = await setup(factory, registry_issuer, tmp_path, monkeypatch)
    try:
        wire = await policy.sign_execution(request.canonical_bytes())
        assert len(provider.preimages) == 1
        assert provider.preimages[0].startswith(b"loom-task-image-execution-grant-v2\x00")
        async with factory.begin() as session:
            delivery = await store.finalize_execution_grant(session, wire=wire, **common)
            assert delivery.grant_envelope.encode() == wire
        # Stable fixed provider is safe to retry at issuance, never at start.
        assert await policy.sign_execution(request.canonical_bytes()) == wire
        async with factory.begin() as session:
            grant = (await session.scalars(select(TaskImageExecutionGrant))).one()
            from loom_task_image_authority.execution_grant import TaskImageExecutionGrantV2

            decoded = TaskImageExecutionGrantV2.model_validate_json(grant.canonical_grant)
            await store.consume_execution_start(session, request=start_request(decoded, wire), **common)
        with pytest.raises(ValueError):
            await policy.sign_execution(request.canonical_bytes())
        assert len(provider.preimages) == 1
    finally:
        await engine.dispose()


async def test_real_mtls_execution_signing_under_read_only_journal_role(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch,
):
    from loom_task_image_authority.publication_transport import HTTPSExecutionSigner
    from loom_task_image_signer.preflight import verify_signer_database_role
    from tests.integration.test_task_image_signer_preflight import role_engine
    from tests.unit.test_task_image_signer_server import service

    factory = registry_authority_session
    request, policy, provider, common, engine = await setup(factory, registry_issuer, tmp_path, monkeypatch)
    try:
        async with role_engine((engine, factory), execution=True) as (restricted, _):
            await verify_signer_database_role(restricted, execution_enabled=True)
            policy._engine = restricted
            async with service(tmp_path, operations=policy) as (_, _, identities):
                async with HTTPSExecutionSigner(**identities["execution"]) as client:
                    wire = await client.sign_execution(request.canonical_bytes(), maximum_reply_bytes=524288)
            async with factory.begin() as session:
                delivery = await store.finalize_execution_grant(session, wire=wire, **common)
                assert delivery.grant_envelope.encode() == wire
            for sql in (
                "SELECT auth_token_hash FROM workers",
                "SELECT * FROM trials",
                "UPDATE task_image_execution_grants SET revoked_at=now()",
                "UPDATE task_image_execution_starts SET expires_at=now()",
                "DELETE FROM task_image_execution_grants",
            ):
                with pytest.raises(DBAPIError):
                    async with restricted.begin() as connection:
                        await connection.execute(text(sql))
            assert len(provider.preimages) == 1
    finally:
        await engine.dispose()


@pytest.mark.parametrize("change", ["request-digest", "plan", "publication", "revoked", "superseded", "key-revoked"])
async def test_invalid_preparation_or_attachments_never_reach_private_provider(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch, change,
):
    factory = registry_authority_session
    request, policy, provider, common, engine = await setup(factory, registry_issuer, tmp_path, monkeypatch)
    data = request.model_dump(mode="json", by_alias=True, exclude_none=True)
    try:
        if change == "request-digest":
            data["grant_sha256"] = "a" * 64
        elif change == "plan":
            data["frozen_plan"] += " "
        elif change == "publication":
            data["publications"][0] += " "
        else:
            async with factory.begin() as session:
                if change == "key-revoked":
                    await session.execute(update(TaskImagePublicationKey).values(status="revoked", revoked_at=common["clock"]()))
                elif change == "revoked":
                    await session.execute(update(TaskImageExecutionGrant).values(revoked_at=common["clock"]()))
                else:
                    # Prepare a genuine later revision after expiry, while the
                    # independently retained keyset remains current.
                    from datetime import timedelta

                    await store.prepare_execution_grant(session, **dict(common, clock=lambda: common["clock"]() + timedelta(seconds=121)))
        with pytest.raises(ValueError):
            await policy.sign_execution(rfc8785.dumps(data))
        assert not provider.preimages
    finally:
        await engine.dispose()


async def test_execution_provider_io_releases_locks_and_rechecks_revocation(
    registry_authority_session, registry_issuer, tmp_path, monkeypatch,
):
    factory = registry_authority_session
    request, policy, provider, common, engine = await setup(factory, registry_issuer, tmp_path, monkeypatch)
    provider.release.clear()
    task = asyncio.create_task(policy.sign_execution(request.canonical_bytes()))
    try:
        await asyncio.wait_for(provider.entered.wait(), 2)
        async with asyncio.timeout(2), factory.begin() as session:
            await session.execute(update(TaskImageExecutionGrant)
                .where(TaskImageExecutionGrant.grant_id == UUID(request.grant_id))
                .values(revoked_at=common["clock"]()))
        provider.release.set()
        with pytest.raises(ValueError):
            await task
        assert hashlib.sha256(provider.preimages[0].split(b"\x00", 1)[1]).hexdigest() == request.grant_sha256
    finally:
        provider.release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await engine.dispose()
