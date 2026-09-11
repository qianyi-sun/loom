"""Native protected release publication is durable but never deletes holds."""

from importlib import import_module
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.admission import ExecutablePreparedBootstrapRevocationV2
from loom_capacity_agent.client import ExecutableProtectedReleasePublishReceiptV2
from tests.integration.test_personal_dev_build_guard_execution import admitted, store
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_withdrawal import bound_input, withdrawal
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


def outbox(session, installation):
    return import_module("loom_capacity_build_guard.release_outbox").BuildGuardReleaseOutbox(session, installation=installation)


async def release_input(values, kind):
    factory, _engine, installation, *_ = values
    if kind == "withdrawn":
        _registration, _digest, _prepared, binding, _bound = await bound_input(values)
        async with factory.begin() as session:
            result = await store(session, installation).withdraw_unregistered_worker(withdrawal(binding))
    else:
        registration, _digest = await admitted(values)
        request = ExecutablePreparedBootstrapRevocationV2(operation_id=uuid4(), binding=registration.binding,
            bootstrap_registration_epoch=1, protected_registration_epoch=2)
        async with factory.begin() as session:
            result = await store(session, installation).revoke_prepared_bootstrap(request)
    return result


@pytest.mark.parametrize("kind", ["withdrawn", "prepared-revoked"])
async def test_native_outbox_commit_replay_and_ack_preserve_holds(prepared_input, kind):
    factory, engine, installation, *_ = prepared_input
    async with factory.begin() as session:
        assert await outbox(session, installation).read_next() is None
    original = await release_input(prepared_input, kind)
    async with factory.begin() as session:
        publication = await outbox(session, installation).read_next()
    assert publication.event_kind == kind
    assert publication.event_id == original.protected_high_water
    assert publication.release.protected_release_sha256 == original.request_digest
    assert publication.release.reporter_incarnation == installation.document.reporter_incarnation
    async with factory.begin() as session:
        assert await outbox(session, installation).read_next() == publication
        checkpoint = await outbox(session, installation).acknowledge(publication,
            manager_acknowledgement_digest=publication.publication_digest)
    async with factory.begin() as session:
        assert await outbox(session, installation).acknowledge(publication,
            manager_acknowledgement_digest=publication.publication_digest) == checkpoint
        assert await outbox(session, installation).read_next() is None
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.release_publication_receipts")) == 1


async def test_outbox_cannot_publish_uncommitted_revocation(prepared_input):
    factory, _engine, installation, *_ = prepared_input
    registration, _ = await admitted(prepared_input)
    request = ExecutablePreparedBootstrapRevocationV2(operation_id=uuid4(), binding=registration.binding,
        bootstrap_registration_epoch=1, protected_registration_epoch=2)
    async with factory.begin() as session:
        await store(session, installation).revoke_prepared_bootstrap(request)
        with pytest.raises(DBAPIError, match="committed"):
            await outbox(session, installation).read_next()


@pytest.mark.parametrize("boundary", ["event", "kind", "digest", "manager"])
async def test_outbox_changed_ack_never_advances_publication(prepared_input, boundary):
    factory, engine, installation, *_ = prepared_input
    await release_input(prepared_input, "withdrawn")
    async with factory.begin() as session:
        publication = await outbox(session, installation).read_next()
    updates = {"event": {"event_id": publication.event_id+1}, "kind": {"event_kind": "prepared-revoked"},
        "digest": {"publication_digest": "f"*64}, "manager": {}}
    changed = publication.model_copy(update=updates[boundary])
    async with factory.begin() as session:
        with pytest.raises((ValueError, DBAPIError)):
            await outbox(session, installation).acknowledge(changed,
                manager_acknowledgement_digest="f"*64 if boundary == "manager" else publication.publication_digest)
        assert await outbox(session, installation).read_next() == publication
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.release_publication_receipts")) == 0


async def test_outbox_coordinator_retries_lost_manager_response_with_same_key(prepared_input):
    factory, engine, installation, *_ = prepared_input
    await release_input(prepared_input, "withdrawn")
    calls = []

    class Publisher:
        async def publish_executable_protected_release(self, publication, *, idempotency_key):
            calls.append((publication, idempotency_key))
            if len(calls) == 1:
                raise TimeoutError("lost manager response")
            return ExecutableProtectedReleasePublishReceiptV2(intent_id=publication.release.binding.intent_id,
                protected_release_sha256=publication.release.protected_release_sha256,
                receipt_digest=publication.publication_digest, replayed=True, executable=True)

    runtime = import_module("loom_capacity_build_guard.release_outbox").BuildReleaseCoordinator(
        session_factory=factory, installation=installation, publisher=Publisher())
    with pytest.raises(TimeoutError):
        await runtime.publish_next()
    assert await runtime.publish_next() is not None
    assert len(calls) == 2 and calls[0] == calls[1]
    assert await runtime.publish_next() is None
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_outbox_corrupt_checkpoint_rolls_back_before_outer_commit(prepared_input, monkeypatch):
    import json

    factory, engine, installation, *_ = prepared_input
    await release_input(prepared_input, "prepared-revoked")
    async with factory.begin() as session:
        publication = await outbox(session, installation).read_next()
        original = session.scalar

        async def corrupt(*args, **kwargs):
            result = json.loads(await original(*args, **kwargs))
            result["event_id"] += 1
            return json.dumps(result, sort_keys=True, separators=(",", ":"))

        monkeypatch.setattr(session, "scalar", corrupt)
        with pytest.raises(ValueError, match="receipt"):
            await outbox(session, installation).acknowledge(publication,
                manager_acknowledgement_digest=publication.publication_digest)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.release_publication_receipts")) == 0
