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


@pytest.mark.parametrize("boundary", ["intent", "release", "receipt", "type"])
async def test_outbox_coordinator_rejects_unrelated_manager_receipt(prepared_input, boundary):
    factory, engine, installation, *_ = prepared_input
    await release_input(prepared_input, "withdrawn")

    class Publisher:
        async def publish_executable_protected_release(self, publication, *, idempotency_key):
            result = ExecutableProtectedReleasePublishReceiptV2(intent_id=publication.release.binding.intent_id,
                protected_release_sha256=publication.release.protected_release_sha256,
                receipt_digest=publication.publication_digest, replayed=False, executable=True)
            changes = {"intent": {"intent_id": uuid4()}, "release": {"protected_release_sha256": "f"*64},
                "receipt": {"receipt_digest": "f"*64}}
            return result.model_dump() if boundary == "type" else result.model_copy(update=changes[boundary])

    runtime = import_module("loom_capacity_build_guard.release_outbox").BuildReleaseCoordinator(
        session_factory=factory, installation=installation, publisher=Publisher())
    with pytest.raises(ValueError, match="receipt"):
        await runtime.publish_next()
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.release_publication_receipts")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_outbox_concurrent_ack_and_committed_cursor(prepared_input):
    import asyncio

    factory, engine, installation, *_ = prepared_input
    await release_input(prepared_input, "withdrawn")
    async with factory.begin() as session:
        publication = await outbox(session, installation).read_next()

    async def acknowledge():
        try:
            async with factory.begin() as session:
                receipt = await outbox(session, installation).acknowledge(publication,
                    manager_acknowledgement_digest=publication.publication_digest)
                with pytest.raises(DBAPIError, match="committed"):
                    await outbox(session, installation).read_next()
            return receipt
        except DBAPIError as exc:
            assert exc.orig.sqlstate == "40001"
            return None

    receipts = [item for item in await asyncio.gather(acknowledge(), acknowledge()) if item is not None]
    assert receipts and all(item == receipts[0] for item in receipts)
    async with factory.begin() as session:
        assert await outbox(session, installation).acknowledge(publication,
            manager_acknowledgement_digest=publication.publication_digest) == receipts[0]
        assert await outbox(session, installation).read_next() is None
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.release_publication_receipts")) == 1


async def test_outbox_acknowledgements_are_private_immutable_and_retained(prepared_input, build_guard_database):
    from alembic import command

    factory, engine, installation, *_ = prepared_input
    await release_input(prepared_input, "prepared-revoked")
    async with factory.begin() as session:
        publication = await outbox(session, installation).read_next()
        await outbox(session, installation).acknowledge(publication,
            manager_acknowledgement_digest=publication.publication_digest)
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.execute(text("SELECT * FROM loom_capacity_build_guard.release_publication_receipts"))
    for statement in ("UPDATE loom_capacity_build_guard.release_publication_receipts SET payload=payload",
        "DELETE FROM loom_capacity_build_guard.release_publication_receipts", "TRUNCATE loom_capacity_build_guard.release_publication_receipts"):
        with pytest.raises(DBAPIError):
            with engine.begin() as connection:
                connection.execute(text(statement))
    with pytest.raises(DBAPIError, match="retained evidence"):
        command.downgrade(build_guard_database[0], "build_guard_0013")


@pytest.mark.parametrize("boundary", ["noncanonical", "manager", "event", "kind", "release", "schema", "extra", "installation"])
async def test_outbox_direct_sql_rejects_changed_publication(prepared_input, boundary):
    import json
    from hashlib import sha256

    from loom_capacity_manager.executable_contracts import canonical_executable_bytes

    factory, engine, installation, *_ = prepared_input
    await release_input(prepared_input, "withdrawn")
    async with factory.begin() as session:
        publication = await outbox(session, installation).read_next()
    payload = json.loads(canonical_executable_bytes(publication))
    if boundary == "event":
        payload["event_id"] += 1
    elif boundary == "kind":
        payload["event_kind"] = "prepared-revoked"
    elif boundary == "release":
        payload["release"]["protected_registration_epoch"] += 1
    elif boundary == "schema":
        payload["schema_version"] = "2"
    elif boundary == "extra":
        payload["free_capacity"] = True
    wire = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    if boundary == "noncanonical":
        wire += b" "
    async with factory.begin() as session:
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.scalar(text("""SELECT loom_capacity_build_guard.acknowledge_protected_release(
                    :installation,CAST(:payload AS jsonb),:wire,:digest,:manager)"""),
                    {"installation": uuid4() if boundary == "installation" else installation.id,
                        "payload": wire.decode("ascii"), "wire": wire, "digest": sha256(wire).hexdigest(),
                        "manager": "f"*64 if boundary == "manager" else publication.publication_digest})
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.release_publication_receipts")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_outbox_pending_events_cannot_be_skipped_and_old_ack_replays(prepared_input):
    from loom_capacity_agent.admission import PublishableExecutableProtectedReleaseV2
    from loom_capacity_build_guard.bootstrap_store import BuildGuardBootstrapStore
    from loom_capacity_manager.executable_contracts import (
        ExecutableProtectedReleaseV2,
        canonical_executable_digest,
    )
    from tests.integration.test_personal_dev_build_guard_bootstrap import bootstrap

    factory, engine, installation, plan, *_ = prepared_input
    await release_input(prepared_input, "withdrawn")
    async with factory.begin() as session:
        first = await outbox(session, installation).read_next()
    proposal = bootstrap(plan)
    proposal = proposal.model_copy(update={"binding": proposal.binding.model_copy(update={"intent_id": uuid4()})})
    async with factory.begin() as session:
        await BuildGuardBootstrapStore(session, installation=installation).register(proposal)
    async with factory.begin() as session:
        revoked = await store(session, installation).revoke_prepared_bootstrap(ExecutablePreparedBootstrapRevocationV2(
            operation_id=uuid4(), binding=proposal.binding, bootstrap_registration_epoch=1, protected_registration_epoch=2))
    release = ExecutableProtectedReleaseV2(binding=revoked.binding, reporter_incarnation=revoked.reporter_incarnation,
        bootstrap_registration_epoch=1, protected_registration_epoch=2, bootstrap_revoked=True,
        protected_release_sha256=revoked.protected_release_sha256)
    second = PublishableExecutableProtectedReleaseV2(event_id=revoked.protected_high_water, event_kind="prepared-revoked",
        release=release, publication_digest=canonical_executable_digest(release))
    assert second.event_id > first.event_id > 1
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match="exact next"):
            await outbox(session, installation).acknowledge(second, manager_acknowledgement_digest=second.publication_digest)
        first_ack = await outbox(session, installation).acknowledge(first, manager_acknowledgement_digest=first.publication_digest)
    async with factory.begin() as session:
        assert await outbox(session, installation).read_next() == second
        await outbox(session, installation).acknowledge(second, manager_acknowledgement_digest=second.publication_digest)
    async with factory.begin() as session:
        assert await outbox(session, installation).acknowledge(first, manager_acknowledgement_digest=first.publication_digest) == first_ack
        assert await outbox(session, installation).read_next() is None
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.release_publication_receipts")) == 2
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_outbox_cannot_acknowledge_another_installed_owner(prepared_input, owner_sessions, tmp_path):
    from loom_capacity_build_guard.installation_store import BuildGuardInstallationStore
    from loom_capacity_manager.executable_contracts import (
        canonical_executable_bytes,
        canonical_executable_digest,
    )
    from tests.unit.test_personal_dev_build_admission import admission_input

    factory, engine, installation, *_ = prepared_input
    await release_input(prepared_input, "withdrawn")
    async with factory.begin() as session:
        publication = await outbox(session, installation).read_next()
    values = admission_input(tmp_path)
    member = values["member"]
    subject, incarnation = uuid4(), uuid4()
    member = member.model_copy(update={
        "configuration": member.configuration.model_copy(update={"subject_id": subject, "subject_incarnation": incarnation}),
        "acknowledgement": member.acknowledgement.model_copy(update={"subject_id": subject, "subject_incarnation": incarnation})})
    owner_factory, owner = owner_sessions
    async with owner_factory.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner}"))
        foreign = await BuildGuardInstallationStore(session, expected_owner_role=owner).retain(member=member, runtime=values["runtime"])
    assert foreign.id != installation.id and foreign.document.owner_user_id != installation.document.owner_user_id
    wire = canonical_executable_bytes(publication)
    async with factory.begin() as session:
        assert await outbox(session, foreign).read_next() is None
        with pytest.raises(DBAPIError, match="installation"):
            async with session.begin_nested():
                await session.scalar(text("""SELECT loom_capacity_build_guard.acknowledge_protected_release(
                    :installation,CAST(:payload AS jsonb),:wire,:digest,:manager)"""),
                    {"installation": foreign.id, "payload": wire.decode("ascii"), "wire": wire,
                        "digest": canonical_executable_digest(publication), "manager": publication.publication_digest})
        assert await outbox(session, installation).read_next() == publication
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.release_publication_receipts")) == 0
