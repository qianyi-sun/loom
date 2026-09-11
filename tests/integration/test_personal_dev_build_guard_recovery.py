"""Management recovery joins real guard state across committed network steps."""

from importlib import import_module

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_hold_retirement import (
    ready_hold,
    release_witness,
    retirement,
)
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_release_outbox import outbox
from tests.integration.test_personal_dev_build_guard_terminal import terminal_input
from tests.integration.test_personal_dev_build_guard_withdrawal import withdrawal
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


def coordinator(factory, installation, manager, **kwargs):
    return import_module("loom_capacity_build_guard.recovery").BuildRecoveryCoordinator(
        session_factory=factory, installation=installation, manager=manager, **kwargs)


async def test_discovery_requires_committed_ack_and_excludes_retired_holds(prepared_input):
    factory, _engine, installation, *_ = prepared_input
    witness = await ready_hold(prepared_input, "prepared-revoked", acknowledge=False)
    async with factory.begin() as session:
        assert (await retirement(session, installation).read_pending()).publications == ()
        publication = await outbox(session, installation).read_next()
        await outbox(session, installation).acknowledge(publication,
            manager_acknowledgement_digest=publication.publication_digest)
        with pytest.raises(DBAPIError, match="committed acknowledgement"):
            async with session.begin_nested():
                await retirement(session, installation).read_pending()
    async with factory.begin() as session:
        pending = await retirement(session, installation).read_pending()
        assert pending.publications == (publication,)
        assert (await retirement(session, installation).read_pending(after_event_id=publication.event_id)).publications == ()
        await retirement(session, installation).retire(witness)
    async with factory.begin() as session:
        assert (await retirement(session, installation).read_pending()).publications == ()


@pytest.mark.parametrize("kind", ["prepared-revoked", "withdrawn", "released"])
async def test_recovery_commits_terminal_before_retiring_without_network_locks(prepared_input, monkeypatch, kind):
    factory, engine, installation, *_ = prepared_input
    terminal = None
    if kind in {"withdrawn", "released"}:
        if kind == "released":
            from tests.integration.test_personal_dev_build_guard_registered_release import (
                registered_release_input,
            )
            from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL

            request, _claim, terminal, _drain = await registered_release_input(prepared_input, monkeypatch)
            async with factory.begin() as session:
                await store(session, installation).acknowledge_release(request, current_worker_credential=CREDENTIAL)
        else:
            terminal, physical = await terminal_input(prepared_input)
            async with factory.begin() as session:
                await store(session, installation).withdraw_unregistered_worker(withdrawal(physical))
        async with factory.begin() as session:
            publication = await outbox(session, installation).read_next()
            await outbox(session, installation).acknowledge(publication,
                manager_acknowledgement_digest=publication.publication_digest)
        witness = release_witness(publication, terminal)
    else:
        witness = await ready_hold(prepared_input, kind)
    calls = []

    class Manager:
        async def get_final_release_witness(self, intent_id):
            assert intent_id == witness.release.binding.intent_id
            # A network callback can acquire installation authority: discovery
            # committed its locks before issuing this authenticated request.
            with engine.begin() as connection:
                connection.execute(text("SET LOCAL lock_timeout='100ms'"))
                connection.execute(text("SELECT id FROM loom_capacity_build_guard.installations WHERE id=:id FOR UPDATE"), {"id": installation.id})
            calls.append("release")
            return witness

        async def get_build_terminal_inventory_evidence(self, intent_id):
            assert intent_id == witness.release.binding.intent_id
            calls.append("terminal")
            return terminal

    runtime = coordinator(factory, installation, Manager())
    results = await runtime.reconcile()
    assert len(results) == 1 and results[0].state == "retired"
    assert calls == (["release", "terminal"] if terminal else ["release"])
    assert await runtime.reconcile() == ()
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.terminal_inventory")) == (terminal is not None)
        assert connection.scalar(text("SELECT count(*) FROM personal_dev_native_build_grants")) == 0


async def test_missing_witness_is_retried_after_cursor_wrap(prepared_input):
    factory, engine, installation, *_ = prepared_input
    witness = await ready_hold(prepared_input, "prepared-revoked")
    responses = [None, witness]

    class Manager:
        async def get_final_release_witness(self, intent_id):
            return responses.pop(0)

    runtime = coordinator(factory, installation, Manager(), batch_size=1)
    assert (await runtime.reconcile())[0].state == "unavailable"
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
    assert (await runtime.reconcile())[0].state == "retired"


async def other_pool_input(values, sessions, tmp_path):
    from datetime import UTC, datetime, timedelta
    from uuid import uuid4

    from loom.personal_dev_build_platform_requests import stage_platform_requests
    from tests.integration.test_personal_dev_build_platform_requests import build_service
    from tests.unit.test_personal_dev_build_admission import admission_input

    factory, engine, installation, original, registration, _request = values
    pool = "oldlab" if original.shapes[0].binding.pool_id == "gb10" else "gb10"
    member, runtime = build_service(tmp_path, registration)
    now = datetime.now(UTC)
    async with sessions.begin() as session:
        requests = await stage_platform_requests(session, registration, member=member, runtime=runtime,
            platforms=("linux/amd64" if pool == "oldlab" else "linux/arm64",), now=now)
    plan = admission_input(tmp_path, pool=pool)["proposal"]
    binding = plan.shapes[0].binding.model_copy(update={"account_id": member.configuration.account_id,
        "intent_id": uuid4(), "tranche_id": uuid4(), "shape_instance_id": "retry-" + uuid4().hex})
    plan = plan.model_copy(update={"lease_not_after": now + timedelta(minutes=2),
        "shapes": (plan.shapes[0].model_copy(update={"binding": binding}),),
        "allowances": (plan.allowances[0].model_copy(update={"protected_attempt_id": requests[0].id,
            "submission_intent_id": binding.intent_id, "shape_instance_id": binding.shape_instance_id}),)})
    return factory, engine, installation, plan, registration, requests[0]


async def test_finite_sweep_retries_early_work_despite_continuous_new_arrivals(prepared_input, sessions, tmp_path):
    factory, engine, installation, *_ = prepared_input
    first = await ready_hold(prepared_input, "prepared-revoked")
    second_values = await other_pool_input(prepared_input, sessions, tmp_path)
    second = await ready_hold(second_values, "prepared-revoked")
    witnesses = {second.release.binding.intent_id: second}
    calls = []

    class Manager:
        async def get_final_release_witness(self, intent_id):
            calls.append(intent_id)
            return witnesses.get(intent_id)

    runtime = coordinator(factory, installation, Manager(), batch_size=1)
    assert (await runtime.reconcile())[0].state == "unavailable"
    assert (await runtime.reconcile())[0].state == "retired"
    # A later arrival reuses the second request after exact cleanup. A moving-end
    # cursor would follow it instead of retrying the first still-pending intent.
    newer = await ready_hold(await other_pool_input(prepared_input, sessions, tmp_path), "prepared-revoked")
    witnesses[newer.release.binding.intent_id] = newer
    witnesses[first.release.binding.intent_id] = first
    assert (await runtime.reconcile())[0].intent_id == first.release.binding.intent_id
    assert calls[:3] == [first.release.binding.intent_id, second.release.binding.intent_id, first.release.binding.intent_id]
    assert (await runtime.reconcile())[0].intent_id == newer.release.binding.intent_id
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0


@pytest.mark.parametrize("boundary", ["release", "terminal", "timeout", "cancel"])
async def test_recovery_failure_preserves_hold_and_cancellation_propagates(prepared_input, boundary):
    import asyncio

    factory, engine, installation, *_ = prepared_input
    terminal, physical = await terminal_input(prepared_input)
    async with factory.begin() as session:
        await store(session, installation).withdraw_unregistered_worker(withdrawal(physical))
    async with factory.begin() as session:
        publication = await outbox(session, installation).read_next()
        await outbox(session, installation).acknowledge(publication, manager_acknowledgement_digest=publication.publication_digest)
    witness = release_witness(publication, terminal)

    class Manager:
        async def get_final_release_witness(self, intent_id):
            if boundary == "cancel":
                raise asyncio.CancelledError
            if boundary == "timeout":
                await asyncio.Event().wait()
            return witness.model_copy(update={"protected_acknowledgement_sha256": "f" * 64}) if boundary == "release" else witness

        async def get_build_terminal_inventory_evidence(self, intent_id):
            return terminal.model_copy(update={"inventory_sequence": terminal.inventory_sequence + 1})

    runtime = coordinator(factory, installation, Manager(), item_timeout_seconds=0.5)
    if boundary == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await runtime.reconcile()
    else:
        result = (await runtime.reconcile())[0]
        assert result.state == "failed"
        assert result.failure == ("timeout" if boundary == "timeout" else "authority")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.terminal_inventory")) == 0


@pytest.mark.parametrize("boundary", ["after", "through", "limit", "installation"])
async def test_discovery_sql_bounds_and_unknown_installation_fail_closed(prepared_input, boundary):
    from uuid import uuid4

    factory, _engine, installation, *_ = prepared_input
    await ready_hold(prepared_input, "prepared-revoked")
    arguments = {"installation": installation.id, "after": 0, "through": None, "limit": 1}
    arguments.update({"after": -1} if boundary == "after" else {"after": 1, "through": 0} if boundary == "through"
        else {"limit": 65} if boundary == "limit" else {"installation": uuid4()})
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match=r"bounds|installation"):
            async with session.begin_nested():
                await session.scalar(text("""SELECT loom_capacity_build_guard.read_pending_retirements(
                    :installation,:after,:through,:limit)"""), arguments)


@pytest.mark.parametrize("boundary", ["grant", "public", "search-path"])
def test_discovery_callable_privilege_drift_is_rejected(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.read_pending_retirements(uuid,bigint,bigint,integer)"
    statements = {"grant": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public"}
    with engine.begin() as connection:
        connection.execute(text(statements[boundary]))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")


async def test_recovery_retry_after_committed_terminal_import(prepared_input, monkeypatch):
    import asyncio

    from loom_capacity_build_guard.hold_retirement import BuildGuardHoldRetirementStore

    factory, engine, installation, *_ = prepared_input
    terminal, physical = await terminal_input(prepared_input)
    async with factory.begin() as session:
        await store(session, installation).withdraw_unregistered_worker(withdrawal(physical))
    async with factory.begin() as session:
        publication = await outbox(session, installation).read_next()
        await outbox(session, installation).acknowledge(publication, manager_acknowledgement_digest=publication.publication_digest)
    witness = release_witness(publication, terminal)

    class Manager:
        async def get_final_release_witness(self, intent_id):
            return witness

        async def get_build_terminal_inventory_evidence(self, intent_id):
            return terminal

    original = BuildGuardHoldRetirementStore.retire

    async def cancelled(self, witness):
        raise asyncio.CancelledError

    monkeypatch.setattr(BuildGuardHoldRetirementStore, "retire", cancelled)
    with pytest.raises(asyncio.CancelledError):
        await coordinator(factory, installation, Manager()).reconcile()
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.terminal_inventory")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
    monkeypatch.setattr(BuildGuardHoldRetirementStore, "retire", original)
    assert (await coordinator(factory, installation, Manager()).reconcile())[0].state == "retired"


@pytest.mark.parametrize("boundary", ["timeout", "transport", "database"])
async def test_one_failed_item_does_not_block_same_page_success(prepared_input, sessions, tmp_path, monkeypatch, boundary):
    import asyncio

    from loom_capacity_agent.client import DemandPublishError
    from loom_capacity_build_guard.hold_retirement import BuildGuardHoldRetirementStore

    factory, engine, installation, *_ = prepared_input
    first = await ready_hold(prepared_input, "prepared-revoked")
    second = await ready_hold(await other_pool_input(prepared_input, sessions, tmp_path), "prepared-revoked")
    original = BuildGuardHoldRetirementStore.retire

    async def fail_one(self, witness):
        if boundary == "database" and witness == first:
            raise DBAPIError("retire", {}, Exception("local database unavailable"))
        return await original(self, witness)

    monkeypatch.setattr(BuildGuardHoldRetirementStore, "retire", fail_one)

    class Manager:
        async def get_final_release_witness(self, intent_id):
            if intent_id == first.release.binding.intent_id:
                if boundary == "timeout":
                    await asyncio.Event().wait()
                if boundary == "transport":
                    raise DemandPublishError("unavailable")
                return first
            assert intent_id == second.release.binding.intent_id
            return second

    results = await coordinator(factory, installation, Manager(), item_timeout_seconds=0.5).reconcile()
    assert [item.state for item in results] == ["failed", "retired"]
    assert results[0].failure == boundary
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


@pytest.mark.parametrize("boundary", ["installation", "cursor", "limit", "publication"])
async def test_discovery_rejects_corrupt_page_receipt(prepared_input, monkeypatch, boundary):
    import json
    from uuid import uuid4

    factory, engine, installation, *_ = prepared_input
    await ready_hold(prepared_input, "prepared-revoked")
    async with factory.begin() as session:
        original = session.scalar

        async def corrupt(*args, **kwargs):
            payload = json.loads(await original(*args, **kwargs))
            if boundary == "installation":
                payload["installation_id"] = str(uuid4())
            elif boundary == "cursor":
                payload["after_event_id"] += 1
            elif boundary == "limit":
                payload["publications"] *= 2
            else:
                payload["publications"][0]["release"]["binding"]["subject_id"] = str(uuid4())
            return json.dumps(payload, sort_keys=True, separators=(",", ":"))

        monkeypatch.setattr(session, "scalar", corrupt)
        with pytest.raises(ValueError):
            await retirement(session, installation).read_pending(limit=1)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
