"""Registered release needs exact closed work before final physical retirement."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.admission import ExecutableDrainRequestV2, ExecutableReleaseRequestV2
from loom_capacity_build_guard.demand_store import BuildGuardDemandStore
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from tests.integration import test_personal_dev_build_guard_registration as registration_module
from tests.integration.test_personal_dev_build_guard_claims import claim_input
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_hold_retirement import (
    release_witness,
    retirement,
)
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_outcomes import outcome_request
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL
from tests.integration.test_personal_dev_build_guard_release_outbox import outbox
from tests.integration.test_personal_dev_build_guard_terminal import terminal_input, terminal_store
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


async def registered_release_input(values, monkeypatch, *, result="failed", drain=True):
    witnesses = []

    async def retained_physical(inputs):
        evidence, physical = await terminal_input(inputs)
        witnesses.append(evidence)
        return None, None, None, physical, None

    monkeypatch.setattr(registration_module, "bound_input", retained_physical)
    claim = await claim_input(values, monkeypatch)
    factory, _engine, installation, *_ = values
    if result != "unclaimed":
        async with factory.begin() as session:
            await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
        if result not in {"live", "interrupted"}:
            async with factory.begin() as session:
                await store(session, installation).record_outcome(outcome_request(claim, result=result), worker_credential=CREDENTIAL)
    if result == "interrupted":
        async with factory.begin() as session:
            await terminal_store(session, installation).import_evidence(witnesses[0])
        async with factory.begin() as session:
            await terminal_store(session, installation).settle_interrupted(claim, terminal_inventory_sha256=canonical_executable_digest(witnesses[0]))
    draining = ExecutableDrainRequestV2(binding=claim.binding, operation_id=uuid4(), worker_id=claim.worker_id,
        worker_incarnation=claim.worker_incarnation, expected_claim_high_water=int(result != "unclaimed"), drain_epoch=3)
    if drain:
        async with factory.begin() as session:
            await store(session, installation).begin_drain(draining)
    release = ExecutableReleaseRequestV2(binding=claim.binding, operation_id=uuid4(),
        reporter_incarnation=installation.document.reporter_incarnation, bootstrap_registration_epoch=1,
        expected_claim_high_water=draining.expected_claim_high_water, protected_registration_epoch=2, release_epoch=4)
    return release, claim, witnesses[0], draining


@pytest.mark.parametrize("result", ["unclaimed", "artifact-ready", "failed", "cancelled", "interrupted"])
async def test_registered_release_retirement_and_correct_post_release_demand(prepared_input, monkeypatch, result):
    from loom_capacity_build_guard.plan_store import BuildGuardPlanStore

    factory, engine, installation, plan, source, platform = prepared_input
    request, _claim, terminal, _drain = await registered_release_input(prepared_input, monkeypatch, result=result)
    async with factory.begin() as session:
        released = await store(session, installation).acknowledge_release(request, current_worker_credential=CREDENTIAL)
        assert released.binding == request.binding and released.live_claim_count == 0
        assert released.bootstrap_revoked and released.worker_credentials_revoked
        assert released.claim_high_water == int(result != "unclaimed")
        with pytest.raises(DBAPIError, match="committed"):
            await outbox(session, installation).read_next()
    async with factory.begin() as session:
        assert await store(session, installation).acknowledge_release(request, current_worker_credential=CREDENTIAL) == released
        observed = await store(session, installation).observe_intent(request.binding)
        assert observed.release == released
        publication = await outbox(session, installation).read_next()
        assert publication.event_kind == "released"
        report = await BuildGuardDemandStore(session, installation=installation).capture(configuration_generation=1)
        assert len(report.current_assignments) == 1 and report.fixed_claims == ()
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
    async with factory.begin() as session:
        await outbox(session, installation).acknowledge(publication, manager_acknowledgement_digest=publication.publication_digest)
        await terminal_store(session, installation).import_evidence(terminal)
    witness = release_witness(publication, terminal)
    async with factory.begin() as session:
        page = await retirement(session, installation).read_pending()
        assert page.publications == (publication,)
        receipt = await retirement(session, installation).retire(witness)
    async with factory.begin() as session:
        assert await retirement(session, installation).retire(witness) == receipt
        report = await BuildGuardDemandStore(session, installation=installation).capture(configuration_generation=1)
        assert report.fixed_claims == report.current_assignments == ()
        assert bool(report.pending_unassigned) == (result in {"unclaimed", "failed", "interrupted"})
        assert (await retirement(session, installation).read_pending()).publications == ()
    binding = plan.shapes[0].binding.model_copy(update={"intent_id": uuid4(), "tranche_id": uuid4(),
        "shape_instance_id": plan.shapes[0].binding.shape_instance_id + "-retry"})
    successor = plan.model_copy(update={"plan_id": uuid4(), "proposal_id": uuid4(), "admission_incarnation": uuid4(),
        "shapes": (plan.shapes[0].model_copy(update={"binding": binding}),),
        "allowances": (plan.allowances[0].model_copy(update={"allowance_id": uuid4(),
            "submission_intent_id": binding.intent_id, "shape_instance_id": binding.shape_instance_id}),)})
    async with factory.begin() as session:
        following = BuildGuardPlanStore(session, installation=installation)
        if result in {"unclaimed", "failed", "interrupted"}:
            await following.prepare(successor, sources={platform.id: source})
        else:
            with pytest.raises(DBAPIError, match="live lease changed"):
                await following.prepare(successor, sources={platform.id: source})
    async with factory.begin() as session:
        # Old terminal replay must not retire a successor assignment.
        assert await retirement(session, installation).retire(witness) == receipt
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == int(result in {"unclaimed", "failed", "interrupted"})


@pytest.mark.parametrize("boundary", ["credential", "live", "no-drain", "reporter", "epoch", "water", "binding", "replay"])
async def test_registered_release_rejects_incomplete_or_changed_authority(prepared_input, monkeypatch, boundary):
    factory, engine, installation, *_ = prepared_input
    request, _claim, _terminal, _drain = await registered_release_input(prepared_input, monkeypatch,
        result="live" if boundary == "live" else "failed", drain=boundary != "no-drain")
    if boundary == "replay":
        async with factory.begin() as session:
            await store(session, installation).acknowledge_release(request, current_worker_credential=CREDENTIAL)
        request = request.model_copy(update={"operation_id": uuid4()})
    elif boundary == "reporter":
        request = request.model_copy(update={"reporter_incarnation": uuid4()})
    elif boundary == "epoch":
        request = request.model_copy(update={"release_epoch": 3})
    elif boundary == "water":
        request = request.model_copy(update={"expected_claim_high_water": 0})
    elif boundary == "binding":
        request = request.model_copy(update={"binding": request.binding.model_copy(update={"account_id": "foreign"})})
    async with factory.begin() as session:
        with pytest.raises((DBAPIError, ValueError)):
            await store(session, installation).acknowledge_release(request,
                current_worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_releases")) == int(boundary == "replay")
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


@pytest.mark.parametrize("boundary", ["exact", "uncommitted", "wrong-proof"])
async def test_manager_can_release_terminal_worker_without_lost_credential(prepared_input, monkeypatch, boundary):
    factory, engine, installation, *_ = prepared_input
    request, _claim, terminal, _drain = await registered_release_input(prepared_input, monkeypatch)
    if boundary != "uncommitted":
        async with factory.begin() as session:
            await terminal_store(session, installation).import_evidence(terminal)
    async with factory.begin() as session:
        if boundary == "uncommitted":
            await terminal_store(session, installation).import_evidence(terminal)
        if boundary == "exact":
            receipt = await terminal_store(session, installation).release_terminal_worker(request,
                terminal_inventory_sha256=canonical_executable_digest(terminal))
            assert receipt.worker_credentials_revoked and receipt.live_claim_count == 0
        else:
            with pytest.raises(DBAPIError, match="terminal"):
                await terminal_store(session, installation).release_terminal_worker(request,
                    terminal_inventory_sha256="f" * 64 if boundary == "wrong-proof" else canonical_executable_digest(terminal))
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_releases")) == int(boundary == "exact")


async def test_registered_release_requires_current_count_not_historical_drain_count(prepared_input, monkeypatch):
    factory, _engine, installation, *_ = prepared_input
    request, claim, _terminal, drain = await registered_release_input(prepared_input, monkeypatch, result="live")
    async with factory.begin() as session:
        historical = await store(session, installation).begin_drain(drain)
        assert historical.live_claim_count == 1
        await store(session, installation).record_outcome(outcome_request(claim), worker_credential=CREDENTIAL)
        with pytest.raises(DBAPIError, match="committed outcome"):
            await store(session, installation).acknowledge_release(request, current_worker_credential=CREDENTIAL)
    async with factory.begin() as session:
        released = await store(session, installation).acknowledge_release(request, current_worker_credential=CREDENTIAL)
        assert released.live_claim_count == 0
    async with factory.begin() as session:
        observed = await store(session, installation).observe_intent(request.binding)
        assert observed.drain == historical and observed.release == released


async def test_registered_release_corrupt_receipt_rolls_back_and_history_blocks_downgrade(prepared_input, monkeypatch, build_guard_database):
    from alembic import command

    factory, engine, installation, *_ = prepared_input
    request, _claim, _terminal, _drain = await registered_release_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        original = session.scalar

        async def corrupt(statement, *args, **kwargs):
            result = await original(statement, *args, **kwargs)
            return result.replace(str(request.reporter_incarnation), str(uuid4())) if "acknowledge_release(" in str(statement) else result

        monkeypatch.setattr(session, "scalar", corrupt)
        with pytest.raises(ValueError, match="receipt"):
            await store(session, installation).acknowledge_release(request, current_worker_credential=CREDENTIAL)
    async with factory.begin() as session:
        assert (await store(session, installation).observe_intent(request.binding)).release is None
        await store(session, installation).acknowledge_release(request, current_worker_credential=CREDENTIAL)
    for statement in ("UPDATE loom_capacity_build_guard.worker_releases SET payload=payload",
        "DELETE FROM loom_capacity_build_guard.worker_releases", "TRUNCATE loom_capacity_build_guard.worker_releases"):
        with engine.begin() as connection, pytest.raises(DBAPIError, match="append-only"):
            connection.execute(text(statement))
    with pytest.raises(DBAPIError, match="retained evidence"):
        command.downgrade(build_guard_database[0], "build_guard_0020")


@pytest.mark.parametrize("boundary", ["worker-grant", "terminal-grant", "public", "search-path", "helper"])
def test_registered_release_privilege_drift_is_rejected(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.acknowledge_release(uuid,jsonb,bytea,text,text)"
    statements = {"worker-grant": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "terminal-grant": f"REVOKE EXECUTE ON FUNCTION loom_capacity_build_guard.release_terminal_worker(uuid,jsonb,bytea,text,text) FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public",
        "helper": "ALTER FUNCTION loom_capacity_build_guard.native_release(uuid,jsonb,bytea,text,text,text) SECURITY DEFINER"}
    with engine.begin() as connection:
        connection.execute(text(statements[boundary]))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")


async def test_worker_and_terminal_release_race_retains_one_winner(prepared_input, monkeypatch):
    import asyncio

    factory, engine, installation, *_ = prepared_input
    worker_request, _claim, terminal, _drain = await registered_release_input(prepared_input, monkeypatch)
    manager_request = worker_request.model_copy(update={"operation_id": uuid4()})
    digest = canonical_executable_digest(terminal)
    async with factory.begin() as session:
        await terminal_store(session, installation).import_evidence(terminal)
    ready = [asyncio.Event(), asyncio.Event()]

    async def compete(index):
        try:
            async with factory.begin() as session:
                ready[index].set()
                await ready[1-index].wait()
                if index:
                    return await terminal_store(session, installation).release_terminal_worker(manager_request,
                        terminal_inventory_sha256=digest)
                return await store(session, installation).acknowledge_release(worker_request,
                    current_worker_credential=CREDENTIAL)
        except DBAPIError as exc:
            return exc

    async with asyncio.timeout(10):
        results = await asyncio.gather(compete(0), compete(1))
    winners = [result for result in results if not isinstance(result, DBAPIError)]
    assert len(winners) == 1
    async with factory.begin() as session:
        assert (await store(session, installation).observe_intent(worker_request.binding)).release == winners[0]
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_releases")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
