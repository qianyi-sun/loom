"""Private discovery must replay retained authority, not synthesize admission."""

from uuid import UUID, uuid4

import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.client import ExecutableAdmissionAcknowledgementReceiptV2
from loom_capacity_build_guard.coordinator import BuildPlanCoordinator
from loom_capacity_build_guard.plan_store import BuildGuardPlanStore
from loom_capacity_build_guard.publication_discovery import BuildGuardPublicationDiscovery
from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionPlanClosureV2,
    canonical_executable_digest,
)
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.mark.parametrize("disposition", ["publication", "closure"])
async def test_discovery_requires_committed_preparation_and_omits_dispositions(prepared_input, disposition):
    factory, engine, installation, plan, *_ = prepared_input
    async with factory.begin() as session:
        discovery = BuildGuardPublicationDiscovery(session, installation=installation)
        assert (await discovery.read_pending()).plans == ()
        await BuildGuardPlanStore(session, installation=installation).prepare(plan)
        with pytest.raises(DBAPIError, match="committed preparation"):
            async with session.begin_nested():
                await discovery.read_pending()
    async with factory.begin() as session:
        discovery = BuildGuardPublicationDiscovery(session, installation=installation)
        page = await discovery.read_pending(limit=1)
        assert len(page.plans) == 1
        assert page.plans[0].plan_id == plan.plan_id
        assert page.plans[0].proposal_digest == canonical_executable_digest(plan)
        assert not page.executable
        assert (await discovery.read_pending(after_plan_id=plan.plan_id, through_plan_id=page.through_plan_id)).plans == ()
        store = BuildGuardPlanStore(session, installation=installation)
        if disposition == "publication":
            await store.authorize_publication(plan.plan_id)
        else:
            await store.close_plan(ExecutableAdmissionPlanClosureV2(closure_id=uuid4(), proposal=plan, close_reason="manager-closed"))
    async with factory.begin() as session:
        assert (await BuildGuardPublicationDiscovery(session, installation=installation).read_pending()).plans == ()
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.assignments")) == 1


async def test_pending_replay_does_not_publish_cancelled_source(prepared_input):
    factory, engine, installation, plan, _source, request = prepared_input

    class Publisher:
        async def publish_executable_admission_acknowledgement(self, *args, **kwargs):
            pytest.fail("cancelled source must not reach manager publication")

    coordinator = BuildPlanCoordinator(factory, installation=installation, publisher=Publisher())
    await coordinator.prepare(plan)
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": request.id})
    assert await coordinator.publish_pending() is False
    assert await coordinator.publish_pending() is False
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_retired_unpublished_plan_is_no_longer_pending(prepared_input):
    from loom_capacity_agent.admission import ExecutablePreparedBootstrapRevocationV2
    from loom_capacity_build_guard.bootstrap_store import BuildGuardBootstrapStore
    from loom_capacity_build_guard.execution_store import BuildGuardExecutionStore
    from tests.integration.test_personal_dev_build_guard_bootstrap import bootstrap
    from tests.integration.test_personal_dev_build_guard_hold_retirement import (
        release_witness,
        retirement,
    )
    from tests.integration.test_personal_dev_build_guard_release_outbox import outbox

    factory, engine, installation, plan, *_ = prepared_input
    proposal = bootstrap(plan)
    async with factory.begin() as session:
        await BuildGuardBootstrapStore(session, installation=installation).register(proposal)
        await BuildGuardPlanStore(session, installation=installation).prepare(plan)
    # Accepted manager acknowledgement was lost: local publication is absent.
    async with factory.begin() as session:
        await BuildGuardExecutionStore(session, installation=installation).revoke_prepared_bootstrap(
            ExecutablePreparedBootstrapRevocationV2(operation_id=uuid4(), binding=proposal.binding,
                bootstrap_registration_epoch=1, protected_registration_epoch=2))
    async with factory.begin() as session:
        publication = await outbox(session, installation).read_next()
        await outbox(session, installation).acknowledge(publication, manager_acknowledgement_digest=publication.publication_digest)
    async with factory.begin() as session:
        assert len((await BuildGuardPublicationDiscovery(session, installation=installation).read_pending()).plans) == 1
        await retirement(session, installation).retire(release_witness(publication, None))
        with pytest.raises(DBAPIError, match="committed retirement"):
            async with session.begin_nested():
                await BuildGuardPublicationDiscovery(session, installation=installation).read_pending()
    async with factory.begin() as session:
        assert (await BuildGuardPublicationDiscovery(session, installation=installation).read_pending()).plans == ()
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.hold_retirements")) == 1


async def test_pending_replay_continues_after_failure_and_wraps_for_retry(prepared_input, sessions, tmp_path):
    from tests.integration.test_personal_dev_build_guard_recovery import other_pool_input

    factory, _engine, installation, first, *_ = prepared_input
    other = await other_pool_input(prepared_input, sessions, tmp_path)
    second = other[3]
    first = first.model_copy(update={"plan_id": UUID(int=100000)})
    second = second.model_copy(update={"plan_id": UUID(int=200000)})
    seen = []

    class Publisher:
        async def publish_executable_admission_acknowledgement(self, ack, *, idempotency_key):
            seen.append(ack.plan_id)
            if len(seen) == 1:
                raise ValueError("invalid manager receipt")
            return ExecutableAdmissionAcknowledgementReceiptV2(proposal_id=ack.proposal_id,
                prepared_plan_digest=ack.prepared_plan_digest, receipt_digest=canonical_executable_digest(ack),
                executable=True, replayed=False)

    coordinator = BuildPlanCoordinator(factory, installation=installation, publisher=Publisher())
    await coordinator.prepare(first)
    await coordinator.prepare(second)
    async with factory.begin() as session:
        discovery = BuildGuardPublicationDiscovery(session, installation=installation)
        page = await discovery.read_pending(limit=1)
        assert [p.plan_id for p in page.plans] == [first.plan_id]
        page = await discovery.read_pending(after_plan_id=first.plan_id, through_plan_id=page.through_plan_id, limit=1)
        assert [p.plan_id for p in page.plans] == [second.plan_id]
    assert await coordinator.publish_pending() is False
    assert seen == [first.plan_id, second.plan_id]
    assert await coordinator.publish_pending() is True
    assert seen == [first.plan_id, second.plan_id, first.plan_id]
    assert await coordinator.publish_pending() is True
    assert len(seen) == 3


@pytest.mark.parametrize("boundary", ["after", "through", "limit", "installation"])
async def test_publication_discovery_rejects_invalid_sql_scope_and_bounds(prepared_input, boundary):
    factory, _engine, installation, *_ = prepared_input
    args = {"installation": installation.id, "after": UUID(int=0), "through": None, "limit": 1}
    args.update({"after": None} if boundary == "after" else {"after": UUID(int=1), "through": UUID(int=0)}
        if boundary == "through" else {"limit": 17} if boundary == "limit" else {"installation": uuid4()})
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match=r"bounds|installation"):
            async with session.begin_nested():
                await session.scalar(text("SELECT loom_capacity_build_guard.read_pending_publications(:installation,:after,:through,:limit)"), args)


@pytest.mark.parametrize("boundary", ["grant", "public", "search-path"])
def test_publication_discovery_privilege_drift_rejected(build_guard_database, boundary):
    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.read_pending_publications(uuid,uuid,uuid,integer)"
    statements = {"grant": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public"}
    with engine.begin() as connection:
        connection.execute(text(statements[boundary]))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")
