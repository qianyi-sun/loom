"""Post-submission evidence is current, read-only, and not a second admission."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom_capacity_manager.executable_contracts import (
    ExecutableIntentCloseV2,
    ExecutablePermitConsumptionV2,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.models import (
    CapacityAuthorityState,
    CapacityDemandReporter,
    CapacityExecutableCommandReceipt,
    CapacityExecutableIntent,
)
from loom_capacity_manager.ownership import OwnershipKeyring
from loom_capacity_manager.store import CapacityManagementStore, CapacityStoreError
from tests.capacity_execution_fixtures import EXECUTOR_KEYS, execution_policy, executor_binding
from tests.integration.test_capacity_manager_execution_store import (
    _inventory_execution,
    _inventory_record,
    _launch_ready,
    _next_inventory,
    _test_only_update_without_guard,
)


@pytest.fixture
async def submitted_allocation(isolated_capacity_postgres_url, request):
    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = CapacityExecutionStore(
        permit_ttl_seconds=getattr(request, "param", 15),
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()}),
    )
    try:
        # The existing reconciliation fixture shares one connection across its
        # sessions; commit that setup before exercising fresh observer sessions.
        async with engine.begin() as connection:
            async with AsyncSession(bind=connection, expire_on_commit=False, join_transaction_mode="create_savepoint") as session:
                permit = await _launch_ready(store, session)
                await store.consume_launch_permit(session, ExecutablePermitConsumptionV2(
                    permit_id=permit.permit_id, permit_digest=store.contract_digest(permit),
                    binding=permit.binding, command_sequence=3,
                ))
                await session.commit()
        yield store, sessions, permit
    finally:
        await engine.dispose()


async def observe(store, session, permit, *, executor=None, policy=None):
    return await store.current_application_allocation(
        session, executor or executor_binding("gb10"), intent_id=permit.binding.intent_id,
        management=CapacityManagementStore(execution_policy=policy or execution_policy()),
    )


async def test_post_submission_observation_does_not_reconsume_or_require_new_capacity(
    submitted_allocation, monkeypatch,
):
    store, sessions, permit = submitted_allocation

    async def no_increase(*args, **kwargs):
        pytest.fail("post-submission evidence must not perform new-capacity admission")

    monkeypatch.setattr(store, "_assert_increase_eligible", no_increase)
    async with sessions() as session:
        before = await session.scalar(select(func.count()).select_from(CapacityExecutableCommandReceipt))
        await session.commit()
        first = await observe(store, session, permit)
        assert not session.in_transaction()
        second = await observe(store, session, permit)
        assert first.subject.binding == permit.binding
        assert first.subject.authority.purpose == "application-worker"
        assert first.permit == permit
        assert first.executable is False
        assert first.permit_consumed_at <= first.observed_at < first.expires_at
        assert first.observed_at <= second.observed_at
        assert first.permit_consumed_at == second.permit_consumed_at
        assert await session.scalar(select(func.count()).select_from(CapacityExecutableCommandReceipt)) == before


@pytest.mark.parametrize("submitted_allocation", (3,), indirect=True)
async def test_consumed_permit_expiry_does_not_revoke_existing_allocation(submitted_allocation):
    store, sessions, permit = submitted_allocation
    # Keep the executor lease live; expiration of the consumed scheduler permit
    # is distinct from current executor/subject/bootstrap/runtime authority.
    await asyncio.sleep(max(0, (permit.expires_at - datetime.now(UTC)).total_seconds()) + 0.01)
    async with sessions() as session:
        result = await observe(store, session, permit)
        assert result.permit.expires_at == permit.expires_at


@pytest.mark.parametrize("state", ("permitted", "closing", "terminal", "released", "quarantined", "accepted"))
async def test_post_submission_observation_rejects_inconsistent_nonlive_state(submitted_allocation, state):
    store, sessions, permit = submitted_allocation
    async with sessions() as session, session.begin():
        # Corruption boundary, not a legal lifecycle transition. Production SQL
        # guards remain enabled outside this explicit disposable-fixture edit.
        await _test_only_update_without_guard(session,
            table_name="capacity_executable_intents", trigger_name="capacity_executable_intent_mutation_guard",
            statement=update(CapacityExecutableIntent).values(state=state))
    async with sessions() as session:
        with pytest.raises(CapacityStoreError):
            await observe(store, session, permit)


@pytest.mark.parametrize("changed", ("freeze", "executor-expired", "operator-policy", "another-pool", "missing-consumption", "permit-digest", "binding-digest", "draining"))
async def test_post_submission_observation_preserves_exact_current_fences(submitted_allocation, changed, monkeypatch):
    store, sessions, permit = submitted_allocation
    policy, executor = execution_policy(), executor_binding("gb10")
    async with sessions() as session, session.begin():
        if changed == "freeze":
            await session.execute(update(CapacityAuthorityState).values(increase_freeze=True))
        elif changed == "executor-expired":
            async def future_now(_session):
                return datetime.now(UTC) + timedelta(minutes=10)
            monkeypatch.setattr("loom_capacity_manager.execution_store._database_now", future_now)
        elif changed == "operator-policy":
            policy = policy.model_copy(update={"executable_new_capacity_ceiling": 2})
        elif changed == "another-pool":
            executor = executor_binding("oldlab")
        else:
            values = {
                "missing-consumption": {"permit_consumed_at": None},
                "permit-digest": {"permit_digest": "f" * 64},
                "binding-digest": {"binding_digest": "f" * 64},
                "draining": {"state": "observed", "observed_state": "draining"},
            }[changed]
            await _test_only_update_without_guard(session,
                table_name="capacity_executable_intents", trigger_name="capacity_executable_intent_mutation_guard",
                statement=update(CapacityExecutableIntent).values(**values))
    async with sessions() as session:
        with pytest.raises(CapacityStoreError):
            await observe(store, session, permit, executor=executor, policy=policy)


async def test_post_submission_observation_refuses_existing_snapshot(submitted_allocation):
    store, sessions, permit = submitted_allocation
    async with sessions() as session:
        await session.execute(text("SELECT 1"))
        with pytest.raises(CapacityStoreError, match="fresh owned transaction"):
            await observe(store, session, permit)
        assert session.in_transaction()


async def test_post_submission_observation_refuses_external_transaction(submitted_allocation):
    store, sessions, permit = submitted_allocation
    async with sessions.kw["bind"].connect() as connection, connection.begin():
        async with AsyncSession(bind=connection, join_transaction_mode="create_savepoint") as session:
            with pytest.raises(CapacityStoreError, match="fresh owned transaction"):
                await observe(store, session, permit)


async def test_post_submission_observation_cannot_refresh_cached_authority(submitted_allocation):
    store, sessions, permit = submitted_allocation
    async with sessions() as session, session.begin():
        inventory = await _next_inventory(session, _inventory_execution(permit.binding), permit.binding,
            records=(_inventory_record(permit.binding, physical_identity="12345"),))
        await store.ingest_executor_inventory(session, inventory)
    async with sessions() as retained:
        # Hold a strong ORM reference across the observation's owned transactions.
        cached = await retained.scalar(select(CapacityExecutableIntent))
        await retained.commit()
        current = await observe(store, retained, permit)
        assert current.observed_slurm_job_id == "12345"
        async with sessions() as writer:
            await store.begin_intent_close(writer, ExecutableIntentCloseV2(binding=permit.binding, command_sequence=4))
        assert cached.state == "observed"
        with pytest.raises(CapacityStoreError):
            await observe(store, retained, permit)


@pytest.mark.parametrize("state", ("pending", "active", "draining", "terminal", "unknown"))
async def test_real_inventory_state_controls_post_submission_observation(submitted_allocation, state):
    store, sessions, permit = submitted_allocation
    async with sessions() as session, session.begin():
        record = _inventory_record(permit.binding, physical_identity="12345", state=state,
            terminal_evidence_sha256="9" * 64 if state == "terminal" else None)
        inventory = await _next_inventory(session, _inventory_execution(permit.binding), permit.binding, records=(record,))
        await store.ingest_executor_inventory(session, inventory)
    async with sessions() as session:
        if state in {"pending", "active"}:
            assert (await observe(store, session, permit)).observed_slurm_job_id == "12345"
        else:
            with pytest.raises(CapacityStoreError):
                await observe(store, session, permit)


async def test_current_observation_requires_serializable(submitted_allocation):
    store, sessions, permit = submitted_allocation
    engine = sessions.kw["bind"].execution_options(isolation_level="READ COMMITTED")
    async with AsyncSession(bind=engine) as session:
        with pytest.raises(CapacityStoreError, match="SERIALIZABLE"):
            await observe(store, session, permit)


async def test_current_observation_bounds_waiting_on_authority(submitted_allocation):
    store, sessions, permit = submitted_allocation
    async with sessions() as writer, writer.begin():
        await writer.scalar(select(CapacityAuthorityState).with_for_update())
        async with sessions() as observer:
            with pytest.raises(CapacityStoreError, match="retry"):
                await asyncio.wait_for(observe(store, observer, permit), timeout=3)
            assert not observer.in_transaction()


@pytest.mark.parametrize("changed", ("missing", "request-digest", "result-digest", "result"))
async def test_current_observation_rejects_corrupted_consumption_receipt(submitted_allocation, changed):
    store, sessions, permit = submitted_allocation
    async with sessions() as writer, writer.begin():
        # Explicit corruption simulation in a disposable test database. The
        # production receipt is append-only; the observer must still validate it.
        predicate = CapacityExecutableCommandReceipt.operation_kind == "permit-consumption"
        statement = delete(CapacityExecutableCommandReceipt).where(predicate)
        if changed != "missing":
            values = {
                "request-digest": {"request_digest": "f" * 64},
                "result-digest": {"result_digest": "f" * 64},
                "result": {"result_payload": {"intent_id": str(permit.binding.intent_id), "executable": False}},
            }[changed]
            statement = update(CapacityExecutableCommandReceipt).where(predicate).values(**values)
        await _test_only_update_without_guard(writer,
            table_name="capacity_executable_command_receipts",
            trigger_name="capacity_executable_command_receipts_append_only_guard", statement=statement)
    async with sessions() as observer:
        with pytest.raises(CapacityStoreError, match="consumption receipt"):
            await observe(store, observer, permit)


async def test_current_observation_rejects_fenced_subject_reporter(submitted_allocation):
    store, sessions, permit = submitted_allocation
    async with sessions() as writer, writer.begin():
        await writer.execute(update(CapacityDemandReporter).where(
            CapacityDemandReporter.subject_id == permit.binding.subject_id,
        ).values(state="fenced"))
    async with sessions() as observer:
        with pytest.raises(CapacityStoreError):
            await observe(store, observer, permit)
