"""Only the exact permitted executor can read current authenticated launch facts."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import update

from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.models import CapacityAuthorityState
from loom_capacity_manager.store import (
    CapacityManagementStore,
    CapacityStoreError,
    ExecutionConflictError,
)
from tests.capacity_execution_fixtures import execution_policy, executor_binding
from tests.integration.test_capacity_manager_execution_store import _launch_ready


async def test_launch_subject_read_returns_exact_permitted_binding(capacity_session):
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    result = await store.launch_subject(
        capacity_session,
        executor_binding("gb10"),
        intent_id=permit.binding.intent_id,
        management=CapacityManagementStore(execution_policy=execution_policy()),
    )
    assert result.binding == permit.binding
    assert result.acknowledgement.candidate == permit.binding.candidate
    assert result.authority.source == "immutable-base"
    assert result.authority.purpose == "application-worker"


async def test_launch_subject_read_rejects_another_pool_executor(capacity_session):
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    with pytest.raises(ExecutionConflictError):
        await store.launch_subject(
            capacity_session,
            executor_binding("oldlab"),
            intent_id=permit.binding.intent_id,
            management=CapacityManagementStore(execution_policy=execution_policy()),
        )


@pytest.mark.parametrize(
    "changed", ("permit-expired", "executor-expired", "freeze", "operator-policy")
)
async def test_launch_subject_read_retains_current_authority_fences(
    capacity_session, changed, monkeypatch
):
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    policy = execution_policy()
    if changed == "operator-policy":
        policy = policy.model_copy(update={"executable_new_capacity_ceiling": 2})
    elif changed == "freeze":
        await capacity_session.execute(update(CapacityAuthorityState).values(increase_freeze=True))
    else:
        # Expiry is a clock transition, not permission to rewrite immutable
        # permit/heartbeat evidence; retain the actual durable rows unchanged.
        future = (
            permit.expires_at + timedelta(seconds=1)
            if changed == "permit-expired"
            else datetime.now(UTC) + timedelta(minutes=10)
        )

        async def future_now(_session):
            return future

        monkeypatch.setattr("loom_capacity_manager.execution_store._database_now", future_now)
    with pytest.raises(CapacityStoreError):
        await store.launch_subject(
            capacity_session,
            executor_binding("gb10"),
            intent_id=permit.binding.intent_id,
            management=CapacityManagementStore(execution_policy=policy),
        )
