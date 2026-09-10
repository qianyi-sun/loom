"""Only the exact permitted executor can read current authenticated launch facts."""

import pytest

from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.store import CapacityManagementStore, ExecutionConflictError
from tests.capacity_execution_fixtures import execution_policy, executor_binding
from tests.integration.test_capacity_manager_execution_store import _launch_ready


async def test_launch_subject_read_returns_exact_permitted_binding(capacity_session):
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    result = await store.launch_subject(capacity_session, executor_binding("gb10"),
        intent_id=permit.binding.intent_id, management=CapacityManagementStore(execution_policy=execution_policy()))
    assert result.binding == permit.binding
    assert result.acknowledgement.candidate == permit.binding.candidate
    assert result.authority.source == "immutable-base"
    assert result.authority.purpose == "application-worker"


async def test_launch_subject_read_rejects_another_pool_executor(capacity_session):
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    with pytest.raises(ExecutionConflictError):
        await store.launch_subject(capacity_session, executor_binding("oldlab"),
            intent_id=permit.binding.intent_id, management=CapacityManagementStore(execution_policy=execution_policy()))
