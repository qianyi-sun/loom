"""Typed inventory has explicit pool authentication and exact schema admission."""

from unittest.mock import AsyncMock

from loom_capacity_manager.execution_store import IngestedExecutableInventory
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from tests.capacity_execution_fixtures import executor_binding
from tests.integration.test_capacity_manager_api import _v2_executor_headers
from tests.integration.test_capacity_manager_api import execution_preparation_api_context as execution_preparation_api_context
from tests.unit.test_capacity_typed_inventory_contracts import typed_inventory


def test_typed_inventory_route_preserves_actor_and_version(execution_preparation_api_context, monkeypatch):
    client, app, *_ = execution_preparation_api_context
    binding = executor_binding("oldlab")
    value = typed_inventory().model_copy(update={
        "records": (), "executor_id": binding.executor_id,
        "executor_incarnation": binding.executor_incarnation, "pool_id": binding.pool_id,
        "pool_generation": binding.pool_generation})
    ingest = AsyncMock(return_value=IngestedExecutableInventory(1, canonical_executable_digest(value), False))
    monkeypatch.setattr(app.state.execution_store, "ingest_typed_executor_inventory", ingest, raising=False)
    path = "/v3/executors/oldlab/inventory"
    payload = value.model_dump(mode="json")
    assert client.put(path, json=payload).status_code == 401
    assert client.put(path, json=payload, headers=_v2_executor_headers("gb10")).status_code == 403
    ingest.assert_not_awaited()
    response = client.put(path, json=payload, headers=_v2_executor_headers("oldlab"))
    assert response.status_code == 200, response.text
    assert ingest.await_args.args[1] == value
    assert "management" in ingest.await_args.kwargs
    ingest.reset_mock()
    for version in (2, 3.0):
        bad = payload | {"schema_version": version}
        assert client.put(path, json=bad, headers=_v2_executor_headers("oldlab")).status_code == 422
    ingest.assert_not_awaited()
