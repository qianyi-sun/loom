"""The launch facts route authenticates pool executors before querying evidence."""

from unittest.mock import AsyncMock

from loom_capacity_manager.launch_subject_contracts import canonical_launch_subject_bytes
from tests.integration.test_capacity_manager_api import (
    _v2_executor_headers,
)
from tests.integration.test_capacity_manager_api import (
    execution_preparation_api_context as execution_preparation_api_context,
)
from tests.unit.test_capacity_launch_subject_contract import response


def test_cleanup_only_work_route_preserves_executor_authorization(execution_preparation_api_context, monkeypatch):
    client, app, *_ = execution_preparation_api_context
    select = AsyncMock(return_value=None)
    monkeypatch.setattr(app.state.execution_store, "next_pool_work", select, raising=False)
    path = "/v2/executors/oldlab/work?cleanup_only=true"
    assert client.get(path).status_code == 401
    assert client.get(path, headers=_v2_executor_headers("gb10")).status_code == 403
    select.assert_not_awaited()
    actual = client.get(path, headers=_v2_executor_headers("oldlab"))
    assert actual.status_code == 200, actual.text
    assert select.await_args.kwargs["cleanup_only"] is True


def test_launch_subject_route_requires_pool_actor_and_returns_canonical_facts(
    execution_preparation_api_context, monkeypatch
):
    client, app, *_ = execution_preparation_api_context
    value = response()
    resolve = AsyncMock(return_value=value)
    monkeypatch.setattr(app.state.execution_store, "launch_subject", resolve, raising=False)
    path = f"/v3/executors/oldlab/intents/{value.binding.intent_id}/launch-subject"
    assert client.get(path).status_code == 401
    assert client.get(path, headers=_v2_executor_headers("gb10")).status_code == 403
    resolve.assert_not_awaited()
    actual = client.get(path, headers=_v2_executor_headers("oldlab"))
    assert actual.status_code == 200, actual.text
    assert actual.content == canonical_launch_subject_bytes(value)
    assert resolve.await_args.kwargs["intent_id"] == value.binding.intent_id
