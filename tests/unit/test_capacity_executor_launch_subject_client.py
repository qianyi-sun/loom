"""Authenticated launch-subject responses must match the requested intent exactly."""

from uuid import UUID

import httpx
import pytest

from loom_capacity_executor.client import ExecutableCapacityExecutorClient, ExecutorTransportError
from loom_capacity_manager.executable_contracts import ExecutionContextV2
from loom_capacity_manager.launch_subject_contracts import ExecutableLaunchSubjectV3, canonical_launch_subject_bytes
from tests.unit.test_capacity_executor_client import _executable_registration, _StreamingOversizeTransport
from tests.unit.test_capacity_executor_typed_launch_renderer import typed_context


@pytest.mark.parametrize("changed", (False, True))
async def test_launch_subject_client_requires_exact_intent_response(changed):
    context = typed_context()
    binding = context.binding
    registration = _executable_registration().model_copy(update={
        "execution": ExecutionContextV2.model_validate(binding.execution.model_dump(exclude={"allocation_epoch", "executable"})),
        "executor_id": binding.executor_id, "executor_incarnation": binding.executor_incarnation,
        "pool_id": binding.pool_id, "pool_generation": binding.pool_generation,
    })
    value = ExecutableLaunchSubjectV3(binding=binding, configuration=context.subject.configuration,
        acknowledgement=context.subject.acknowledgement, authority=context.subject.authority)
    if changed:
        value = value.model_copy(update={"binding": binding.model_copy(update={"intent_id": UUID(int=990001)})})
    calls = []
    def handler(request):
        calls.append(request)
        assert request.headers["Authorization"] == "Bearer test-executor-secret"
        assert request.url.path == f"/v3/executors/{binding.pool_id}/intents/{binding.intent_id}/launch-subject"
        return httpx.Response(200, content=canonical_launch_subject_bytes(value))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = ExecutableCapacityExecutorClient(registration, manager_origin="https://manager.example.test",
            bearer_token="test-executor-secret", http_client=http)
        if changed:
            with pytest.raises(ExecutorTransportError):
                await client.launch_subject(binding)
        else:
            assert await client.launch_subject(binding) == value
    assert len(calls) == 1


async def test_launch_subject_client_stops_reading_at_byte_bound():
    from tests.unit.test_capacity_executor_client import _executable_intent

    transport = _StreamingOversizeTransport()
    async with httpx.AsyncClient(transport=transport) as http:
        client = ExecutableCapacityExecutorClient(_executable_registration(), manager_origin="https://manager.example.test",
            bearer_token="test-executor-secret", http_client=http)
        with pytest.raises(ExecutorTransportError):
            await client.launch_subject(_executable_intent())
    assert not transport.stream.read_past_limit
