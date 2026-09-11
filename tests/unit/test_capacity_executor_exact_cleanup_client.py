"""A pending native admission may poll only its exact authenticated cleanup."""

from uuid import uuid4

import httpx
import pytest

from loom_capacity_executor.client import ExecutableCapacityExecutorClient, ExecutorTransportError
from tests.unit.test_capacity_executor_client import _executable_intent, _executable_registration
from tests.unit.test_capacity_executor_executable import close_fixture


@pytest.mark.parametrize("response_kind", ["exact", "foreign", "increase", "absent"])
async def test_exact_cleanup_query_rejects_ignored_selector(response_kind):
    binding = _executable_intent()
    close = close_fixture(binding,command_sequence=1)

    def handler(request):
        assert request.url.params["cleanup_only"] == "true"
        assert request.url.params["cleanup_intent_id"] == str(binding.intent_id)
        value = close
        if response_kind == "foreign":
            value = close.model_copy(update={"binding":binding.model_copy(update={"intent_id":uuid4()})})
        elif response_kind == "increase":
            value = binding
        return (httpx.Response(200,content=b"null") if response_kind == "absent"
            else httpx.Response(200,json=value.model_dump(mode="json")))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = ExecutableCapacityExecutorClient(_executable_registration(),manager_origin="https://manager.test",
            bearer_token="executor-secret",http_client=http)
        if response_kind in {"foreign","increase"}:
            with pytest.raises(ExecutorTransportError):
                await client.next_executable_work(0,cleanup_only=True,cleanup_intent_id=binding.intent_id)
        else:
            assert await client.next_executable_work(0,cleanup_only=True,cleanup_intent_id=binding.intent_id) == (
                None if response_kind == "absent" else close)


async def test_exact_intent_selector_cannot_request_new_work():
    def unexpected(request):
        pytest.fail("unsafe selector must fail before networking")

    async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected)) as http:
        client = ExecutableCapacityExecutorClient(_executable_registration(),manager_origin="https://manager.test",
            bearer_token="executor-secret",http_client=http)
        with pytest.raises(ValueError):
            await client.next_executable_work(0,cleanup_intent_id=uuid4())
