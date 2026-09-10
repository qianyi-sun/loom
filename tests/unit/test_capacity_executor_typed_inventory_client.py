"""Typed inventory uses its own authenticated endpoint and exact receipt bytes."""

import json

import httpx
import pytest

from loom_capacity_executor.client import ExecutableCapacityExecutorClient, ExecutorTransportError
from loom_capacity_manager.executable_contracts import (
    canonical_executable_bytes,
    canonical_executable_digest,
)
from tests.unit.test_capacity_executor_client import _executable_registration
from tests.unit.test_capacity_typed_inventory_contracts import typed_inventory


@pytest.mark.parametrize("changed_receipt", (False, True))
async def test_typed_inventory_client_preserves_payload_and_receipt(changed_receipt):
    value = typed_inventory()
    registration = _executable_registration().model_copy(
        update={
            "execution": value.execution,
            "executor_id": value.executor_id,
            "executor_incarnation": value.executor_incarnation,
            "pool_id": value.pool_id,
            "pool_generation": value.pool_generation,
        }
    )

    def handler(request):
        assert request.url.path == f"/v3/executors/{value.pool_id}/inventory"
        assert request.headers["Authorization"] == "Bearer executor-secret"
        assert request.content == canonical_executable_bytes(value)
        receipt = {
            "inventory_sequence": value.inventory_sequence,
            "inventory_digest": "f" * 64 if changed_receipt else canonical_executable_digest(value),
            "replayed": False,
            "executable": True,
        }
        return httpx.Response(200, content=json.dumps(receipt).encode())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = ExecutableCapacityExecutorClient(
            registration,
            manager_origin="https://manager.example.test",
            bearer_token="executor-secret",
            http_client=http,
        )
        if changed_receipt:
            with pytest.raises(ExecutorTransportError, match="receipt"):
                await client.ingest_executable_inventory(value)
        else:
            receipt = await client.ingest_executable_inventory(value)
            assert receipt.inventory_digest == canonical_executable_digest(value)


async def test_typed_inventory_client_rejects_wrong_executor_without_network():
    value = typed_inventory()

    def handler(request):
        raise AssertionError("foreign inventory reached network")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = ExecutableCapacityExecutorClient(
            _executable_registration(),
            manager_origin="https://manager.example.test",
            bearer_token="executor-secret",
            http_client=http,
        )
        with pytest.raises(ExecutorTransportError):
            await client.ingest_executable_inventory(value)
