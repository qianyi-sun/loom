"""Authenticated launch-subject responses must match the requested intent exactly."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest

from loom_capacity_executor.client import ExecutableCapacityExecutorClient, ExecutorTransportError
from loom_capacity_manager.contracts import MAX_CONTRACT_BYTES, canonical_digest
from loom_capacity_manager.executable_contracts import ExecutionContextV2
from loom_capacity_manager.launch_subject_contracts import (
    ExecutableLaunchSubjectV3,
    canonical_launch_subject_bytes,
)
from tests.unit.test_capacity_executor_client import _executable_registration
from tests.unit.test_capacity_executor_typed_launch_renderer import typed_context
from tests.unit.test_capacity_launch_subject_contract import current_application_observation


@pytest.mark.parametrize("changed", (False, True))
@pytest.mark.parametrize("large", (False, True))
async def test_launch_subject_client_requires_exact_intent_response(changed, large):
    context = typed_context()
    binding = context.binding
    registration = _executable_registration().model_copy(
        update={
            "execution": ExecutionContextV2.model_validate(
                binding.execution.model_dump(exclude={"allocation_epoch", "executable"})
            ),
            "executor_id": binding.executor_id,
            "executor_incarnation": binding.executor_incarnation,
            "pool_id": binding.pool_id,
            "pool_generation": binding.pool_generation,
        }
    )
    value = ExecutableLaunchSubjectV3(
        binding=binding,
        configuration=context.subject.configuration,
        acknowledgement=context.subject.acknowledgement,
        authority=context.subject.authority,
    )
    if large:
        profile = value.configuration.profiles[0]
        shapes = tuple(
            sorted(
                (
                    *profile.worker_shapes,
                    *(
                        profile.worker_shapes[0].model_copy(
                            update={"shape_id": f"extra-{index:04d}"}
                        )
                        for index in range(200)
                    ),
                ),
                key=lambda shape: shape.shape_id,
            )
        )
        configuration = value.configuration.model_copy(
            update={
                "profiles": (
                    profile.model_copy(update={"worker_shapes": shapes}),
                    *value.configuration.profiles[1:],
                )
            }
        )
        authority = value.authority.model_copy(
            update={
                "configuration": value.authority.configuration.model_copy(
                    update={"digest": canonical_digest(configuration)}
                )
            }
        )
        value = ExecutableLaunchSubjectV3(
            binding=binding,
            configuration=configuration,
            acknowledgement=value.acknowledgement,
            authority=authority,
        )
        assert len(canonical_launch_subject_bytes(value)) > 64 * 1024
    if changed:
        value = value.model_copy(
            update={"binding": binding.model_copy(update={"intent_id": UUID(int=990001)})}
        )
    calls = []

    def handler(request):
        calls.append(request)
        assert request.headers["Authorization"] == "Bearer test-executor-secret"
        assert (
            request.url.path
            == f"/v3/executors/{binding.pool_id}/intents/{binding.intent_id}/launch-subject"
        )
        return httpx.Response(200, content=canonical_launch_subject_bytes(value))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = ExecutableCapacityExecutorClient(
            registration,
            manager_origin="https://manager.example.test",
            bearer_token="test-executor-secret",
            http_client=http,
        )
        if changed:
            with pytest.raises(ExecutorTransportError):
                await client.launch_subject(binding)
        else:
            assert await client.launch_subject(binding) == value
    assert len(calls) == 1


@pytest.mark.parametrize("changed", (None, "intent", "expired", "future", "noncanonical", "oversize", "forbidden", "unavailable"))
async def test_current_application_allocation_client_checks_fresh_exact_evidence(changed):
    from loom_capacity_executor.client import ExecutorRejectedError
    from loom_capacity_manager.launch_subject_contracts import (
        canonical_current_application_allocation_bytes,
    )

    value = current_application_observation()
    binding = value.subject.binding
    registration = _executable_registration().model_copy(update={
        "execution": ExecutionContextV2.model_validate(binding.execution.model_dump(exclude={"allocation_epoch", "executable"})),
        "executor_id": binding.executor_id, "executor_incarnation": binding.executor_incarnation,
        "pool_id": binding.pool_id, "pool_generation": binding.pool_generation,
    })
    if changed == "intent":
        other = binding.model_copy(update={"intent_id": UUID(int=990001)})
        value = value.model_copy(update={
            "subject": value.subject.model_copy(update={"binding": other}),
            "permit": value.permit.model_copy(update={"binding": other}),
        })
    if changed in {"expired", "future"}:
        when = datetime.now(UTC) + timedelta(minutes=-1 if changed == "expired" else 1)
        value = value.model_copy(update={
            "permit_consumed_at": when - timedelta(seconds=1), "observed_at": when,
            "expires_at": when + timedelta(seconds=10),
            "permit": value.permit.model_copy(update={"expires_at": when + timedelta(seconds=15)}),
        })

    def handler(request):
        assert request.url.path.endswith(f"/{binding.intent_id}/current-application-allocation")
        assert request.headers["Authorization"] == "Bearer test-executor-secret"
        payload = canonical_current_application_allocation_bytes(value)
        if changed == "noncanonical":
            payload = b" " + payload
        if changed == "oversize":
            payload = b"x" * (MAX_CONTRACT_BYTES + 1)
        status = 403 if changed == "forbidden" else 503 if changed == "unavailable" else 200
        return httpx.Response(status, content=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = ExecutableCapacityExecutorClient(registration, manager_origin="https://manager.example.test", bearer_token="test-executor-secret", http_client=http)
        if changed:
            with pytest.raises(ExecutorRejectedError if changed == "forbidden" else ExecutorTransportError):
                await client.current_application_allocation(binding)
        else:
            assert await client.current_application_allocation(binding) == value


async def test_launch_subject_client_stops_reading_at_byte_bound():
    from tests.unit.test_capacity_executor_client import _executable_intent

    class OversizeStream(httpx.AsyncByteStream):
        read_past_limit = False

        async def __aiter__(self):
            for _ in range(MAX_CONTRACT_BYTES // 65536):
                yield b"x" * 65536
            yield b"x"
            self.read_past_limit = True
            raise AssertionError("client read beyond the launch-facts limit")

    stream = OversizeStream()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=stream))
    ) as http:
        client = ExecutableCapacityExecutorClient(
            _executable_registration(),
            manager_origin="https://manager.example.test",
            bearer_token="test-executor-secret",
            http_client=http,
        )
        with pytest.raises(ExecutorTransportError, match="exceeds its byte bound"):
            await client.launch_subject(_executable_intent())
    assert not stream.read_past_limit
