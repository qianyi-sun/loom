"""Exercise the protected transport for the full execution epoch lifecycle."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from loom_capacity_manager.executable_contracts import (
    ExecutionActivationV2,
    ExecutionAuthorityV2,
    ExecutionDrainV2,
    ExecutionRetirementExecutorCheckpointV2,
    ExecutionRetirementV2,
)
from loom_cli.rollout.operator.protected_capacity_manager_client import (
    ProtectedCapacityManagerClient,
    ProtectedCapacityManagerClientError,
)
from tests.loom_cli.rollout.operator.test_protected_capacity_manager_client import (
    _KEYS,
    _credentials,
    _execution_preparation,
    _HTTPClient,
    _prepared_execution,
    _Response,
)


def _transitions():
    prepared = _prepared_execution()
    binding = {
        "authority_incarnation": prepared.authority_incarnation,
        "expected_writer_epoch": prepared.writer_epoch,
        "execution_epoch": prepared.execution_epoch,
        "execution_manifest_sha256": prepared.execution_manifest_sha256,
    }
    activate = ExecutionActivationV2(
        **binding,
        prepared_readiness_sha256="b" * 64,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
    )
    drain = ExecutionDrainV2(
        **binding,
        expected_executable_new_capacity_ceiling=1,
        expected_executable_new_capacity_rate_per_minute=1,
    )
    retire = ExecutionRetirementV2(
        **binding,
        executor_checkpoints=tuple(
            ExecutionRetirementExecutorCheckpointV2(
                executor_id=executor.executor_id,
                executor_incarnation=executor.executor_incarnation,
                pool_id=executor.pool_id,
                pool_generation=executor.pool_generation,
                heartbeat_sequence=1,
                command_sequence=0,
                journal_sequence=0,
                journal_digest="0" * 64,
                inventory_sequence=1,
                inventory_digest="c" * 64,
            )
            for executor in _execution_preparation().executors
        ),
    )
    active = ExecutionAuthorityV2.model_validate(
        prepared.model_dump(mode="python")
        | {
            "execution_state": "active",
            "executable_new_capacity_ceiling": 1,
            "executable_new_capacity_rate_per_minute": 1,
        }
    )
    drained = ExecutionAuthorityV2.model_validate(
        active.model_dump(mode="python")
        | {
            "execution_state": "drain-only",
            "executable_new_capacity_ceiling": 0,
            "executable_new_capacity_rate_per_minute": 0,
        }
    )
    retired = {
        "execution_epoch": prepared.execution_epoch,
        "execution_manifest_sha256": prepared.execution_manifest_sha256,
        "retired_at": "2026-09-08T18:00:00Z",
        "replayed": False,
    }
    return (activate, drain, retire), (active, drained, retired)


def _client(tmp_path: Path, responses):
    credentials = _credentials(tmp_path)
    http = _HTTPClient([_Response(json.dumps(value).encode()) for value in responses])
    client = ProtectedCapacityManagerClient(
        origin="https://127.0.0.1:43210",
        credentials_root=credentials,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        client_factory=lambda _context: http,
    )
    return client, http, credentials


def test_execution_transitions_use_exact_contracts_and_distinct_credentials(tmp_path: Path):
    requests, (active, drained, retired) = _transitions()
    client, http, credentials = _client(
        tmp_path, [active.model_dump(mode="json"), drained.model_dump(mode="json"), retired]
    )
    assert client.activate_execution(requests[0], _KEYS[0]) == active
    assert client.drain_execution(requests[1], _KEYS[1]) == drained
    result = client.retire_execution(requests[2], _KEYS[2])
    assert result.execution_epoch == requests[2].execution_epoch
    assert result.execution_manifest_sha256 == requests[2].execution_manifest_sha256
    assert result.retired_at == datetime(2026, 9, 8, 18, tzinfo=UTC)
    assert result.replayed is False
    assert http.closed
    assert [call["url"] for call in http.calls] == [
        "https://127.0.0.1:43210/v2/execution-preparations/1/activate",
        "https://127.0.0.1:43210/v2/execution-epochs/1/drain",
        "https://127.0.0.1:43210/v2/execution-epochs/1/retire",
    ]
    for index, credential in enumerate(("manager-activate", "manager-drain", "manager-retire")):
        call = http.calls[index]
        assert call["method"] == "POST"
        assert call["headers"]["Idempotency-Key"] == str(_KEYS[index])
        assert call["headers"]["Authorization"] == (
            f"Bearer {credentials.joinpath(credential, 'bearer-token').read_text()}"
        )
        assert json.loads(call["content"]) == requests[index].model_dump(mode="json")


@pytest.mark.parametrize("operation", ["activate", "drain"])
@pytest.mark.parametrize(
    "change",
    [
        {"authority_incarnation": str(UUID(int=789))},
        {"writer_epoch": 999},
        {"execution_epoch": 999},
        {"execution_manifest_sha256": "e" * 64},
        {"trusted_fleet_release_sha256": "0" * 64},
        {
            "execution_state": "prepared",
            "executable_new_capacity_ceiling": 0,
            "executable_new_capacity_rate_per_minute": 0,
        },
        {
            "execution_state": "active",
            "executable_new_capacity_ceiling": 2,
            "executable_new_capacity_rate_per_minute": 1,
        },
        {
            "execution_state": "active",
            "executable_new_capacity_ceiling": 1,
            "executable_new_capacity_rate_per_minute": 2,
        },
        {"executable": False},
        {"extra_authority": "unexpected"},
    ],
)
def test_execution_transition_rejects_wrong_authority(tmp_path: Path, operation, change):
    requests, responses = _transitions()
    index = 0 if operation == "activate" else 1
    response = responses[index].model_dump(mode="json") | change
    client, http, _credentials_root = _client(tmp_path, [response])
    with pytest.raises(ProtectedCapacityManagerClientError, match="unexpected"):
        getattr(client, f"{operation}_execution")(requests[index], _KEYS[0])
    assert http.closed
    assert len(http.calls) == 1


@pytest.mark.parametrize(
    "change",
    [
        {"execution_epoch": 999},
        {"execution_manifest_sha256": "f" * 64},
        {"retired_at": "2026-09-08T18:00:00"},
        {"replayed": "false"},
        {"extra_authority": "unexpected"},
    ],
)
def test_retirement_rejects_wrong_or_malformed_result(tmp_path: Path, change):
    requests, responses = _transitions()
    client, http, _credentials_root = _client(tmp_path, [responses[2] | change])
    with pytest.raises(ProtectedCapacityManagerClientError, match="unexpected"):
        client.retire_execution(requests[2], _KEYS[0])
    assert http.closed


@pytest.mark.parametrize("operation", ["activate", "drain", "retire"])
def test_execution_transition_rejects_untyped_request_before_transport(tmp_path: Path, operation):
    client, http, _credentials_root = _client(tmp_path, [])
    with pytest.raises(TypeError):
        getattr(client, f"{operation}_execution")({}, _KEYS[0])
    assert not http.calls


@pytest.mark.parametrize("operation,index", [("activate", 0), ("drain", 1), ("retire", 2)])
@pytest.mark.parametrize("key", [None, UUID(int=0), "not-a-uuid"])
def test_execution_transition_requires_nonzero_idempotency_key(
    tmp_path: Path, operation, index, key
):
    requests, _responses = _transitions()
    client, http, _credentials_root = _client(tmp_path, [])
    with pytest.raises(ValueError, match="idempotency"):
        getattr(client, f"{operation}_execution")(requests[index], key)
    assert not http.calls


@pytest.mark.parametrize("operation,index", [("activate", 0), ("drain", 1), ("retire", 2)])
@pytest.mark.parametrize("status", [401, 403, 409, 500])
def test_execution_transition_does_not_retry_rejection_or_ambiguous_failure(
    tmp_path: Path, operation, index, status
):
    requests, _responses = _transitions()
    client, http, _credentials_root = _client(tmp_path, [])
    http.responses.append(_Response(b'{"detail":"private diagnostic"}', status_code=status))
    with pytest.raises(ProtectedCapacityManagerClientError, match="unexpected") as error:
        getattr(client, f"{operation}_execution")(requests[index], _KEYS[0])
    assert "private diagnostic" not in str(error.value)
    assert len(http.calls) == 1
    assert http.closed


def test_retirement_replay_keeps_exact_request_and_idempotency_key(tmp_path: Path):
    requests, responses = _transitions()
    client, http, _credentials_root = _client(
        tmp_path, [responses[2], responses[2] | {"replayed": True}]
    )
    first = client.retire_execution(requests[2], _KEYS[0])
    replay = client.retire_execution(requests[2], _KEYS[0])
    assert first.replayed is False and replay.replayed is True
    assert first.execution_epoch == replay.execution_epoch
    assert first.execution_manifest_sha256 == replay.execution_manifest_sha256
    assert first.retired_at == replay.retired_at
    assert len(http.calls) == 2
    assert http.calls[0]["content"] == http.calls[1]["content"]
    assert http.calls[0]["headers"]["Idempotency-Key"] == str(_KEYS[0])
    assert http.calls[1]["headers"]["Idempotency-Key"] == str(_KEYS[0])
