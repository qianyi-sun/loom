from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import httpx
import pytest

from loom_capacity_executor.client import (
    CapacityExecutorClient,
    ExecutableCapacityExecutorClient,
    ExecutorRejectedError,
    ExecutorTransportError,
)
from loom_capacity_executor.dry_run import DryRunExecutorBinding
from loom_capacity_executor.journal import ExecutorJournal, JournalRegressionError
from loom_capacity_executor.remote import RemoteDryRunPoolExecutor
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorRegistrationV2,
    ExecutableIntentCloseV2,
    ExecutableLaunchPermitV2,
    ExecutablePartialReleaseV2,
    ExecutableReleasedShapeV2,
    ExecutableReservationAcceptanceV2,
    ExecutableReservationProposalV2,
    ExecutionContextV2,
    StrictV2Model,
)
from loom_capacity_manager.grant_contracts import (
    DryRunReservationAcceptanceV1,
    ReservationShapeV1,
    canonical_grant_digest,
)
from tests.unit.test_capacity_executor_launch_renderer import launch_context_fixture


def _binding() -> DryRunExecutorBinding:
    return DryRunExecutorBinding(
        authority_incarnation=UUID(int=10),
        writer_epoch=4,
        executor_id="oldlab-executor",
        executor_incarnation=UUID(int=11),
        pool_id="oldlab",
        pool_generation=2,
    )


def _checkpoint(binding: DryRunExecutorBinding) -> dict[str, object]:
    return {
        "executor_row_id": str(UUID(int=12)),
        "authority_incarnation": str(binding.authority_incarnation),
        "writer_epoch": binding.writer_epoch,
        "executor_id": binding.executor_id,
        "executor_incarnation": str(binding.executor_incarnation),
        "pool_id": binding.pool_id,
        "pool_generation": binding.pool_generation,
        "command_sequence": 0,
        "journal_sequence": 0,
        "journal_digest": "0" * 64,
        "inventory_sequence": 0,
        "lease_expires_at": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        "executable": False,
    }


def _executable_registration() -> ExecutableExecutorRegistrationV2:
    launch = launch_context_fixture()
    return ExecutableExecutorRegistrationV2(
        execution=ExecutionContextV2.model_validate(
            launch.binding.execution.model_dump(exclude={"allocation_epoch", "executable"})
        ),
        executor_id=launch.binding.executor_id,
        executor_incarnation=launch.binding.executor_incarnation,
        pool_id=launch.binding.pool_id,
        pool_generation=launch.binding.pool_generation,
        signing_key_id=launch.ownership_key.signing_key_id,
        signing_key_sha256=launch.ownership_key.public_key_sha256,
        local_authority_sha256="a" * 64,
        controller_authority_sha256=launch.controller_authority.controller_authority_sha256,
    )


def _executable_work() -> tuple[StrictV2Model, ...]:
    binding = launch_context_fixture().binding
    proposal = ExecutableReservationProposalV2(
        tranche_id=binding.tranche_id,
        execution=binding.execution,
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        account_id=binding.account_id,
        tier_id=binding.tier_id,
        candidate=binding.candidate,
        candidate_generation=binding.candidate_generation,
        deployment_generation=binding.deployment_generation,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        shapes=(
            ReservationShapeV1(
                shape_instance_id=binding.shape_instance_id,
                intent_id=binding.intent_id,
                shape_id=binding.shape_id,
                profile_id=binding.profile_id,
                profile_generation=binding.profile_generation,
                profile_digest=binding.profile_digest,
                concurrency_slots=binding.concurrency_slots,
                resources=binding.resources,
                node_ids=binding.node_ids,
            ),
        ),
    )
    permit = ExecutableLaunchPermitV2(
        permit_id=UUID(int=202),
        binding=binding,
        permit_epoch=1,
        launch_rank=1,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    close = ExecutableIntentCloseV2(binding=binding, command_sequence=1)
    release = ExecutablePartialReleaseV2(
        execution=binding.execution,
        tranche_id=binding.tranche_id,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        command_sequence=1,
        releases=(
            ExecutableReleasedShapeV2(
                binding=binding,
                inventory_sequence=1,
                terminal_kind="unused",
                terminal_identity="unused-shape",
                terminal_evidence_sha256="b" * 64,
                protected_registration_epoch=2,
                bootstrap_revoked=True,
                protected_release_sha256="c" * 64,
            ),
        ),
    )
    return proposal, binding, permit, close, release


def _executable_receipt_digest(payload: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        ).encode("ascii")
    ).hexdigest()


@pytest.mark.parametrize("expected", _executable_work())
async def test_executable_client_decodes_each_exact_pool_work_shape(
    expected: StrictV2Model,
) -> None:
    registration = _executable_registration()

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/v2/executors/{registration.pool_id}/work"
        assert request.headers["Authorization"] == "Bearer executor-secret"
        return httpx.Response(200, content=expected.model_dump_json().encode("ascii"))

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = ExecutableCapacityExecutorClient(
        registration,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )
    try:
        assert await client.next_executable_work(0) == expected
    finally:
        await http.aclose()


async def test_executable_client_validates_canonical_receipt_digest() -> None:
    registration = _executable_registration()
    binding = launch_context_fixture().binding
    close = ExecutableIntentCloseV2(binding=binding, command_sequence=1)
    payload: dict[str, object] = {
        "intent_id": str(binding.intent_id),
        "executable": True,
    }

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                **payload,
                "receipt_digest": _executable_receipt_digest(payload),
                "replayed": False,
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = ExecutableCapacityExecutorClient(
        registration,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )
    try:
        assert (await client.close_executable_intent(close)).intent_id == binding.intent_id
    finally:
        await http.aclose()


# Production break caught: reservation acceptance has no pool_id field, so the
# generic contract validator rejected a valid pool-routed request before HTTP.
async def test_executable_client_transports_pool_routed_reservation_acceptance() -> None:
    registration = _executable_registration()
    binding = launch_context_fixture().binding
    acceptance = ExecutableReservationAcceptanceV2(
        execution=binding.execution,
        tranche_id=binding.tranche_id,
        proposal_digest="d" * 64,
        pool_generation=binding.pool_generation,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        command_sequence=1,
    )
    payload: dict[str, object] = {
        "tranche_id": str(binding.tranche_id),
        "intent_ids": [str(binding.intent_id)],
        "executable": True,
    }
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path == (
            f"/v2/executors/{registration.pool_id}/reservations/{binding.tranche_id}/accept"
        )
        assert request.headers["Authorization"] == "Bearer executor-secret"
        assert json.loads(request.content) == acceptance.model_dump(mode="json")
        return httpx.Response(
            200,
            json={
                **payload,
                "receipt_digest": _executable_receipt_digest(payload),
                "replayed": False,
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = ExecutableCapacityExecutorClient(
        registration,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )
    try:
        receipt = await client.accept_executable_reservation(acceptance)
        assert receipt.intent_ids == (binding.intent_id,)
        assert requests != []
    finally:
        await http.aclose()


# Production break caught: matching execution digests alone could hide a changed
# drain-only/new-capacity authority in a pool-routed acceptance contract.
async def test_executable_client_rejects_changed_acceptance_execution_authority() -> None:
    registration = _executable_registration()
    binding = launch_context_fixture().binding
    changed_execution = binding.execution.model_copy(
        update={
            "execution_state": "drain-only",
            "executable_new_capacity_ceiling": 0,
            "executable_new_capacity_rate_per_minute": 0,
        }
    )
    acceptance = ExecutableReservationAcceptanceV2(
        execution=changed_execution,
        tranche_id=binding.tranche_id,
        proposal_digest="d" * 64,
        pool_generation=binding.pool_generation,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        command_sequence=1,
    )
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = ExecutableCapacityExecutorClient(
        registration,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )
    try:
        with pytest.raises(ExecutorTransportError, match="execution binding changed"):
            await client.accept_executable_reservation(acceptance)
        assert requests == []
    finally:
        await http.aclose()


@pytest.mark.parametrize(
    ("response", "error"),
    (
        (httpx.Response(409), ExecutorRejectedError),
        (httpx.Response(503), ExecutorTransportError),
        (httpx.ConnectTimeout("manager timed out"), ExecutorTransportError),
    ),
)
async def test_executable_client_distinguishes_verified_rejection_from_unknown_failure(
    response: httpx.Response | httpx.HTTPError,
    error: type[ExecutorTransportError],
) -> None:
    registration = _executable_registration()
    close = ExecutableIntentCloseV2(
        binding=launch_context_fixture().binding,
        command_sequence=1,
    )

    async def handler(_request: httpx.Request) -> httpx.Response:
        if isinstance(response, httpx.HTTPError):
            raise response
        return response

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = ExecutableCapacityExecutorClient(
        registration,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )
    try:
        with pytest.raises(error):
            await client.close_executable_intent(close)
    finally:
        await http.aclose()


async def test_remote_executor_retries_exact_journaled_command_after_transport_loss(
    tmp_path: Path,
) -> None:
    binding = _binding()
    acceptance = DryRunReservationAcceptanceV1(
        tranche_id=UUID(int=20),
        proposal_digest="a" * 64,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        command_sequence=1,
    )
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        if request.method == "GET":
            return httpx.Response(200, json=_checkpoint(binding))
        attempts += 1
        if attempts == 1:
            return httpx.Response(503)
        return httpx.Response(
            200,
            json={
                "tranche_id": str(acceptance.tranche_id),
                "intent_ids": [str(UUID(int=21))],
                "replayed": True,
                "executable": False,
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = CapacityExecutorClient(
        binding,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )
    path = tmp_path / "executor.journal"
    with ExecutorJournal(path) as journal:
        executor = RemoteDryRunPoolExecutor(binding, journal, client)
        with pytest.raises(ExecutorTransportError, match="status 503"):
            await executor.accept_reservation(acceptance)
        assert journal.pending_requests()[0].event_kind == "reservation-accept-requested"

    with ExecutorJournal(path) as journal:
        recovered = RemoteDryRunPoolExecutor(binding, journal, client)
        receipt = await recovered.accept_reservation(acceptance)
        assert receipt.replayed is True
        assert journal.pending_requests() == ()
        assert journal.head.sequence == 2
    await http.aclose()
    assert attempts == 2


async def test_remote_executor_journals_authenticated_client_rejection(
    tmp_path: Path,
) -> None:
    binding = _binding()
    acceptance = DryRunReservationAcceptanceV1(
        tranche_id=UUID(int=22),
        proposal_digest="b" * 64,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        command_sequence=1,
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=_checkpoint(binding))
        return httpx.Response(409, json={"detail": "capacity state conflict"})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = CapacityExecutorClient(
        binding,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )
    with ExecutorJournal(tmp_path / "executor.journal") as journal:
        executor = RemoteDryRunPoolExecutor(binding, journal, client)
        with pytest.raises(ExecutorRejectedError, match="status 409"):
            await executor.accept_reservation(acceptance)
        assert journal.pending_requests() == ()
        rejected = journal.latest("tranche", str(acceptance.tranche_id))
        assert rejected is not None
        assert rejected.event_kind == "reservation-accept-rejected"
        assert rejected.payload_digest == canonical_grant_digest(acceptance)
    await http.aclose()


async def test_remote_executor_heartbeat_carries_verified_central_checkpoint(
    tmp_path: Path,
) -> None:
    binding = _binding()
    seen: list[dict[str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=_checkpoint(binding))
        seen.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "executor_row_id": str(UUID(int=12)),
                "heartbeat_sequence": 1,
                "journal_sequence": 0,
                "lease_expires_at": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
                "replayed": False,
                "executable": False,
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = CapacityExecutorClient(
        binding,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )
    with ExecutorJournal(tmp_path / "executor.journal") as journal:
        executor = RemoteDryRunPoolExecutor(binding, journal, client)
        await executor.heartbeat(heartbeat_sequence=1)
    await http.aclose()

    assert seen[0]["journal_checkpoint_sequence"] == 0
    assert seen[0]["journal_checkpoint_digest"] == "0" * 64


async def test_remote_executor_rejects_empty_journal_above_command_highwater(
    tmp_path: Path,
) -> None:
    binding = _binding()
    post_calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal post_calls
        checkpoint = _checkpoint(binding)
        checkpoint["command_sequence"] = 1
        if request.method == "GET":
            return httpx.Response(200, json=checkpoint)
        post_calls += 1
        return httpx.Response(500)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = CapacityExecutorClient(
        binding,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )
    acceptance = DryRunReservationAcceptanceV1(
        tranche_id=UUID(int=20),
        proposal_digest="a" * 64,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        command_sequence=2,
    )
    with ExecutorJournal(tmp_path / "executor.journal") as journal:
        executor = RemoteDryRunPoolExecutor(binding, journal, client)
        with pytest.raises(JournalRegressionError, match="command high-water"):
            await executor.accept_reservation(acceptance)
    await http.aclose()
    assert post_calls == 0
