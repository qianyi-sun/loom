from __future__ import annotations

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
from loom_capacity_executor.remote_executable import RemoteExecutablePoolExecutor
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapProposalV2,
    canonical_executable_digest,
)
from loom_capacity_manager.grant_contracts import (
    DryRunReservationAcceptanceV1,
    canonical_grant_digest,
)
from tests.unit.test_capacity_executor_client import (
    _executable_intent,
    _executable_registration,
    _receipt_digest,
)


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


async def test_remote_executable_executor_journals_only_bootstrap_proposal(
    tmp_path: Path,
) -> None:
    registration = _executable_registration()
    proposal = ExecutableBootstrapProposalV2(
        binding=_executable_intent(),
        command_sequence=1,
        proposal_epoch=1,
        bootstrap_sha256="9" * 64,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "execution_epoch": registration.execution.execution_epoch,
                    "execution_manifest_sha256": (
                        registration.execution.execution_manifest_sha256
                    ),
                    "executor_id": registration.executor_id,
                    "executor_incarnation": str(registration.executor_incarnation),
                    "pool_id": registration.pool_id,
                    "pool_generation": registration.pool_generation,
                    "command_sequence": 0,
                    "journal_sequence": 0,
                    "journal_digest": "0" * 64,
                    "inventory_sequence": 0,
                    "lease_expires_at": (
                        datetime.now(UTC) + timedelta(minutes=1)
                    ).isoformat(),
                    "executable": True,
                },
            )
        payload = {
            "intent_id": str(proposal.binding.intent_id),
            "proposal_epoch": proposal.proposal_epoch,
            "proposal_digest": canonical_executable_digest(proposal),
            "executable": True,
        }
        return httpx.Response(
            200,
            json={
                "intent_id": payload["intent_id"],
                "proposal_epoch": payload["proposal_epoch"],
                "receipt_digest": _receipt_digest(payload),
                "replayed": False,
                "executable": True,
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = ExecutableCapacityExecutorClient(
        registration,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )
    with ExecutorJournal(tmp_path / "executable.journal") as journal:
        executor = RemoteExecutablePoolExecutor(registration, journal, client)
        receipt = await executor.propose_bootstrap(proposal)
        latest = journal.latest("intent", str(proposal.binding.intent_id))
        assert receipt.proposal_epoch == 1
        assert latest is not None
        assert latest.event_kind == "bootstrap-propose-confirmed"
        assert latest.payload_digest == canonical_executable_digest(proposal)
        assert not hasattr(executor, "register_bootstrap")
    await http.aclose()
