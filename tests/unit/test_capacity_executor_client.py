from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest

from loom_capacity_executor.client import (
    CapacityExecutorClient,
    ExecutableCapacityExecutorClient,
    ExecutorTransportError,
)
from loom_capacity_executor.dry_run import DryRunExecutorBinding
from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableBootstrapProposalV2,
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorInventoryV2,
    ExecutableExecutorRegistrationV2,
    ExecutableIntentBindingV2,
    ExecutableIntentCloseV2,
    ExecutablePartialReleaseV2,
    ExecutablePermitConsumptionV2,
    ExecutableReleasedShapeV2,
    ExecutableReservationAcceptanceV2,
    ExecutableSubmissionRecoveryV2,
    ExecutionContextV2,
    ExecutionFenceV2,
    canonical_executable_digest,
)
from loom_capacity_manager.grant_contracts import (
    DryRunExecutorHeartbeatV1,
    DryRunReservationAcceptanceV1,
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


def _executable_registration() -> ExecutableExecutorRegistrationV2:
    return ExecutableExecutorRegistrationV2(
        execution=ExecutionContextV2(
            authority_incarnation=UUID(int=30),
            writer_epoch=4,
            configuration_epoch=5,
            execution_epoch=6,
            execution_manifest_sha256="1" * 64,
            execution_state="active",
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
            trusted_fleet_release_sha256="2" * 64,
        ),
        executor_id="oldlab-executor",
        executor_incarnation=UUID(int=31),
        pool_id="oldlab",
        pool_generation=2,
        signing_key_id="oldlab-key",
        signing_key_sha256="3" * 64,
        local_authority_sha256="4" * 64,
        controller_authority_sha256="5" * 64,
    )


def _executable_intent() -> ExecutableIntentBindingV2:
    registration = _executable_registration()
    return ExecutableIntentBindingV2(
        execution=ExecutionFenceV2(
            **registration.execution.model_dump(
                mode="python", exclude={"schema_version", "executable"}
            ),
            allocation_epoch=7,
        ),
        tranche_id=UUID(int=32),
        intent_id=UUID(int=33),
        shape_instance_id="shape-oldlab-1",
        subject_id=UUID(int=34),
        subject_incarnation=UUID(int=35),
        account_id="owner-1",
        tier_id="development",
        candidate=CandidateBindingV2(
            algorithm="source-sha256",
            identity="6" * 64,
            publication_sha256="7" * 64,
        ),
        candidate_generation=1,
        deployment_generation=1,
        pool_id=registration.pool_id,
        pool_generation=registration.pool_generation,
        executor_id=registration.executor_id,
        executor_incarnation=registration.executor_incarnation,
        shape_id="one-slot",
        profile_id="oldlab-profile",
        profile_generation=1,
        profile_digest="8" * 64,
        concurrency_slots=1,
        resources=ResourceVectorV1(slots=1, cpu_millicores=1_000),
        node_ids=("oldlab1",),
    )


def _receipt_digest(payload: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            default=str,
        ).encode("ascii")
    ).hexdigest()


async def test_executor_client_fetches_exact_checkpoint_and_accepts_over_https() -> None:
    seen: list[httpx.Request] = []
    binding = _binding()
    acceptance = DryRunReservationAcceptanceV1(
        tranche_id=UUID(int=20),
        proposal_digest="a" * 64,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        command_sequence=1,
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
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
                },
            )
        assert json.loads(request.content) == acceptance.model_dump(mode="json")
        return httpx.Response(
            200,
            json={
                "tranche_id": str(acceptance.tranche_id),
                "intent_ids": [str(UUID(int=21))],
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
    checkpoint = await client.checkpoint()
    accepted = await client.accept_reservation(acceptance)
    await http.aclose()

    assert checkpoint.journal_digest == "0" * 64
    assert accepted.tranche_id == acceptance.tranche_id
    assert [request.url.path for request in seen] == [
        "/v1/executors/oldlab/checkpoint",
        f"/v1/executors/oldlab/reservations/{acceptance.tranche_id}/accept",
    ]
    assert all(request.headers["authorization"] == "Bearer executor-secret" for request in seen)
    assert seen[1].headers["content-type"] == "application/json"


async def test_executor_client_rejects_binding_drift_bad_receipts_and_redirects() -> None:
    binding = _binding()
    calls = 0

    async def bad_receipt(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"unexpected": True})

    http = httpx.AsyncClient(transport=httpx.MockTransport(bad_receipt))
    client = CapacityExecutorClient(
        binding,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )
    wrong = DryRunExecutorHeartbeatV1(
        authority_incarnation=binding.authority_incarnation,
        writer_epoch=binding.writer_epoch,
        executor_id="gb10-executor",
        executor_incarnation=binding.executor_incarnation,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        heartbeat_sequence=1,
        journal_sequence=0,
        journal_digest="0" * 64,
    )
    with pytest.raises(ExecutorTransportError, match="binding"):
        await client.heartbeat(wrong)
    assert calls == 0

    with pytest.raises(ExecutorTransportError, match="receipt"):
        await client.checkpoint()
    await http.aclose()

    async def redirected(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(307, headers={"Location": "https://evil.invalid"})

    redirected_http = httpx.AsyncClient(transport=httpx.MockTransport(redirected))
    redirected_client = CapacityExecutorClient(
        binding,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=redirected_http,
    )
    with pytest.raises(ExecutorTransportError, match="status 307"):
        await redirected_client.checkpoint()
    await redirected_http.aclose()


async def test_executor_client_rejects_unsafe_origins_and_tokens() -> None:
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: httpx.Response(200)))
    with pytest.raises(ValueError, match="HTTPS origin"):
        CapacityExecutorClient(
            _binding(),
            manager_origin="http://capacity.example.test",
            bearer_token="secret",
            http_client=http,
        )
    await http.aclose()
    with pytest.raises(ValueError, match="credential"):
        CapacityExecutorClient(
            _binding(),
            manager_origin="https://capacity.example.test",
            bearer_token="bad token",
            http_client=http,
        )
    with pytest.raises(ValueError, match="credential"):
        CapacityExecutorClient(
            _binding(),
            manager_origin="https://capacity.example.test",
            bearer_token="x" * 4097,
            http_client=http,
        )


async def test_executable_client_proposes_only_a_bootstrap_hash() -> None:
    registration = _executable_registration()
    proposal = ExecutableBootstrapProposalV2(
        binding=_executable_intent(),
        command_sequence=2,
        proposal_epoch=1,
        bootstrap_sha256="9" * 64,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    payload: dict[str, object] = {
        "intent_id": str(proposal.binding.intent_id),
        "proposal_epoch": proposal.proposal_epoch,
        "proposal_digest": canonical_executable_digest(proposal),
        "executable": True,
    }
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        assert json.loads(request.content) == proposal.model_dump(mode="json")
        return httpx.Response(
            200,
            json={
                "intent_id": payload["intent_id"],
                "proposal_epoch": payload["proposal_epoch"],
                "receipt_digest": _receipt_digest(
                    {
                        "intent_id": payload["intent_id"],
                        "proposal_epoch": payload["proposal_epoch"],
                        "proposal_digest": payload["proposal_digest"],
                        "executable": True,
                    }
                ),
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
    try:
        receipt = await client.propose_executable_bootstrap(proposal)
    finally:
        await http.aclose()

    assert receipt.intent_id == proposal.binding.intent_id
    assert receipt.proposal_epoch == proposal.proposal_epoch
    assert [request.url.path for request in seen] == [
        f"/v2/executors/oldlab/intents/{proposal.binding.intent_id}/bootstrap-proposals"
    ]
    assert not hasattr(client, "register_executable_bootstrap")


async def test_executable_client_covers_the_complete_manager_transport() -> None:
    registration = _executable_registration()
    binding = _executable_intent()
    heartbeat = ExecutableExecutorHeartbeatV2(
        execution=registration.execution,
        executor_id=registration.executor_id,
        executor_incarnation=registration.executor_incarnation,
        pool_id=registration.pool_id,
        pool_generation=registration.pool_generation,
        heartbeat_sequence=1,
        journal_sequence=0,
        journal_digest="0" * 64,
    )
    inventory = ExecutableExecutorInventoryV2(
        execution=registration.execution,
        executor_id=registration.executor_id,
        executor_incarnation=registration.executor_incarnation,
        pool_id=registration.pool_id,
        pool_generation=registration.pool_generation,
        inventory_sequence=1,
        journal_sequence=0,
        journal_digest="0" * 64,
    )
    acceptance = ExecutableReservationAcceptanceV2(
        execution=binding.execution,
        tranche_id=binding.tranche_id,
        proposal_digest="a" * 64,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        command_sequence=1,
    )
    permit_id = UUID(int=40)
    consumption = ExecutablePermitConsumptionV2(
        permit_id=permit_id,
        permit_digest="b" * 64,
        binding=binding,
        command_sequence=2,
    )
    recovery = ExecutableSubmissionRecoveryV2(
        binding=binding,
        permit_id=permit_id,
        permit_digest=consumption.permit_digest,
        command_sequence=3,
        inventory_sequence=1,
        inventory_digest=canonical_executable_digest(inventory),
        controller_query_completed_at=datetime.now(UTC),
        submit_process_absent=True,
        scheduler_submission_absent=True,
        controller_evidence_sha256="c" * 64,
    )
    close = ExecutableIntentCloseV2(binding=binding, command_sequence=4)
    release = ExecutablePartialReleaseV2(
        execution=binding.execution,
        tranche_id=binding.tranche_id,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        command_sequence=5,
        releases=(
            ExecutableReleasedShapeV2(
                binding=binding,
                inventory_sequence=2,
                terminal_kind="unused",
                terminal_identity="unused-shape-oldlab-1",
                terminal_evidence_sha256="d" * 64,
                protected_registration_epoch=2,
                bootstrap_revoked=True,
                protected_release_sha256="e" * 64,
            ),
        ),
    )
    seen: list[tuple[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        seen.append((request.method, path))
        if path.endswith("/checkpoint"):
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
        if path.endswith("/heartbeat"):
            assert json.loads(request.content) == heartbeat.model_dump(mode="json")
            return httpx.Response(
                200,
                json={
                    "heartbeat_sequence": heartbeat.heartbeat_sequence,
                    "lease_expires_at": (
                        datetime.now(UTC) + timedelta(minutes=1)
                    ).isoformat(),
                    "replayed": False,
                    "executable": True,
                },
            )
        if path.endswith("/inventory"):
            assert json.loads(request.content) == inventory.model_dump(mode="json")
            return httpx.Response(
                200,
                json={
                    "inventory_sequence": inventory.inventory_sequence,
                    "inventory_digest": canonical_executable_digest(inventory),
                    "replayed": False,
                    "executable": True,
                },
            )
        if path.endswith("/work"):
            return httpx.Response(200, content=binding.model_dump_json().encode("ascii"))
        contracts = {
            f"/reservations/{binding.tranche_id}/accept": acceptance,
            f"/permits/{permit_id}/consume": consumption,
            f"/permits/{permit_id}/recover": recovery,
            f"/intents/{binding.intent_id}/close": close,
            f"/reservations/{binding.tranche_id}/release": release,
        }
        contract = next(value for suffix, value in contracts.items() if path.endswith(suffix))
        assert json.loads(request.content) == contract.model_dump(mode="json")
        if contract is acceptance:
            payload = {
                "tranche_id": str(binding.tranche_id),
                "intent_ids": [str(binding.intent_id)],
                "executable": True,
            }
            body = {
                **payload,
                "receipt_digest": _receipt_digest(payload),
                "replayed": False,
            }
        elif contract is consumption:
            payload = {
                "permit_id": str(permit_id),
                "intent_id": str(binding.intent_id),
                "executable": True,
            }
            body = {
                **payload,
                "receipt_digest": _receipt_digest(payload),
                "replayed": False,
            }
        elif contract is recovery:
            payload = {
                "intent_id": str(binding.intent_id),
                "recovery": recovery.model_dump(mode="json"),
                "executable": True,
            }
            body = {
                "intent_id": str(binding.intent_id),
                "receipt_digest": _receipt_digest(payload),
                "replayed": False,
                "executable": True,
            }
        elif contract is close:
            payload = {"intent_id": str(binding.intent_id), "executable": True}
            body = {
                **payload,
                "receipt_digest": _receipt_digest(payload),
                "replayed": False,
            }
        else:
            payload = {
                "tranche_id": str(binding.tranche_id),
                "released_shape_ids": [binding.shape_instance_id],
                "executable": True,
            }
            body = {
                **payload,
                "receipt_digest": _receipt_digest(payload),
                "replayed": False,
            }
        return httpx.Response(200, json=body)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = ExecutableCapacityExecutorClient(
        registration,
        manager_origin="https://capacity.example.test",
        bearer_token="executor-secret",
        http_client=http,
    )
    try:
        assert (await client.executable_checkpoint()).command_sequence == 0
        assert (await client.heartbeat_executable_executor(heartbeat)).heartbeat_sequence == 1
        assert (await client.ingest_executable_inventory(inventory)).inventory_sequence == 1
        assert await client.next_executable_work(0) == binding
        assert (await client.accept_executable_reservation(acceptance)).intent_ids == (
            binding.intent_id,
        )
        assert (await client.consume_executable_permit(consumption)).permit_id == permit_id
        assert (await client.recover_executable_submission(recovery)).intent_id == binding.intent_id
        assert (await client.close_executable_intent(close)).intent_id == binding.intent_id
        assert (await client.release_executable_shapes(release)).released_shape_ids == (
            binding.shape_instance_id,
        )
    finally:
        await http.aclose()

    assert seen == [
        ("GET", "/v2/executors/oldlab/checkpoint"),
        ("PUT", "/v2/executors/oldlab/heartbeat"),
        ("PUT", "/v2/executors/oldlab/inventory"),
        ("GET", "/v2/executors/oldlab/work"),
        ("POST", f"/v2/executors/oldlab/reservations/{binding.tranche_id}/accept"),
        ("POST", f"/v2/executors/oldlab/permits/{permit_id}/consume"),
        ("POST", f"/v2/executors/oldlab/permits/{permit_id}/recover"),
        ("POST", f"/v2/executors/oldlab/intents/{binding.intent_id}/close"),
        ("POST", f"/v2/executors/oldlab/reservations/{binding.tranche_id}/release"),
    ]
