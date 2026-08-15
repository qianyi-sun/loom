"""Authenticated, bounded publication for trusted demand reports."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from loom_capacity_agent.admission import PreparedProtectedReleaseV1
from loom_capacity_agent.client import (
    DemandPublishError,
    DemandReporterClient,
    DemandReporterTLSFiles,
    build_reporter_tls_context,
    read_owner_only_bytes,
)
from loom_capacity_agent.contracts import (
    AgentPoolCapabilityV1,
    AgentRegistrationV1,
    GuardDemandObservationV1,
    ReporterConfigurationV1,
)
from loom_capacity_agent.reporter import build_demand_snapshot
from loom_capacity_guard.contracts import canonical_digest as guard_canonical_digest
from loom_capacity_manager.contracts import ResourceVectorV1, canonical_digest
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutableBootstrapAcknowledgementV2,
    ExecutableBootstrapProposalV2,
    ExecutableIntentBindingV2,
    ExecutionFenceV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_manager.grant_contracts import (
    DryRunProtectedReleaseAcknowledgementV1,
    canonical_grant_digest,
)
from tests.unit.test_capacity_agent_admission_contracts import publishable_release_fixture


def _configuration() -> ReporterConfigurationV1:
    registration = AgentRegistrationV1(
        environment_id="dev-alice",
        subject_id=uuid4(),
        subject_incarnation=uuid4(),
        authority_incarnation=uuid4(),
        agent_incarnation=uuid4(),
        reporter_incarnation=uuid4(),
        candidate_digest="a" * 64,
        deployment_generation=7,
        configuration_generation=11,
    )
    return ReporterConfigurationV1(
        **registration.model_dump(mode="python"),
        protected_admission_sha256="8" * 64,
        pool_capabilities=(
            AgentPoolCapabilityV1(
                capability_id="oldlab-x86-none",
                pool_id="oldlab",
                operating_system="linux",
                cpu_architecture="x86_64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
            AgentPoolCapabilityV1(
                capability_id="gb10-arm-none",
                pool_id="gb10",
                operating_system="linux",
                cpu_architecture="arm64",
                gpu_vendor="none",
                network_policies=("public",),
            ),
        ),
    )


def _executable_bootstrap(
    configuration: ReporterConfigurationV1,
) -> ExecutableBootstrapProposalV2:
    binding = ExecutableIntentBindingV2(
        execution=ExecutionFenceV2(
            authority_incarnation=UUID(int=101),
            writer_epoch=2,
            configuration_epoch=3,
            execution_epoch=4,
            execution_manifest_sha256="1" * 64,
            execution_state="active",
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
            trusted_fleet_release_sha256="2" * 64,
            allocation_epoch=5,
        ),
        tranche_id=UUID(int=102),
        intent_id=UUID(int=103),
        shape_instance_id="shape-oldlab-1",
        subject_id=configuration.subject_id,
        subject_incarnation=configuration.subject_incarnation,
        account_id="owner-1",
        tier_id="development",
        candidate=CandidateBindingV2(
            algorithm="source-sha256",
            identity="3" * 64,
            publication_sha256="4" * 64,
        ),
        candidate_generation=1,
        deployment_generation=configuration.deployment_generation,
        pool_id="oldlab",
        pool_generation=1,
        executor_id="oldlab-executor",
        executor_incarnation=UUID(int=104),
        shape_id="one-slot",
        profile_id="oldlab-profile",
        profile_generation=1,
        profile_digest="5" * 64,
        concurrency_slots=1,
        resources=ResourceVectorV1(slots=1, cpu_millicores=1_000),
        node_ids=("oldlab1",),
    )
    return ExecutableBootstrapProposalV2(
        binding=binding,
        command_sequence=2,
        proposal_epoch=1,
        bootstrap_sha256="6" * 64,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )


def _snapshot(configuration: ReporterConfigurationV1):  # type: ignore[no-untyped-def]
    observation = GuardDemandObservationV1(
        **{field: getattr(configuration, field) for field in AgentRegistrationV1.model_fields},
        sequence=1,
        source_observed_at="2026-08-10T12:00:00Z",
        attempts=(),
    )
    return build_demand_snapshot(observation, configuration)


def _protected_release(
    configuration: ReporterConfigurationV1,
) -> PreparedProtectedReleaseV1:
    return PreparedProtectedReleaseV1(
        **{field: getattr(configuration, field) for field in AgentRegistrationV1.model_fields},
        release_id=uuid4(),
        plan_id=uuid4(),
        admission_incarnation=uuid4(),
        manager_authority_incarnation=uuid4(),
        manager_writer_epoch=1,
        manager_configuration_epoch=2,
        manager_allocation_epoch=3,
        tranche_id=uuid4(),
        pool_id="oldlab",
        pool_generation=1,
        shape_instance_id="shape-oldlab-1",
        submission_intent_id=uuid4(),
        bootstrap_registration_epoch=0,
        protected_registration_epoch=1,
        bootstrap_revoked=True,
    )


def _executable_publication(configuration: ReporterConfigurationV1):  # type: ignore[no-untyped-def]
    publication = publishable_release_fixture()
    candidate = publication.release.binding.candidate.model_copy(
        update={
            "identity": configuration.candidate_digest,
            "publication_sha256": configuration.candidate_digest,
        }
    )
    binding = publication.release.binding.model_copy(
        update={
            "subject_id": configuration.subject_id,
            "subject_incarnation": configuration.subject_incarnation,
            "deployment_generation": configuration.deployment_generation,
            "candidate": candidate,
        }
    )
    release = publication.release.model_copy(
        update={
            "binding": binding,
            "reporter_incarnation": configuration.reporter_incarnation,
        }
    )
    return publication.model_copy(
        update={
            "release": release,
            "publication_digest": canonical_executable_digest(release),
        }
    )


def _executable_receipt_digest(payload: dict[str, object]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _manager_executable_release_receipt_digest(configuration: ReporterConfigurationV1) -> str:
    publication = _executable_publication(configuration)
    return canonical_executable_digest(publication.release)


@pytest.mark.asyncio
async def test_publish_uses_exact_subject_endpoint_and_verifies_receipt() -> None:
    configuration = _configuration()
    snapshot = _snapshot(configuration)
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "snapshot_id": str(uuid4()),
                "digest": canonical_digest(snapshot),
                "sequence": snapshot.sequence,
                "replayed": False,
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal:8443",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        receipt = await client.publish(snapshot)
    finally:
        await http.aclose()

    assert receipt.sequence == 1
    assert receipt.replayed is False
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "PUT"
    assert request.url == httpx.URL(
        f"https://capacity.internal:8443/v1/reports/demand/{configuration.subject_id}"
    )
    assert request.headers["Authorization"] == "Bearer reporter-secret"
    assert request.headers["Content-Type"] == "application/json"
    assert json.loads(request.content) == snapshot.model_dump(mode="json", exclude_none=False)


@pytest.mark.asyncio
async def test_publish_rejects_binding_mismatch_before_network() -> None:
    configuration = _configuration()
    snapshot = _snapshot(configuration).model_copy(update={"sequence": 2})
    stale = snapshot.model_copy(update={"subject_incarnation": uuid4()})
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        with pytest.raises(DemandPublishError, match="binding"):
            await client.publish(stale)
    finally:
        await http.aclose()
    assert calls == 0


@pytest.mark.asyncio
async def test_publish_protected_release_maps_local_fence_and_verifies_receipt() -> None:
    configuration = _configuration()
    release = _protected_release(configuration)
    idempotency_key = uuid4()
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        acknowledgement = DryRunProtectedReleaseAcknowledgementV1.model_validate_json(
            request.content
        )
        return httpx.Response(
            200,
            json={
                "acknowledgement_id": str(uuid4()),
                "shape_instance_id": release.shape_instance_id,
                "acknowledgement_digest": canonical_grant_digest(acknowledgement),
                "replayed": False,
                "executable": False,
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        receipt = await client.publish_protected_release(
            release,
            idempotency_key=idempotency_key,
        )
    finally:
        await http.aclose()

    assert receipt.shape_instance_id == release.shape_instance_id
    assert receipt.executable is False
    assert len(seen) == 1
    request = seen[0]
    assert request.url == httpx.URL(
        "https://capacity.internal/v1/reports/protected-releases/"
        f"{release.subject_id}/{release.shape_instance_id}"
    )
    assert request.headers["Idempotency-Key"] == str(idempotency_key)
    acknowledgement = DryRunProtectedReleaseAcknowledgementV1.model_validate_json(request.content)
    assert acknowledgement.protected_release_sha256 == guard_canonical_digest(release)
    assert acknowledgement.intent_id == release.submission_intent_id


@pytest.mark.asyncio
async def test_subject_client_fetches_and_acknowledges_executable_bootstrap() -> None:
    configuration = _configuration()
    proposal = _executable_bootstrap(configuration)
    acknowledgement = ExecutableBootstrapAcknowledgementV2(
        binding=proposal.binding,
        proposal_epoch=proposal.proposal_epoch,
        proposal_digest=canonical_executable_digest(proposal),
        reporter_incarnation=configuration.reporter_incarnation,
        bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256="7" * 64,
        protected_admission_sha256="8" * 64,
    )
    idempotency_key = uuid4()
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.method == "GET":
            return httpx.Response(200, content=proposal.model_dump_json().encode("ascii"))
        assert json.loads(request.content) == acknowledgement.model_dump(mode="json")
        return httpx.Response(
            200,
            json={
                "intent_id": str(acknowledgement.binding.intent_id),
                "bootstrap_registration_epoch": (acknowledgement.bootstrap_registration_epoch),
                "receipt_digest": canonical_executable_digest(acknowledgement),
                "replayed": False,
                "executable": True,
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        fetched = await client.next_executable_bootstrap()
        receipt = await client.publish_executable_bootstrap_acknowledgement(
            acknowledgement,
            idempotency_key=idempotency_key,
        )
    finally:
        await http.aclose()

    assert fetched == proposal
    assert receipt.intent_id == proposal.binding.intent_id
    assert [request.url.path for request in seen] == [
        f"/v2/subjects/{configuration.subject_id}/bootstrap-work",
        f"/v2/subjects/{configuration.subject_id}/intents/"
        f"{proposal.binding.intent_id}/bootstrap-acknowledgements",
    ]
    assert seen[1].headers["Idempotency-Key"] == str(idempotency_key)


@pytest.mark.asyncio
async def test_publish_protected_release_rejects_invalid_idempotency_before_network() -> None:
    configuration = _configuration()
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        with pytest.raises(DemandPublishError, match="idempotency"):
            await client.publish_protected_release(
                _protected_release(configuration),
                idempotency_key="not-a-uuid",  # type: ignore[arg-type]
            )
    finally:
        await http.aclose()
    assert calls == 0


@pytest.mark.asyncio
async def test_publish_executable_protected_release_uses_exact_v2_reporter_request() -> None:
    configuration = _configuration()
    publication = _executable_publication(configuration)
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "intent_id": str(publication.release.binding.intent_id),
                "protected_release_sha256": publication.release.protected_release_sha256,
                "receipt_digest": canonical_executable_digest(publication.release),
                "replayed": False,
                "executable": True,
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        receipt = await client.publish_executable_protected_release(
            publication,
            idempotency_key=uuid4(),
        )
    finally:
        await http.aclose()

    assert receipt.intent_id == publication.release.binding.intent_id
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "PUT"
    assert request.url == httpx.URL(
        "https://capacity.internal/v2/reports/protected-releases/"
        f"{publication.release.binding.subject_id}/{publication.release.binding.shape_instance_id}"
    )
    assert request.headers["Authorization"] == "Bearer reporter-secret"
    assert request.headers["Content-Type"] == "application/json"
    assert request.content == canonical_executable_bytes(publication.release)


@pytest.mark.asyncio
@pytest.mark.parametrize("replayed", (False, True))
async def test_publish_executable_protected_release_accepts_manager_digest_for_full_release_payload(
    replayed: bool,
) -> None:
    configuration = _configuration()
    publication = _executable_publication(configuration)

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "intent_id": str(publication.release.binding.intent_id),
                "protected_release_sha256": publication.release.protected_release_sha256,
                "receipt_digest": canonical_executable_digest(publication.release),
                "replayed": replayed,
                "executable": True,
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        receipt = await client.publish_executable_protected_release(
            publication,
            idempotency_key=uuid4(),
        )
    finally:
        await http.aclose()

    assert receipt.replayed is replayed
    assert receipt.receipt_digest == canonical_executable_digest(publication.release)


@pytest.mark.asyncio
async def test_publish_executable_protected_release_rejects_binding_mismatch_before_network() -> (
    None
):
    configuration = _configuration()
    stale_release = _executable_publication(configuration).release.model_copy(
        update={"reporter_incarnation": uuid4()}
    )
    publication = _executable_publication(configuration).model_copy(
        update={
            "release": stale_release,
            "publication_digest": canonical_executable_digest(stale_release),
        }
    )
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        with pytest.raises(DemandPublishError, match="binding"):
            await client.publish_executable_protected_release(
                publication,
                idempotency_key=uuid4(),
            )
    finally:
        await http.aclose()

    assert calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("release_update", "expected_message"),
    (
        ({"binding": {"subject_id": uuid4()}}, "subject_id"),
        ({"binding": {"subject_incarnation": uuid4()}}, "subject_incarnation"),
        ({"reporter_incarnation": uuid4()}, "reporter_incarnation"),
    ),
)
async def test_publish_executable_protected_release_rejects_each_binding_mismatch_before_network(
    release_update: dict[str, object],
    expected_message: str,
) -> None:
    configuration = _configuration()
    publication = _executable_publication(configuration)
    release = publication.release
    binding_update = release_update.get("binding")
    if isinstance(binding_update, dict):
        release = release.model_copy(
            update={"binding": release.binding.model_copy(update=binding_update)}
        )
    release = release.model_copy(
        update={key: value for key, value in release_update.items() if key != "binding"}
    )
    publication = publication.model_copy(
        update={
            "release": release,
            "publication_digest": canonical_executable_digest(release),
        }
    )
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        with pytest.raises(DemandPublishError, match=expected_message):
            await client.publish_executable_protected_release(
                publication,
                idempotency_key=uuid4(),
            )
    finally:
        await http.aclose()

    assert calls == 0


@pytest.mark.asyncio
async def test_publish_executable_protected_release_rejects_invalid_idempotency_before_network() -> (
    None
):
    configuration = _configuration()
    publication = _executable_publication(configuration)
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        with pytest.raises(DemandPublishError, match="idempotency"):
            await client.publish_executable_protected_release(
                publication,
                idempotency_key="not-a-uuid",  # type: ignore[arg-type]
            )
    finally:
        await http.aclose()

    assert calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response_json",
    (
        {
            "intent_id": str(uuid4()),
            "protected_release_sha256": "4" * 64,
            "receipt_digest": "6" * 64,
            "replayed": False,
            "executable": True,
        },
        {
            "intent_id": str(uuid4()),
            "protected_release_sha256": "0" * 64,
            "receipt_digest": "6" * 64,
            "replayed": False,
            "executable": True,
        },
        {
            "intent_id": str(uuid4()),
            "protected_release_sha256": "4" * 64,
            "receipt_digest": "6" * 64,
            "replayed": False,
            "executable": False,
        },
    ),
)
async def test_publish_executable_protected_release_rejects_changed_receipt(
    response_json: dict[str, object],
) -> None:
    configuration = _configuration()
    publication = _executable_publication(configuration)
    response_json = {
        **response_json,
        "intent_id": str(response_json["intent_id"]),
    }

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=response_json)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        with pytest.raises(DemandPublishError, match="receipt"):
            await client.publish_executable_protected_release(
                publication,
                idempotency_key=uuid4(),
            )
    finally:
        await http.aclose()


@pytest.mark.asyncio
async def test_publish_executable_protected_release_rejects_tampered_receipt_digest() -> None:
    configuration = _configuration()
    publication = _executable_publication(configuration)

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "intent_id": str(publication.release.binding.intent_id),
                "protected_release_sha256": publication.release.protected_release_sha256,
                "receipt_digest": "0" * 64,
                "replayed": False,
                "executable": True,
            },
        )

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        with pytest.raises(DemandPublishError, match="digest"):
            await client.publish_executable_protected_release(
                publication,
                idempotency_key=uuid4(),
            )
    finally:
        await http.aclose()

    assert canonical_executable_digest(publication.release) != "0" * 64


@pytest.mark.asyncio
async def test_publish_executable_protected_release_rejects_oversized_receipt() -> None:
    configuration = _configuration()
    publication = _executable_publication(configuration)

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * (16 * 1024 + 1))

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        with pytest.raises(DemandPublishError, match="byte bound"):
            await client.publish_executable_protected_release(
                publication,
                idempotency_key=uuid4(),
            )
    finally:
        await http.aclose()


@pytest.mark.asyncio
async def test_publish_executable_protected_release_rejects_non_200_response() -> None:
    configuration = _configuration()
    publication = _executable_publication(configuration)

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"detail": "conflict"})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token="reporter-secret",
        http_client=http,
    )
    try:
        with pytest.raises(DemandPublishError, match="status 409"):
            await client.publish_executable_protected_release(
                publication,
                idempotency_key=uuid4(),
            )
    finally:
        await http.aclose()


@pytest.mark.asyncio
async def test_publish_errors_are_bounded_and_never_echo_credentials() -> None:
    configuration = _configuration()
    snapshot = _snapshot(configuration)
    secret = "never-echo-this-reporter-secret"

    async def denied(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text=f"upstream accidentally echoed {secret}")

    denied_http = httpx.AsyncClient(transport=httpx.MockTransport(denied))
    denied_client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token=secret,
        http_client=denied_http,
    )
    try:
        with pytest.raises(DemandPublishError) as caught:
            await denied_client.publish(snapshot)
    finally:
        await denied_http.aclose()
    assert "503" in str(caught.value)
    assert secret not in str(caught.value)
    assert "accidentally" not in str(caught.value)

    async def wrong_receipt(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "snapshot_id": str(uuid4()),
                "digest": "b" * 64,
                "sequence": snapshot.sequence,
                "replayed": True,
            },
        )

    wrong_http = httpx.AsyncClient(transport=httpx.MockTransport(wrong_receipt))
    wrong_client = DemandReporterClient(
        configuration,
        manager_origin="https://capacity.internal",
        bearer_token=secret,
        http_client=wrong_http,
    )
    try:
        with pytest.raises(DemandPublishError, match="receipt"):
            await wrong_client.publish(snapshot)
    finally:
        await wrong_http.aclose()


def _owner_file(path: Path, payload: bytes) -> Path:
    path.write_bytes(payload)
    path.chmod(0o600)
    return path


def test_owner_only_file_reader_rejects_unsafe_paths_and_bounds(tmp_path: Path) -> None:
    safe = _owner_file(tmp_path / "safe", b"value\n")
    assert read_owner_only_bytes(safe, max_bytes=16) == b"value\n"

    broad = _owner_file(tmp_path / "broad", b"value")
    broad.chmod(0o640)
    with pytest.raises(ValueError, match="0600"):
        read_owner_only_bytes(broad)

    target = _owner_file(tmp_path / "target", b"value")
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="nonsymlink"):
        read_owner_only_bytes(link)

    oversized = _owner_file(tmp_path / "oversized", b"x" * 17)
    with pytest.raises(ValueError, match="maximum"):
        read_owner_only_bytes(oversized, max_bytes=16)


def test_tls_builder_uses_verified_open_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ca = _owner_file(tmp_path / "ca.pem", b"CA DATA")
    cert = _owner_file(tmp_path / "client.pem", b"CERT DATA")
    key = _owner_file(tmp_path / "client.key", b"KEY DATA")
    calls: dict[str, Any] = {}

    class FakeContext:
        minimum_version: Any = None
        check_hostname: bool = False
        verify_mode: Any = None

        def load_verify_locations(self, *, cadata: str) -> None:
            calls["ca"] = cadata

        def load_cert_chain(self, certfile: str, keyfile: str) -> None:
            calls["certfile"] = certfile
            calls["keyfile"] = keyfile
            assert Path(certfile).read_bytes() == b"CERT DATA"
            assert Path(keyfile).read_bytes() == b"KEY DATA"

    fake = FakeContext()
    monkeypatch.setattr("ssl.create_default_context", lambda: fake)
    result = build_reporter_tls_context(
        DemandReporterTLSFiles(ca_file=ca, certificate_file=cert, private_key_file=key)
    )
    assert result is fake
    assert calls["ca"] == "CA DATA"
    assert str(calls["certfile"]).startswith("/proc/self/fd/")
    assert str(calls["keyfile"]).startswith("/proc/self/fd/")


@pytest.mark.parametrize(
    "origin",
    (
        "http://capacity.internal",
        "https://user@capacity.internal",
        "https://capacity.internal/path",
        "https://capacity.internal?query=yes",
        "https://capacity.internal/#fragment",
    ),
)
def test_client_rejects_non_origin_or_non_https_manager_urls(origin: str) -> None:
    configuration = _configuration()
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: httpx.Response(200)))
    try:
        with pytest.raises(ValueError, match="manager origin"):
            DemandReporterClient(
                configuration,
                manager_origin=origin,
                bearer_token="reporter-secret",
                http_client=http,
            )
    finally:
        # This constructor-only test does not enter an event loop; closing the
        # unconnected MockTransport client is unnecessary.
        pass
