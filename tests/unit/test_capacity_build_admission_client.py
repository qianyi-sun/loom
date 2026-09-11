"""Build admission client stays pool-bound and replays exact request evidence."""

import json
from importlib import import_module

import httpx
import pytest

from loom_capacity_agent.admission import (
    ExecutablePreparedBootstrapRevocationV2,
    PreparedExecutableAdmissionV2,
    ProtectedIntentObservationV2,
    RevokedExecutableBootstrapV2,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapRegistrationV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from tests.unit.test_capacity_executor_typed_launch_renderer import typed_context


def registration(pool="gb10"):
    return ExecutableBootstrapRegistrationV2(binding=typed_context(pool=pool).binding,
        command_sequence=2,bootstrap_registration_epoch=1,bootstrap_evidence_sha256="a"*64)


def client_for(http, request, *, origin="https://management.test"):
    module = import_module("loom_capacity_executor.build_admission_client")
    binding = request.binding
    identity = module.BuildAdmissionExecutorV1(pool_id=binding.pool_id,pool_generation=binding.pool_generation,
        executor_id=binding.executor_id,executor_incarnation=binding.executor_incarnation)
    return module.BuildAdmissionClient(identity,origin=origin,bearer_token="admission-only-secret",http_client=http)


def receipt(request):
    digest = canonical_executable_digest(request)
    return PreparedExecutableAdmissionV2(subject_id=request.binding.subject_id,
        subject_incarnation=request.binding.subject_incarnation,intent_id=request.binding.intent_id,
        bootstrap_registration_epoch=1,bootstrap_sha256="b"*64,request_digest=digest,
        admission_digest=digest,protected_high_water=1)


def native_registration(pool="gb10"):
    from hashlib import sha256
    from uuid import uuid4

    from loom_capacity_agent.admission import ExecutableWorkerRegistrationV2

    return ExecutableWorkerRegistrationV2(operation_id=uuid4(), binding=registration(pool).binding,
        bootstrap_registration_epoch=1, protected_registration_epoch=2, slurm_job_id="1234",
        worker_id=uuid4(), worker_incarnation=uuid4(), worker_credential_sha256=sha256(b"w" * 43).hexdigest())


@pytest.mark.parametrize("boundary", ["exact", "digest", "offset", "truncated", "base64", "oversize", "redirect"])
async def test_source_client_checks_bounded_exact_reply(boundary):
    import base64
    from uuid import uuid4

    from loom_capacity_agent.build_admission import BuildClaimRequestV1, BuildSourceReadReceiptV1
    from loom_capacity_executor.build_admission_client import BuildAdmissionTransportError
    from loom_capacity_manager.contracts import canonical_digest

    worker = native_registration()
    claim = BuildClaimRequestV1(binding=worker.binding, operation_id=uuid4(), request_id=uuid4(),
        worker_id=worker.worker_id, worker_incarnation=worker.worker_incarnation)
    result = BuildSourceReadReceiptV1(claim_digest=canonical_digest(claim), source_binding_sha256="a" * 64,
        archive_sha256="b" * 64, archive_size_bytes=100, offset=0,
        data_base64=base64.b64encode(b"source").decode("ascii"))

    async def handle(outgoing):
        assert outgoing.url.path.endswith("/source")
        body = json.loads(outgoing.content)
        assert body["claim"] == claim.model_dump(mode="json")
        assert body["offset"] == 0 and body["length"] == 6
        assert body["worker_credential"] == "x" * 43
        payload = result.model_dump(mode="json")
        if boundary == "digest":
            payload["claim_digest"] = "f" * 64
        elif boundary == "offset":
            payload["offset"] = 1
        elif boundary == "truncated":
            payload["data_base64"] = base64.b64encode(b"short").decode("ascii")
        elif boundary == "base64":
            payload["data_base64"] = "!bad"
        elif boundary == "oversize":
            return httpx.Response(200, content=b"x" * 1400001)
        elif boundary == "redirect":
            return httpx.Response(307, headers={"Location": "https://foreign.test/source"})
        return httpx.Response(200, content=json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = client_for(http, claim)
        if boundary == "exact":
            assert (await client.read_source(claim, worker_credential="x" * 43, offset=0, length=6)).data == b"source"
        else:
            with pytest.raises(BuildAdmissionTransportError):
                await client.read_source(claim, worker_credential="x" * 43, offset=0, length=6)


@pytest.mark.parametrize("pool", ["gb10", "oldlab"])
@pytest.mark.parametrize("boundary", ["exact", "subject_id", "subject_incarnation", "intent_id",
    "worker_id", "worker_incarnation", "predecessor_worker_incarnation",
    "protected_registration_epoch", "request_digest", "registration_digest"])
async def test_client_validates_native_registration_receipt(pool, boundary):
    from uuid import uuid4

    from loom_capacity_agent.admission import RegisteredExecutableWorkerV2

    request = native_registration(pool)
    digest = canonical_executable_digest(request)
    expected = RegisteredExecutableWorkerV2(subject_id=request.binding.subject_id,
        subject_incarnation=request.binding.subject_incarnation, intent_id=request.binding.intent_id,
        worker_id=request.worker_id, worker_incarnation=request.worker_incarnation,
        protected_registration_epoch=2, request_digest=digest, registration_digest=digest,
        protected_high_water=3)

    async def handle(outgoing):
        assert outgoing.url.path.endswith("/register")
        assert outgoing.headers["Authorization"] == "Bearer admission-only-secret"
        assert json.loads(outgoing.content) == {"schema_version": 1,
            "registration": request.model_dump(mode="json"), "bootstrap_capability": "b" * 43}
        changed = expected
        if boundary != "exact":
            value = 3 if boundary.endswith("epoch") else "f" * 64 if boundary.endswith("digest") else uuid4()
            changed = expected.model_copy(update={boundary: value})
        return httpx.Response(200, content=canonical_executable_bytes(changed))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = client_for(http, request)
        if boundary == "exact":
            assert await client.register_worker(request, bootstrap_capability="b" * 43) == expected
        else:
            with pytest.raises(RuntimeError, match="binding"):
                await client.register_worker(request, bootstrap_capability="b" * 43)


@pytest.mark.parametrize("capability", ["short", "b" * 513, " " * 43, "é" * 43])
async def test_client_rejects_invalid_registration_secret_before_transport(capability):
    async def unexpected(outgoing):
        pytest.fail("invalid secret must not reach transport")

    async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected)) as http:
        request = native_registration()
        with pytest.raises(ValueError):
            await client_for(http, request).register_worker(request, bootstrap_capability=capability)


@pytest.mark.parametrize("pool", ["gb10", "oldlab"])
@pytest.mark.parametrize("boundary", ["exact", "request", "digest", "noncanonical", "rejected"])
async def test_client_authenticates_exact_native_platform_claim(pool, boundary):
    from uuid import uuid4

    from loom_capacity_agent.build_admission import BuildClaimReceiptV1, BuildClaimRequestV1
    from loom_capacity_manager.contracts import canonical_bytes, canonical_digest

    worker = native_registration(pool)
    request = BuildClaimRequestV1(binding=worker.binding, operation_id=uuid4(), request_id=uuid4(),
        worker_id=worker.worker_id, worker_incarnation=worker.worker_incarnation)
    expected = BuildClaimReceiptV1(request=request, request_digest=canonical_digest(request))

    async def handle(outgoing):
        assert outgoing.url.path.endswith("/claim")
        assert json.loads(outgoing.content) == {"schema_version": 1,
            "claim": request.model_dump(mode="json"), "worker_credential": "w" * 43}
        response = expected
        if boundary == "request":
            response = response.model_copy(update={"request": request.model_copy(update={"request_id": uuid4()})})
        elif boundary == "digest":
            response = response.model_copy(update={"request_digest": "f" * 64})
        if boundary == "rejected":
            return httpx.Response(409, content=b"private credential and database diagnostics")
        return httpx.Response(200, content=canonical_bytes(response) + (b" " if boundary == "noncanonical" else b""))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = client_for(http, worker)
        if boundary == "exact":
            assert await client.claim_platform(request, worker_credential="w" * 43) == expected
        else:
            with pytest.raises(RuntimeError) as failure:
                await client.claim_platform(request, worker_credential="w" * 43)
            assert "private credential" not in str(failure.value)


@pytest.mark.parametrize("boundary", ["exact", "worker", "count", "digest"])
@pytest.mark.parametrize("live_count", [0, 1])
async def test_client_validates_registered_native_drain(boundary, live_count):
    from uuid import uuid4

    from loom_capacity_agent.admission import DrainedExecutableWorkerV2, ExecutableDrainRequestV2

    worker = native_registration()
    request = ExecutableDrainRequestV2(binding=worker.binding, operation_id=uuid4(),
        worker_id=worker.worker_id, worker_incarnation=worker.worker_incarnation,
        expected_claim_high_water=1, drain_epoch=3)
    digest = canonical_executable_digest(request)
    expected = DrainedExecutableWorkerV2(subject_id=worker.binding.subject_id,
        subject_incarnation=worker.binding.subject_incarnation, intent_id=worker.binding.intent_id,
        worker_id=worker.worker_id, worker_incarnation=worker.worker_incarnation,
        claim_high_water=1, live_claim_count=live_count, drain_epoch=3, request_digest=digest,
        drain_digest=digest, protected_high_water=4)

    async def handle(outgoing):
        assert outgoing.url.path.endswith("/drain")
        assert outgoing.content == canonical_executable_bytes(request)
        result = expected
        if boundary == "worker":
            result = result.model_copy(update={"worker_incarnation": uuid4()})
        elif boundary == "count":
            result = result.model_copy(update={"live_claim_count": 2})
        elif boundary == "digest":
            result = result.model_copy(update={"drain_digest": "f" * 64})
        return httpx.Response(200, content=canonical_executable_bytes(result))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = client_for(http, worker)
        if boundary == "exact":
            assert await client.begin_drain(request) == expected
        else:
            with pytest.raises(RuntimeError, match="binding"):
                await client.begin_drain(request)


@pytest.mark.parametrize("boundary", ["exact", "subject_id", "subject_incarnation", "intent_id",
    "slurm_job_id", "ownership_evidence_sha256", "bootstrap_registration_epoch",
    "protected_registration_epoch", "request_digest", "withdrawal_digest"])
async def test_client_validates_bound_bootstrap_withdrawal(boundary):
    from uuid import uuid4

    from loom_capacity_agent.admission import (
        ExecutableWorkerWithdrawalRequestV2,
        WithdrawnExecutableWorkerV2,
    )

    request = ExecutableWorkerWithdrawalRequestV2(operation_id=uuid4(), binding=registration().binding,
        bootstrap_registration_epoch=1, protected_registration_epoch=2,
        slurm_job_id="1234", ownership_evidence_sha256="b"*64)
    digest = canonical_executable_digest(request)
    expected = WithdrawnExecutableWorkerV2(subject_id=request.binding.subject_id,
        subject_incarnation=request.binding.subject_incarnation, intent_id=request.binding.intent_id,
        bootstrap_registration_epoch=1, protected_registration_epoch=2,
        slurm_job_id="1234", ownership_evidence_sha256="b"*64,
        request_digest=digest, withdrawal_digest=digest, protected_high_water=3)

    async def handle(outgoing):
        assert outgoing.url.path.endswith("/withdraw")
        assert outgoing.content == canonical_executable_bytes(request)
        changed = expected
        if boundary != "exact":
            value = (uuid4() if boundary in {"subject_id", "subject_incarnation", "intent_id"}
                else 4 if boundary.endswith("epoch") else "9999" if boundary == "slurm_job_id" else "f"*64)
            changed = expected.model_copy(update={boundary:value})
        return httpx.Response(200, content=canonical_executable_bytes(changed))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = client_for(http, request)
        if boundary == "exact":
            assert await client.withdraw_unregistered_worker(request) == expected
        else:
            with pytest.raises(RuntimeError, match="binding"):
                await client.withdraw_unregistered_worker(request)


@pytest.mark.parametrize("boundary", ["exact", "binding", "noncanonical"])
async def test_client_validates_exact_protected_observation(boundary):
    request = registration()
    expected = ProtectedIntentObservationV2(binding=request.binding, bootstrap_registration_epoch=1)

    async def handle(outgoing):
        assert outgoing.url.path.endswith("/observe")
        assert outgoing.content == canonical_executable_bytes(request.binding)
        result = expected
        if boundary == "binding":
            result = result.model_copy(update={"binding":request.binding.model_copy(update={"account_id":"foreign"})})
        wire = canonical_executable_bytes(result)
        return httpx.Response(200, content=wire + (b" " if boundary == "noncanonical" else b""))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = client_for(http, request)
        if boundary == "exact":
            assert await client.observe_intent(request.binding) == expected
        else:
            with pytest.raises(RuntimeError, match=r"binding|invalid"):
                await client.observe_intent(request.binding)


@pytest.mark.parametrize("boundary", ["exact", "binding", "digest", "epoch"])
async def test_client_validates_unbound_bootstrap_revocation(boundary):
    from uuid import uuid4

    registration_request = registration()
    request = ExecutablePreparedBootstrapRevocationV2(operation_id=uuid4(), binding=registration_request.binding,
        bootstrap_registration_epoch=1, protected_registration_epoch=2)
    digest = canonical_executable_digest(request)
    receipt = RevokedExecutableBootstrapV2(binding=request.binding, reporter_incarnation=uuid4(),
        bootstrap_registration_epoch=1, protected_registration_epoch=2, request_digest=digest,
        protected_release_sha256=digest, protected_high_water=3)

    async def handle(outgoing):
        assert outgoing.url.path.endswith("/revoke-bootstrap")
        assert outgoing.content == canonical_executable_bytes(request)
        result = receipt
        if boundary == "binding":
            result = result.model_copy(update={"binding":request.binding.model_copy(update={"account_id":"foreign"})})
        elif boundary == "digest":
            result = result.model_copy(update={"protected_release_sha256":"c"*64})
        elif boundary == "epoch":
            result = result.model_copy(update={"protected_registration_epoch":3})
        return httpx.Response(200, content=canonical_executable_bytes(result))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = client_for(http, registration_request)
        if boundary == "exact":
            assert await client.revoke_prepared_bootstrap(request) == receipt
        else:
            with pytest.raises(RuntimeError, match="binding"):
                await client.revoke_prepared_bootstrap(request)


@pytest.mark.parametrize("pool", ["gb10", "oldlab"])
async def test_client_replays_exact_preparation_after_lost_reply(pool):
    request = registration(pool)
    delivered = []

    async def handle(outgoing):
        delivered.append(outgoing.content)
        assert outgoing.headers["Authorization"] == "Bearer admission-only-secret"
        assert outgoing.url.path == f"/api/v1/internal/capacity-build/pools/{pool}/intents/{request.binding.intent_id}/prepare"
        assert json.loads(outgoing.content)["registration"] == request.model_dump(mode="json")
        if len(delivered)==1:
            raise httpx.ReadTimeout("lost",request=outgoing)
        return httpx.Response(200,content=canonical_executable_bytes(receipt(request)))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = client_for(http,request)
        with pytest.raises(RuntimeError,match="transport"):
            await client.prepare_worker(request,bootstrap_sha256="b"*64)
        assert await client.prepare_worker(request,bootstrap_sha256="b"*64) == receipt(request)
    assert delivered[0] == delivered[1]


@pytest.mark.parametrize("boundary", ["wrong-digest", "wrong-hash", "oversized", "redirect", "reject", "pool"])
async def test_client_rejects_changed_evidence_and_transport(boundary):
    request = registration()
    outgoing_requests = []

    async def handle(outgoing):
        outgoing_requests.append(outgoing)
        result = receipt(request)
        if boundary=="wrong-digest":
            result = result.model_copy(update={"admission_digest":"c"*64})
        elif boundary=="wrong-hash":
            result = result.model_copy(update={"bootstrap_sha256":"c"*64})
        if boundary=="oversized":
            return httpx.Response(200,content=b"x"*(64*1024+1))
        if boundary=="redirect":
            return httpx.Response(307,headers={"Location":"https://foreign.test/stolen"})
        if boundary=="reject":
            return httpx.Response(409,content=b"private database diagnostic")
        return httpx.Response(200,content=canonical_executable_bytes(result))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle),follow_redirects=True) as http:
        client = client_for(http,request)
        if boundary=="pool":
            request = request.model_copy(update={"binding":request.binding.model_copy(update={"pool_id":"oldlab"})})
        with pytest.raises((RuntimeError,ValueError)) as failure:
            await client.prepare_worker(request,bootstrap_sha256="b"*64)
        assert "private database diagnostic" not in str(failure.value)
    assert len(outgoing_requests) == (0 if boundary=="pool" else 1)


@pytest.mark.parametrize("origin", ["http://management.test", "https://user:password@management.test", "https://management.test/path"])
async def test_client_requires_exact_https_origin(origin):
    async with httpx.AsyncClient() as http:
        with pytest.raises(ValueError):
            client_for(http,registration(),origin=origin)


@pytest.mark.parametrize("boundary", ["missing-token", "unsafe-token", "invalid-token", "missing-ca", "unsafe-ca", "timeout"])
def test_file_configuration_fails_before_opening_http_client(tmp_path,monkeypatch,boundary):
    from loom_capacity_agent.client import DemandReporterConnection, DemandReporterTLSFiles
    from tests.unit.test_capacity_agent_client import _owner_file

    module = import_module("loom_capacity_executor.build_admission_client")
    request = registration()
    identity = module.BuildAdmissionExecutorV1(pool_id=request.binding.pool_id,pool_generation=request.binding.pool_generation,
        executor_id=request.binding.executor_id,executor_incarnation=request.binding.executor_incarnation)
    token = tmp_path/"token"
    if boundary!="missing-token":
        _owner_file(token,b"invalid internal space" if boundary=="invalid-token" else b"scoped-token")
        if boundary=="unsafe-token":
            token.chmod(0o644)
    ca = tmp_path/"ca.pem"
    if boundary!="missing-ca":
        _owner_file(ca,b"invalid PEM")
        if boundary=="unsafe-ca":
            ca.chmod(0o644)
    connection = DemandReporterConnection(manager_origin="https://management.test",bearer_token_file=token,
        tls_files=DemandReporterTLSFiles(ca_file=ca,certificate_file=tmp_path/"client.pem",private_key_file=tmp_path/"key.pem"),
        timeout_seconds=0.001 if boundary=="timeout" else 5.0)

    def unexpected_http(*args,**kwargs):
        pytest.fail("invalid credentials/configuration must be rejected before creating a client")

    monkeypatch.setattr(module.httpx,"AsyncClient",unexpected_http)
    with pytest.raises((ValueError,OSError)):
        module.BuildAdmissionClient.from_files(identity,connection)
