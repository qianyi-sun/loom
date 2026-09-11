"""Authenticated pool admission commits before a reply can leave management."""

from importlib import import_module
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import text

from loom_capacity_manager.auth import CapacityPrincipalVerifier
from loom_capacity_manager.executable_contracts import (
    canonical_executable_bytes,
    canonical_executable_digest,
)
from tests.integration.test_personal_dev_build_guard_execution import admitted, physical
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions
from tests.unit.test_capacity_auth import _pool_executor, _write_registry


def application(prepared_input, tmp_path):
    factory, _engine, _installation, plan, _source, _request = prepared_input
    binding = plan.shapes[0].binding
    document = _pool_executor()
    document.update(pool_id=binding.pool_id,executor_id=binding.executor_id,
        executor_incarnation=str(binding.executor_incarnation),executor_pool_generation=binding.pool_generation)
    verifier = CapacityPrincipalVerifier.from_pool_executor_file(
        _write_registry(tmp_path / "admission-principals.json", [document]))
    app = FastAPI()
    app.state.personal_dev_build_admission_sessions = factory
    app.state.personal_dev_build_admission_verifier = verifier
    app.include_router(import_module("loom_service.routes.personal_dev_build_admission").router,
        prefix="/api/v1/internal")
    return app


def route(registration, operation):
    return f"/api/v1/internal/capacity-build/pools/{registration.binding.pool_id}/intents/{registration.binding.intent_id}/{operation}"


def preparation(registration, digest):
    return {"schema_version":1,"registration":registration.model_dump(mode="json"),"bootstrap_sha256":digest}


@pytest.mark.parametrize("boundary", ["exact", "disabled", "secret", "credential", "pool", "http"])
async def test_http_native_registration_commits_and_replays(prepared_input, tmp_path, monkeypatch, boundary):
    from tests.integration.test_personal_dev_build_guard_registration import (
        BOOTSTRAP,
        registration_input,
    )

    _factory, engine, *_ = prepared_input
    request, _physical = await registration_input(prepared_input, monkeypatch)
    app = application(prepared_input, tmp_path)
    app.state.personal_dev_build_admission_mode = "prepare-bind-only" if boundary == "disabled" else "native-registration"
    base = "http://management.test" if boundary == "http" else "https://management.test"
    token = "wrong" if boundary == "credential" else "executor-secret"
    url = route(request, "register")
    if boundary == "pool":
        url = url.replace(f"/pools/{request.binding.pool_id}/", "/pools/foreign/")
    envelope = {"schema_version": 1, "registration": request.model_dump(mode="json"),
        "bootstrap_capability": "x" * 43 if boundary == "secret" else BOOTSTRAP}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=base,
        headers={"Authorization": f"Bearer {token}"}) as client:
        reply = await client.post(url, json=envelope)
        if boundary == "exact":
            assert reply.status_code == 200, reply.text
            assert reply.json()["worker_id"] == str(request.worker_id)
            replay = await client.post(url, json=envelope)
            assert replay.content == reply.content
        else:
            assert reply.status_code in {401, 403, 409, 503}, reply.text
        assert BOOTSTRAP not in reply.text
        assert "executor-secret" not in reply.text
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_registrations")) == (1 if boundary == "exact" else 0)
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


@pytest.mark.parametrize("boundary", ["mode", "credential", "body", "worker", "duplicate-authorization"])
async def test_http_claim_failures_retain_no_claim(prepared_input, tmp_path, monkeypatch, boundary):
    from tests.integration.test_personal_dev_build_guard_claims import claim_input
    from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL

    request = await claim_input(prepared_input, monkeypatch)
    _factory, engine, *_ = prepared_input
    app = application(prepared_input, tmp_path)
    app.state.personal_dev_build_admission_mode = "native-registration" if boundary == "mode" else "native-claims"
    if boundary == "worker":
        request = request.model_copy(update={"worker_incarnation": uuid4()})
    envelope = {"schema_version": 1, "claim": request.model_dump(mode="json"),
        "worker_credential": "x" * 43 if boundary == "credential" else CREDENTIAL}
    headers = [("Authorization", "Bearer executor-secret")]
    if boundary == "duplicate-authorization":
        headers += headers
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.test",
        headers=headers) as client:
        reply = await client.post(route(request, "claim"),
            **({"content": b"{"} if boundary == "body" else {"json": envelope}))
        assert reply.status_code in {400, 401, 409, 503}, reply.text
        assert CREDENTIAL not in reply.text and "executor-secret" not in reply.text
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_claims")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


async def test_http_prepare_bind_and_lost_reply_replay_are_committed(prepared_input, tmp_path):
    _factory, engine, _installation, _plan, _source, _request = prepared_input
    registration, digest = await admitted(prepared_input)
    app = application(prepared_input, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="https://management.test",
        headers={"Authorization":"Bearer executor-secret"}) as client:
        first = await client.post(route(registration,"prepare"),json=preparation(registration,digest))
        assert first.status_code == 200, first.text
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == 1
        assert "executor-secret" not in first.text
        assert "postgresql" not in first.text
        replay = await client.post(route(registration,"prepare"),json=preparation(registration,digest))
        assert replay.content == first.content
        binding = physical(registration)
        bound = await client.post(route(registration,"bind"),content=canonical_executable_bytes(binding))
        assert bound.status_code == 200, bound.text
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == 2
        replay = await client.post(route(registration,"bind"),content=canonical_executable_bytes(binding))
        assert replay.content == bound.content


async def test_http_revocation_before_preparation_is_committed_and_replayable(prepared_input, tmp_path):
    from loom_capacity_agent.admission import ExecutablePreparedBootstrapRevocationV2
    from loom_capacity_executor.build_admission_client import (
        BuildAdmissionClient,
        BuildAdmissionExecutorV1,
    )

    _factory, engine, _installation, _plan, _source, platform_request = prepared_input
    registration, _digest = await admitted(prepared_input)
    with engine.begin() as connection:
        connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id":platform_request.id})
    app = application(prepared_input, tmp_path)
    # Use the production client and real private SQL. TLS transport itself is
    # separately covered by the loopback mTLS test in this module.
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        binding = registration.binding
        identity = BuildAdmissionExecutorV1(pool_id=binding.pool_id,pool_generation=binding.pool_generation,
            executor_id=binding.executor_id,executor_incarnation=binding.executor_incarnation)
        client = BuildAdmissionClient(identity, origin="https://management.test",
            bearer_token="executor-secret", http_client=http)
        request = ExecutablePreparedBootstrapRevocationV2(operation_id=uuid4(), binding=registration.binding,
            bootstrap_registration_epoch=1, protected_registration_epoch=2)
        receipt = await client.revoke_prepared_bootstrap(request)
        assert await client.revoke_prepared_bootstrap(request) == receipt
        assert (await client.observe_intent(request.binding)).prepared_revocation == receipt
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.bootstrap_revocations")) == 1
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == 0
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


@pytest.mark.parametrize("operation", ["prepare", "observe", "revoke-bootstrap", "withdraw"])
@pytest.mark.parametrize("boundary", ["credential", "path-pool", "path-intent", "pool-generation", "executor", "incarnation", "subject", "body", "oversized", "http"])
async def test_http_rejects_untrusted_admission_without_writes(prepared_input, tmp_path, boundary, operation):
    _factory, engine, _installation, _plan, _source, _request = prepared_input
    registration, digest = await admitted(prepared_input)
    app = application(prepared_input,tmp_path)
    url = route(registration,operation)
    token = "wrong" if boundary=="credential" else "executor-secret"
    if boundary=="path-pool":
        url = url.replace(f"/pools/{registration.binding.pool_id}/", "/pools/foreign/")
    elif boundary=="path-intent":
        url = url.replace(str(registration.binding.intent_id),str(uuid4()))
    elif boundary in {"pool-generation", "executor", "incarnation", "subject"}:
        changes = {"pool-generation":{"pool_generation":999},"executor":{"executor_id":"foreign"},
            "incarnation":{"executor_incarnation":uuid4()},"subject":{"subject_id":uuid4()}}[boundary]
        registration = registration.model_copy(update={"binding":registration.binding.model_copy(update=changes)})
    base = "http://management.test" if boundary=="http" else "https://management.test"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url=base,
        headers={"Authorization":f"Bearer {token}"}) as client:
        if boundary in {"body", "oversized"}:
            reply = await client.post(url,content=b"{" if boundary=="body" else b"x"*(1024*1024+1))
        elif operation == "observe":
            reply = await client.post(url,content=canonical_executable_bytes(registration.binding))
        elif operation == "withdraw":
            from tests.integration.test_personal_dev_build_guard_withdrawal import withdrawal

            reply = await client.post(url,content=canonical_executable_bytes(withdrawal(physical(registration))))
        elif operation == "revoke-bootstrap":
            from loom_capacity_agent.admission import ExecutablePreparedBootstrapRevocationV2

            revoke = ExecutablePreparedBootstrapRevocationV2(operation_id=uuid4(), binding=registration.binding,
                bootstrap_registration_epoch=1,protected_registration_epoch=2)
            reply = await client.post(url,content=canonical_executable_bytes(revoke))
        else:
            reply = await client.post(url,json=preparation(registration,digest))
    assert reply.status_code in {400,401,403,409,413}, reply.text
    assert "postgresql" not in reply.text
    assert "executor-secret" not in reply.text
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.bootstrap_revocations")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_withdrawals")) == 0


async def test_http_defaults_closed_without_private_configuration(tmp_path):
    app = FastAPI()
    app.include_router(import_module("loom_service.routes.personal_dev_build_admission").router,prefix="/api/v1/internal")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="https://management.test") as client:
        result = await client.post(f"/api/v1/internal/capacity-build/pools/gb10/intents/{uuid4()}/prepare",content=b"{}")
    assert result.status_code == 503


async def test_real_service_mounts_admission_closed_by_default(monkeypatch):
    from loom_service.app import create_app
    from loom_service.config import LoomServiceSettings
    from tests.unit.test_service_root_landing import _base_env

    for name,value in _base_env().items():
        monkeypatch.setenv(name,value)
    app = create_app(LoomServiceSettings(_env_file=None))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="https://management.test") as client:
        result = await client.post(f"/api/v1/internal/capacity-build/pools/gb10/intents/{uuid4()}/prepare",content=b"{}")
    assert result.status_code == 503


@pytest.mark.parametrize("operation", ["prepare", "register", "claim", "drain", "outcome", "release"])
async def test_http_commit_failure_cannot_emit_preparation_receipt(prepared_input,tmp_path,monkeypatch,operation):
    from sqlalchemy import event

    factory, engine, _installation, _plan, _source, _request = prepared_input
    if operation == "release":
        from tests.integration.test_personal_dev_build_guard_registered_release import (
            registered_release_input,
        )
        from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL

        registration, _claim, _terminal, _drain = await registered_release_input(prepared_input, monkeypatch)
        payload = {"schema_version": 1, "release": registration.model_dump(mode="json"), "worker_credential": CREDENTIAL}
    elif operation == "outcome":
        from tests.integration.test_personal_dev_build_guard_drain import drain_input
        from tests.integration.test_personal_dev_build_guard_outcomes import outcome_request
        from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL

        _drain, registration = await drain_input(prepared_input, monkeypatch, claimed=True)
        payload = {"schema_version": 1, "outcome": outcome_request(registration).model_dump(mode="json"),
            "worker_credential": CREDENTIAL}
    elif operation == "drain":
        from tests.integration.test_personal_dev_build_guard_drain import drain_input

        registration, _claim = await drain_input(prepared_input, monkeypatch, claimed=True)
        payload = registration.model_dump(mode="json")
    elif operation == "claim":
        from tests.integration.test_personal_dev_build_guard_claims import claim_input
        from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL

        registration = await claim_input(prepared_input, monkeypatch)
        payload = {"schema_version": 1, "claim": registration.model_dump(mode="json"),
            "worker_credential": CREDENTIAL}
    elif operation == "register":
        from tests.integration.test_personal_dev_build_guard_registration import (
            BOOTSTRAP,
            registration_input,
        )

        registration, _physical = await registration_input(prepared_input, monkeypatch)
        payload = {"schema_version": 1, "registration": registration.model_dump(mode="json"),
            "bootstrap_capability": BOOTSTRAP}
    else:
        registration,digest = await admitted(prepared_input)
        payload = preparation(registration, digest)
    app = application(prepared_input,tmp_path)
    app.state.personal_dev_build_admission_mode = "native-claims"
    reached_outer_commit = []
    store_completed = []
    store_type = import_module("loom_capacity_build_guard.execution_store").BuildGuardExecutionStore
    method = {"prepare": "prepare_worker", "register": "register_worker", "claim": "claim_platform", "drain": "begin_drain", "outcome": "record_outcome", "release": "acknowledge_release"}[operation]
    original = getattr(store_type, method)

    async def prepare_then_observe(*args,**kwargs):
        receipt = await original(*args,**kwargs)
        store_completed.append(True)
        return receipt

    monkeypatch.setattr(store_type,method,prepare_then_observe)

    def fail_commit(session):
        if session.in_nested_transaction():
            return
        assert store_completed == [True]
        reached_outer_commit.append(True)
        session.execute(text("SELECT 1/0"))

    target = factory.class_.sync_session_class
    event.listen(target,"before_commit",fail_commit)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="https://management.test",
            headers={"Authorization":"Bearer executor-secret"}) as client:
            result = await client.post(route(registration,operation),json=payload)
        assert result.status_code == 409
        assert reached_outer_commit == [True]
        assert "admission_digest" not in result.text
        assert "registration_digest" not in result.text
        assert "drain_digest" not in result.text
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == (0 if operation == "prepare" else 2)
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_registrations")) == (1 if operation in {"claim", "drain", "outcome"} else 0)
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_claims")) == (1 if operation in {"drain", "outcome"} else 0)
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_drains")) == 0
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_outcomes")) == 0
    finally:
        event.remove(target,"before_commit",fail_commit)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="https://management.test",
        headers={"Authorization":"Bearer executor-secret"}) as client:
        assert (await client.post(route(registration,operation),json=payload)).status_code == 200


@pytest.mark.parametrize("mode", ["prepare-bind-only", "native-registration", "native-claims"])
async def test_outcome_route_requires_claims_mode_and_exact_worker_credential(prepared_input, tmp_path, monkeypatch, mode):
    from tests.integration.test_personal_dev_build_guard_drain import drain_input
    from tests.integration.test_personal_dev_build_guard_outcomes import outcome_request

    _factory, engine, _installation, *_ = prepared_input
    _drain, claim = await drain_input(prepared_input, monkeypatch, claimed=True)
    app = application(prepared_input, tmp_path)
    app.state.personal_dev_build_admission_mode = mode
    payload = {"schema_version": 1, "outcome": outcome_request(claim).model_dump(mode="json"), "worker_credential": "x" * 43}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.test",
        headers={"Authorization": "Bearer executor-secret"}) as client:
        reply = await client.post(route(claim, "outcome"), json=payload)
        assert reply.status_code == (409 if mode == "native-claims" else 503)
        assert "x" * 43 not in reply.text
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_outcomes")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1


@pytest.mark.parametrize("pinned", [False,True,"typed"])
@pytest.mark.parametrize("register", [False, True, "claim", "outcome"])
async def test_real_mtls_client_reaches_guard_and_rejects_untrusted_peer(prepared_input,tmp_path,pinned,register,monkeypatch):
    import asyncio
    import socket
    import ssl
    from hashlib import sha256

    import uvicorn
    from cryptography.hazmat.primitives import serialization

    from loom_capacity_agent.client import DemandReporterConnection, DemandReporterTLSFiles
    from loom_capacity_executor.build_admission_client import (
        BuildAdmissionClient,
        BuildAdmissionExecutorV1,
    )
    from tests.integration import test_personal_dev_build_guard_execution as execution
    from tests.integration.test_capacity_manager_mtls import (
        _new_ca,
        _private_key_bytes,
        _signed_certificate,
        _write,
    )
    from tests.integration.test_personal_dev_build_guard_registration import BOOTSTRAP

    original = execution.bootstrap
    monkeypatch.setattr(execution, "bootstrap", lambda proposal: original(proposal).model_copy(
        update={"bootstrap_sha256": sha256(BOOTSTRAP.encode("ascii")).hexdigest()}))
    _factory,engine,_installation,_plan,_source,_request = prepared_input
    registration,digest = await admitted(prepared_input)
    app = application(prepared_input,tmp_path)
    app.state.personal_dev_build_admission_mode = ("native-claims" if register in {"claim", "outcome"}
        else "native-registration" if register else "prepare-bind-only")
    ca_key,ca = _new_ca("build-admission-ca")
    server_key,server_cert = _signed_certificate("localhost",ca_key,ca,server=True)
    client_key,client_cert = _signed_certificate("build-executor",ca_key,ca,server=False)
    pem = serialization.Encoding.PEM
    ca_path = _write(tmp_path/"ca.pem",ca.public_bytes(pem))
    server_cert_path = _write(tmp_path/"server.pem",server_cert.public_bytes(pem))
    server_key_path = _write(tmp_path/"server-key.pem",_private_key_bytes(server_key))
    tls_files = DemandReporterTLSFiles(ca_file=ca_path,
        certificate_file=_write(tmp_path/"client.pem",client_cert.public_bytes(pem)),
        private_key_file=_write(tmp_path/"client-key.pem",_private_key_bytes(client_key)))
    token_path = _write(tmp_path/"executor-token",b"executor-secret")
    listener = socket.socket()
    listener.bind(("127.0.0.1",0))
    origin = f"https://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app,log_level="error",lifespan="off",
        ssl_keyfile=str(server_key_path),ssl_certfile=str(server_cert_path),ssl_ca_certs=str(ca_path),
        ssl_cert_reqs=ssl.CERT_REQUIRED))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    client = None
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if task.done():
                    await task
                    raise AssertionError("test TLS server failed to start")
                await asyncio.sleep(0.01)
        binding = registration.binding
        identity = BuildAdmissionExecutorV1(pool_id=binding.pool_id,pool_generation=binding.pool_generation,
            executor_id=binding.executor_id,executor_incarnation=binding.executor_incarnation)
        if pinned:
            from hashlib import sha256

            from loom_capacity_executor.pinned_admission_transport import (
                PinnedBuildAdmissionConnectionV1,
            )

            paths = {"bearer_token":token_path,"ca":ca_path,
                "certificate":tls_files.certificate_file,"private_key":tls_files.private_key_file}
            config = PinnedBuildAdmissionConnectionV1.model_validate({"origin":origin,
                **{name:{"path":str(path),"sha256":sha256(path.read_bytes()).hexdigest()} for name,path in paths.items()}})
            if pinned == "typed":
                from loom_capacity_executor.typed_admission import (
                    TypedAdmissionDirectoryV3,
                    TypedAdmissionEntryV3,
                    TypedAdmissionRouter,
                )

                entry = TypedAdmissionEntryV3(subject_id=binding.subject_id,subject_incarnation=binding.subject_incarnation,
                    configuration_epoch=binding.execution.configuration_epoch,deployment_generation=binding.deployment_generation,
                    candidate_generation=binding.candidate_generation,candidate_sha256=canonical_executable_digest(binding.candidate),
                    account_id=binding.account_id,purpose="personal-build-worker",protected_admission_sha256="a"*64,build=config)
                directory = TypedAdmissionDirectoryV3(executor=identity,entries=(entry,))
                wire = canonical_executable_bytes(directory)
                path = _write(tmp_path/"typed-admission.json",wire)
                client = TypedAdmissionRouter(path,expected_sha256=sha256(wire).hexdigest(),executor=identity)
            else:
                client = BuildAdmissionClient.from_pinned_files(identity,config)
        else:
            client = BuildAdmissionClient.from_files(identity,DemandReporterConnection(manager_origin=origin,
                bearer_token_file=token_path,tls_files=tls_files,timeout_seconds=5.0))
        prepared = await client.prepare_worker(registration,bootstrap_sha256=digest)
        assert prepared.intent_id == binding.intent_id
        observation = await client.observe_intent(binding)
        assert observation.binding == binding
        assert observation.bootstrap_registration_epoch == 1
        assert observation.worker_id is None
        assert observation.release is None
        physical_request = physical(registration)
        bound = await client.bind_slurm_job(physical_request)
        assert bound.intent_id == binding.intent_id
        assert await client.observe_intent(binding) == observation
        from tests.integration.test_personal_dev_build_guard_withdrawal import withdrawal

        if register:
            from tests.unit.test_capacity_build_admission_client import native_registration

            worker = native_registration(binding.pool_id).model_copy(update={
                "binding": binding, "slurm_job_id": physical_request.slurm_job_id})
            registered = await client.register_worker(worker, bootstrap_capability=BOOTSTRAP)
            assert await client.register_worker(worker, bootstrap_capability=BOOTSTRAP) == registered
            assert (await client.observe_intent(binding)).worker_id == worker.worker_id
            if register in {"claim", "outcome"}:
                from loom_capacity_agent.build_admission import BuildClaimRequestV1

                claim = BuildClaimRequestV1(binding=binding, operation_id=uuid4(), request_id=_request.id,
                    worker_id=worker.worker_id, worker_incarnation=worker.worker_incarnation)
                claimed = await client.claim_platform(claim, worker_credential="w" * 43)
                assert claimed.request == claim
                assert await client.claim_platform(claim, worker_credential="w" * 43) == claimed
                assert (await client.observe_intent(binding)).claim_high_water == 1
                if register == "outcome":
                    from tests.integration.test_personal_dev_build_guard_outcomes import (
                        outcome_request,
                    )

                    outcome = outcome_request(claim)
                    completed = await client.record_outcome(outcome, worker_credential="w" * 43)
                    assert completed.request == outcome and not completed.executable
                    assert await client.record_outcome(outcome, worker_credential="w" * 43) == completed
            from loom_capacity_agent.admission import ExecutableDrainRequestV2

            drain = ExecutableDrainRequestV2(binding=binding, operation_id=uuid4(),
                worker_id=worker.worker_id, worker_incarnation=worker.worker_incarnation,
                expected_claim_high_water=int(register in {"claim", "outcome"}), drain_epoch=3)
            drained = await client.begin_drain(drain)
            assert drained.live_claim_count == int(register == "claim")
            assert await client.begin_drain(drain) == drained
            assert (await client.observe_intent(binding)).drain == drained
            if register != "claim":
                from loom_capacity_agent.admission import ExecutableReleaseRequestV2

                release = ExecutableReleaseRequestV2(binding=binding, operation_id=uuid4(),
                    reporter_incarnation=_installation.document.reporter_incarnation,
                    bootstrap_registration_epoch=1, protected_registration_epoch=2,
                    expected_claim_high_water=drain.expected_claim_high_water, release_epoch=4)
                released = await client.acknowledge_release(release, current_worker_credential="w" * 43)
                assert released.live_claim_count == 0 and released.worker_credentials_revoked
                assert await client.acknowledge_release(release, current_worker_credential="w" * 43) == released
                assert (await client.observe_intent(binding)).release == released
        else:
            withdraw = withdrawal(physical_request)
            withdrawn = await client.withdraw_unregistered_worker(withdraw)
            assert await client.withdraw_unregistered_worker(withdraw) == withdrawn
            assert (await client.observe_intent(binding)).withdrawal == withdrawn
        # A valid bearer without its client certificate cannot reach the API.
        context = ssl.create_default_context(cafile=str(ca_path))
        async with httpx.AsyncClient(verify=context,trust_env=False,timeout=2) as unauthenticated:
            with pytest.raises(httpx.HTTPError):
                await unauthenticated.post(origin+route(registration,"prepare"),
                    headers={"Authorization":"Bearer executor-secret"},json=preparation(registration,digest))
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == 2
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_withdrawals")) == (0 if register else 1)
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_registrations")) == (1 if register else 0)
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_claims")) == (1 if register in {"claim", "outcome"} else 0)
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.platform_outcomes")) == int(register == "outcome")
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_drains")) == (1 if register else 0)
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
    finally:
        if client is not None and hasattr(client,"aclose"):
            await client.aclose()
        server.should_exit = True
        try:
            async with asyncio.timeout(5):
                await task
        finally:
            listener.close()
