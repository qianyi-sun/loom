"""Typed admission chooses pinned purpose without consulting a live permit."""

import json
from hashlib import sha256
from importlib import import_module
from uuid import uuid4

import pytest

from loom_capacity_manager.executable_contracts import (
    canonical_executable_bytes,
    canonical_executable_digest,
)
from tests.unit.test_capacity_agent_client import _owner_file
from tests.unit.test_capacity_build_admission_client import registration
from tests.unit.test_capacity_build_pinned_transport import pinned_inputs
from tests.unit.test_capacity_executor_typed_launch_renderer import typed_context


def configured(tmp_path,pool,purpose):
    module = import_module("loom_capacity_executor.typed_admission")
    request = registration(pool).model_copy(update={"binding": typed_context(pool=pool,purpose=purpose).binding})
    binding = request.binding
    database = _owner_file(tmp_path/"database-url",b"postgresql+psycopg://executor:private@database.test/app?sslmode=verify-full")
    entry = module.TypedAdmissionEntryV3(subject_id=binding.subject_id,subject_incarnation=binding.subject_incarnation,
        configuration_epoch=binding.execution.configuration_epoch,deployment_generation=binding.deployment_generation,
        candidate_generation=binding.candidate_generation,candidate_sha256=canonical_executable_digest(binding.candidate),
        account_id=binding.account_id,purpose=purpose,protected_admission_sha256="a"*64,
        database={"path":str(database),"sha256":sha256(database.read_bytes()).hexdigest()} if purpose=="application-worker" else None,
        build={"origin":"https://management.test",**pinned_inputs(tmp_path)} if purpose=="personal-build-worker" else None)
    document = module.TypedAdmissionDirectoryV3(executor={"pool_id":pool,"pool_generation":binding.pool_generation,
        "executor_id":binding.executor_id,"executor_incarnation":binding.executor_incarnation},entries=(entry,))
    wire = canonical_executable_bytes(document)
    path = _owner_file(tmp_path/"routes.json",wire)
    return module,request,document,path,sha256(wire).hexdigest()


@pytest.mark.parametrize("pool", ["oldlab", "gb10"])
async def test_application_claim_passes_routing_binding_into_atomic_admission(tmp_path, pool):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    module, request, document, path, digest = configured(tmp_path, pool, "application-worker")
    proposal = object()
    admit = AsyncMock(return_value=None)
    close = AsyncMock()
    router = module.TypedAdmissionRouter(path, expected_sha256=digest, executor=document.executor,
        application_client_factory=lambda *args, **kwargs: SimpleNamespace(
            admit_claim_for_intent=admit, aclose=close))
    assert await router.admit_claim(request.binding, proposal) is None
    admit.assert_awaited_once_with(request.binding, proposal)
    close.assert_awaited_once()


@pytest.mark.parametrize("pool", ["gb10", "oldlab"])
@pytest.mark.parametrize("operation", ["claim", "outcome", "source"])
async def test_native_claim_cannot_fall_back_to_application_authority(tmp_path, pool, operation):
    from loom_capacity_agent.build_admission import BuildClaimRequestV1, BuildOutcomeRequestV1

    module, request, document, path, digest = configured(tmp_path, pool, "application-worker")

    def unexpected(*args, **kwargs):
        pytest.fail("native claim must not open application transport")

    router = module.TypedAdmissionRouter(path, expected_sha256=digest, executor=document.executor,
        application_client_factory=unexpected, build_client_factory=unexpected)
    claim = BuildClaimRequestV1(binding=request.binding, operation_id=uuid4(), request_id=uuid4(),
        worker_id=uuid4(), worker_incarnation=uuid4())
    with pytest.raises(ValueError, match="build-purpose"):
        if operation == "source":
            await router.read_source(claim, worker_credential="w" * 43, offset=0, length=10)
        elif operation == "outcome":
            await router.record_outcome(BuildOutcomeRequestV1(claim=claim, operation_id=uuid4(), result="failed"), worker_credential="w" * 43)
        else:
            await router.claim_platform(claim, worker_credential="w" * 43)


@pytest.mark.parametrize("pool", ["oldlab", "gb10"])
@pytest.mark.parametrize("boundary", ["exact", "failure", "invalid", "changed-root"])
async def test_native_source_uses_pinned_route_and_closes_per_read(tmp_path, pool, boundary):
    import base64
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from loom_capacity_agent.build_admission import BuildClaimRequestV1, BuildSourceReadReceiptV1
    from loom_capacity_manager.contracts import canonical_digest

    module, request, document, path, digest = configured(tmp_path, pool, "personal-build-worker")
    claim = BuildClaimRequestV1(binding=request.binding, operation_id=uuid4(), request_id=uuid4(),
        worker_id=uuid4(), worker_incarnation=uuid4())
    receipt = BuildSourceReadReceiptV1(claim_digest=canonical_digest(claim), source_binding_sha256="a" * 64,
        archive_sha256="b" * 64, archive_size_bytes=20, offset=4, data_base64=base64.b64encode(b"source").decode("ascii"))
    read = AsyncMock(return_value=object() if boundary == "invalid" else receipt,
        side_effect=RuntimeError("lost read") if boundary == "failure" else None)
    close = AsyncMock()
    opened = []

    def build(identity, connection):
        assert identity == document.executor and connection == document.entries[0].build
        opened.append(connection)
        return SimpleNamespace(read_source=read, aclose=close)

    def application(*args, **kwargs):
        pytest.fail("source read must never use application authority")

    router = module.TypedAdmissionRouter(path, expected_sha256=digest, executor=document.executor,
        application_client_factory=application, build_client_factory=build)
    if boundary == "changed-root":
        path.write_bytes(path.read_bytes() + b" ")
    if boundary == "exact":
        assert await router.read_source(claim, worker_credential="w" * 43, offset=4, length=6) == receipt
    else:
        with pytest.raises((ValueError, RuntimeError)):
            await router.read_source(claim, worker_credential="w" * 43, offset=4, length=6)
    if boundary == "changed-root":
        assert opened == []
    else:
        read.assert_awaited_once_with(claim, worker_credential="w" * 43, offset=4, length=6)
        close.assert_awaited_once()


@pytest.mark.parametrize("pool", ["gb10","oldlab"])
@pytest.mark.parametrize("purpose", ["application-worker","personal-build-worker"])
@pytest.mark.parametrize("failure", [False,True])
async def test_typed_route_uses_exact_purpose_and_always_closes_client(tmp_path,pool,purpose,failure):
    module,request,document,path,digest = configured(tmp_path,pool,purpose)
    events = []

    class Client:
        async def prepare_worker(self,value,**kwargs):
            assert value == request
            events.append("prepared")
            if failure:
                raise RuntimeError("lost reply")
            return "receipt"

        async def aclose(self):
            events.append("closed")

    def application(url,**kwargs):
        assert purpose == "application-worker"
        assert b"sslmode=verify-full" in url
        assert kwargs == {"subject_id":request.binding.subject_id,"subject_incarnation":request.binding.subject_incarnation}
        events.append("application")
        return Client()

    def build(identity,connection):
        assert purpose == "personal-build-worker"
        assert identity == document.executor
        assert connection == document.entries[0].build
        events.append("build")
        return Client()

    client = module.TypedAdmissionRouter(path,expected_sha256=digest,executor=document.executor,
        application_client_factory=application,build_client_factory=build)
    assert client.purpose(request.binding) == purpose
    assert client.bootstrap_handoff_route_sha256(request.binding) == canonical_executable_digest(document.entries[0])
    if failure:
        with pytest.raises(RuntimeError,match="lost reply"):
            await client.prepare_worker(request,bootstrap_sha256="b"*64)
    else:
        assert await client.prepare_worker(request,bootstrap_sha256="b"*64) == "receipt"
    assert events == ["application" if purpose=="application-worker" else "build","prepared","closed"]


@pytest.mark.parametrize("boundary", ["subject","incarnation","account","candidate-generation","deployment-generation",
    "configuration","candidate","pool","pool-generation","executor-id","executor","root","noncanonical","unsupported"])
async def test_typed_route_rejects_drift_before_creating_clients(tmp_path,boundary):
    module,request,document,path,digest = configured(tmp_path,"gb10","personal-build-worker")
    changes = {"subject":{"subject_id":uuid4()},"incarnation":{"subject_incarnation":uuid4()},
        "account":{"account_id":"foreign"},"candidate-generation":{"candidate_generation":99},
        "deployment-generation":{"deployment_generation":99},"configuration":{"execution":request.binding.execution.model_copy(update={"configuration_epoch":99})},
        "candidate":{"candidate":request.binding.candidate.model_copy(update={"publication_sha256":"f"*64})},
        "pool":{"pool_id":"oldlab"},"pool-generation":{"pool_generation":99},
        "executor-id":{"executor_id":"foreign"},"executor":{"executor_incarnation":uuid4()}}

    def unexpected(*args,**kwargs):
        pytest.fail("invalid typed routing must not create a credential-bearing client")

    if boundary in {"root","noncanonical"}:
        wire = path.read_bytes() + b" "
        path.write_bytes(wire)
        if boundary == "noncanonical":
            digest = sha256(wire).hexdigest()
    with pytest.raises((ValueError,RuntimeError)):
        client = module.TypedAdmissionRouter(path,expected_sha256=digest,executor=document.executor,
            application_client_factory=unexpected,build_client_factory=unexpected)
        if boundary == "unsupported":
            await client.admit_claim(request.binding, object())
        else:
            changed = request.model_copy(update={"binding":request.binding.model_copy(update=changes.get(boundary,{}))})
            await client.prepare_worker(changed,bootstrap_sha256="b"*64)


@pytest.mark.parametrize("boundary", ["duplicate","mixed","purpose","foreign-executor"])
def test_typed_directory_rejects_ambiguous_authority(tmp_path,boundary):
    module,_request,document,path,_digest = configured(tmp_path,"gb10","personal-build-worker")
    payload = document.model_dump(mode="json")
    if boundary == "duplicate":
        payload["entries"].append(payload["entries"][0])
    elif boundary == "mixed":
        payload["entries"][0]["database"] = payload["entries"][0]["build"]["bearer_token"]
    elif boundary == "purpose":
        payload["entries"][0]["purpose"] = "application-worker"
    else:
        payload["executor"]["executor_id"] = "foreign"
    wire = json.dumps(payload,sort_keys=True,separators=(",",":"),ensure_ascii=True).encode("ascii")
    path.write_bytes(wire)
    with pytest.raises((ValueError,RuntimeError)):
        module.TypedAdmissionRouter(path,expected_sha256=sha256(wire).hexdigest(),executor=document.executor)


@pytest.mark.parametrize("boundary", ["root", "database", "database-mode"])
async def test_typed_route_rechecks_inputs_after_construction(tmp_path, boundary):
    from pathlib import Path

    module,request,document,path,digest = configured(tmp_path,"oldlab","application-worker")

    def unexpected(*args, **kwargs):
        pytest.fail("changed route must not create a client")

    router = module.TypedAdmissionRouter(path,expected_sha256=digest,executor=document.executor,
        application_client_factory=unexpected,build_client_factory=unexpected)
    if boundary == "root":
        path.write_bytes(path.read_bytes() + b" ")
    else:
        credential = Path(document.entries[0].database.path)
        if boundary == "database":
            credential.write_bytes(b"foreign-database")
        else:
            credential.chmod(0o644)
    with pytest.raises((ValueError,RuntimeError,OSError)):
        await router.prepare_worker(request,bootstrap_sha256="b"*64)


@pytest.mark.parametrize("method", ["observe_current_bootstrap", "admit_claim"])
async def test_all_unimplemented_native_consumers_reject_before_transport(tmp_path,method):
    from types import SimpleNamespace

    module,request,document,path,digest = configured(tmp_path,"gb10","personal-build-worker")

    def unexpected(*args, **kwargs):
        pytest.fail("unsupported native consumer must not open transport")

    router = module.TypedAdmissionRouter(path,expected_sha256=digest,executor=document.executor,
        application_client_factory=unexpected,build_client_factory=unexpected)
    value = SimpleNamespace(binding=request.binding)
    args = (request.binding, value) if method == "admit_claim" else (value,)
    with pytest.raises(RuntimeError,match="not implemented"):
        await getattr(router, method)(*args)


@pytest.mark.parametrize("method", ["bind_slurm_job", "observe_intent", "revoke_prepared_bootstrap", "withdraw_unregistered_worker", "register_worker", "begin_drain"])
@pytest.mark.parametrize("purpose", ["application-worker", "personal-build-worker"])
async def test_typed_lifecycle_routes_exact_arguments_and_closes(tmp_path,method,purpose):
    from types import SimpleNamespace

    module,request,document,path,digest = configured(tmp_path,"gb10",purpose)
    value = request.binding if method == "observe_intent" else SimpleNamespace(binding=request.binding)
    events = []

    options = {"bootstrap_capability": "b" * 43} if method == "register_worker" else {}

    async def operation(incoming, **kwargs):
        assert incoming is value
        assert kwargs == options
        events.append(method)
        return "receipt"

    async def close():
        events.append("closed")

    def factory(*args, **kwargs):
        return SimpleNamespace(**{method:operation,"aclose":close})

    router = module.TypedAdmissionRouter(path,expected_sha256=digest,executor=document.executor,
        application_client_factory=factory,build_client_factory=factory)
    assert await getattr(router,method)(value, **options) == "receipt"
    assert events == [method,"closed"]


@pytest.mark.parametrize("pool", ["oldlab", "gb10"])
async def test_current_bootstrap_routes_only_application_credentials(tmp_path, pool):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    module, request, document, path, digest = configured(tmp_path, pool, "application-worker")
    observe = AsyncMock(return_value="current-evidence")
    close = AsyncMock()
    router = module.TypedAdmissionRouter(path, expected_sha256=digest, executor=document.executor,
        application_client_factory=lambda *args, **kwargs: SimpleNamespace(
            observe_current_bootstrap=observe, aclose=close))
    assert await router.observe_current_bootstrap(request) == "current-evidence"
    observe.assert_awaited_once_with(request)
    close.assert_awaited_once()
