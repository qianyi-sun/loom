"""Node bootstrap keeps the controller's typed application entry identity."""

import hashlib
from importlib import import_module
from types import SimpleNamespace

import pytest

from loom_capacity_manager.executable_contracts import canonical_executable_digest
from tests.unit.test_capacity_typed_admission_routing import configured
from tests.unit.test_native_bootstrap_delivery import delivery as delivery
from tests.unit.test_native_bootstrap_receiver import private_delivery as private_delivery


@pytest.mark.parametrize("pool", ("oldlab", "gb10"))
async def test_node_application_route_preserves_exact_controller_digest_and_closes_client(tmp_path, monkeypatch, pool):
    module = import_module("loom_capacity_executor.native_application_admission")
    typed, request, document, path, digest = configured(tmp_path, pool, "application-worker")
    events = []

    async def observe(value):
        assert value is request
        events.append("observe")
        return "current-bootstrap"

    async def register(value, *, bootstrap_capability):
        assert value is request and bootstrap_capability == "disposable-capability"
        events.append("register")
        return "registered"

    async def close():
        events.append("closed")

    def client(url, **kwargs):
        assert url.startswith(b"postgresql+")
        assert kwargs == {"subject_id": request.binding.subject_id, "subject_incarnation": request.binding.subject_incarnation}
        return SimpleNamespace(observe_current_bootstrap=observe, register_worker=register, aclose=close)

    monkeypatch.setattr(module.DatabaseExecutableAdmissionClient, "from_database_url_bytes", staticmethod(client))
    controller = typed.TypedAdmissionRouter(path, expected_sha256=digest, executor=document.executor)
    node = module.ApplicationBootstrapAdmission(path, expected_sha256=digest, executor=document.executor)
    assert node.bootstrap_handoff_route_sha256(request.binding) == controller.bootstrap_handoff_route_sha256(request.binding)
    assert node.bootstrap_handoff_route_sha256(request.binding) == canonical_executable_digest(document.entries[0])
    assert await node.observe_current_bootstrap(request) == "current-bootstrap"
    assert await node.register_worker(request, bootstrap_capability="disposable-capability") == "registered"
    assert events == ["observe", "closed", "register", "closed"]


@pytest.mark.parametrize("change", ("build-route", "executor", "route-file", "binding-executor"))
async def test_node_application_route_refuses_other_authority_before_credentials(tmp_path, monkeypatch, change):
    module = import_module("loom_capacity_executor.native_application_admission")
    _typed, request, document, path, digest = configured(tmp_path, "gb10",
        "personal-build-worker" if change == "build-route" else "application-worker")

    def forbidden(*args, **kwargs):
        pytest.fail("invalid node route must not open a credential-bearing client")

    monkeypatch.setattr(module.DatabaseExecutableAdmissionClient, "from_database_url_bytes", staticmethod(forbidden))
    identity = document.executor.model_copy(update={"executor_id": "foreign"}) if change == "executor" else document.executor
    with pytest.raises(ValueError):
        node = module.ApplicationBootstrapAdmission(path, expected_sha256=digest, executor=identity)
        if change == "route-file":
            path.write_bytes(path.read_bytes() + b" ")
        if change == "binding-executor":
            request = request.model_copy(update={"binding": request.binding.model_copy(update={"executor_id": "foreign"})})
        await node.observe_current_bootstrap(request)


def test_fixed_receiver_selects_explicit_typed_application_route(private_delivery):
    from loom_capacity_executor.native_bootstrap_delivery import _canonical
    from loom_capacity_executor.native_bootstrap_receiver import NativeBootstrapReceiverConfigV1, _load_fixed_receiver
    from loom_capacity_executor.slurm_contracts import SlurmFileIdentityV2

    _typed, request, document, path, digest = configured(private_delivery.node, "oldlab", "application-worker")
    binding = request.binding
    config = NativeBootstrapReceiverConfigV1(directory=str(private_delivery.node), target_node=binding.node_ids[0],
        pool_id=binding.pool_id, trusted_release_sha256=binding.execution.trusted_fleet_release_sha256,
        admission_directory=str(path), admission_directory_sha256=digest, typed_application_executor=document.executor)
    wire = _canonical(config)
    config_path = private_delivery.node / "receiver.json"
    config_path.write_bytes(wire)
    config_path.chmod(0o600)
    receiver = _load_fixed_receiver(SlurmFileIdentityV2(path=str(config_path), sha256=hashlib.sha256(wire).hexdigest(), owner_uid=config_path.stat().st_uid))
    assert receiver.admission.bootstrap_handoff_route_sha256(binding) == canonical_executable_digest(document.entries[0])
    assert "typed_application_executor" in config.model_dump(mode="json")


def test_legacy_receiver_config_serialization_does_not_add_typed_authority(tmp_path):
    from loom_capacity_executor.native_bootstrap_delivery import _canonical
    from loom_capacity_executor.native_bootstrap_receiver import NativeBootstrapReceiverConfigV1

    config = NativeBootstrapReceiverConfigV1(directory=str(tmp_path), target_node="node", pool_id="oldlab",
        trusted_release_sha256="a" * 64, admission_directory=str(tmp_path), admission_directory_sha256="b" * 64)
    assert "typed_application_executor" not in _canonical(config).decode()


def test_native_launcher_selects_typed_application_without_legacy_fallback(tmp_path):
    from loom_capacity_executor.trusted_launcher import TrustedLauncherConfigV2, _launcher_admission
    from tests.unit.test_capacity_executor_bootstrap_handoff import _trusted_candidate_config_payload, _write_candidate
    from tests.unit.test_worker_native_entrypoint import _configured_bootstrap

    _typed, request, document, path, digest = configured(tmp_path, "gb10", "application-worker")
    candidate = tmp_path / "docker"
    _write_candidate(candidate)
    original = _trusted_candidate_config_payload(handoff_directory=tmp_path,
        admission_directory=tmp_path, candidate_path=candidate)
    legacy = TrustedLauncherConfigV2.model_validate(original)
    assert "typed_application_executor" not in legacy.model_dump()
    bootstrap = _configured_bootstrap()
    original.update(admission_directory=str(path), admission_directory_sha256=digest,
        typed_application_executor=document.executor, candidate_argv=(str(candidate),), native_worker={
        "native_execution": bootstrap.native_execution,
        "canonical_worker_settings": bootstrap.canonical_worker_settings,
        "docker_config_directory": "/etc/loom/empty-docker", "pids_max": 128})
    config = TrustedLauncherConfigV2.model_validate(original)

    def forbidden(*args, **kwargs):
        pytest.fail("explicit typed route must never fall back to V2")

    node = _launcher_admission(config, forbidden)
    assert node.bootstrap_handoff_route_sha256(request.binding) == canonical_executable_digest(document.entries[0])
    original["native_worker"] = None
    with pytest.raises(ValueError):
        TrustedLauncherConfigV2.model_validate(original)
