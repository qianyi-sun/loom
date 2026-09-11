"""Service startup joins pinned reporter scope to the real private installation."""

from hashlib import sha256
from importlib import import_module
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.contracts import canonical_bytes
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_management_runtime import configuration_for
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions
from tests.unit.test_capacity_agent_client import _owner_file


def inputs(values, tmp_path):
    module = import_module("loom_service.personal_dev_build_management")
    factory, _engine, installation, *_ = values
    files = {}
    for name in ("bearer_token", "ca", "certificate", "private_key"):
        data = ("test-only-" + name).encode("ascii")
        path = _owner_file(tmp_path / name, data)
        files[name] = module.BuildManagementFileV1(path=str(path), sha256=sha256(data).hexdigest())
    scope = module.BuildManagementScopeV1(installation=installation.document, reporter=configuration_for(installation),
        manager_origin="https://manager.example", **files)
    config = module.BuildManagementServiceConfigV1(mode="recovery-only", scopes=(scope,))
    wire = canonical_bytes(config)
    path = _owner_file(tmp_path / "management.json", wire)
    settings = SimpleNamespace(personal_dev_build_management_config_file=path,
        personal_dev_build_management_config_sha256=sha256(wire).hexdigest())
    return settings, config, SimpleNamespace(sessions=factory, mode="native-claims")


@pytest.mark.parametrize("boundary", ["exact", "native-source", "credential-replaced", "document", "procedure", "credential", "reporter", "duplicate", "intake-mode", "admission"])
async def test_management_startup_requires_exact_private_installed_scope(prepared_input, tmp_path, monkeypatch, boundary):
    module = import_module("loom_service.personal_dev_build_management")
    settings, config, admission = inputs(prepared_input, tmp_path)
    if boundary == "native-source":
        admission.mode = "native-source"
    _factory, engine, _installation, *_ = prepared_input
    calls = []
    opened_paths = []

    class Client:
        async def aclose(self):
            calls.append("closed")

    def open_client(reporter, connection):
        assert reporter == config.scopes[0].reporter
        assert connection.bearer_token_file.read_bytes() == b"test-only-bearer_token"
        opened_paths.append(connection.bearer_token_file)
        calls.append("opened")
        return Client()

    monkeypatch.setattr(module.DemandReporterClient, "from_files", open_client)
    if boundary == "credential-replaced":
        original = module._assert_private_agent

        async def replaced(*args, **kwargs):
            _owner_file(tmp_path / "bearer_token", b"changed-after-verified-read")
            await original(*args, **kwargs)

        monkeypatch.setattr(module, "_assert_private_agent", replaced)
    elif boundary == "document":
        scope = config.scopes[0]
        config = config.model_copy(update={"scopes": (scope.model_copy(update={"installation":
            scope.installation.model_copy(update={"candidate_generation": scope.installation.candidate_generation+1})}),)})
    elif boundary == "procedure":
        with engine.begin() as connection:
            connection.execute(text("ALTER FUNCTION loom_capacity_build_guard.assert_management_installation(uuid,bytea) SET search_path=public"))
    elif boundary == "credential":
        _owner_file(tmp_path / "bearer_token", b"substituted-token")
    elif boundary == "reporter":
        scope = config.scopes[0]
        from uuid import uuid4

        config = config.model_copy(update={"scopes": (scope.model_copy(update={"reporter": scope.reporter.model_copy(
            update={"reporter_incarnation": uuid4()})}),)})
    elif boundary == "duplicate":
        config = config.model_copy(update={"scopes": config.scopes * 2})
    elif boundary == "intake-mode":
        config = config.model_copy(update={"mode": "enabled"})
    elif boundary == "admission":
        admission = None
    wire = canonical_bytes(config)
    _owner_file(settings.personal_dev_build_management_config_file, wire)
    settings.personal_dev_build_management_config_sha256 = sha256(wire).hexdigest()
    if boundary in {"exact", "credential-replaced", "native-source"}:
        runtime = await module.build_personal_build_management_runtime(settings, admission=admission)
        assert len(runtime.managers) == 1 and len(runtime.clients) == 1
        await runtime.aclose()
        assert calls == ["opened", "closed"]
        assert all(not path.exists() for path in opened_paths)
    else:
        with pytest.raises((ValueError, RuntimeError, DBAPIError)):
            await module.build_personal_build_management_runtime(settings, admission=admission)
        assert calls == []
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0


async def test_management_installation_requires_committed_owner_retention(owner_sessions, tmp_path):
    from loom_capacity_build_guard.installation_store import BuildGuardInstallationStore
    from tests.unit.test_personal_dev_build_admission import admission_input

    sessions, owner = owner_sessions
    values = admission_input(tmp_path)
    async with sessions.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner}"))
        installed = await BuildGuardInstallationStore(session, expected_owner_role=owner).retain(
            member=values["member"], runtime=values["runtime"])
        with pytest.raises(DBAPIError, match="committed retention"):
            async with session.begin_nested():
                await session.scalar(text("SELECT loom_capacity_build_guard.assert_management_installation(:id,:wire)"),
                    {"id": installed.id, "wire": installed.wire_payload})


@pytest.mark.parametrize("fail_second", [False, True])
async def test_management_loads_real_tls_and_closes_partial_client_construction(prepared_input, owner_sessions, tmp_path, monkeypatch, fail_second):
    from uuid import uuid4

    from cryptography.hazmat.primitives import serialization

    from loom_capacity_build_guard.installation_store import BuildGuardInstallationStore
    from tests.integration.test_capacity_manager_mtls import (
        _new_ca,
        _private_key_bytes,
        _signed_certificate,
    )
    from tests.integration.test_personal_dev_build_platform_requests import build_service

    module = import_module("loom_service.personal_dev_build_management")
    settings, config, admission = inputs(prepared_input, tmp_path)
    _factory, _engine, _installation, _plan, source, *_ = prepared_input
    member, runtime = build_service(tmp_path, source)
    reporter = uuid4()
    deployment = member.configuration.deployment_generation + 1
    member = member.model_copy(update={"configuration": member.configuration.model_copy(update={
        "deployment_generation": deployment, "demand_reporter_incarnation": reporter}),
        "acknowledgement": member.acknowledgement.model_copy(update={"deployment_generation": deployment, "reporter_incarnation": reporter})})
    owner_factory, owner = owner_sessions
    async with owner_factory.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner}"))
        successor = await BuildGuardInstallationStore(session, expected_owner_role=owner).retain(member=member, runtime=runtime)
    ca_key, ca = _new_ca("build-management-ca")
    key, certificate = _signed_certificate("build-reporter", ca_key, ca, server=False)
    pem = serialization.Encoding.PEM
    tls = {}
    for name, payload in (("ca", ca.public_bytes(pem)), ("certificate", certificate.public_bytes(pem)), ("private_key", _private_key_bytes(key))):
        path = _owner_file(tmp_path / name, payload)
        tls[name] = module.BuildManagementFileV1(path=str(path), sha256=sha256(payload).hexdigest())
    first = config.scopes[0].model_copy(update=tls)
    second_token = _owner_file(tmp_path / "second-token", b"test-only-second-reporter-token")
    second = first.model_copy(update={"installation": successor.document, "reporter": configuration_for(successor),
        "bearer_token": module.BuildManagementFileV1(path=str(second_token), sha256=sha256(second_token.read_bytes()).hexdigest())})
    wire = canonical_bytes(config.model_copy(update={"scopes": (first, second)}))
    _owner_file(settings.personal_dev_build_management_config_file, wire)
    settings.personal_dev_build_management_config_sha256 = sha256(wire).hexdigest()
    original = module.DemandReporterClient.from_files
    created = []

    def create(configuration, connection):
        if fail_second and created:
            raise ValueError("second client construction rejected")
        client = original(configuration, connection)
        created.append(client)
        return client

    monkeypatch.setattr(module.DemandReporterClient, "from_files", create)
    if fail_second:
        with pytest.raises(ValueError, match="second client"):
            await module.build_personal_build_management_runtime(settings, admission=admission)
        assert len(created) == 1
    else:
        managed = await module.build_personal_build_management_runtime(settings, admission=admission)
        assert len(managed.managers) == len(created) == 2
        await managed.aclose()
    assert all(client._http.is_closed for client in created)
