"""Protected onboarding joins authenticated membership to committed guard scope."""

import json
from hashlib import sha256
from importlib import import_module
from uuid import UUID, uuid4

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom.personal_dev_typed_membership_client import (
    CapacityManagerPersonalDevTypedMembershipClient,
    PersonalDevTypedMembershipEnvelopeV1,
)
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from loom_capacity_manager.membership_contracts import PersonalMembershipCheckpointV1
from loom_capacity_manager.membership_digest import canonical_membership_event_head
from loom_capacity_manager.typed_membership_commands import (
    PersonalMembershipResultV2,
    derive_build_member,
)
from loom_service.personal_dev_build_management import (
    BuildManagementFileV1,
    BuildManagementServiceConfigV1,
)
from tests.integration.test_capacity_manager_mtls import (
    _new_ca,
    _private_key_bytes,
    _signed_certificate,
)
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.unit.test_capacity_agent_client import _configuration, _owner_file
from tests.unit.test_capacity_typed_membership_commands import typed_build_mutation
from tests.unit.test_personal_dev_build_runtime_installation import installation_input
from tests.unit.test_personal_dev_build_runtime_publication import _evidence


def installer_config(tmp_path, owner):
    module = import_module("loom_capacity_build_guard.installer")
    publication, preparation, pools = installation_input(tmp_path)
    _contracts, value, request = typed_build_mutation()
    token = b"test-only-owner-build-reporter"
    request = request.model_copy(update={"execution": request.execution.model_copy(update={
        "execution_manifest_sha256": canonical_executable_digest(preparation)}),
        "command": request.command.model_copy(update={"acknowledgement": request.command.acknowledgement.model_copy(update={
            "candidate": publication.candidate}), "projection": request.command.projection.model_copy(update={
                "demand_reporter_token_sha256": sha256(token).hexdigest()})})})
    member = derive_build_member(request, preparation, value.fleet)
    envelope = PersonalDevTypedMembershipEnvelopeV1(request=request,
        expected_checkpoint=PersonalMembershipCheckpointV1(execution=request.execution, namespace_id=request.namespace_id,
            revision=request.expected_revision, head_sha256="a" * 64), idempotency_key=UUID(int=990001))
    ca_key, ca = _new_ca("installer-ca")
    key, certificate = _signed_certificate("installer-client", ca_key, ca, server=False)
    pem = serialization.Encoding.PEM

    def pin(name, wire):
        path = _owner_file(tmp_path / name, wire)
        return BuildManagementFileV1(path=str(path), sha256=sha256(wire).hexdigest())

    tls = dict(ca=pin("ca.pem", ca.public_bytes(pem)), certificate=pin("client.pem", certificate.public_bytes(pem)),
        private_key=pin("client.key", _private_key_bytes(key)))
    management = module.BuildScopeClientFilesV1(bearer_token=pin("management.token", b"test-only-private-membership-manager"), **tls)
    reporter_files = module.BuildScopeClientFilesV1(bearer_token=pin("reporter.token", token), **tls)
    reporter = _configuration().model_copy(update={
        "subject_id": member.configuration.subject_id, "subject_incarnation": member.configuration.subject_incarnation,
        "authority_incarnation": request.execution.authority_incarnation, "agent_incarnation": uuid4(),
        "reporter_incarnation": member.configuration.demand_reporter_incarnation,
        "candidate_identity_algorithm": publication.candidate.algorithm, "candidate_identity": publication.candidate.identity,
        "candidate_publication_sha256": publication.candidate.publication_sha256,
        "deployment_generation": member.configuration.deployment_generation,
        "configuration_generation": member.configuration.configuration_generation,
        "protected_admission_sha256": member.acknowledgement.protected_admission_sha256})
    release_path = tmp_path / "trusted-release.json"
    release = json.loads(release_path.read_bytes())
    directory = tmp_path / "registry"
    directory.mkdir(mode=0o700)
    return module, module.BuildScopeInstallConfigV1(envelope=envelope, preparation=preparation, fleet=value.fleet,
        pools=tuple(module.BuildScopePoolProfilesV1(policy=policy, profiles=profiles) for policy, profiles in pools),
        trusted_release=BuildManagementFileV1(path=str(release_path), sha256=sha256(release_path.read_bytes()).hexdigest()),
        release_evidence=pin("release-evidence.json", json.dumps(_evidence(release), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")),
        manager_origin="https://capacity.test", management_credentials=management,
        reporter_credentials=reporter_files, reporter=reporter,
        database_url=pin("installer.db", b"postgresql+psycopg://installer:test-only@database.test/loom?sslmode=verify-full"),
        owner_role=owner, registry_directory=str(directory), expected_registry_sha256=None)


def response_for(config, *, replayed=False):
    request = config.envelope.request
    member = derive_build_member(request, config.preparation, config.fleet)
    head = canonical_membership_event_head(actor=config.preparation.personal_membership.management_principal_id,
        execution_epoch=request.execution.execution_epoch, idempotency_key=config.envelope.idempotency_key,
        operation_id=request.command.projection.operation_id, previous_sha256=config.envelope.expected_checkpoint.head_sha256,
        request_digest=canonical_digest(request), request_payload=request.model_dump(mode="json"), member=member, revision=member.revision)
    return PersonalMembershipResultV2(revision=member.revision, head_sha256=head, member=member, replayed=replayed)


@pytest.mark.parametrize("failure", ["none", "reply-lost", "output-failed"])
async def test_installer_commits_before_publication_and_replays_same_membership(owner_sessions, tmp_path, monkeypatch, failure):
    sessions, owner = owner_sessions
    module, config = installer_config(tmp_path, owner)
    prepared = module.prepare_build_scope_installation(config)
    sent = []

    async def handle(request):
        sent.append(request.content)
        if failure == "reply-lost" and len(sent) == 1:
            raise httpx.ReadTimeout("test-only lost committed reply")
        return httpx.Response(200, content=canonical_bytes(response_for(config, replayed=len(sent)>1)), headers={"Content-Type": "application/json"})

    original = module.BuildScopeRegistry.publish
    if failure == "output-failed":
        def fail(*args, **kwargs):
            raise OSError("test-only output failure")
        monkeypatch.setattr(module.BuildScopeRegistry, "publish", fail)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        manager = CapacityManagerPersonalDevTypedMembershipClient(manager_origin=config.manager_origin,
            bearer_token="test-only-private-membership-manager", http_client=http)
        if failure != "none":
            with pytest.raises((OSError, RuntimeError)):
                await module.install_build_scope(prepared, sessions=sessions, manager=manager)
            assert not (tmp_path / "registry" / "management.json").exists()
            async with sessions.begin() as session:
                assert await session.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.installations")) == int(failure == "output-failed")
            monkeypatch.setattr(module.BuildScopeRegistry, "publish", original)
        result = await module.install_build_scope(prepared, sessions=sessions, manager=manager)
        again = await module.install_build_scope(prepared, sessions=sessions, manager=manager)
        assert again == result
    assert all(wire == canonical_bytes(config.envelope.request) for wire in sent)
    installed = BuildManagementServiceConfigV1.model_validate_json((tmp_path / "registry" / "management.json").read_bytes())
    assert installed == result and len(installed.scopes) == 1
    async with sessions.begin() as session:
        document = installed.scopes[0].installation
        assert await session.scalar(text("SELECT loom_capacity_build_guard.assert_management_installation(:id,:wire)"),
            {"id": document.id, "wire": canonical_bytes(document)}) is True
        assert await session.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0


@pytest.mark.parametrize("boundary", ["reporter-token", "reporter-generation", "shared-token", "runtime", "publication", "owner-role"])
async def test_installer_rejects_inconsistent_protected_inputs_before_membership(owner_sessions, tmp_path, boundary):
    sessions, owner = owner_sessions
    module, config = installer_config(tmp_path, owner)
    if boundary == "reporter-token":
        _owner_file(tmp_path / "reporter.token", b"changed")
    elif boundary == "reporter-generation":
        config = config.model_copy(update={"reporter": config.reporter.model_copy(update={"configuration_generation": 999})})
    elif boundary == "shared-token":
        config = config.model_copy(update={"management_credentials": config.reporter_credentials})
    elif boundary == "runtime":
        config = config.model_copy(update={"pools": config.pools[:1]})
    elif boundary == "publication":
        config = config.model_copy(update={"trusted_release": config.trusted_release.model_copy(update={"sha256": "e" * 64})})
    else:
        config = config.model_copy(update={"owner_role": "foreign_owner"})
    async def forbidden(_request):
        pytest.fail("invalid installer scope reached manager")
    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as http:
        manager = CapacityManagerPersonalDevTypedMembershipClient(manager_origin=config.manager_origin,
            bearer_token="test-only-private-membership-manager", http_client=http)
        with pytest.raises((ValueError, DBAPIError)):
            prepared = module.prepare_build_scope_installation(config)
            await module.install_build_scope(prepared, sessions=sessions, manager=manager)
    async with sessions() as session:
        assert await session.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.installations")) == 0


async def test_installer_output_loads_actual_private_service_runtime(owner_sessions, build_guard_database, tmp_path):
    from types import SimpleNamespace

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom_service.personal_dev_build_management import build_personal_build_management_runtime

    sessions, owner = owner_sessions
    module, config = installer_config(tmp_path, owner)
    prepared = module.prepare_build_scope_installation(config)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200,
        content=canonical_bytes(response_for(config)), headers={"Content-Type": "application/json"}))) as http:
        manager = CapacityManagerPersonalDevTypedMembershipClient(manager_origin=config.manager_origin,
            bearer_token="test-only-private-membership-manager", http_client=http)
        result = await module.install_build_scope(prepared, sessions=sessions, manager=manager)
    # A later installer input rotation cannot invalidate an installed snapshot.
    _owner_file(tmp_path / "reporter.token", b"later-input-rotation")
    engine = create_async_engine(build_guard_database[4].set(drivername="postgresql+psycopg"), isolation_level="SERIALIZABLE")
    try:
        settings = SimpleNamespace(personal_dev_build_management_config_file=tmp_path / "registry" / f"management-{sha256(canonical_bytes(result)).hexdigest()}.json",
            personal_dev_build_management_config_sha256=sha256(canonical_bytes(result)).hexdigest())
        admission = SimpleNamespace(mode="native-claims", sessions=async_sessionmaker(engine))
        runtime = await build_personal_build_management_runtime(settings, admission=admission)
        assert len(runtime.managers) == len(runtime.clients) == 1
        assert runtime.clients[0]._configuration == prepared.scope.reporter
        await runtime.aclose()
        assert runtime.clients[0]._http.is_closed
    finally:
        await engine.dispose()


async def test_installer_second_owner_preserves_first_snapshot_and_fences_stale_registry(owner_sessions, tmp_path):
    from loom_capacity_manager.build_value_contracts import personal_build_subject_id

    sessions, owner = owner_sessions
    module, first = installer_config(tmp_path, owner)
    second_path = tmp_path / "second-owner"
    second_path.mkdir()
    _module, second = installer_config(second_path, owner)
    request = second.envelope.request
    owner_id, incarnation, reporter_id = uuid4(), uuid4(), uuid4()
    subject_id = personal_build_subject_id(request.namespace_id, owner_id)
    reporter_token = b"test-only-second-owner-reporter"
    _owner_file(second_path / "reporter.token", reporter_token)
    token_file = second.reporter_credentials.bearer_token.model_copy(update={"sha256": sha256(reporter_token).hexdigest()})
    first_result = response_for(first)
    request = request.model_copy(update={"expected_revision": first_result.revision,
        "command": request.command.model_copy(update={
            "projection": request.command.projection.model_copy(update={"owner_id": owner_id, "subject_incarnation": incarnation,
                "demand_reporter_incarnation": reporter_id, "demand_reporter_token_sha256": token_file.sha256, "operation_id": uuid4()}),
            "acknowledgement": request.command.acknowledgement.model_copy(update={"subject_id": subject_id,
                "subject_incarnation": incarnation, "reporter_incarnation": reporter_id})})})
    second = second.model_copy(update={"registry_directory": first.registry_directory,
        "reporter_credentials": second.reporter_credentials.model_copy(update={"bearer_token": token_file}),
        "reporter": second.reporter.model_copy(update={"subject_id": subject_id, "subject_incarnation": incarnation, "reporter_incarnation": reporter_id}),
        "envelope": second.envelope.model_copy(update={"request": request, "idempotency_key": uuid4(),
            "expected_checkpoint": second.envelope.expected_checkpoint.model_copy(update={"revision": first_result.revision,
                "head_sha256": first_result.head_sha256})})})
    sent = []
    current = first

    async def handle(request):
        sent.append(request.content)
        return httpx.Response(200, content=canonical_bytes(response_for(current)), headers={"Content-Type": "application/json"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        manager = CapacityManagerPersonalDevTypedMembershipClient(manager_origin=first.manager_origin,
            bearer_token="test-only-private-membership-manager", http_client=http)
        initial = await module.install_build_scope(module.prepare_build_scope_installation(first), sessions=sessions, manager=manager)
        initial_wire = canonical_bytes(initial)
        with pytest.raises(ValueError, match="registry changed"):
            await module.install_build_scope(module.prepare_build_scope_installation(second), sessions=sessions, manager=manager)
        assert len(sent) == 1
        current = second.model_copy(update={"expected_registry_sha256": sha256(initial_wire).hexdigest()})
        result = await module.install_build_scope(module.prepare_build_scope_installation(current), sessions=sessions, manager=manager)
        assert result.scopes[0] == initial.scopes[0]
        assert {scope.installation.owner_user_id for scope in result.scopes} == {first_result.member.owner_id, owner_id}
    assert (tmp_path / "registry" / f"management-{sha256(initial_wire).hexdigest()}.json").read_bytes() == initial_wire
    assert all(b"test-only-private-membership-manager" not in path.read_bytes() for path in (tmp_path / "registry").iterdir())
    async with sessions() as session:
        assert await session.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.installations")) == 2
