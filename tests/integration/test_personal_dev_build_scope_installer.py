"""Protected onboarding joins authenticated membership to committed guard scope."""

import json
from hashlib import sha256
from importlib import import_module
from uuid import UUID, uuid4

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from sqlalchemy import text

from loom.personal_dev_typed_membership_client import CapacityManagerPersonalDevTypedMembershipClient, PersonalDevTypedMembershipEnvelopeV1
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from loom_capacity_manager.membership_contracts import PersonalMembershipCheckpointV1
from loom_capacity_manager.membership_digest import canonical_membership_event_head
from loom_capacity_manager.typed_membership_commands import PersonalMembershipResultV2, derive_build_member
from loom_service.personal_dev_build_management import BuildManagementFileV1, BuildManagementServiceConfigV1
from tests.integration.test_capacity_manager_mtls import _new_ca, _private_key_bytes, _signed_certificate
from tests.integration.test_personal_dev_build_guard_installations import owner_sessions as owner_sessions
from tests.integration.test_personal_dev_build_guard_migrations import build_guard_database as build_guard_database
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
        with pytest.raises(Exception):
            prepared = module.prepare_build_scope_installation(config)
            await module.install_build_scope(prepared, sessions=sessions, manager=manager)
    async with sessions() as session:
        assert await session.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.installations")) == 0
