"""Version storage-aware activation without reinterpreting historical intent bytes."""

import json
from dataclasses import fields, replace
from uuid import uuid4

import httpx
import pytest

from loom.dev_instance_runtime import KubectlClient
from loom.personal_dev_activation import PersonalDevActivationIntentRequest
from loom.personal_dev_activation_agent import (
    HttpPersonalDevActivationAuthority,
    KubectlPersonalDevActivationExecutor,
)
from loom.personal_dev_incarnation_storage import PersonalDevStorageBindingV1
from loom_capacity_manager.contracts import canonical_digest
from loom_service.routes.dev_instances import (
    _personal_activation_intent_response,
    _personal_environment_response,
)
from tests.unit.test_personal_dev_activation_agent import _ActivationKubectlRunner, _intent
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


def _binding():
    intent = _intent()
    return PersonalDevStorageBindingV1(
        layout="incarnation-v1", environment_name=intent.environment_name,
        subject_id=intent.subject_id, subject_incarnation=intent.subject_incarnation,
        owner_user_id=uuid4(), owner_team_id=uuid4(),
    )


def _v2():
    binding = _binding()
    return replace(_intent(), schema_version=2, storage_binding=binding,
                   storage_binding_sha256=canonical_digest(binding))


def test_legacy_activation_canonical_and_http_forms_remain_exact():
    intent = _intent()
    wire = _personal_activation_intent_response(intent).model_dump(mode="json")
    assert "schema_version" not in wire
    assert "storage_binding" not in wire
    assert "storage_binding_sha256" not in wire
    wire.pop("intent_sha256")
    wire["schema_version"] = 1
    wire["intent_created_at"] = wire["intent_created_at"].replace("+00:00", "Z")
    assert intent.canonical_bytes() == json.dumps(wire, sort_keys=True, separators=(",", ":")).encode()


def test_bound_activation_canonical_bytes_and_response_pin_full_binding():
    intent = _v2()
    canonical = json.loads(intent.canonical_bytes())
    assert canonical["schema_version"] == 2
    assert canonical["storage_binding"] == intent.storage_binding.model_dump(mode="json")
    assert canonical["storage_binding_sha256"] == canonical_digest(intent.storage_binding)
    wire = _personal_activation_intent_response(intent).model_dump(mode="json")
    assert wire["schema_version"] == 2
    assert wire["storage_binding"] == canonical["storage_binding"]
    assert wire["storage_binding_sha256"] == canonical["storage_binding_sha256"]
    assert wire["intent_sha256"] == intent.intent_sha256


@pytest.mark.parametrize("changes", (
    {"schema_version": 1}, {"schema_version": True}, {"schema_version": 2.0},
    {"storage_binding": None}, {"storage_binding_sha256": None},
    {"storage_binding_sha256": "0" * 64}, {"environment_name": "bob"},
    {"subject_id": uuid4()}, {"subject_incarnation": uuid4()},
))
def test_activation_rejects_mixed_version_and_storage_coordinates(changes):
    with pytest.raises(ValueError, match="storage"):
        replace(_v2(), **changes)


@pytest.mark.parametrize("changes", (
    {}, {"schema_version": 1}, {"schema_version": None}, {"storage_binding": None},
    {"storage_binding_sha256": "0" * 64}, {"extra": "forbidden"},
))
async def test_activation_http_v2_roundtrip_and_fail_closed(changes):
    intent = _v2()
    wire = _personal_activation_intent_response(intent).model_dump(mode="json")
    wire.update(changes)
    async with httpx.AsyncClient(base_url="https://management.example", transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=wire),
    )) as client:
        request = PersonalDevActivationIntentRequest(
            agent_key_id="personal-dev-agent-v1", request_nonce=uuid4(), requested_at=intent.intent_created_at,
        )
        authority = HttpPersonalDevActivationAuthority(client)
        if changes:
            with pytest.raises(RuntimeError, match="response is invalid"):
                await authority.next_intent(request, signature="1" * 128)
        else:
            assert await authority.next_intent(request, signature="1" * 128) == intent


def test_owner_environment_response_uses_persisted_storage_names():
    claim = _bound_claim()
    response = _personal_environment_response(claim.environment)
    assert response.identity.database == claim.operation.storage_binding.identity.database
    assert response.identity.task_bucket == claim.operation.storage_binding.identity.task_bucket


@pytest.mark.parametrize("tamper", (None, "environment_binding", "environment_owner", "candidate_owner", "digest"))
async def test_activation_reader_checks_durable_storage_before_returning_intent(monkeypatch, tamper):
    from loom.db.schema import (
        DevInstance,
        DevLifecycleOperation,
        DevLifecycleOperationAttempt,
        PersonalDevCandidate,
    )
    from loom.personal_dev_environment_store import (
        PersonalDevEnvironmentOperationFencedError,
        SqlAlchemyPersonalDevActivationIntentReader,
    )

    claim = _bound_claim()
    claim = replace(claim, operation=replace(claim.operation, readiness_evidence_sha256="a" * 64))

    def row(model, record):
        values = {f.name: getattr(record, f.name) for f in fields(record) if f.name in model.__table__.columns}
        binding = values.get("storage_binding")
        if binding is not None:
            values.update(storage_binding=binding.model_dump(mode="json"), storage_binding_sha256=canonical_digest(binding))
        return model(**values)

    operation = row(DevLifecycleOperation, claim.operation)
    environment = row(DevInstance, claim.environment)
    attempt = row(DevLifecycleOperationAttempt, claim.attempt)
    candidate = row(PersonalDevCandidate, claim.candidate)
    if tamper == "environment_binding":
        environment.storage_binding = environment.storage_binding_sha256 = None
    elif tamper == "environment_owner":
        environment.owner_user_id = uuid4()
    elif tamper == "candidate_owner":
        candidate.owner_user_id = uuid4()
    elif tamper == "digest":
        operation.storage_binding_sha256 = "0" * 64

    # Publication is independently verified elsewhere; exercise actual ORM
    # storage deserialization and reader ownership fences, not a fake binding.
    monkeypatch.setattr("loom.personal_dev_environment_store.validate_personal_dev_candidate_publication",
                        lambda record, document: (document, candidate.publication_sha256, "unused"))

    class Session:
        async def execute(self, statement):
            return self

        def one_or_none(self):
            return operation, environment, attempt, candidate

    reader = SqlAlchemyPersonalDevActivationIntentReader(Session())
    if tamper:
        with pytest.raises(PersonalDevEnvironmentOperationFencedError):
            await reader.next_intent()
    else:
        intent = await reader.next_intent()
        assert intent.schema_version == 2
        assert intent.storage_binding == claim.operation.storage_binding
        assert intent.storage_binding_sha256 == canonical_digest(intent.storage_binding)


async def test_independent_activation_observes_exact_bound_physical_identity(monkeypatch):
    intent = _v2()
    seen = []

    async def observe(kubectl, identity, config):
        seen.append(identity)
        raise RuntimeError("stop before readiness/mutation")

    monkeypatch.setattr("loom.personal_dev_activation_agent.observe_personal_dev_candidate_generation", observe)
    executor = KubectlPersonalDevActivationExecutor(
        KubectlClient("kubectl", field_manager="loom-personal-dev-activation-agent"),
        "https://minio.example",
    )
    with pytest.raises(RuntimeError, match="stop before"):
        await executor.activate(intent)
    assert seen == [intent.storage_binding.identity]


async def test_bound_readiness_digest_covers_namespace_uid_and_storage_binding():
    from loom.dev_instance_manifest import dev_instance_manifest_documents
    from loom.dev_instance_runtime import (
        DevInstanceRuntimeError,
        observe_personal_dev_candidate_generation,
    )

    intent = _v2()
    identity = intent.storage_binding.identity
    runner = _ActivationKubectlRunner(intent)
    kubectl = KubectlClient("kubectl", field_manager="loom-personal-dev-activation-agent", runner=runner)
    executor = KubectlPersonalDevActivationExecutor(kubectl, "https://minio.example")
    config = executor._config(intent)
    namespace = dev_instance_manifest_documents(identity, config)[0]
    namespace["metadata"]["uid"] = "original-namespace"
    runner.resources[("namespace", identity.namespace)] = namespace
    original = await observe_personal_dev_candidate_generation(kubectl, identity, config)
    namespace["metadata"]["uid"] = "replacement-namespace"
    replacement = await observe_personal_dev_candidate_generation(kubectl, identity, config)
    assert replacement.resource_evidence_sha256 != original.resource_evidence_sha256
    namespace["metadata"]["annotations"]["loom.dev/storage-binding-sha256"] = "0" * 64
    with pytest.raises(DevInstanceRuntimeError):
        await observe_personal_dev_candidate_generation(kubectl, identity, config)


def test_renderer_rejects_mixed_lifecycle_and_storage_incarnations():
    from loom.dev_instance_manifest import dev_instance_manifest_documents

    intent = _v2()
    executor = KubectlPersonalDevActivationExecutor(
        KubectlClient("kubectl", field_manager="loom-personal-dev-activation-agent"), "https://minio.example",
    )
    config = executor._config(intent)
    config = replace(config, lifecycle_binding=replace(config.lifecycle_binding, subject_incarnation=uuid4()))
    with pytest.raises(ValueError, match="storage"):
        dev_instance_manifest_documents(intent.storage_binding.identity, config)
