"""Trusted membership observations reject caller substitutions and stale leases."""

import base64
import json
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from importlib import import_module
from uuid import uuid4

import pytest
import yaml

from loom.personal_dev_capacity import personal_dev_capacity_projection
from loom.personal_dev_capacity_identity import capacity_runtime_database_url
from loom.personal_dev_capacity_runtime import (
    CapacityDatabaseInstallation,
    KubectlPersonalDevCapacityInstaller,
    PersonalDevCapacityRuntimeConfig,
    protected_capacity_database_admission_digest,
)
from loom.personal_dev_incarnation_storage import (
    PersonalDevStorageBindingV1,
    personal_dev_storage_secret_data,
)
from loom.personal_dev_membership_checkpoint import PersonalDevMembershipEnvelopeV1
from loom_capacity_agent.client import DemandReporterTLSFiles
from loom_capacity_agent.contracts import AgentPoolCapabilityV1
from loom_capacity_manager.contracts import canonical_digest
from loom_capacity_manager.membership_contracts import PersonalApplicationMembershipMutationV1
from tests.unit.test_personal_dev_membership_checkpoint import membership_envelope_values
from tests.unit.test_personal_dev_reconciler import _NOW, _RUNTIME_DATABASE_URL, _claim


@pytest.mark.parametrize("value", ((), (1, 1), (1, 2, 3), (0,), (-1,), (True,), (3,), [1]))
def test_retirement_rejects_unbounded_or_nonmonotonic_generation_sets(value):
    module = import_module("loom.personal_dev_membership_runtime")
    with pytest.raises(ValueError, match="retirement"):
        module._retirement_generations(value, target=3)


def _successor_retirement_claim():
    from loom.personal_dev_membership_successor import PersonalDevMembershipSuccessorBindingV1
    from tests.unit.test_personal_dev_membership_successor import successor_case

    parent, accepted, values = successor_case("destroy", "terminal-not-committed")
    binding = PersonalDevMembershipSuccessorBindingV1.model_validate_json(json.dumps(values))
    child = replace(
        parent.operation, id=uuid4(), operation_epoch=3, expected_operation_epoch=2,
        capacity_membership_envelope=None, membership_predecessor_operation_id=parent.operation.id,
        membership_accepted_operation_id=accepted.id,
        membership_predecessor_envelope_sha256=binding.predecessor_envelope_sha256,
        membership_successor_binding=binding, membership_successor_binding_sha256=canonical_digest(binding),
        membership_continuation_kind="destroy",
    )
    return _current_claim(parent, child), binding


def _current_claim(claim, operation):
    return replace(claim, operation=operation,
                   environment=replace(claim.environment, operation_id=operation.id, operation_epoch=operation.operation_epoch),
                   attempt=replace(claim.attempt, operation_id=operation.id, operation_epoch=operation.operation_epoch))


def _bind_storage(claim, installer):
    op = claim.operation
    binding = PersonalDevStorageBindingV1(layout="incarnation-v1", environment_name=op.environment_name,
        subject_id=op.subject_id, subject_incarnation=op.subject_incarnation,
        owner_user_id=op.owner_user_id, owner_team_id=op.owner_team_id)
    claim = replace(claim, operation=replace(op, storage_binding=binding),
                    environment=replace(claim.environment, storage_binding=binding))
    installer._kubectl.storage_identity = binding.identity
    installer._kubectl.secrets["loom-protected-worker-runtime"] = {
        **personal_dev_storage_secret_data(binding.identity),
        "database-url": capacity_runtime_database_url(_RUNTIME_DATABASE_URL, binding.identity, "r" * 48).encode(),
    }
    return claim


def test_successor_retirement_uses_only_exact_reviewed_adopted_and_failed_generations():
    from loom.personal_dev_membership_runtime import retirement_from_generation

    claim, binding = _successor_retirement_claim()
    assert retirement_from_generation(claim, binding.authority.execution) == (1, 2)


@pytest.mark.parametrize("change", (
    "digest", "predecessor", "envelope", "accepted", "owner", "team", "subject", "incarnation",
    "candidate", "publication", "deployment", "reporter", "admission", "epoch", "continuation", "execution",
))
def test_successor_retirement_rejects_reviewed_binding_drift(change):
    from loom.personal_dev_membership_runtime import retirement_from_generation

    claim, binding = _successor_retirement_claim()
    fields = {
        "digest": {"membership_successor_binding_sha256": "e" * 64},
        "predecessor": {"membership_predecessor_operation_id": uuid4()},
        "envelope": {"membership_predecessor_envelope_sha256": "e" * 64},
        "accepted": {"membership_accepted_operation_id": uuid4()},
        "owner": {"owner_user_id": uuid4()}, "team": {"owner_team_id": uuid4()},
        "subject": {"subject_id": uuid4()}, "incarnation": {"subject_incarnation": uuid4()},
        "candidate": {"candidate_sha": "e" * 64}, "deployment": {"deployment_generation": 2},
        "reporter": {"capacity_reporter_incarnation": uuid4()},
        "admission": {"protected_admission_sha256": "e" * 64},
        "epoch": {"operation_epoch": 4}, "continuation": {"membership_continuation_kind": "update"},
    }
    claim = replace(claim, operation=replace(claim.operation, **fields.get(change, {})))
    if change == "publication":
        claim = replace(claim, candidate=replace(claim.candidate, publication_sha256="e" * 64))
    execution = binding.authority.execution
    if change == "execution":
        execution = execution.model_copy(update={"execution_epoch": 100})
    with pytest.raises(ValueError, match="successor"):
        retirement_from_generation(claim, execution)


def _membership_claim():
    claim = _claim(state="activating")
    return replace(claim, operation=replace(claim.operation, capacity_mode="membership-v1"))


def test_observation_requires_trusted_execution_pin():
    module = import_module("loom.personal_dev_membership_runtime")
    checkpoint = membership_envelope_values()["expected_checkpoint"]
    claim = _membership_claim()
    module.validate_membership_observation_context(claim, checkpoint, checkpoint.execution, _NOW)
    for pin in (
        None,
        checkpoint.execution.model_copy(update={"execution_manifest_sha256": "e" * 64}),
    ):
        with pytest.raises(ValueError, match="execution"):
            module.validate_membership_observation_context(claim, checkpoint, pin, _NOW)


@pytest.mark.parametrize("change", ("expired", "attempt", "operation", "mode", "pending"))
def test_observation_rejects_stale_or_pending_claim(change):
    module = import_module("loom.personal_dev_membership_runtime")
    checkpoint = membership_envelope_values()["expected_checkpoint"]
    claim = _membership_claim()
    if change == "expired":
        claim = replace(
            claim, attempt=replace(claim.attempt, lease_expires_at=_NOW - timedelta(seconds=1))
        )
    elif change == "attempt":
        claim = replace(claim, attempt=replace(claim.attempt, operation_epoch=99))
    elif change == "operation":
        claim = replace(claim, operation=replace(claim.operation, operation_epoch=99))
    elif change == "mode":
        claim = replace(claim, operation=replace(claim.operation, capacity_mode="shadow-v1"))
    else:
        claim = replace(
            claim, operation=replace(claim.operation, capacity_membership_envelope=object())
        )
    with pytest.raises(ValueError):
        module.validate_membership_observation_context(
            claim, checkpoint, checkpoint.execution, _NOW
        )


class _Kubectl:
    def __init__(self):
        self.secrets = {
            "loom-protected-worker-runtime": {"database-url": _RUNTIME_DATABASE_URL.encode()}
        }
        self.resources = {}
        self.applies = 0
        self.waits = 0

    async def read_secret_optional(self, namespace, name):
        return self.secrets.get(name)

    async def read_storage_namespace(self, identity):
        assert identity == self.storage_identity
        return {"metadata": {"name": identity.namespace, "uid": "fixture-uid"}}

    async def read_resource_json(self, *, namespace, kind, name):
        return deepcopy(self.resources[(kind, name)])

    async def apply(self, payload):
        self.applies += 1
        for document in yaml.safe_load_all(payload):
            name = document["metadata"]["name"]
            if document["kind"] == "Secret":
                self.secrets[name] = {
                    key: base64.b64decode(value) for key, value in document["data"].items()
                }
            else:
                self.resources[(document["kind"], name)] = document

    async def wait_deployment(self, namespace, name):
        self.waits += 1


class _Database:
    def __init__(self):
        self.convergences = 0
        self.observations = []

    async def converge(self, *, identity, configuration, credentials, **kwargs):
        self.convergences += 1
        runtime_url = capacity_runtime_database_url(_RUNTIME_DATABASE_URL, identity, credentials.runtime_password)
        return CapacityDatabaseInstallation(
            protected_admission_sha256=protected_capacity_database_admission_digest(
                identity=identity,
                configuration=configuration,
                runtime_database_url=runtime_url,
            ),
            agent_database_url=f"postgresql+psycopg://agent:{credentials.agent_password}@db/{identity.database}",
            runtime_database_url=runtime_url,
        )

    async def observe_membership(self, **kwargs):
        self.observations.append(kwargs)
        return {"legacy_writer_high_water": 0, "reporter_high_water": 7}


@pytest.fixture
def installer(tmp_path):
    def credential(name):
        path = tmp_path / name
        path.write_text(name)
        path.chmod(0o600)
        return path

    checkpoint = membership_envelope_values()["expected_checkpoint"]
    return KubectlPersonalDevCapacityInstaller(
        kubectl=_Kubectl(),
        database=_Database(),
        membership_execution=checkpoint.execution,
        config=PersonalDevCapacityRuntimeConfig(
            manager_origin="https://loom-capacity-manager.loom-dev.svc.cluster.local:8443",
            tls_files=DemandReporterTLSFiles(
                ca_file=credential("ca.pem"),
                certificate_file=credential("certificate.pem"),
                private_key_file=credential("private-key.pem"),
            ),
            trusted_agent_image="registry.example/loom-service@sha256:" + "1" * 64,
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
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("bound_storage", (False, True))
async def test_observation_reads_installed_state_and_retry_verification_is_read_only(installer, bound_storage):
    checkpoint = membership_envelope_values()["expected_checkpoint"]
    claim = _membership_claim()
    if bound_storage:
        claim = _bind_storage(claim, installer)
    installation = await installer.converge(claim)
    observation = await installer.observe_membership(
        claim, installation, checkpoint, observed_at=_NOW
    )
    assert observation.acknowledgement.legacy_writer_high_water == 0
    assert observation.observation_lease_epoch == claim.attempt.lease_epoch
    assert observation.execution == checkpoint.execution
    projection = personal_dev_capacity_projection(
        claim, installation, expected_configuration_epoch=checkpoint.execution.configuration_epoch
    )
    request = PersonalApplicationMembershipMutationV1(
        execution=checkpoint.execution,
        namespace_id=checkpoint.namespace_id,
        expected_revision=checkpoint.revision,
        projection=projection,
        acknowledgement=observation.acknowledgement,
    )
    envelope = PersonalDevMembershipEnvelopeV1(
        management_principal_id="manager",
        idempotency_key=uuid4(),
        expected_checkpoint=checkpoint,
        request=request,
        request_sha256=canonical_digest(request),
        observation=observation,
    )
    applies = installer._kubectl.applies
    claim = replace(claim, attempt=replace(claim.attempt, lease_epoch=99))
    await installer.verify_membership_publishing(claim, envelope)
    assert installer._kubectl.applies == applies
    assert installer._database.convergences == 1
    assert installer._kubectl.waits == 1
    wrong_request = request.model_copy(
        update={
            "projection": projection.model_copy(update={"candidate_publication_sha256": "e" * 64})
        }
    )
    with pytest.raises(ValueError, match="persisted operation"):
        await installer.verify_membership_publishing(
            claim, envelope.model_copy(update={"request": wrong_request})
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption",
    ("publication", "manifest", "image", "installation", "token", "extra-env", "privileged"),
)
async def test_observation_rejects_forged_installed_evidence(installer, corruption):
    checkpoint = membership_envelope_values()["expected_checkpoint"]
    claim = _membership_claim()
    installation = await installer.converge(claim)
    if corruption == "publication":
        claim = replace(claim, candidate=replace(claim.candidate, publication_sha256="e" * 64))
    elif corruption == "manifest":
        checkpoint = checkpoint.model_copy(
            update={
                "execution": checkpoint.execution.model_copy(
                    update={"execution_manifest_sha256": "e" * 64}
                )
            }
        )
    elif corruption == "image":
        installer._kubectl.resources[("Deployment", "loom-capacity-agent")]["spec"]["template"][
            "spec"
        ]["containers"][0]["image"] = "forged:latest"
    elif corruption == "installation":
        installation = replace(installation, capacity_agent_installation_sha256="e" * 64)
    elif corruption == "token":
        installation = replace(installation, reporter_token="forged" * 8)
    elif corruption == "extra-env":
        installer._kubectl.resources[("Deployment", "loom-capacity-agent")]["spec"]["template"][
            "spec"
        ]["containers"][0]["env"] = [{"name": "PYTHONPATH", "value": "/untrusted"}]
    else:
        installer._kubectl.resources[("Deployment", "loom-capacity-agent")]["spec"]["template"][
            "spec"
        ]["containers"][0]["securityContext"]["privileged"] = True
    with pytest.raises((ValueError, RuntimeError)):
        await installer.observe_membership(claim, installation, checkpoint, observed_at=_NOW)
    assert not installer._database.observations


@pytest.mark.asyncio
@pytest.mark.parametrize("candidate_status", ("ready", "failed"))
@pytest.mark.parametrize("bound_storage", (False, True))
async def test_retirement_preserves_credentials_and_stable_installation(installer, candidate_status, bound_storage):
    checkpoint = membership_envelope_values()["expected_checkpoint"]
    claim = _membership_claim()
    if bound_storage:
        claim = _bind_storage(claim, installer)
    installation = await installer.converge(claim)
    secrets = deepcopy(installer._kubectl.secrets)
    prior_deployment = deepcopy(installer._kubectl.resources[("Deployment", "loom-capacity-agent")])
    operation = replace(
        claim.operation,
        id=uuid4(),
        kind="destroy",
        operation_epoch=2,
        expected_operation_epoch=1,
        capacity_reporter_incarnation=installation.reporter_incarnation,
        capacity_reporter_token_sha256=installation.reporter_token_sha256,
        protected_admission_sha256=installation.protected_admission_sha256,
        capacity_agent_installation_sha256=installation.capacity_agent_installation_sha256,
        capacity_supported_pool_ids=installation.supported_pool_ids,
        capacity_supported_architectures=installation.supported_architectures,
    )
    claim = replace(
        _current_claim(claim, operation),
        candidate=replace(claim.candidate, status=candidate_status),
    )
    observation = await installer.observe_membership_retirement(claim, checkpoint, observed_at=_NOW)
    assert observation.acknowledgement.configuration_generation == 2
    assert (
        observation.capacity_agent_installation_sha256
        == installation.capacity_agent_installation_sha256
    )
    assert installer._database.convergences == 1
    assert installer._database.observations[0]["retirement_from_generation"] == 1
    for name, fields in secrets.items():
        for key, value in fields.items():
            if key != "reporter-configuration.json":
                assert installer._kubectl.secrets[name][key] == value
    assert (
        await installer.observe_membership_retirement(claim, checkpoint, observed_at=_NOW)
    ) == observation
    # Crash after the new Secret was applied but before the Deployment changed.
    installer._kubectl.resources[("Deployment", "loom-capacity-agent")] = prior_deployment
    assert (
        await installer.observe_membership_retirement(claim, checkpoint, observed_at=_NOW)
    ) == observation


@pytest.mark.asyncio
@pytest.mark.parametrize("secret_generation,deployment_generation", ((1, 1), (2, 2), (2, 1), (1, 2)))
@pytest.mark.parametrize("credential_operation", ("retained", "foreign"))
async def test_successor_retirement_resumes_exact_retained_kubernetes_generations(
    installer, secret_generation, deployment_generation, credential_operation,
):
    from loom.personal_dev_membership_successor import PersonalDevMembershipSuccessorBindingV1
    from loom_capacity_manager.executable_contracts import canonical_executable_digest

    claim, binding = _successor_retirement_claim()
    operation = claim.operation
    retained_fields = (
        "capacity_reporter_incarnation", "capacity_reporter_token_sha256", "protected_admission_sha256",
        "capacity_agent_installation_sha256", "capacity_supported_pool_ids", "capacity_supported_architectures",
    )
    lineage_fields = (
        "membership_predecessor_operation_id", "membership_accepted_operation_id",
        "membership_predecessor_envelope_sha256", "membership_successor_binding",
        "membership_successor_binding_sha256", "membership_continuation_kind",
    )
    initial_operation = replace(
        operation, id=binding.accepted_operation_id, kind="create", operation_epoch=1,
        expected_operation_epoch=0, **dict.fromkeys((*retained_fields, *lineage_fields)),
    )
    initial = _current_claim(claim, initial_operation)
    installation = await installer.converge(initial)
    snapshots = {1: (deepcopy(installer._kubectl.secrets), deepcopy(installer._kubectl.resources))}
    installed_values = {
        "capacity_reporter_incarnation": installation.reporter_incarnation,
        "capacity_reporter_token_sha256": installation.reporter_token_sha256,
        "protected_admission_sha256": installation.protected_admission_sha256,
        "capacity_agent_installation_sha256": installation.capacity_agent_installation_sha256,
        "capacity_supported_pool_ids": installation.supported_pool_ids,
        "capacity_supported_architectures": installation.supported_architectures,
    }
    prior = replace(operation, id=binding.predecessor_operation_id, operation_epoch=2,
                    expected_operation_epoch=1, **installed_values, **dict.fromkeys(lineage_fields))
    prior_claim = _current_claim(claim, prior)
    checkpoint = membership_envelope_values()["expected_checkpoint"]
    await installer.observe_membership_retirement(prior_claim, checkpoint, observed_at=_NOW)
    snapshots[2] = (deepcopy(installer._kubectl.secrets), deepcopy(installer._kubectl.resources))

    # Independently reviewed adoption must identify the actual retained installation.
    member = binding.adopted_member
    member = member.model_copy(update={
        "configuration": member.configuration.model_copy(update={"demand_reporter_incarnation": installation.reporter_incarnation}),
        "acknowledgement": member.acknowledgement.model_copy(update={
            "reporter_incarnation": installation.reporter_incarnation,
            "protected_admission_sha256": installation.protected_admission_sha256,
        }),
    })
    preparation = binding.authority.preparation.model_copy(update={"subject_acknowledgements": (member.acknowledgement,)})
    execution = binding.authority.execution.model_copy(update={"execution_manifest_sha256": canonical_executable_digest(preparation)})
    authority = binding.authority.model_copy(update={"preparation": preparation, "execution": execution})
    configuration = binding.current_configuration.model_copy(update={
        "subjects": (binding.current_configuration.subjects[0].model_copy(update={"digest": canonical_digest(member.configuration)}),),
    })
    binding = PersonalDevMembershipSuccessorBindingV1.model_validate_json(binding.model_copy(update={
        "authority": authority, "current_configuration": configuration, "adopted_member": member,
    }).model_dump_json())
    operation = replace(operation, **installed_values, membership_successor_binding=binding,
                        membership_successor_binding_sha256=canonical_digest(binding))
    claim = _current_claim(claim, operation)
    installer._membership_execution = execution
    checkpoint = checkpoint.model_copy(update={"execution": execution, "namespace_id": authority.namespace_id})
    installer._kubectl.secrets = deepcopy(snapshots[secret_generation][0])
    installer._kubectl.resources = deepcopy(snapshots[deployment_generation][1])
    installer._database.observations.clear()
    if credential_operation == "foreign":
        from loom.personal_dev_capacity_runtime import _CREDENTIALS_SECRET_NAME, _SECRET_NAME

        foreign = str(uuid4()).encode("ascii")
        for name in (_CREDENTIALS_SECRET_NAME, _SECRET_NAME):
            installer._kubectl.secrets[name]["operation-id"] = foreign
        applies = installer._kubectl.applies
        with pytest.raises(ValueError, match="retained credential operation"):
            await installer.observe_membership_retirement(claim, checkpoint, observed_at=_NOW)
        assert not installer._database.observations
        assert installer._kubectl.applies == applies
        return
    observed = await installer.observe_membership_retirement(claim, checkpoint, observed_at=_NOW)
    assert observed.acknowledgement.configuration_generation == 3
    assert installer._database.observations[0]["retirement_from_generation"] == (1, 2)
    assert installer._database.convergences == 1
    # An interrupted target apply can leave the target Secret beside either retained Deployment.
    for generation in (1, 2, 3):
        if generation < 3:
            installer._kubectl.resources = deepcopy(snapshots[generation][1])
        assert await installer.observe_membership_retirement(claim, checkpoint, observed_at=_NOW) == observed
