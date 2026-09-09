"""Trusted membership observations reject caller substitutions and stale leases."""

import base64
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from importlib import import_module
from uuid import uuid4

import pytest
import yaml

from loom.personal_dev_capacity import personal_dev_capacity_projection
from loom.personal_dev_capacity_runtime import (
    CapacityDatabaseInstallation,
    KubectlPersonalDevCapacityInstaller,
    PersonalDevCapacityRuntimeConfig,
    protected_capacity_database_admission_digest,
)
from loom.personal_dev_membership_checkpoint import PersonalDevMembershipEnvelopeV1
from loom_capacity_agent.client import DemandReporterTLSFiles
from loom_capacity_agent.contracts import AgentPoolCapabilityV1
from loom_capacity_manager.contracts import canonical_digest
from loom_capacity_manager.membership_contracts import PersonalApplicationMembershipMutationV1
from tests.unit.test_personal_dev_membership_checkpoint import membership_envelope_values
from tests.unit.test_personal_dev_reconciler import _NOW, _RUNTIME_DATABASE_URL, _claim


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
        return CapacityDatabaseInstallation(
            protected_admission_sha256=protected_capacity_database_admission_digest(
                identity=identity,
                configuration=configuration,
                runtime_database_url=_RUNTIME_DATABASE_URL,
            ),
            agent_database_url=f"postgresql+psycopg://agent:{credentials.agent_password}@db/loom_dev_alice",
            runtime_database_url=_RUNTIME_DATABASE_URL,
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
async def test_observation_reads_installed_state_and_retry_verification_is_read_only(installer):
    checkpoint = membership_envelope_values()["expected_checkpoint"]
    claim = _membership_claim()
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
async def test_retirement_preserves_credentials_and_stable_installation(installer, candidate_status):
    checkpoint = membership_envelope_values()["expected_checkpoint"]
    claim = _membership_claim()
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
        claim,
        operation=operation,
        attempt=replace(claim.attempt, operation_id=operation.id, operation_epoch=2),
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
