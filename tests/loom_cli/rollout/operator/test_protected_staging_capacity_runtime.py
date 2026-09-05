from __future__ import annotations

import base64
import json
import os
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import ClassVar
from uuid import UUID

import pytest
import yaml
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from loom_capacity_manager.contracts import FleetManifestV1, canonical_digest_excluding
from loom_cli.rollout.operator.final_gate_plan import FinalGatePlan
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
)
from loom_cli.rollout.operator.protected_staging_capacity_runtime import (
    KubernetesProtectedStagingCapacityRuntime,
)
from tests.loom_cli.rollout.operator.protected_execution_prerequisite_fixtures import (
    execution_prerequisite_artifact,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _execution_plan, _lease, _plan
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_manager_runtime_component import (
    _candidate,
    _ManagerCluster,
)


class _NoCommandRunner:
    environment: ClassVar[dict[str, str]] = {}

    def __getattr__(self, name: str):
        raise AssertionError(f"protected convergence unexpectedly used {name}")


class _DatabaseRunner:
    def __init__(self, plan, seed: dict[str, object], *, database_state: str) -> None:
        self.environment = {"KUBECONFIG": "/fixed"}
        self.plan = plan
        self.seed = seed
        self.database_state = database_state
        self.objects: dict[str, dict[str, object]] = {}
        self.created_objects: dict[str, dict[str, object]] = {}
        self.calls: list[tuple[str, ...]] = []

    def _registration(self) -> dict[str, object]:
        return {
            "agent_incarnation": self.seed["agent_incarnation"],
            "allocation_epoch": 0,
            "authority_incarnation": self.seed["authority_incarnation"],
            "authority_mode": "disabled",
            "candidate_digest": self.plan.artifact_bundle_digest,
            "candidate_identity": self.plan.candidate_sha,
            "candidate_identity_algorithm": "git-sha1",
            "candidate_publication_sha256": self.plan.artifact_bundle_digest,
            "configuration_generation": self.plan.starting_mutation_epoch + 1,
            "deployment_generation": self.plan.starting_mutation_epoch + 1,
            "environment_id": "staging",
            "reporter_high_water": 0,
            "reporter_incarnation": self.seed["reporter_incarnation"],
            "schema_version": 1,
            "subject_id": self.seed["subject_id"],
            "subject_incarnation": self.seed["subject_incarnation"],
        }

    @staticmethod
    def _role(*, login: bool, inherit: bool, password: bool) -> dict[str, object]:
        return {
            "bypass_rls": False,
            "can_login": login,
            "create_db": False,
            "create_role": False,
            "has_password": password,
            "inherit": inherit,
            "memberships": 0,
            "replication": False,
            "superuser": False,
        }

    def _details(self) -> dict[str, object]:
        registration = self._registration()
        authority = {
            field: registration[field]
            for field in (
                "allocation_epoch",
                "authority_incarnation",
                "authority_mode",
                "candidate_digest",
                "configuration_generation",
                "deployment_generation",
                "environment_id",
                "reporter_high_water",
                "reporter_incarnation",
                "schema_version",
                "subject_id",
                "subject_incarnation",
            )
        }
        return {
            "agent_role": "loom_cap_staging_agent",
            "authority": authority,
            "registration": registration,
            "roles": {
                "loom_cap_staging_agent": self._role(login=True, inherit=False, password=True),
                "loom_cap_staging_executor": self._role(login=False, inherit=False, password=False),
                "loom_cap_staging_migrator": self._role(login=False, inherit=True, password=False),
                "loom_cap_staging_observer": self._role(login=True, inherit=False, password=True),
                "loom_cap_staging_owner": self._role(login=False, inherit=False, password=False),
                "loom_cap_staging_runtime": self._role(login=True, inherit=False, password=True),
            },
            "runtime_role": "loom_cap_staging_runtime",
        }

    def capture_stdout(self, argv, *, env, timeout_seconds):
        assert env == self.environment
        assert timeout_seconds == 30.0
        command = tuple(argv)
        self.calls.append(command)
        joined = " ".join(command)
        if "to_regclass" in joined:
            if self.database_state == "absent":
                return b"absent\n"
            return b"loom_capacity_guard.capacity_guard_alembic_version\n"
        if "version_num" in joined:
            return b"guard_0029\n"
        if "current_protected_runtime_registration" in joined:
            return json.dumps(self._registration(), sort_keys=True).encode()
        if "agent_runtime_authority" in joined:
            details = self._details()
            if self.database_state == "drifted":
                details["runtime_role"] = "loom_cap_other_runtime"
            return json.dumps(details, sort_keys=True).encode()
        if "get secret,job" in joined:
            return json.dumps(
                {"apiVersion": "v1", "kind": "List", "items": list(self.objects.values())},
                sort_keys=True,
            ).encode()
        raise AssertionError(f"unexpected capture: {command}")

    def run_status(self, argv, *, env, input_payload, timeout_seconds):
        assert env == self.environment
        assert timeout_seconds == 60.0
        assert input_payload is not None
        command = tuple(argv)
        self.calls.append(command)
        expected = {
            document["kind"]: document
            for document in yaml.safe_load_all(input_payload)
            if document is not None
        }
        observed = deepcopy(self.objects)
        for document in observed.values():
            document.pop("status", None)
        return 0 if expected == observed else 1

    def run_checked(self, argv, *, env, input_payload, timeout_seconds):
        assert env == self.environment
        command = tuple(argv)
        self.calls.append(command)
        if "create" in command:
            assert timeout_seconds == 60.0
            assert input_payload is not None
            self.objects = {
                document["kind"]: document
                for document in yaml.safe_load_all(input_payload)
                if document is not None
            }
            self.created_objects = deepcopy(self.objects)
            return
        if "wait" in command:
            assert timeout_seconds == 660.0
            assert input_payload is None
            self.database_state = "exact"
            job = self.objects["Job"]
            job["status"] = {"succeeded": 1}
            return
        if "delete" in command:
            assert timeout_seconds == 60.0
            assert input_payload is None
            kind = "Job" if "job" in command else "Secret"
            self.objects.pop(kind)
            return
        raise AssertionError(f"unexpected mutation: {command}")


class _ControllerPrerequisiteTransport:
    authority_sha256 = "a" * 64

    def observe(self, _request):
        raise AssertionError("controller prerequisite transport unexpectedly observed")

    def converge(self, _request):
        raise AssertionError("controller prerequisite transport unexpectedly converged")


def _controller_prerequisite_transports() -> dict[str, _ControllerPrerequisiteTransport]:
    return {
        "gb10": _ControllerPrerequisiteTransport(),
        "oldlab": _ControllerPrerequisiteTransport(),
    }


class _PoolCredentialTransport:
    def observe(self, _request):
        raise AssertionError("pool credential transport unexpectedly observed")

    def publish(self, _request):
        raise AssertionError("pool credential transport unexpectedly published")


def _pool_credential_transports() -> dict[str, _PoolCredentialTransport]:
    return {
        "gb10": _PoolCredentialTransport(),
        "oldlab": _PoolCredentialTransport(),
    }


class _PreparedControllerTransport:
    def observe(self, _request):
        raise AssertionError("prepared controller transport unexpectedly observed")

    def converge_files(self, _request):
        raise AssertionError("prepared controller transport unexpectedly converged")

    def enable_timer(self, _request):
        raise AssertionError("prepared controller timer unexpectedly enabled")

    def run_tick(self, _request):
        raise AssertionError("prepared controller tick unexpectedly ran")

    def disable_timer(self, _request):
        raise AssertionError("prepared controller timer unexpectedly disabled")


def _prepared_controller_transports() -> dict[str, _PreparedControllerTransport]:
    return {
        "gb10": _PreparedControllerTransport(),
        "oldlab": _PreparedControllerTransport(),
    }


def _runtime(
    tmp_path: Path,
    *,
    controller_prerequisite_transports: dict[str, _ControllerPrerequisiteTransport] | None = None,
    pool_credential_transports: dict[str, _PoolCredentialTransport] | None = None,
    prepared_controller_transports: dict[str, _PreparedControllerTransport] | None = None,
) -> KubernetesProtectedStagingCapacityRuntime:
    return KubernetesProtectedStagingCapacityRuntime(
        runner=_NoCommandRunner(),  # type: ignore[arg-type]
        state_root=tmp_path / "state",
        candidate_root=tmp_path / "candidate",
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        container_registry="registry.example.test/loom",
        controller_prerequisite_transports=controller_prerequisite_transports or {},
        pool_credential_transports=pool_credential_transports or {},
        prepared_controller_transports=prepared_controller_transports or {},
        execution_preparation_dependency_guard=lambda _plan, _artifact: "d" * 64,
    )


def test_runtime_builds_fixed_chain_and_epoch_drift_blocks_every_component(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.READY,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch,
    )
    components = _runtime(tmp_path).components(plan, epoch_guard=lambda _plan: epoch)

    assert tuple(component.component_id for component in components) == (
        "staging-capacity-credentials",
        "staging-capacity-database",
        "staging-protected-runtime-secret",
        "capacity-manager-runtime",
        "capacity-manager-configuration",
        "staging-capacity-agent",
    )
    assert [component.classify(plan).state for component in components] == [
        ComponentState.DRIFTED,
    ] * 6
    for component in components:
        with pytest.raises(RuntimeError, match="epoch ownership changed"):
            component.apply(plan)


def test_execution_plan_converges_both_controller_prerequisites_before_credentials(
    tmp_path: Path,
) -> None:
    plan = _execution_plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.READY,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch,
    )

    components = _runtime(
        tmp_path,
        controller_prerequisite_transports=_controller_prerequisite_transports(),
        pool_credential_transports=_pool_credential_transports(),
        prepared_controller_transports=_prepared_controller_transports(),
    ).components(plan, epoch_guard=lambda _plan: epoch)

    assert tuple(component.component_id for component in components) == (
        "staging-capacity-credentials",
        "staging-capacity-database",
        "staging-protected-runtime-secret",
        "oldlab-controller-prerequisite",
        "gb10-controller-prerequisite",
        "staging-capacity-execution-credentials",
        "capacity-manager-runtime",
        "capacity-manager-configuration",
        "staging-capacity-agent",
        "capacity-execution-preparation",
    )


def test_execution_plan_rejects_missing_controller_prerequisite_transport(
    tmp_path: Path,
) -> None:
    plan = _execution_plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )

    with pytest.raises(ValueError, match="controller prerequisite transports are incomplete"):
        _runtime(
            tmp_path,
            pool_credential_transports=_pool_credential_transports(),
            prepared_controller_transports=_prepared_controller_transports(),
        ).components(plan, epoch_guard=lambda _plan: epoch)


def test_execution_plan_rejects_missing_pool_credential_transport(tmp_path: Path) -> None:
    plan = _execution_plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )

    with pytest.raises(ValueError, match="pool credential transports are incomplete"):
        _runtime(
            tmp_path,
            controller_prerequisite_transports=_controller_prerequisite_transports(),
            prepared_controller_transports=_prepared_controller_transports(),
        ).components(plan, epoch_guard=lambda _plan: epoch)


def test_controller_prerequisite_components_dispatch_pool_specific_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _execution_plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    classified: list[tuple[str, FinalGatePlan]] = []

    class _ControllerComponent:
        def __init__(self, pool_id: str) -> None:
            self.pool_id = pool_id

        def classify(self, bound_plan: FinalGatePlan) -> tuple[ComponentState, str]:
            classified.append((self.pool_id, bound_plan))
            return ComponentState.EXACT, self.pool_id

    monkeypatch.setattr(
        KubernetesProtectedStagingCapacityRuntime,
        "_controller_prerequisite_component",
        lambda _runtime, pool_id: _ControllerComponent(pool_id),
        raising=False,
    )
    components = {
        component.component_id: component
        for component in _runtime(
            tmp_path,
            controller_prerequisite_transports=_controller_prerequisite_transports(),
            pool_credential_transports=_pool_credential_transports(),
            prepared_controller_transports=_prepared_controller_transports(),
        ).components(plan, epoch_guard=lambda _plan: epoch)
    }

    assert components["oldlab-controller-prerequisite"].classify(plan).state is ComponentState.EXACT
    assert components["gb10-controller-prerequisite"].classify(plan).state is ComponentState.EXACT
    assert classified == [("oldlab", plan), ("gb10", plan)]


def test_preparation_dependency_rechecks_every_task_43_to_45_component(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _execution_plan(tmp_path)
    runtime = _runtime(
        tmp_path,
        controller_prerequisite_transports=_controller_prerequisite_transports(),
        pool_credential_transports=_pool_credential_transports(),
        prepared_controller_transports=_prepared_controller_transports(),
    )
    calls: list[str] = []

    class _ExactComponent:
        def __init__(self, component_id: str) -> None:
            self.component_id = component_id

        def classify(self, _plan: FinalGatePlan) -> tuple[ComponentState, str]:
            calls.append(self.component_id)
            return ComponentState.EXACT, self.component_id

    monkeypatch.setattr(
        KubernetesProtectedStagingCapacityRuntime,
        "_controller_prerequisite_component",
        lambda _runtime, pool_id: _ExactComponent(f"controller-{pool_id}"),
    )
    monkeypatch.setattr(
        KubernetesProtectedStagingCapacityRuntime,
        "_execution_credential_component",
        lambda _runtime: _ExactComponent("execution-credentials"),
    )
    monkeypatch.setattr(
        KubernetesProtectedStagingCapacityRuntime,
        "_manager_runtime_component",
        lambda _runtime: _ExactComponent("manager-runtime"),
    )
    monkeypatch.setattr(
        KubernetesProtectedStagingCapacityRuntime,
        "_manager_configuration_component",
        lambda _runtime: _ExactComponent("manager-configuration"),
    )

    digest = runtime._execution_preparation_dependency(
        plan,
        execution_prerequisite_artifact(),
    )

    assert len(digest) == 64
    assert calls == [
        "controller-gb10",
        "controller-oldlab",
        "execution-credentials",
        "manager-runtime",
        "manager-configuration",
    ]


def _certificate(
    *,
    common_name: str,
    issuer_key: rsa.RSAPrivateKey,
    issuer_name: x509.Name,
    subject_key: rsa.RSAPrivateKey,
    is_ca: bool = False,
    uri_san: str | None = None,
) -> x509.Certificate:
    now = datetime.now(UTC)
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .issuer_name(issuer_name)
        .public_key(subject_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=is_ca, path_length=None), critical=True)
    )
    if not is_ca:
        builder = builder.add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False
        )
    if uri_san is not None:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.UniformResourceIdentifier(uri_san)]),
            critical=False,
        )
    return builder.sign(issuer_key, hashes.SHA256())


def _write_bootstrap(
    runtime: KubernetesProtectedStagingCapacityRuntime,
) -> dict[str, rsa.RSAPrivateKey]:
    runtime.state_root.mkdir(mode=0o700)
    runtime.credentials_root.parent.mkdir(mode=0o700)
    runtime.credentials_root.mkdir(mode=0o700)
    client_ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    client_ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "manager-client-ca")])
    client_ca_cert = _certificate(
        common_name="manager-client-ca",
        issuer_key=client_ca_key,
        issuer_name=client_ca_name,
        subject_key=client_ca_key,
        is_ca=True,
    )
    manager_ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    manager_ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "manager-server-ca")])
    manager_ca_cert = _certificate(
        common_name="manager-server-ca",
        issuer_key=manager_ca_key,
        issuer_name=manager_ca_name,
        subject_key=manager_ca_key,
        is_ca=True,
    )
    client_ca_path = runtime.credentials_root / "client-ca.pem"
    client_ca_path.write_bytes(client_ca_cert.public_bytes(serialization.Encoding.PEM))
    client_ca_path.chmod(0o600)
    clients = {
        "configuration-read": (
            "bearer-token",
            "certificate.pem",
            "manager-ca.pem",
            "private-key.pem",
        ),
        "configuration-fleet": (
            "bearer-token",
            "certificate.pem",
            "manager-ca.pem",
            "private-key.pem",
        ),
        "configuration-subject": (
            "bearer-token",
            "certificate.pem",
            "manager-ca.pem",
            "private-key.pem",
        ),
        "configuration-activate": (
            "bearer-token",
            "certificate.pem",
            "manager-ca.pem",
            "private-key.pem",
        ),
        "staging-reporter": (
            "certificate.pem",
            "manager-ca.pem",
            "private-key.pem",
        ),
        **{
            principal: (
                "bearer-token",
                "certificate.pem",
                "manager-ca.pem",
                "private-key.pem",
            )
            for principal in (
                "manager-read",
                "manager-prepare",
                "manager-activate",
                "manager-drain",
                "manager-retire",
                "manager-abort",
                "pool-executor-gb10",
                "pool-executor-oldlab",
            )
        },
    }
    private_keys: dict[str, rsa.RSAPrivateKey] = {"client-ca": client_ca_key}
    for directory_name, file_names in clients.items():
        directory = runtime.credentials_root / directory_name
        directory.mkdir(mode=0o700)
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        private_keys[directory_name] = private_key
        certificate = _certificate(
            common_name=directory_name,
            issuer_key=client_ca_key,
            issuer_name=client_ca_cert.subject,
            subject_key=private_key,
            uri_san=(
                f"spiffe://loom.openai.dev/staging/capacity/{directory_name}"
                if directory_name.startswith(("manager-", "pool-executor-"))
                else None
            ),
        )
        for file_name in file_names:
            path = directory / file_name
            if file_name == "certificate.pem":
                payload = certificate.public_bytes(serialization.Encoding.PEM)
            elif file_name == "manager-ca.pem":
                payload = manager_ca_cert.public_bytes(serialization.Encoding.PEM)
            elif file_name == "private-key.pem":
                payload = private_key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                )
            else:
                payload = f"token-{directory_name}-{'x' * 48}".encode("ascii")
            path.write_bytes(payload)
            path.chmod(0o600)
    for pool in ("gb10", "oldlab"):
        directory = runtime.credentials_root / f"pool-ownership-{pool}"
        directory.mkdir(mode=0o700)
        path = directory / "ownership-private-key"
        path.write_bytes(
            ed25519.Ed25519PrivateKey.generate().private_bytes(
                serialization.Encoding.Raw,
                serialization.PrivateFormat.Raw,
                serialization.NoEncryption(),
            )
        )
        path.chmod(0o600)
    return private_keys


def test_runtime_exposes_exact_execution_credential_metadata(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    _write_bootstrap(runtime)

    metadata_reader = getattr(runtime, "read_execution_credential_metadata", None)
    assert metadata_reader is not None, "runtime execution credential metadata reader is missing"
    metadata = metadata_reader()

    assert set(metadata) == {
        "manager-abort",
        "manager-activate",
        "manager-drain",
        "manager-prepare",
        "manager-read",
        "manager-retire",
        "pool-executor-gb10",
        "pool-executor-oldlab",
        "pool-ownership-gb10",
        "pool-ownership-oldlab",
    }


def test_credentials_component_persists_one_candidate_independent_seed(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    _write_bootstrap(runtime)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)
    exact = component.classify(plan)

    assert exact.state is ComponentState.EXACT
    assert runtime.credential_seed_path.stat().st_mode & 0o777 == 0o600
    seed = json.loads(runtime.credential_seed_path.read_text())
    assert set(seed) == {
        "agent_database_password",
        "agent_incarnation",
        "authority_incarnation",
        "migrator_database_password",
        "observer_database_password",
        "reporter_incarnation",
        "reporter_token",
        "runtime_database_password",
        "schema_version",
        "subject_id",
        "subject_incarnation",
    }
    assert seed["schema_version"] == 1
    assert plan.candidate_sha not in runtime.credential_seed_path.read_text()
    before = runtime.credential_seed_path.read_bytes()
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
    assert runtime.credential_seed_path.read_bytes() == before


def test_runtime_issues_bootstrap_authority_only_for_absent_seed_and_frozen_shadow(
    tmp_path: Path,
) -> None:
    """Catch using bootstrap mode after execution can increase capacity."""
    runtime = _runtime(tmp_path)
    _write_bootstrap(runtime)
    lease = _lease()

    digest = runtime.zero_ceiling_bootstrap_authority(lease)

    assert len(digest) == 64
    assert digest != "0" * 64
    assert runtime.zero_ceiling_bootstrap_authority(lease) == digest
    with pytest.raises(RuntimeError, match="unavailable"):
        runtime.zero_ceiling_bootstrap_authority(object())  # type: ignore[arg-type]
    runtime._create_credential_seed()
    with pytest.raises(RuntimeError, match="unavailable"):
        runtime.zero_ceiling_bootstrap_authority(lease)


def test_credentials_component_rejects_bootstrap_mode_drift(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    _write_bootstrap(runtime)
    (runtime.credentials_root / "configuration-read" / "bearer-token").chmod(0o640)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )

    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]

    assert component.classify(plan).state is ComponentState.DRIFTED


def test_credentials_component_rejects_certificate_key_mismatch(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    private_keys = _write_bootstrap(runtime)
    mismatched = private_keys["configuration-fleet"].private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    path = runtime.credentials_root / "configuration-read" / "private-key.pem"
    path.write_bytes(mismatched)
    path.chmod(0o600)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )

    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]

    assert component.classify(plan).state is ComponentState.DRIFTED


def test_credentials_component_rejects_distinct_ca_certificates_with_one_key(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    private_keys = _write_bootstrap(runtime)
    client_ca_key = private_keys["client-ca"]
    manager_ca_name = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "manager-server-ca-with-client-key")]
    )
    manager_ca = _certificate(
        common_name="manager-server-ca-with-client-key",
        issuer_key=client_ca_key,
        issuer_name=manager_ca_name,
        subject_key=client_ca_key,
        is_ca=True,
    )
    manager_ca_payload = manager_ca.public_bytes(serialization.Encoding.PEM)
    for directory_name in (
        "configuration-read",
        "configuration-fleet",
        "configuration-subject",
        "configuration-activate",
        "staging-reporter",
    ):
        path = runtime.credentials_root / directory_name / "manager-ca.pem"
        path.write_bytes(manager_ca_payload)
        path.chmod(0o600)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )

    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]

    assert component.classify(plan).state is ComponentState.DRIFTED


def test_credentials_component_rejects_certificate_from_another_ca(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    private_keys = _write_bootstrap(runtime)
    foreign_ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    foreign_ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "foreign-ca")])
    foreign_certificate = _certificate(
        common_name="configuration-read",
        issuer_key=foreign_ca_key,
        issuer_name=foreign_ca_name,
        subject_key=private_keys["configuration-read"],
    )
    path = runtime.credentials_root / "configuration-read" / "certificate.pem"
    path.write_bytes(foreign_certificate.public_bytes(serialization.Encoding.PEM))
    path.chmod(0o600)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )

    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]

    assert component.classify(plan).state is ComponentState.DRIFTED


def test_credentials_component_rejects_reused_reporter_key(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    _write_bootstrap(runtime)
    source = runtime.credentials_root / "configuration-read" / "private-key.pem"
    target = runtime.credentials_root / "staging-reporter" / "private-key.pem"
    target.write_bytes(source.read_bytes())
    target.chmod(0o600)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )

    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]

    assert component.classify(plan).state is ComponentState.DRIFTED


def test_manager_runtime_component_is_reachable_through_protected_chain(
    tmp_path: Path,
) -> None:
    seed_runtime = _runtime(tmp_path)
    _write_bootstrap(seed_runtime)
    plan = replace(
        _plan(tmp_path),
        image_digests={
            **_plan(tmp_path).image_digests,
            "loom-capacity-manager": "sha256:" + "9" * 64,
        },
    )
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    seed_runtime.components(plan, epoch_guard=lambda _plan: epoch)[0].apply(plan)
    candidate = _candidate(tmp_path)
    cluster = _ManagerCluster(candidate)
    runtime = KubernetesProtectedStagingCapacityRuntime(
        runner=cluster,  # type: ignore[arg-type]
        state_root=seed_runtime.state_root,
        candidate_root=candidate,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        container_registry="registry.example.test/loom",
    )
    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[3]

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)

    assert component.classify(plan).state is ComponentState.EXACT


def test_manager_configuration_component_is_reachable_after_manager_runtime(
    tmp_path: Path,
) -> None:
    from tests.loom_cli.rollout.operator.test_protected_staging_capacity_manager_configuration_component import (
        _active_document,
        _Client,
        _live_fleet,
    )

    seed_runtime = _runtime(tmp_path)
    _write_bootstrap(seed_runtime)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    seed_runtime.components(plan, epoch_guard=lambda _plan: epoch)[0].apply(plan)
    seed = json.loads(seed_runtime.credential_seed_path.read_text())
    fleet_payload = _live_fleet().model_dump(mode="python")
    fleet_payload["authority_incarnation"] = UUID(str(seed["authority_incarnation"]))
    fleet_payload["fleet_digest"] = "0" * 64
    provisional = FleetManifestV1.model_validate(fleet_payload)
    fleet_payload["fleet_digest"] = canonical_digest_excluding(provisional, "fleet_digest")
    fleet = FleetManifestV1.model_validate(fleet_payload)
    client = _Client(_active_document(fleet, ()))

    @contextmanager
    def client_context(**_kwargs):
        yield client

    class _ConfigurationRunner:
        environment: ClassVar[dict[str, str]] = {"KUBECONFIG": "/protected/kubeconfig"}

    runtime = KubernetesProtectedStagingCapacityRuntime(
        runner=_ConfigurationRunner(),  # type: ignore[arg-type]
        state_root=seed_runtime.state_root,
        candidate_root=seed_runtime.candidate_root,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        container_registry="registry.example.test/loom",
        manager_configuration_client_context=client_context,
    )
    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[4]

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)
    assert component.classify(plan).state is ComponentState.EXACT


def _database_component(
    tmp_path: Path,
    *,
    database_state: str,
):
    seed_runtime = _runtime(tmp_path)
    _write_bootstrap(seed_runtime)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    seed_runtime.components(plan, epoch_guard=lambda _plan: epoch)[0].apply(plan)
    seed = json.loads(seed_runtime.credential_seed_path.read_text())
    runner = _DatabaseRunner(plan, seed, database_state=database_state)
    runtime = KubernetesProtectedStagingCapacityRuntime(
        runner=runner,  # type: ignore[arg-type]
        state_root=seed_runtime.state_root,
        candidate_root=seed_runtime.candidate_root,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        container_registry="registry.example.test/loom",
    )
    return plan, runner, runtime.components(plan, epoch_guard=lambda _plan: epoch)[1]


def test_database_component_bootstraps_with_candidate_image_then_removes_credentials(
    tmp_path: Path,
) -> None:
    plan, runner, component = _database_component(tmp_path, database_state="absent")

    assert component.classify(plan).state is ComponentState.READY

    component.apply(plan)

    assert component.classify(plan).state is ComponentState.EXACT
    assert runner.objects == {}
    mutations = [call for call in runner.calls if {"create", "wait", "delete"} & set(call)]
    assert [
        next(item for item in ("create", "wait", "delete") if item in call) for call in mutations
    ] == [
        "create",
        "wait",
        "delete",
        "delete",
    ]
    secret = runner.created_objects["Secret"]
    job = runner.created_objects["Job"]
    assert secret["immutable"] is True
    configuration = json.loads(
        base64.b64decode(secret["data"]["reporter-configuration.json"], validate=True)
    )
    assert configuration["authority_mode"] == "disabled"
    assert configuration["allocation_epoch"] == 0
    assert configuration["candidate_digest"] == plan.artifact_bundle_digest
    assert configuration["candidate_identity_algorithm"] == "git-sha1"
    assert configuration["candidate_identity"] == plan.candidate_sha
    assert configuration["candidate_publication_sha256"] == plan.artifact_bundle_digest
    assert configuration["deployment_generation"] == plan.starting_mutation_epoch + 1
    assert configuration["configuration_generation"] == plan.starting_mutation_epoch + 1
    pod_spec = job["spec"]["template"]["spec"]
    assert pod_spec["automountServiceAccountToken"] is False
    assert pod_spec["securityContext"] == {
        "fsGroup": 65532,
        "fsGroupChangePolicy": "OnRootMismatch",
        "runAsGroup": 65532,
        "runAsNonRoot": True,
        "runAsUser": 65532,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    container = pod_spec["containers"][0]
    assert container["image"] == (
        "registry.example.test/loom/loom-control-plane@" + plan.image_digests["loom-control-plane"]
    )
    assert container["command"] == [
        "python",
        "-I",
        "-B",
        "-m",
        "loom.staging_capacity_database_bootstrap",
    ]
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
    }
    volumes = {volume["name"]: volume["secret"] for volume in pod_spec["volumes"]}
    assert {volume["secretName"] for volume in volumes.values()} == {
        "loom-staging-capacity-database-bootstrap",
        "loom-postgres-cnpg-credentials",
        "loom-postgres-ca",
    }
    assert volumes["bootstrap"]["items"] == [
        {"key": "reporter-configuration.json", "path": "reporter-configuration.json"},
        {"key": "seed.json", "path": "seed.json"},
    ]
    assert volumes["postgres-admin"]["items"] == [
        {"key": "password", "path": "password"},
        {"key": "username", "path": "username"},
    ]
    assert volumes["postgres-ca"]["items"] == [{"key": "ca.crt", "path": "ca.crt"}]


def test_database_component_recovers_exact_completed_residue_by_cleanup_only(
    tmp_path: Path,
) -> None:
    plan, runner, component = _database_component(tmp_path, database_state="absent")
    component.apply(plan)
    runner.objects = deepcopy(runner.created_objects)
    runner.objects["Job"]["status"] = {"succeeded": 1}
    runner.database_state = "exact"
    runner.calls.clear()

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)

    assert runner.objects == {}
    assert all("create" not in call and "wait" not in call for call in runner.calls)
    assert component.classify(plan).state is ComponentState.EXACT


def test_database_component_rejects_partial_bootstrap_resource_set(tmp_path: Path) -> None:
    plan, runner, component = _database_component(tmp_path, database_state="absent")
    runner.objects = {
        "Secret": {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "loom-staging-capacity-database-bootstrap",
                "namespace": "loom-staging",
            },
        }
    }

    assert component.classify(plan).state is ComponentState.DRIFTED


def test_database_component_rejects_immutable_database_identity_drift(
    tmp_path: Path,
) -> None:
    plan, _runner, component = _database_component(tmp_path, database_state="drifted")

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
