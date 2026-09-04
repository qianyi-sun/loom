from __future__ import annotations

import base64
import json
import os
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import ClassVar
from uuid import NAMESPACE_URL, uuid5

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from sqlalchemy.engine import make_url

from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
)
from loom_cli.rollout.operator.protected_staging_capacity_runtime import (
    KubernetesProtectedStagingCapacityRuntime,
)
from loom_cli.rollout.operator.protected_staging_capacity_runtime_secret_component import (
    KubernetesProtectedStagingCapacityRuntimeSecretComponent,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_runtime import (
    _runtime,
    _write_bootstrap,
)


def _ca_certificate() -> bytes:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "loom-postgres-ca")])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return certificate.public_bytes(serialization.Encoding.PEM)


def _seed() -> dict[str, object]:
    return {
        "agent_database_password": "a" * 48,
        "agent_incarnation": str(uuid5(NAMESPACE_URL, "loom:staging:capacity-agent:v1")),
        "authority_incarnation": str(uuid5(NAMESPACE_URL, "loom:staging:capacity-authority:v1")),
        "migrator_database_password": "m" * 48,
        "observer_database_password": "o" * 48,
        "reporter_incarnation": "0d598e5b-0acd-4d37-8d6f-227e1a4f7e32",
        "reporter_token": "t" * 48,
        "runtime_database_password": "runtime-password-" + "r" * 48,
        "schema_version": 1,
        "subject_id": str(uuid5(NAMESPACE_URL, "loom:staging:capacity-subject")),
        "subject_incarnation": str(uuid5(NAMESPACE_URL, "loom:staging:capacity-subject:v1")),
    }


class _Runner:
    environment: ClassVar[dict[str, str]] = {"KUBECONFIG": "/fixed"}

    def __init__(self, ca_certificate: bytes) -> None:
        self.ca_certificate = ca_certificate
        self.target: dict[str, object] | None = None
        self.calls: list[tuple[str, ...]] = []

    def capture_stdout(self, argv, *, env, timeout_seconds):
        assert env == self.environment
        assert timeout_seconds == 30.0
        command = tuple(argv)
        self.calls.append(command)
        if "secret/loom-postgres-ca" in command:
            return base64.b64encode(self.ca_certificate)
        if "secret/loom-protected-worker-runtime" in command:
            return b"" if self.target is None else json.dumps(self.target).encode()
        raise AssertionError(f"unexpected capture: {command}")

    def run_status(self, argv, *, env, input_payload, timeout_seconds):
        assert env == self.environment
        assert timeout_seconds == 60.0
        assert input_payload is not None
        self.calls.append(tuple(argv))
        desired = json.loads(input_payload)
        observed = deepcopy(self.target)
        if observed is not None:
            metadata = observed["metadata"]
            for key in ("managedFields", "resourceVersion", "uid"):
                metadata.pop(key, None)
        return 0 if observed == desired else 1

    def capture_stdout_with_input(self, argv, *, env, input_payload, timeout_seconds):
        assert env == self.environment
        assert timeout_seconds == 60.0
        self.calls.append(tuple(argv))
        value = json.loads(input_payload)
        metadata = value["metadata"]
        metadata["uid"] = "44e2299b-2ae1-45a5-bbab-c67409ea6e72"
        metadata["resourceVersion"] = "17"
        metadata["managedFields"] = [
            {
                "apiVersion": "v1",
                "fieldsType": "FieldsV1",
                "fieldsV1": {
                    "f:data": {"f:ca.crt": {}, "f:database-url": {}},
                },
                "manager": "loom-staging-protected-runtime-secret",
                "operation": "Apply",
            }
        ]
        self.target = value
        return json.dumps(value).encode()


def _component(runner: _Runner) -> KubernetesProtectedStagingCapacityRuntimeSecretComponent:
    return KubernetesProtectedStagingCapacityRuntimeSecretComponent(
        runner=runner,
        seed_reader=_seed,
    )


def test_runtime_secret_converges_only_runtime_url_and_ca_certificate(tmp_path: Path) -> None:
    runner = _Runner(_ca_certificate())
    component = _component(runner)
    plan = _plan(tmp_path)

    assert component.classify(plan)[0] is ComponentState.READY
    component.apply(plan)

    assert component.classify(plan)[0] is ComponentState.EXACT
    assert runner.target is not None
    assert set(runner.target["data"]) == {"ca.crt", "database-url"}
    assert base64.b64decode(runner.target["data"]["ca.crt"], validate=True) == runner.ca_certificate
    database_url = base64.b64decode(runner.target["data"]["database-url"], validate=True).decode(
        "ascii"
    )
    parsed = make_url(database_url)
    assert parsed.drivername == "postgresql+psycopg"
    assert parsed.username == "loom_cap_staging_runtime"
    assert parsed.password == _seed()["runtime_database_password"]
    assert parsed.host == "loom-postgres-rw.loom-staging.svc.cluster.local"
    assert parsed.port == 5432
    assert parsed.database == "loom"
    assert parsed.query == {
        "sslmode": "verify-full",
        "sslrootcert": "/run/loom/protected-worker-runtime/files/ca.crt",
    }
    apply_calls = [call for call in runner.calls if "apply" in call]
    assert len(apply_calls) == 1
    assert "--force-conflicts" not in apply_calls[0]


def test_runtime_secret_rejects_foreign_data_field_owner(tmp_path: Path) -> None:
    runner = _Runner(_ca_certificate())
    component = _component(runner)
    plan = _plan(tmp_path)
    component.apply(plan)
    assert runner.target is not None
    runner.target["metadata"]["managedFields"][0]["manager"] = "foreign-manager"

    assert component.classify(plan)[0] is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state drifted"):
        component.apply(plan)


def test_runtime_secret_rejects_invalid_postgres_ca(tmp_path: Path) -> None:
    component = _component(_Runner(b"not-a-certificate"))

    assert component.classify(_plan(tmp_path))[0] is ComponentState.DRIFTED


def test_runtime_dispatches_third_journal_slot_to_runtime_secret_component(
    tmp_path: Path,
) -> None:
    credential_runtime = _runtime(tmp_path)
    _write_bootstrap(credential_runtime)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    credential_runtime.components(plan, epoch_guard=lambda _plan: epoch)[0].apply(plan)
    runner = _Runner(_ca_certificate())
    runtime = KubernetesProtectedStagingCapacityRuntime(
        runner=runner,  # type: ignore[arg-type]
        state_root=credential_runtime.state_root,
        candidate_root=credential_runtime.candidate_root,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        container_registry="registry.example.test/loom",
    )
    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[2]

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)
    assert component.classify(plan).state is ComponentState.EXACT
