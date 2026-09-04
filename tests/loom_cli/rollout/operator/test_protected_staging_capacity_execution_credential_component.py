from __future__ import annotations

import base64
import importlib
import importlib.util
import json
import os
from dataclasses import replace
from pathlib import Path
from typing import ClassVar

import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519

from loom_capacity_manager.ownership import public_key_fingerprint
from loom_cli.rollout.operator.final_gate_plan import FinalGatePlan
from loom_cli.rollout.operator.protected_apply_journal import ComponentState
from loom_cli.rollout.operator.protected_execution_prerequisite_store import (
    ProtectedExecutionPrerequisiteStore,
)
from tests.loom_cli.rollout.operator.protected_execution_prerequisite_fixtures import (
    execution_prerequisite_artifact,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import (
    _artifacts,
    _attestation,
    _baseline,
    _envelope,
    _lease,
    _predecessor_evidence,
    _systemd_evidence,
)
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_execution_credentials import (
    _credentials,
)

MODULE = "loom_cli.rollout.operator.protected_staging_capacity_execution_credential_component"
_BACKUP_NAMES = (
    "loom-capacity-execution-operator",
    "loom-capacity-executor-gb10",
    "loom-capacity-executor-oldlab",
)


def _plan_and_artifact(tmp_path: Path, bundle):  # type: ignore[no-untyped-def]
    artifacts = _artifacts(tmp_path)
    lease = _lease()
    fingerprints = {
        pool: public_key_fingerprint(
            ed25519.Ed25519PrivateKey.from_private_bytes(private_key).public_key()
        )
        for pool, private_key in bundle.ownership_private_keys.items()
    }
    original = execution_prerequisite_artifact(
        core_bundle_sha256=artifacts.bundle_digest,
        backup_lease_sha256=lease.evidence_digest,
    )
    pools = tuple(
        pool.model_copy(update={"signing_key_sha256": fingerprints[pool.pool_id]})
        for pool in original.executor_profile_seed.pools
    )
    seed = replace(original.executor_profile_seed, pools=pools)
    policy = original.execution_policy.model_copy(
        update={
            "executors": tuple(
                executor.model_copy(update={"signing_key_sha256": fingerprints[executor.pool_id]})
                for executor in original.execution_policy.executors
            )
        }
    )
    artifact = replace(
        original,
        source_configuration_epoch=lease.manager_configuration_epoch,
        source_configuration_sha256=lease.manager_configuration_digest,
        credential_metadata_sha256=bundle.metadata_sha256,
        executor_profile_seed=seed,
        execution_policy=policy,
    )
    store = ProtectedExecutionPrerequisiteStore(
        tmp_path / "execution-authority",
        service_uid=os.geteuid(),
    )
    publication = store.publish(artifact)
    attestation = _attestation(artifact, execution_prerequisite_path=publication.path)
    plan = FinalGatePlan.build(
        _envelope(attestation),
        attestation,
        artifacts,
        lease,
        _baseline(),
        _systemd_evidence(),
        _predecessor_evidence(),
        execution_prerequisite_publication=publication,
        execution_prerequisite_store=store,
    )
    return plan, artifact


class _Runner:
    environment: ClassVar[dict[str, str]] = {"KUBECONFIG": "/fixed"}

    def __init__(self) -> None:
        registry = {
            "schema_version": 1,
            "principals": [
                {
                    "principal_id": "existing-operator",
                    "token_sha256": "1" * 64,
                    "scopes": ["capacity:read", "capacity:reconcile"],
                    "subject_id": None,
                    "subject_incarnation": None,
                    "demand_reporter_incarnation": None,
                    "pool_id": None,
                    "pool_reporter_incarnation": None,
                }
            ],
        }
        self.manager_data = {
            "ownership-public-keys.json": base64.b64encode(
                b'{"schema_version":1,"keys":[]}\n'
            ).decode("ascii"),
            "principals.json": base64.b64encode(
                (json.dumps(registry) + "\n").encode("ascii")
            ).decode("ascii"),
        }
        self.manager_resource_version = 1
        self.manager_owner = "kubectl-create"
        self.secrets: dict[str, dict[str, object]] = {}
        self.calls: list[tuple[str, ...]] = []
        self.fail_create: str | None = None
        self.create_counts = {name: 0 for name in _BACKUP_NAMES}

    def _manager(self) -> dict[str, object]:
        return {
            "apiVersion": "v1",
            "data": dict(self.manager_data),
            "kind": "Secret",
            "metadata": {
                "managedFields": [
                    {
                        "apiVersion": "v1",
                        "fieldsType": "FieldsV1",
                        "fieldsV1": {
                            "f:data": {
                                "f:ownership-public-keys.json": {},
                                "f:principals.json": {},
                            }
                        },
                        "manager": self.manager_owner,
                        "operation": "Update",
                    }
                ],
                "name": "loom-capacity-manager",
                "namespace": "loom-dev",
                "resourceVersion": str(self.manager_resource_version),
                "uid": "11111111-1111-4111-8111-111111111111",
            },
            "type": "Opaque",
        }

    def capture_stdout(self, argv, *, env, timeout_seconds):  # type: ignore[no-untyped-def]
        assert env == self.environment
        assert timeout_seconds == 60.0
        command = tuple(argv)
        self.calls.append(command)
        if "secret/loom-capacity-manager" in command:
            return json.dumps(self._manager(), sort_keys=True).encode("ascii")
        for name in _BACKUP_NAMES:
            if f"secret/{name}" in command:
                value = self.secrets.get(name)
                return b"" if value is None else json.dumps(value, sort_keys=True).encode("ascii")
        raise AssertionError(f"unexpected query: {command}")

    def capture_stdout_with_input(
        self,
        argv,
        *,
        env,
        input_payload,
        timeout_seconds,  # type: ignore[no-untyped-def]
    ):
        assert env == self.environment
        assert timeout_seconds == 60.0
        command = tuple(argv)
        self.calls.append(command)
        if "patch" in command:
            operations = json.loads(input_payload)
            tests = {(item["path"], item["value"]) for item in operations if item["op"] == "test"}
            assert ("/metadata/resourceVersion", str(self.manager_resource_version)) in tests
            for item in operations:
                if item["op"] == "replace":
                    self.manager_data[item["path"].removeprefix("/data/")] = item["value"]
            self.manager_resource_version += 1
            self.manager_owner = "loom-staging-capacity-execution-credentials"
            return json.dumps(self._manager(), sort_keys=True).encode("ascii")
        document = json.loads(input_payload)
        name = document["metadata"]["name"]
        if name == self.fail_create:
            raise RuntimeError("injected Secret create failure")
        assert name not in self.secrets
        self.create_counts[name] += 1
        document["metadata"].update(
            {
                "managedFields": [
                    {
                        "apiVersion": "v1",
                        "fieldsType": "FieldsV1",
                        "fieldsV1": {"f:data": {f"f:{key}": {} for key in document["data"]}},
                        "manager": "loom-staging-capacity-execution-credentials",
                        "operation": "Update",
                    }
                ],
                "resourceVersion": str(len(self.secrets) + 1),
                "uid": f"22222222-2222-4222-8222-{len(self.secrets) + 1:012d}",
            }
        )
        self.secrets[name] = document
        return json.dumps(document, sort_keys=True).encode("ascii")


def _component(tmp_path: Path, runner: _Runner):  # type: ignore[no-untyped-def]
    assert importlib.util.find_spec(MODULE) is not None, "execution credential component is missing"
    module = importlib.import_module(MODULE)
    component_type = getattr(
        module,
        "KubernetesProtectedStagingExecutionCredentialComponent",
        None,
    )
    assert component_type is not None, "execution credential component type is missing"
    credentials_module = importlib.import_module(
        "loom_cli.rollout.operator.protected_staging_capacity_execution_credentials"
    )
    bundle = credentials_module.load_execution_credential_bundle(
        _credentials(tmp_path),
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )
    plan, artifact = _plan_and_artifact(tmp_path, bundle)
    transport_module = importlib.import_module(
        "loom_cli.rollout.operator.protected_pool_credential_transport"
    )
    transport_root = tmp_path / "controller-runtime"
    transport_root.mkdir(mode=0o700)
    transports = {
        pool: transport_module.FixedLocalPoolCredentialTransport(
            pool_id=pool,
            target_directory=transport_root / pool,
            service_uid=os.geteuid(),
            service_gid=os.getegid(),
        )
        for pool in ("gb10", "oldlab")
    }
    return (
        component_type(
            runner=runner,
            credential_bundle_reader=lambda: credentials_module.load_execution_credential_bundle(
                tmp_path / "state/protected-capacity/credentials",
                expected_uid=os.geteuid(),
                expected_gid=os.getegid(),
            ),
            prerequisite_reader=lambda _plan: artifact,
            pool_credential_transports=transports,
        ),
        plan,
        transport_root,
    )


def test_component_converges_manager_authority_and_three_immutable_secrets(
    tmp_path: Path,
) -> None:
    runner = _Runner()
    component, plan, transport_root = _component(tmp_path, runner)

    assert component.classify(plan)[0] is ComponentState.READY
    component.apply(plan)

    assert component.classify(plan)[0] is ComponentState.EXACT
    principals = json.loads(
        base64.b64decode(runner.manager_data["principals.json"], validate=True)
    )["principals"]
    assert {principal["principal_id"] for principal in principals} >= {
        "manager-read",
        "manager-prepare",
        "manager-activate",
        "manager-drain",
        "manager-retire",
        "manager-abort",
        "pool-executor-gb10",
        "pool-executor-oldlab",
    }
    assert set(runner.secrets) == set(_BACKUP_NAMES)
    assert all(secret["immutable"] is True for secret in runner.secrets.values())
    assert {path.name for path in (transport_root / "gb10").iterdir()} == {
        "bearer-token",
        "client-certificate.pem",
        "client-private-key.pem",
        "manager-ca.pem",
        "ownership-private-key",
    }
    assert {path.name for path in (transport_root / "oldlab").iterdir()} == {
        "bearer-token",
        "client-certificate.pem",
        "client-private-key.pem",
        "manager-ca.pem",
        "ownership-private-key",
    }
    assert not any("rollout" in call or "systemctl" in call for call in runner.calls)


def test_component_recovers_after_partial_immutable_secret_creation(tmp_path: Path) -> None:
    runner = _Runner()
    runner.fail_create = "loom-capacity-executor-gb10"
    component, plan, _transport_root = _component(tmp_path, runner)

    with pytest.raises(RuntimeError, match="injected Secret create failure"):
        component.apply(plan)

    assert set(runner.secrets) == {"loom-capacity-execution-operator"}
    assert component.classify(plan)[0] is ComponentState.READY
    runner.fail_create = None
    component.apply(plan)
    assert component.classify(plan)[0] is ComponentState.EXACT
    assert runner.create_counts["loom-capacity-execution-operator"] == 1


def test_component_rejects_foreign_manager_secret_field_ownership(tmp_path: Path) -> None:
    runner = _Runner()
    runner.manager_owner = "foreign-controller"
    component, plan, _transport_root = _component(tmp_path, runner)

    assert component.classify(plan)[0] is ComponentState.DRIFTED
    with pytest.raises(ValueError, match="foreign field ownership"):
        component.apply(plan)
    assert not any("patch" in call for call in runner.calls)


def test_component_detects_source_rotation_after_backup_secret_creation(
    tmp_path: Path,
) -> None:
    runner = _Runner()
    component, plan, transport_root = _component(tmp_path, runner)
    original_bundle = component.credential_bundle_reader()
    rotated_root = tmp_path / "rotated"
    rotated_root.mkdir(mode=0o700)
    credentials_module = importlib.import_module(
        "loom_cli.rollout.operator.protected_staging_capacity_execution_credentials"
    )
    rotated_bundle = credentials_module.load_execution_credential_bundle(
        _credentials(rotated_root),
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )
    reads = 0

    def rotating_reader():  # type: ignore[no-untyped-def]
        nonlocal reads
        reads += 1
        return rotated_bundle if reads >= 6 else original_bundle

    component = replace(component, credential_bundle_reader=rotating_reader)

    with pytest.raises(RuntimeError, match="source changed before mutation"):
        component.apply(plan)

    assert set(runner.secrets) == {"loom-capacity-execution-operator"}
    assert not (transport_root / "gb10").exists()
    assert not (transport_root / "oldlab").exists()


def test_component_revalidates_source_after_each_pool_transport(tmp_path: Path) -> None:
    runner = _Runner()
    component, plan, transport_root = _component(tmp_path, runner)
    original_reader = component.credential_bundle_reader
    rotated_root = tmp_path / "rotated"
    rotated_root.mkdir(mode=0o700)
    credentials_module = importlib.import_module(
        "loom_cli.rollout.operator.protected_staging_capacity_execution_credentials"
    )
    rotated_bundle = credentials_module.load_execution_credential_bundle(
        _credentials(rotated_root),
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )
    rotated = False
    gb10_transport = component.pool_credential_transports["gb10"]

    class RotateAfterPublish:
        def observe(self, payload):  # type: ignore[no-untyped-def]
            return gb10_transport.observe(payload)

        def publish(self, payload):  # type: ignore[no-untyped-def]
            nonlocal rotated
            evidence = gb10_transport.publish(payload)
            rotated = True
            return evidence

    def rotating_reader():  # type: ignore[no-untyped-def]
        return rotated_bundle if rotated else original_reader()

    component = replace(
        component,
        credential_bundle_reader=rotating_reader,
        pool_credential_transports={
            "gb10": RotateAfterPublish(),
            "oldlab": component.pool_credential_transports["oldlab"],
        },
    )

    with pytest.raises(RuntimeError, match="source changed before mutation"):
        component.apply(plan)

    assert (transport_root / "gb10").is_dir()
    assert not (transport_root / "oldlab").exists()
