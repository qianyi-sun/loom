from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import shutil
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from typing import ClassVar
from uuid import UUID

import pytest
import yaml

from loom_cli.capacity_control_plane import (
    load_capacity_control_plane_profile,
    render_capacity_control_plane_manifests,
)
from loom_cli.rollout.operator.protected_apply_journal import ComponentState
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan

MODULE = "loom_cli.rollout.operator.protected_staging_capacity_manager_runtime_component"


def _mutate_registry():
    assert importlib.util.find_spec(MODULE) is not None, "manager runtime component is missing"
    module = importlib.import_module(MODULE)
    mutation = getattr(module, "_principal_registry_with_staging_reporter", None)
    assert mutation is not None, "principal registry mutation helper is missing"
    return mutation


def _component_type():
    module = importlib.import_module(MODULE)
    component = getattr(
        module,
        "KubernetesProtectedStagingCapacityManagerRuntimeComponent",
        None,
    )
    assert component is not None, "manager runtime component type is missing"
    return component


def _registry() -> dict[str, object]:
    return {
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
            },
            {
                "principal_id": "configuration-activate",
                "token_sha256": "2" * 64,
                "scopes": ["capacity:configure:activate"],
                "subject_id": None,
                "subject_incarnation": None,
                "demand_reporter_incarnation": None,
                "pool_id": None,
                "pool_reporter_incarnation": None,
            },
            {
                "principal_id": "existing-demand-reporter",
                "token_sha256": "3" * 64,
                "scopes": ["capacity:report:demand"],
                "subject_id": "00000000-0000-4000-8000-000000000201",
                "subject_incarnation": "00000000-0000-4000-8000-000000000202",
                "demand_reporter_incarnation": "00000000-0000-4000-8000-000000000203",
                "pool_id": None,
                "pool_reporter_incarnation": None,
            },
        ],
    }


def _seed() -> dict[str, object]:
    return {
        "authority_incarnation": "841e79c2-8a76-4eeb-af56-f6d03bcb1bd8",
        "reporter_token": "staging-reporter-token-" + "x" * 48,
        "reporter_incarnation": "00000000-0000-4000-8000-000000000303",
        "subject_id": "00000000-0000-4000-8000-000000000301",
        "subject_incarnation": "00000000-0000-4000-8000-000000000302",
    }


def _candidate(tmp_path: Path) -> Path:
    candidate = tmp_path / "candidate"
    profile = candidate / "deploy" / "dev-fleet" / "capacity-control-plane.toml"
    profile.parent.mkdir(parents=True)
    shutil.copyfile("deploy/dev-fleet/capacity-control-plane.toml", profile)
    return candidate


def _plan_with_manager(tmp_path: Path):
    plan = _plan(tmp_path)
    return replace(
        plan,
        image_digests={
            **plan.image_digests,
            "loom-capacity-manager": "sha256:" + "9" * 64,
        },
    )


_SECRET_KEYS = {
    "client-ca.pem",
    "database-url",
    "global-execution-signing-key",
    "health-certificate.pem",
    "health-private-key.pem",
    "ownership-public-keys.json",
    "postgres-database",
    "postgres-password",
    "postgres-user",
    "principals.json",
    "server-ca.pem",
    "server-certificate.pem",
    "server-private-key.pem",
}


class _ManagerCluster:
    environment: ClassVar[dict[str, str]] = {"KUBECONFIG": "/exact"}

    def __init__(self, candidate: Path, *, conflicting_principal: bool = False) -> None:
        registry = _registry()
        if conflicting_principal:
            registry["principals"].append(  # type: ignore[union-attr]
                {
                    "principal_id": "staging-demand-reporter",
                    "token_sha256": "3" * 64,
                    "scopes": ["capacity:report:demand"],
                    "subject_id": "00000000-0000-4000-8000-000000000401",
                    "subject_incarnation": "00000000-0000-4000-8000-000000000402",
                    "demand_reporter_incarnation": "00000000-0000-4000-8000-000000000403",
                    "pool_id": None,
                    "pool_reporter_incarnation": None,
                }
            )
        self.secret_uid = "34044ac3-1a1a-4fbe-ac27-05d03312cfe2"
        self.secret_resource_version = "17"
        self.deployment_uid = "2743d330-ecb6-468c-bddc-4234fc1029ee"
        self.deployment_resource_version = "29"
        self.deployment_generation = 6
        self.calls: list[tuple[tuple[str, ...], bytes | None]] = []
        self.rollout_status_calls = 0
        self.race_secret = False
        self.race_compensation_secret = False
        self.race_deployment = False
        self.secret_patch_calls = 0
        self.unknown_deployment_manager = False
        self.secret_owner = "kubectl-patch"
        self.deployment_owner = "loom-capacity-control-plane"
        self.deployment_runtime_owner = False
        encoded_registry = _encode(
            (json.dumps(registry, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
        )
        self.secret_data = {
            key: (_encode(f"unchanged-{key}".encode("ascii"))) for key in _SECRET_KEYS
        }
        self.secret_data["principals.json"] = encoded_registry
        profile = load_capacity_control_plane_profile(
            candidate / "deploy" / "dev-fleet" / "capacity-control-plane.toml"
        )
        rendered = render_capacity_control_plane_manifests(
            profile,
            manager_image=("registry.example.test/loom/loom-capacity-manager@sha256:" + "8" * 64),
            authority_incarnation=UUID("841e79c2-8a76-4eeb-af56-f6d03bcb1bd8"),
        )
        self.deployment = self._server_normalize(
            next(
                document
                for document in yaml.safe_load_all(rendered)
                if document is not None
                and document.get("kind") == "Deployment"
                and document.get("metadata", {}).get("name") == "loom-capacity-manager"
            )
        )

    @staticmethod
    def _server_normalize(deployment: dict[str, object]) -> dict[str, object]:
        normalized = deepcopy(deployment)
        spec = normalized["spec"]
        assert isinstance(spec, dict)
        spec.setdefault("progressDeadlineSeconds", 600)
        return normalized

    def _secret(self) -> dict[str, object]:
        return {
            "apiVersion": "v1",
            "data": deepcopy(self.secret_data),
            "kind": "Secret",
            "metadata": {
                "managedFields": [
                    {
                        "apiVersion": "v1",
                        "fieldsType": "FieldsV1",
                        "fieldsV1": {"f:data": {"f:database-url": {}}},
                        "manager": "kubectl-create",
                        "operation": "Update",
                    },
                    {
                        "apiVersion": "v1",
                        "fieldsType": "FieldsV1",
                        "fieldsV1": {
                            "f:data": {
                                "f:client-ca.pem": {},
                                "f:global-execution-signing-key": {},
                                "f:principals.json": {},
                            }
                        },
                        "manager": self.secret_owner,
                        "operation": "Update",
                    },
                ],
                "name": "loom-capacity-manager",
                "namespace": "loom-dev",
                "resourceVersion": self.secret_resource_version,
                "uid": self.secret_uid,
            },
            "type": "Opaque",
        }

    def _deployment(self) -> dict[str, object]:
        deployment = deepcopy(self.deployment)
        metadata = deployment["metadata"]
        metadata.update(
            {
                "generation": self.deployment_generation,
                "managedFields": [
                    {
                        "apiVersion": "apps/v1",
                        "fieldsType": "FieldsV1",
                        "fieldsV1": {"f:spec": {}},
                        "manager": (
                            "foreign-deployment-manager"
                            if self.unknown_deployment_manager
                            else self.deployment_owner
                        ),
                        "operation": (
                            "Update"
                            if self.deployment_owner == "loom-staging-capacity-manager-runtime"
                            else "Apply"
                        ),
                    },
                    *(
                        [
                            {
                                "apiVersion": "apps/v1",
                                "fieldsType": "FieldsV1",
                                "fieldsV1": {"f:spec": {}},
                                "manager": "loom-staging-capacity-manager-runtime",
                                "operation": "Update",
                            }
                        ]
                        if self.deployment_runtime_owner
                        else []
                    ),
                    {
                        "apiVersion": "apps/v1",
                        "fieldsType": "FieldsV1",
                        "fieldsV1": {"f:status": {}},
                        "manager": "k3s",
                        "operation": "Update",
                        "subresource": "status",
                    },
                ],
                "resourceVersion": self.deployment_resource_version,
                "uid": self.deployment_uid,
            }
        )
        deployment["status"] = {
            "availableReplicas": 1,
            "observedGeneration": self.deployment_generation,
            "readyReplicas": 1,
            "replicas": 1,
            "updatedReplicas": 1,
        }
        return deployment

    def capture_stdout(self, argv, *, env, timeout_seconds):
        assert env == self.environment
        assert timeout_seconds == 60.0
        command = tuple(argv)
        self.calls.append((command, None))
        if "secret/loom-capacity-manager" in command:
            return json.dumps(self._secret(), sort_keys=True).encode("ascii")
        if "deployment/loom-capacity-manager" in command:
            return json.dumps(self._deployment(), sort_keys=True).encode("ascii")
        raise AssertionError(command)

    def capture_stdout_with_input(
        self,
        argv,
        *,
        env,
        input_payload,
        timeout_seconds,
    ):
        assert env == self.environment
        assert timeout_seconds == 60.0
        command = tuple(argv)
        self.calls.append((command, input_payload))
        if "patch" in command:
            operations = json.loads(input_payload)
            self.secret_patch_calls += 1
            if self.race_secret:
                self.secret_resource_version = "18"
            elif self.race_compensation_secret and self.secret_patch_calls == 2:
                self.secret_resource_version = "19"
            expected_tests = {
                (operation["path"], operation["value"])
                for operation in operations
                if operation["op"] == "test"
            }
            if (
                ("/metadata/uid", self.secret_uid) not in expected_tests
                or (
                    "/metadata/resourceVersion",
                    self.secret_resource_version,
                )
                not in expected_tests
                or (
                    "/data/principals.json",
                    self.secret_data["principals.json"],
                )
                not in expected_tests
            ):
                raise RuntimeError("protected Secret resource version changed")
            replacement = next(
                operation["value"]
                for operation in operations
                if operation["op"] == "replace" and operation["path"] == "/data/principals.json"
            )
            self.secret_data["principals.json"] = replacement
            self.secret_owner = "loom-staging-capacity-manager-runtime"
            self.secret_resource_version = str(int(self.secret_resource_version) + 1)
            return json.dumps(self._secret(), sort_keys=True).encode("ascii")
        if "replace" in command and "--dry-run=server" in command:
            return json.dumps(
                self._server_normalize(json.loads(input_payload)), sort_keys=True
            ).encode("ascii")
        if "replace" in command:
            desired = json.loads(input_payload)
            if self.race_deployment:
                self.deployment_resource_version = "30"
            metadata = desired["metadata"]
            if (
                metadata.get("uid") != self.deployment_uid
                or metadata.get("resourceVersion") != self.deployment_resource_version
            ):
                raise RuntimeError("protected Deployment resource version changed")
            metadata.pop("uid")
            metadata.pop("resourceVersion")
            self.deployment = self._server_normalize(desired)
            self.deployment_runtime_owner = True
            self.deployment_generation += 1
            self.deployment_resource_version = str(int(self.deployment_resource_version) + 1)
            return json.dumps(self._deployment(), sort_keys=True).encode("ascii")
        raise AssertionError(command)

    def run_checked(self, argv, *, env, input_payload, timeout_seconds):
        assert env == self.environment
        assert input_payload is None
        assert timeout_seconds == 660.0
        command = tuple(argv)
        self.calls.append((command, None))
        assert "rollout" in command and "status" in command
        self.rollout_status_calls += 1


def _encode(payload: bytes) -> str:
    import base64

    return base64.b64encode(payload).decode("ascii")


def test_registry_mutation_preserves_existing_principals_and_adds_one_bound_reporter() -> None:
    registry = _registry()
    seed = _seed()

    mutated = _mutate_registry()(
        json.dumps(registry).encode("ascii"),
        seed=seed,
    )

    parsed = json.loads(mutated)
    assert parsed["principals"][-1] == {
        "demand_reporter_incarnation": seed["reporter_incarnation"],
        "executor_id": None,
        "executor_incarnation": None,
        "executor_pool_generation": None,
        "pool_id": None,
        "pool_reporter_incarnation": None,
        "principal_id": "staging-demand-reporter",
        "scopes": ["capacity:report:demand"],
        "subject_id": seed["subject_id"],
        "subject_incarnation": seed["subject_incarnation"],
        "token_sha256": hashlib.sha256(str(seed["reporter_token"]).encode("ascii")).hexdigest(),
    }
    principals = {principal["principal_id"]: principal for principal in parsed["principals"]}
    assert principals["existing-operator"] == registry["principals"][0]
    assert principals["existing-demand-reporter"] == registry["principals"][2]
    assert principals["configuration-activate"] == {
        "principal_id": "configuration-activate",
        "token_sha256": "2" * 64,
        "scopes": [
            "capacity:configure:activate",
            "capacity:configure:rollback",
        ],
        "subject_id": None,
        "subject_incarnation": None,
        "demand_reporter_incarnation": None,
        "pool_id": None,
        "pool_reporter_incarnation": None,
    }
    assert parsed["principals"][-1] == {
        "demand_reporter_incarnation": seed["reporter_incarnation"],
        "executor_id": None,
        "executor_incarnation": None,
        "executor_pool_generation": None,
        "pool_id": None,
        "pool_reporter_incarnation": None,
        "principal_id": "staging-demand-reporter",
        "scopes": ["capacity:report:demand"],
        "subject_id": seed["subject_id"],
        "subject_incarnation": seed["subject_incarnation"],
        "token_sha256": hashlib.sha256(str(seed["reporter_token"]).encode("ascii")).hexdigest(),
    }
    assert str(seed["reporter_token"]).encode("ascii") not in mutated
    assert mutated == (json.dumps(parsed, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "ascii"
    )


def test_manager_accepts_execution_component_ownership_of_exact_registry(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    cluster = _ManagerCluster(candidate)
    cluster.secret_owner = "loom-staging-capacity-execution-credentials"
    plan = _plan_with_manager(tmp_path)
    component = _component(cluster, candidate)

    assert component.classify(plan)[0] is ComponentState.READY


def test_registry_mutation_is_idempotent_and_rejects_conflicting_binding() -> None:
    seed = _seed()
    first = _mutate_registry()(
        json.dumps(_registry()).encode("ascii"),
        seed=seed,
    )

    assert _mutate_registry()(first, seed=seed) == first

    conflicting = json.loads(first)
    conflicting["principals"][-1]["subject_incarnation"] = "00000000-0000-4000-8000-000000000304"
    with pytest.raises(ValueError, match="staging demand reporter conflicts"):
        _mutate_registry()(
            json.dumps(conflicting).encode("ascii"),
            seed=seed,
        )


@pytest.mark.parametrize(
    ("mutate_registry", "message"),
    (
        (
            lambda registry: registry["principals"].pop(1),
            "capacity principal registry is invalid",
        ),
        (
            lambda registry: registry["principals"].__setitem__(
                1,
                {
                    **registry["principals"][1],  # type: ignore[index]
                    "subject_id": "00000000-0000-4000-8000-000000000451",
                },
            ),
            "capacity principal registry is invalid",
        ),
        (
            lambda registry: registry["principals"].append(  # type: ignore[union-attr]
                {
                    "principal_id": "duplicate-rollback-owner",
                    "token_sha256": "4" * 64,
                    "scopes": ["capacity:configure:rollback"],
                    "subject_id": None,
                    "subject_incarnation": None,
                    "demand_reporter_incarnation": None,
                    "pool_id": None,
                    "pool_reporter_incarnation": None,
                }
            ),
            "capacity principal registry is invalid",
        ),
        (
            lambda registry: registry["principals"].append(  # type: ignore[union-attr]
                dict(registry["principals"][1])  # type: ignore[index]
            ),
            "capacity principal registry is invalid",
        ),
    ),
)
def test_registry_mutation_requires_exact_single_configuration_activate_principal(
    mutate_registry,  # type: ignore[no-untyped-def]
    message: str,
) -> None:
    registry = _registry()
    mutate_registry(registry)

    with pytest.raises(ValueError, match=message):
        _mutate_registry()(
            json.dumps(registry).encode("ascii"),
            seed=_seed(),
        )


def test_registry_mutation_rejects_duplicate_json_keys() -> None:
    payload = b'{"schema_version":1,"schema_version":1,"principals":[]}'

    with pytest.raises(ValueError, match="duplicate"):
        _mutate_registry()(payload, seed=_seed())


def test_manager_runtime_deployment_replace_argv_has_one_kubectl_executable() -> None:
    component = _component_type()
    common = (
        "kubectl",
        "--namespace",
        "loom-dev",
        "replace",
        "--field-manager=loom-staging-capacity-manager-runtime",
    )

    assert component._deployment_replace_argv(dry_run=False) == (
        *common,
        "--show-managed-fields",
        "--output=json",
        "--validate=strict",
        "--request-timeout=60s",
        "-f",
        "-",
    )
    assert component._deployment_replace_argv(dry_run=True) == (
        *common,
        "--dry-run=server",
        "--show-managed-fields",
        "--output=json",
        "--validate=strict",
        "--request-timeout=60s",
        "-f",
        "-",
    )


def _component(cluster: _ManagerCluster, candidate: Path, *, seed_reader=_seed):
    return _component_type()(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        seed_reader=seed_reader,
    )


def test_manager_runtime_preserves_secret_and_rolls_out_trusted_candidate(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    plan = _plan_with_manager(tmp_path)
    cluster = _ManagerCluster(candidate)
    original_data = deepcopy(cluster.secret_data)
    component = _component(cluster, candidate)

    assert component.classify(plan)[0] is ComponentState.READY
    component.apply(plan)

    assert component.classify(plan)[0] is ComponentState.EXACT
    assert set(cluster.secret_data) == _SECRET_KEYS
    assert {
        key: value for key, value in cluster.secret_data.items() if key != "principals.json"
    } == {key: value for key, value in original_data.items() if key != "principals.json"}
    principal_payload = __import__("base64").b64decode(
        cluster.secret_data["principals.json"], validate=True
    )
    principals = json.loads(principal_payload)["principals"]
    assert [principal["principal_id"] for principal in principals] == [
        "existing-operator",
        "configuration-activate",
        "existing-demand-reporter",
        "staging-demand-reporter",
    ]
    principal_scopes = {
        principal["principal_id"]: set(principal["scopes"]) for principal in principals
    }
    assert principal_scopes["configuration-activate"] == {
        "capacity:configure:activate",
        "capacity:configure:rollback",
    }
    assert all(
        "capacity:configure:rollback" not in scopes
        for principal_id, scopes in principal_scopes.items()
        if principal_id != "configuration-activate"
    )
    desired_image = "registry.example.test/loom/loom-capacity-manager@sha256:" + "9" * 64
    template = cluster.deployment["spec"]["template"]
    assert template["metadata"]["annotations"] == {
        "loom.yylx.dev/principal-registry-sha256": hashlib.sha256(principal_payload).hexdigest()
    }
    pod_spec = template["spec"]
    assert [container["image"] for container in pod_spec["initContainers"]] == [desired_image]
    assert [container["image"] for container in pod_spec["containers"]] == [
        desired_image,
        desired_image,
    ]
    assert cluster.deployment["spec"]["replicas"] == 1
    assert cluster.deployment["spec"]["progressDeadlineSeconds"] == 600
    assert cluster.deployment["spec"]["strategy"] == {"type": "Recreate"}
    assert pod_spec["serviceAccountName"] == "loom-capacity-witness-publisher"
    assert cluster.rollout_status_calls == 1
    assert {entry["manager"] for entry in cluster._secret()["metadata"]["managedFields"]} == {
        "kubectl-create",
        "loom-staging-capacity-manager-runtime",
    }
    assert {entry["manager"] for entry in cluster._deployment()["metadata"]["managedFields"]} == {
        "loom-capacity-control-plane",
        "loom-staging-capacity-manager-runtime",
        "k3s",
    }
    mutation_commands = [command for command, payload in cluster.calls if payload is not None]
    assert all("--force-conflicts" not in command for command in mutation_commands)
    assert any(
        "patch" in command
        and "--type=json" in command
        and "--patch-file=/dev/stdin" in command
        and "--field-manager=loom-staging-capacity-manager-runtime" in command
        for command in mutation_commands
    )
    assert any(
        "replace" in command
        and "--dry-run=server" not in command
        and "--field-manager=loom-staging-capacity-manager-runtime" in command
        for command in mutation_commands
    )


def test_manager_runtime_preserves_unrelated_secret_data_byte_for_byte(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    plan = _plan_with_manager(tmp_path)
    cluster = _ManagerCluster(candidate)
    cluster.secret_data["unrelated-empty"] = ""
    cluster.secret_data["unrelated-bytes"] = _encode(b"preserve-me")
    original_extra_data = {
        key: cluster.secret_data[key] for key in ("unrelated-empty", "unrelated-bytes")
    }
    component = _component(cluster, candidate)

    assert component.classify(plan)[0] is ComponentState.READY
    component.apply(plan)

    assert {
        key: cluster.secret_data[key] for key in ("unrelated-empty", "unrelated-bytes")
    } == original_extra_data


def test_manager_runtime_rejects_seed_drift_before_any_live_mutation(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    plan = _plan_with_manager(tmp_path)
    cluster = _ManagerCluster(candidate)
    original_registry = cluster.secret_data["principals.json"]
    first_seed = _seed()
    changed_seed = _seed()
    changed_seed["reporter_token"] = "rotated-staging-reporter-token-" + "y" * 48
    seeds = iter((first_seed, changed_seed))

    with pytest.raises(RuntimeError, match="credential seed changed before mutation"):
        _component(cluster, candidate, seed_reader=lambda: next(seeds)).apply(plan)

    assert cluster.secret_data["principals.json"] == original_registry
    assert not any(
        "patch" in command or ("replace" in command and "--dry-run=server" not in command)
        for command, _payload in cluster.calls
    )


def test_manager_runtime_never_mixes_seed_generations_across_mutations(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    plan = _plan_with_manager(tmp_path)
    cluster = _ManagerCluster(candidate)
    first_seed = _seed()
    changed_seed = _seed()
    changed_seed["authority_incarnation"] = "841e79c2-8a76-4eeb-af56-f6d03bcb1bd9"
    seeds = iter((first_seed, first_seed, changed_seed))

    with pytest.raises(RuntimeError, match="Secret was restored after credential seed changed"):
        _component(cluster, candidate, seed_reader=lambda: next(seeds)).apply(plan)

    assert cluster.secret_owner == "loom-staging-capacity-manager-runtime"
    assert cluster.deployment_owner == "loom-capacity-control-plane"
    assert cluster.rollout_status_calls == 0

    _component(cluster, candidate, seed_reader=lambda: first_seed).apply(plan)

    assert _component(cluster, candidate, seed_reader=lambda: first_seed).classify(plan)[0] is (
        ComponentState.EXACT
    )


def test_manager_runtime_compensates_persistent_seed_rotation_after_secret_patch(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    plan = _plan_with_manager(tmp_path)
    cluster = _ManagerCluster(candidate)
    cluster.secret_data["unrelated-empty"] = ""
    cluster.secret_data["unrelated-bytes"] = _encode(b"preserve-me")
    original_registry = cluster.secret_data["principals.json"]
    original_unrelated = {
        key: cluster.secret_data[key] for key in ("unrelated-empty", "unrelated-bytes")
    }
    first_seed = _seed()
    rotated_seed = _seed()
    rotated_seed["reporter_token"] = "rotated-staging-reporter-token-" + "z" * 48
    bound_component = _component(cluster, candidate, seed_reader=lambda: first_seed)
    bound_snapshot = bound_component._snapshot(plan)
    cluster.deployment = cluster._server_normalize(bound_snapshot.desired_deployment)
    cluster.deployment_owner = "loom-staging-capacity-manager-runtime"
    cluster.calls.clear()
    seeds = iter((first_seed, first_seed, rotated_seed))

    with pytest.raises(RuntimeError, match="Secret was restored after credential seed changed"):
        _component(cluster, candidate, seed_reader=lambda: next(seeds)).apply(plan)

    assert cluster.secret_data["principals.json"] == original_registry
    assert {
        key: cluster.secret_data[key] for key in ("unrelated-empty", "unrelated-bytes")
    } == original_unrelated
    assert cluster.rollout_status_calls == 0
    patch_payloads = [
        payload for command, payload in cluster.calls if "patch" in command and payload is not None
    ]
    assert len(patch_payloads) == 2
    patched_registry = next(
        operation["value"]
        for operation in json.loads(patch_payloads[0])
        if operation["op"] == "replace" and operation["path"] == "/data/principals.json"
    )
    compensation = json.loads(patch_payloads[1])
    assert {
        (operation["path"], operation["value"])
        for operation in compensation
        if operation["op"] == "test"
    } >= {
        ("/metadata/uid", cluster.secret_uid),
        ("/metadata/resourceVersion", "18"),
        ("/data/principals.json", patched_registry),
    }
    assert not any(
        "replace" in command and "--dry-run=server" not in command
        for command, _payload in cluster.calls
    )
    assert str(rotated_seed["reporter_token"]).encode("ascii") not in b"".join(
        payload for _command, payload in cluster.calls if payload is not None
    )

    _component(cluster, candidate, seed_reader=lambda: rotated_seed).apply(plan)

    assert _component(cluster, candidate, seed_reader=lambda: rotated_seed).classify(plan)[0] is (
        ComponentState.EXACT
    )


def test_manager_runtime_compensates_late_seed_rotation_before_deployment(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    plan = _plan_with_manager(tmp_path)
    cluster = _ManagerCluster(candidate)
    cluster.secret_data["unrelated-empty"] = ""
    cluster.secret_data["unrelated-bytes"] = _encode(b"preserve-me")
    original_registry = cluster.secret_data["principals.json"]
    original_unrelated = {
        key: cluster.secret_data[key] for key in ("unrelated-empty", "unrelated-bytes")
    }
    first_seed = _seed()
    rotated_seed = _seed()
    rotated_seed["reporter_incarnation"] = "00000000-0000-4000-8000-000000000398"
    seeds = iter((first_seed, first_seed, first_seed, rotated_seed))

    with pytest.raises(RuntimeError, match="Secret was restored after credential seed changed"):
        _component(cluster, candidate, seed_reader=lambda: next(seeds)).apply(plan)

    assert cluster.secret_data["principals.json"] == original_registry
    assert {
        key: cluster.secret_data[key] for key in ("unrelated-empty", "unrelated-bytes")
    } == original_unrelated
    assert cluster.rollout_status_calls == 0
    assert not any(
        "replace" in command and "--dry-run=server" not in command
        for command, _payload in cluster.calls
    )
    assert str(rotated_seed["reporter_token"]).encode("ascii") not in b"".join(
        payload for _command, payload in cluster.calls if payload is not None
    )

    _component(cluster, candidate, seed_reader=lambda: rotated_seed).apply(plan)

    assert _component(cluster, candidate, seed_reader=lambda: rotated_seed).classify(plan)[0] is (
        ComponentState.EXACT
    )


def test_manager_runtime_reports_a_lost_compensation_fence_without_deployment_mutation(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    plan = _plan_with_manager(tmp_path)
    cluster = _ManagerCluster(candidate)
    cluster.race_compensation_secret = True
    first_seed = _seed()
    rotated_seed = _seed()
    rotated_seed["reporter_incarnation"] = "00000000-0000-4000-8000-000000000399"
    seeds = iter((first_seed, first_seed, rotated_seed))

    with pytest.raises(RuntimeError, match="Secret compensation lost its fence"):
        _component(cluster, candidate, seed_reader=lambda: next(seeds)).apply(plan)

    assert cluster.secret_patch_calls == 2
    assert cluster.rollout_status_calls == 0
    assert not any(
        "replace" in command and "--dry-run=server" not in command
        for command, _payload in cluster.calls
    )
    assert str(rotated_seed["reporter_token"]).encode("ascii") not in b"".join(
        payload for _command, payload in cluster.calls if payload is not None
    )


def test_manager_runtime_conflicting_principal_or_unknown_owner_is_drift(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    plan = _plan_with_manager(tmp_path)
    conflicting = _ManagerCluster(candidate, conflicting_principal=True)
    unknown_owner = _ManagerCluster(candidate)
    unknown_owner.unknown_deployment_manager = True

    assert _component(conflicting, candidate).classify(plan)[0] is ComponentState.DRIFTED
    assert _component(unknown_owner, candidate).classify(plan)[0] is ComponentState.DRIFTED
    for cluster in (conflicting, unknown_owner):
        assert all(
            "patch" not in command
            and not ("replace" in command and "--dry-run=server" not in command)
            for command, _payload in cluster.calls
        )


def test_manager_runtime_uid_resource_version_fences_both_mutations(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    plan = _plan_with_manager(tmp_path)
    secret_race = _ManagerCluster(candidate)
    secret_race.race_secret = True

    with pytest.raises(RuntimeError, match="Secret resource version changed"):
        _component(secret_race, candidate).apply(plan)
    assert (
        secret_race.secret_data["principals.json"]
        == _ManagerCluster(candidate).secret_data["principals.json"]
    )

    deployment_race = _ManagerCluster(candidate)
    deployment_race.race_deployment = True
    with pytest.raises(RuntimeError, match="Deployment resource version changed"):
        _component(deployment_race, candidate).apply(plan)
    assert deployment_race.rollout_status_calls == 0
