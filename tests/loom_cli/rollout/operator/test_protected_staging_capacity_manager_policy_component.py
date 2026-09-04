from __future__ import annotations

import base64
import hashlib
import importlib
import importlib.util
import ipaddress
import json
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
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from loom_capacity_manager.executable_contracts import (
    ExecutionPreparationV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_cli.capacity_control_plane import (
    load_capacity_control_plane_profile,
    render_capacity_control_plane_manifests,
)
from loom_cli.rollout.operator.protected_apply_journal import ComponentState
from loom_cli.rollout.operator.protected_execution_prerequisite_store import (
    ProtectedExecutionPrerequisitePublication,
    ProtectedExecutionPrerequisiteStore,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _execution_plan
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_manager_runtime_component import (
    _SECRET_KEYS,
    _candidate,
    _registry,
    _seed,
)

MODULE = "loom_cli.rollout.operator.protected_staging_capacity_manager_policy_component"
_COMPONENT_LABEL = "loom.carin.dev/protected-component"
_COMPONENT_VALUE = "staging-capacity-manager-policy"
_FIELD_MANAGER = "loom-staging-capacity-manager-runtime"
_ROUTER_IDENTITY = (
    "Deployment",
    "loom-capacity-router",
    "loom-capacity-manager-router",
)


def _plan_and_prerequisite(tmp_path: Path):  # type: ignore[no-untyped-def]
    plan = _execution_plan(tmp_path)
    plan = replace(
        plan,
        image_digests={
            **plan.image_digests,
            "loom-capacity-manager": "sha256:" + "9" * 64,
        },
    )
    store = ProtectedExecutionPrerequisiteStore(
        tmp_path / "execution-authority",
        service_uid=tmp_path.stat().st_uid,
    )
    prerequisite = store.read(
        ProtectedExecutionPrerequisitePublication(
            path=Path(plan.execution_prerequisite_artifact_path or ""),
            artifact_sha256=plan.execution_prerequisite_artifact_sha256 or "",
        )
    )
    return plan, prerequisite


def _server_certificate(*, include_router_ip: bool = True) -> bytes:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "loom-capacity-manager")])
    now = datetime.now(UTC)
    identities: list[x509.GeneralName] = [
        x509.DNSName("loom-capacity-manager.loom-dev.svc.cluster.local")
    ]
    if include_router_ip:
        identities.append(x509.IPAddress(ipaddress.ip_address("192.168.50.103")))
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName(identities),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    return certificate.public_bytes(serialization.Encoding.PEM)


def _identity(document):  # type: ignore[no-untyped-def]
    metadata = document["metadata"]
    return document["kind"], metadata.get("namespace", ""), metadata["name"]


def _projection(document):  # type: ignore[no-untyped-def]
    value = deepcopy(document)
    value.pop("status", None)
    metadata = value["metadata"]
    for field in (
        "creationTimestamp",
        "generation",
        "managedFields",
        "resourceVersion",
        "uid",
    ):
        metadata.pop(field, None)
    spec = value.get("spec")
    if isinstance(spec, dict):
        template = spec.get("template")
        if isinstance(template, dict) and isinstance(template.get("metadata"), dict):
            template["metadata"].pop("creationTimestamp", None)
    return value


class _PolicyCluster:
    environment: ClassVar[dict[str, str]] = {"KUBECONFIG": "/exact"}

    def __init__(self, candidate: Path) -> None:
        profile = load_capacity_control_plane_profile(
            candidate / "deploy" / "dev-fleet" / "capacity-control-plane.toml"
        )
        rendered = render_capacity_control_plane_manifests(
            profile,
            manager_image=("registry.example.test/loom/loom-capacity-manager@sha256:" + "8" * 64),
            authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        )
        documents = [item for item in yaml.safe_load_all(rendered) if isinstance(item, dict)]
        self.resources: dict[tuple[str, str, str], dict[str, object]] = {}
        for document in documents:
            key = _identity(document)
            if key not in {
                ("Deployment", "loom-dev", "loom-capacity-manager"),
                ("NetworkPolicy", "loom-dev", "capacity-manager-ingress"),
            }:
                continue
            self.resources[key] = self._stored(document, existing=None)
        self.secret_uid = "34044ac3-1a1a-4fbe-ac27-05d03312cfe2"
        self.secret_resource_version = 17
        self.secret_owner = "loom-staging-capacity-execution-credentials"
        self.secret_data = {
            key: base64.b64encode(f"unchanged-{key}".encode("ascii")).decode("ascii")
            for key in _SECRET_KEYS
        }
        self.secret_data["principals.json"] = base64.b64encode(
            (json.dumps(_registry(), sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
        ).decode("ascii")
        self.secret_data["server-certificate.pem"] = base64.b64encode(_server_certificate()).decode(
            "ascii"
        )
        self.calls: list[tuple[tuple[str, ...], bytes | None]] = []
        self.rollouts: list[tuple[str, str]] = []
        self.fail_rollout_once: tuple[str, str] | None = None
        self.fail_readback_once = False
        self.diff_status: int | None = None
        self.status = {
            "schema_version": 1,
            "authority_incarnation": str(_seed()["authority_incarnation"]),
            "observer_principal_id": "manager-read",
            "writer_epoch": 0,
            "configuration_epoch": 9,
            "configuration_digest": "9" * 64,
            "report_freshness_counts": {},
            "latest_shadow_epoch": None,
            "latest_shadow_input_digest": None,
            "account_slots": {},
            "tier_slots": {},
            "pool_slots": {},
            "blocker_counts": {},
            "increase_freeze": True,
            "execution_epoch": 0,
            "execution_state": "shadow",
            "execution_manifest_sha256": None,
            "executable_new_capacity_ceiling": 0,
        }
        self._resource_version = 100

    def _secret(self) -> dict[str, object]:
        return {
            "apiVersion": "v1",
            "data": dict(self.secret_data),
            "kind": "Secret",
            "metadata": {
                "managedFields": [
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
                    }
                ],
                "name": "loom-capacity-manager",
                "namespace": "loom-dev",
                "resourceVersion": str(self.secret_resource_version),
                "uid": self.secret_uid,
            },
            "type": "Opaque",
        }

    def _stored(
        self,
        desired: dict[str, object],
        *,
        existing: dict[str, object] | None,
    ) -> dict[str, object]:
        value = deepcopy(desired)
        metadata = value["metadata"]
        assert isinstance(metadata, dict)
        if existing is None:
            uid = f"11111111-1111-4111-8111-{len(self.resources) + 1:012d}"
            resource_version = str(len(self.resources) + 1)
        else:
            existing_metadata = existing["metadata"]
            assert isinstance(existing_metadata, dict)
            uid = existing_metadata["uid"]
            resource_version = str(int(str(existing_metadata["resourceVersion"])) + 1)
        metadata.update(
            {
                "managedFields": [
                    {
                        "apiVersion": value["apiVersion"],
                        "fieldsType": "FieldsV1",
                        "fieldsV1": {"f:spec": {}},
                        "manager": (
                            "loom-capacity-control-plane" if existing is None else _FIELD_MANAGER
                        ),
                        "operation": "Apply" if existing is None else "Update",
                    }
                ],
                "resourceVersion": resource_version,
                "uid": uid,
            }
        )
        if value["kind"] == "Deployment":
            spec = value["spec"]
            assert isinstance(spec, dict)
            spec.setdefault("progressDeadlineSeconds", 600)
            replicas = spec["replicas"]
            assert isinstance(replicas, int)
            metadata["generation"] = (
                1
                if existing is None
                else int(
                    str(existing["metadata"].get("generation", 1))  # type: ignore[union-attr]
                )
                + 1
            )
            metadata["managedFields"].append(  # type: ignore[union-attr]
                {
                    "apiVersion": "apps/v1",
                    "fieldsType": "FieldsV1",
                    "fieldsV1": {"f:status": {}},
                    "manager": "k3s",
                    "operation": "Update",
                    "subresource": "status",
                }
            )
            value["status"] = {
                "availableReplicas": replicas,
                "observedGeneration": metadata["generation"],
                "readyReplicas": replicas,
                "replicas": replicas,
                "updatedReplicas": replicas,
            }
        return value

    def capture_stdout(self, argv, *, env, timeout_seconds):  # type: ignore[no-untyped-def]
        assert env == self.environment
        command = tuple(argv)
        self.calls.append((command, None))
        if "secret/loom-capacity-manager" in command:
            assert timeout_seconds == 60.0
            return json.dumps(self._secret(), sort_keys=True).encode("ascii")
        assert timeout_seconds == 30.0
        if any(item.startswith("--selector=") for item in command):
            if self.fail_readback_once and _ROUTER_IDENTITY in self.resources:
                self.fail_readback_once = False
                raise RuntimeError("injected readback failure")
            namespaced = "--all-namespaces" in command
            items = []
            for (_kind, namespace, _name), resource in self.resources.items():
                labels = resource["metadata"].get("labels", {})  # type: ignore[union-attr]
                if labels.get(_COMPONENT_LABEL) != _COMPONENT_VALUE:
                    continue
                if namespaced != bool(namespace):
                    continue
                items.append(resource)
            return json.dumps({"apiVersion": "v1", "items": items, "kind": "List"}).encode()
        target = next(item for item in command if "/" in item and not item.startswith("--"))
        kind_name, name = target.split("/", 1)
        kind = {
            "configmap": "ConfigMap",
            "deployment": "Deployment",
            "namespace": "Namespace",
            "networkpolicy": "NetworkPolicy",
        }[kind_name]
        namespace = command[command.index("--namespace") + 1] if "--namespace" in command else ""
        value = self.resources.get((kind, namespace, name))
        return b"" if value is None else json.dumps(value, sort_keys=True).encode()

    def run_status(
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
        self.calls.append((command, input_payload))
        if self.diff_status is not None:
            return self.diff_status
        desired = next(item for item in yaml.safe_load_all(input_payload) if item)
        observed = self.resources.get(_identity(desired))
        normalized = deepcopy(desired)
        if normalized["kind"] == "Deployment":
            spec = normalized["spec"]
            assert isinstance(spec, dict)
            spec.setdefault("progressDeadlineSeconds", 600)
        return 1 if observed is None or _projection(observed) != _projection(normalized) else 0

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
        self.calls.append((command, input_payload))
        if "patch" in command and "secret/loom-capacity-manager" in command:
            operations = json.loads(input_payload)
            tests = {
                (operation["path"], operation["value"])
                for operation in operations
                if operation["op"] == "test"
            }
            assert ("/metadata/uid", self.secret_uid) in tests
            assert (
                "/metadata/resourceVersion",
                str(self.secret_resource_version),
            ) in tests
            assert (
                "/data/principals.json",
                self.secret_data["principals.json"],
            ) in tests
            replacement = next(
                operation["value"]
                for operation in operations
                if operation["op"] == "replace" and operation["path"] == "/data/principals.json"
            )
            self.secret_data["principals.json"] = replacement
            self.secret_owner = _FIELD_MANAGER
            self.secret_resource_version += 1
            return json.dumps(self._secret(), sort_keys=True).encode("ascii")
        desired = next(item for item in yaml.safe_load_all(input_payload) if item)
        key = _identity(desired)
        existing = self.resources.get(key)
        metadata = desired["metadata"]
        if "replace" in command:
            assert existing is not None
            existing_metadata = existing["metadata"]
            assert metadata["uid"] == existing_metadata["uid"]
            assert metadata["resourceVersion"] == existing_metadata["resourceVersion"]
            metadata.pop("uid")
            metadata.pop("resourceVersion")
        else:
            assert "create" in command and existing is None
        stored = self._stored(desired, existing=existing)
        stored_metadata = stored["metadata"]
        assert isinstance(stored_metadata, dict)
        managed = stored_metadata["managedFields"]
        assert isinstance(managed, list) and isinstance(managed[0], dict)
        managed[0]["manager"] = _FIELD_MANAGER
        managed[0]["operation"] = "Update"
        if "--dry-run=server" in command:
            return json.dumps(stored, sort_keys=True).encode()
        self.resources[key] = stored
        return json.dumps(stored, sort_keys=True).encode()

    def run_checked(
        self,
        argv,
        *,
        env,
        input_payload,
        timeout_seconds,  # type: ignore[no-untyped-def]
    ):
        assert env == self.environment
        assert input_payload is None
        assert timeout_seconds == 660.0
        command = tuple(argv)
        self.calls.append((command, None))
        name = next(item.removeprefix("deployment/") for item in command if "deployment/" in item)
        namespace = command[command.index("--namespace") + 1]
        if self.fail_rollout_once == (namespace, name):
            self.fail_rollout_once = None
            raise RuntimeError("injected rollout failure")
        self.rollouts.append((namespace, name))


def test_policy_resource_builder_selects_only_bound_router_and_manager_resources(
    tmp_path: Path,
) -> None:
    assert importlib.util.find_spec(MODULE) is not None, "manager policy component is missing"
    module = importlib.import_module(MODULE)
    builder = getattr(module, "build_manager_policy_resource_documents", None)
    assert builder is not None, "manager policy resource builder is missing"
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    registry = b'{"principals":[],"schema_version":1}\n'

    resources = builder(
        plan,
        prerequisite=prerequisite,
        candidate_root=_candidate(tmp_path),
        container_registry="registry.example.test/loom",
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=registry,
    )

    policy_name = f"loom-capacity-execution-policy-{prerequisite.execution_policy_sha256[:32]}"
    assert set(resources) == {
        ("Namespace", "", "loom-capacity-router"),
        ("ConfigMap", "loom-dev", policy_name),
        ("Deployment", "loom-dev", "loom-capacity-manager"),
        ("Deployment", "loom-capacity-router", "loom-capacity-manager-router"),
        ("NetworkPolicy", "loom-dev", "capacity-manager-ingress"),
        (
            "NetworkPolicy",
            "loom-capacity-router",
            "capacity-manager-router-default-deny",
        ),
        ("NetworkPolicy", "loom-capacity-router", "capacity-manager-router-ingress"),
        ("NetworkPolicy", "loom-capacity-router", "capacity-manager-router-egress"),
    }
    assert all(
        resource["metadata"]["labels"][_COMPONENT_LABEL] == _COMPONENT_VALUE
        for resource in resources.values()
    )
    namespace = resources[("Namespace", "", "loom-capacity-router")]
    assert namespace["metadata"]["labels"]["kubernetes.io/metadata.name"] == (
        "loom-capacity-router"
    )
    policy = resources[("ConfigMap", "loom-dev", policy_name)]
    assert policy["immutable"] is True
    assert policy["data"] == {
        "execution-policy.json": canonical_executable_bytes(prerequisite.execution_policy).decode(
            "ascii"
        )
    }
    manager = resources[("Deployment", "loom-dev", "loom-capacity-manager")]
    manager_template = manager["spec"]["template"]
    assert manager_template["metadata"]["annotations"] == {
        "loom.yylx.dev/principal-registry-sha256": hashlib.sha256(registry).hexdigest()
    }
    manager_container = next(
        item for item in manager_template["spec"]["containers"] if item["name"] == "manager"
    )
    assert {item["name"]: item["value"] for item in manager_container["env"]}[
        "LOOM_CAPACITY_EXECUTION_POLICY_SHA256"
    ] == prerequisite.execution_policy_sha256
    assert "--required-server-ip-san" in manager_container["readinessProbe"]["exec"]["command"]
    assert "192.168.50.103" in manager_container["readinessProbe"]["exec"]["command"]
    router = resources[("Deployment", "loom-capacity-router", "loom-capacity-manager-router")]
    allowed_addresses = tuple(router["spec"]["template"]["spec"]["containers"][0]["args"][1::2])
    assert allowed_addresses == tuple(
        route.removesuffix("/32")
        for route in sorted(set(prerequisite.manager_client_cidrs.values()))
    )
    ingress = resources[
        ("NetworkPolicy", "loom-capacity-router", "capacity-manager-router-ingress")
    ]
    assert ingress["spec"]["ingress"][0]["from"] == [
        {"ipBlock": {"cidr": route}}
        for route in sorted(set(prerequisite.manager_client_cidrs.values()))
    ]


def test_policy_component_converges_foundations_before_private_router(
    tmp_path: Path,
) -> None:
    module = importlib.import_module(MODULE)
    component_type = getattr(
        module,
        "KubernetesProtectedStagingCapacityManagerPolicyComponent",
        None,
    )
    authority_type = getattr(module, "ManagerPolicyRuntimeAuthority", None)
    assert component_type is not None, "manager policy component type is missing"
    assert authority_type is not None, "manager policy runtime authority is missing"
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    authority = authority_type(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[],"schema_version":1}\n',
        server_certificate=_server_certificate(),
    )
    component = component_type(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        prerequisite_reader=lambda _plan: prerequisite,
        runtime_authority_reader=lambda: authority,
        manager_status_reader=lambda: dict(cluster.status),
    )

    assert component.classify(plan)[0] is ComponentState.READY
    component.apply(plan)

    assert component.classify(plan)[0] is ComponentState.EXACT
    assert set(cluster.resources) == {
        ("Namespace", "", "loom-capacity-router"),
        (
            "ConfigMap",
            "loom-dev",
            f"loom-capacity-execution-policy-{prerequisite.execution_policy_sha256[:32]}",
        ),
        ("Deployment", "loom-dev", "loom-capacity-manager"),
        ("Deployment", "loom-capacity-router", "loom-capacity-manager-router"),
        ("NetworkPolicy", "loom-dev", "capacity-manager-ingress"),
        (
            "NetworkPolicy",
            "loom-capacity-router",
            "capacity-manager-router-default-deny",
        ),
        ("NetworkPolicy", "loom-capacity-router", "capacity-manager-router-ingress"),
        ("NetworkPolicy", "loom-capacity-router", "capacity-manager-router-egress"),
    }
    mutations = [
        _identity(next(item for item in yaml.safe_load_all(payload) if item))
        for command, payload in cluster.calls
        if payload is not None
        and ("create" in command or "replace" in command)
        and "--dry-run=server" not in command
    ]
    normalizations = [
        _identity(next(item for item in yaml.safe_load_all(payload) if item))
        for command, payload in cluster.calls
        if payload is not None
        and ("create" in command or "replace" in command)
        and "--dry-run=server" in command
    ]
    assert normalizations == mutations
    assert mutations[0] == ("Namespace", "", "loom-capacity-router")
    assert mutations[-1] == (
        "Deployment",
        "loom-capacity-router",
        "loom-capacity-manager-router",
    )
    assert mutations.index(("Deployment", "loom-dev", "loom-capacity-manager")) < len(mutations) - 1
    assert cluster.rollouts == [
        ("loom-dev", "loom-capacity-manager"),
        ("loom-capacity-router", "loom-capacity-manager-router"),
    ]
    assert all("--force-conflicts" not in command for command, _payload in cluster.calls)
    assert all(
        "--show-managed-fields" in command
        for command, payload in cluster.calls
        if payload is not None and ("create" in command or "replace" in command)
    )
    assert all(
        "--validate=strict" in command
        and "--output=json" in command
        and "--request-timeout=60s" in command
        for command, payload in cluster.calls
        if payload is not None
        and ("create" in command or "replace" in command)
        and "--dry-run=server" in command
    )


def test_policy_component_keeps_exact_artifact_bound_prepared_runtime_exact(
    tmp_path: Path,
) -> None:
    """Catch treating the component's own exact preparation as dependency drift."""

    module = importlib.import_module(MODULE)
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    authority = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[],"schema_version":1}\n',
        server_certificate=_server_certificate(),
    )
    component = module.KubernetesProtectedStagingCapacityManagerPolicyComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        prerequisite_reader=lambda _plan: prerequisite,
        runtime_authority_reader=lambda: authority,
        manager_status_reader=lambda: dict(cluster.status),
    )
    component.apply(plan)
    writer_epoch = 11
    configuration_epoch = prerequisite.source_configuration_epoch + 1
    request = ExecutionPreparationV2(
        authority_incarnation=authority.authority_incarnation,
        expected_writer_epoch=writer_epoch,
        configuration_epoch=configuration_epoch,
        fleet_generation=prerequisite.desired_fleet_generation,
        fleet_digest=prerequisite.desired_fleet_sha256,
        trusted_fleet_release_sha256=(prerequisite.execution_policy.trusted_fleet_release_sha256),
        requested_ceiling=(prerequisite.execution_policy.executable_new_capacity_ceiling),
        requested_rate_per_minute=(
            prerequisite.execution_policy.executable_new_capacity_rate_per_minute
        ),
        executors=prerequisite.execution_policy.executors,
        subject_acknowledgements=(prerequisite.execution_policy.subject_acknowledgements),
        legacy_writer_fences=prerequisite.execution_policy.legacy_writer_fences,
        rollback_evidence_sha256=(prerequisite.execution_policy.rollback_evidence_sha256),
    )
    cluster.status.update(
        {
            "writer_epoch": writer_epoch,
            "configuration_epoch": configuration_epoch,
            "execution_epoch": 1,
            "execution_state": "prepared",
            "execution_manifest_sha256": canonical_executable_digest(request),
        }
    )

    assert component.classify(plan)[0] is ComponentState.EXACT

    cluster.status["execution_manifest_sha256"] = "f" * 64
    assert component.classify(plan)[0] is ComponentState.DRIFTED


def test_policy_component_disables_router_after_ambiguous_rollout_failure(
    tmp_path: Path,
) -> None:
    module = importlib.import_module(MODULE)
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    cluster.fail_rollout_once = (
        "loom-capacity-router",
        "loom-capacity-manager-router",
    )
    authority = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[],"schema_version":1}\n',
        server_certificate=_server_certificate(),
    )
    component = module.KubernetesProtectedStagingCapacityManagerPolicyComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        prerequisite_reader=lambda _plan: prerequisite,
        runtime_authority_reader=lambda: authority,
        manager_status_reader=lambda: dict(cluster.status),
    )

    with pytest.raises(RuntimeError, match="router was disabled"):
        component.apply(plan)

    router = cluster.resources[
        ("Deployment", "loom-capacity-router", "loom-capacity-manager-router")
    ]
    assert router["spec"]["replicas"] == 0
    assert component.classify(plan)[0] is ComponentState.READY

    component.apply(plan)
    assert component.classify(plan)[0] is ComponentState.EXACT


def test_policy_component_disables_router_after_post_create_readback_failure(
    tmp_path: Path,
) -> None:
    module = importlib.import_module(MODULE)
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    cluster.fail_readback_once = True
    authority = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[],"schema_version":1}\n',
        server_certificate=_server_certificate(),
    )
    component = module.KubernetesProtectedStagingCapacityManagerPolicyComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        prerequisite_reader=lambda _plan: prerequisite,
        runtime_authority_reader=lambda: authority,
        manager_status_reader=lambda: dict(cluster.status),
    )

    with pytest.raises(RuntimeError, match="router was disabled"):
        component.apply(plan)

    router = cluster.resources[_ROUTER_IDENTITY]
    assert router["spec"]["replicas"] == 0
    disabled_replacements = [
        command
        for command, payload in cluster.calls
        if payload is not None
        and "replace" in command
        and _identity(next(item for item in yaml.safe_load_all(payload) if item))
        == _ROUTER_IDENTITY
        and next(item for item in yaml.safe_load_all(payload) if item)["spec"]["replicas"] == 0
    ]
    assert len(disabled_replacements) == 2
    assert "--dry-run=server" in disabled_replacements[0]
    assert "--dry-run=server" not in disabled_replacements[1]


def test_policy_component_rolls_out_manager_before_creating_router(
    tmp_path: Path,
) -> None:
    module = importlib.import_module(MODULE)
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    cluster.fail_rollout_once = ("loom-dev", "loom-capacity-manager")
    authority = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[],"schema_version":1}\n',
        server_certificate=_server_certificate(),
    )
    component = module.KubernetesProtectedStagingCapacityManagerPolicyComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        prerequisite_reader=lambda _plan: prerequisite,
        runtime_authority_reader=lambda: authority,
        manager_status_reader=lambda: dict(cluster.status),
    )

    with pytest.raises(RuntimeError, match="injected rollout failure"):
        component.apply(plan)

    assert (
        "Deployment",
        "loom-capacity-router",
        "loom-capacity-manager-router",
    ) not in cluster.resources


def test_policy_component_disables_existing_router_before_manager_repair(
    tmp_path: Path,
) -> None:
    module = importlib.import_module(MODULE)
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    authority = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[],"schema_version":1}\n',
        server_certificate=_server_certificate(),
    )
    component = module.KubernetesProtectedStagingCapacityManagerPolicyComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        prerequisite_reader=lambda _plan: prerequisite,
        runtime_authority_reader=lambda: authority,
        manager_status_reader=lambda: dict(cluster.status),
    )
    component.apply(plan)
    manager = cluster.resources[("Deployment", "loom-dev", "loom-capacity-manager")]
    manager["status"]["readyReplicas"] = 0
    cluster.fail_rollout_once = ("loom-dev", "loom-capacity-manager")

    assert component.classify(plan)[0] is ComponentState.READY
    with pytest.raises(RuntimeError):
        component.apply(plan)

    assert cluster.resources[_ROUTER_IDENTITY]["spec"]["replicas"] == 0


def test_schema7_manager_runtime_converges_registry_and_policy_as_one_component(
    tmp_path: Path,
) -> None:
    manager_module = importlib.import_module(
        "loom_cli.rollout.operator.protected_staging_capacity_manager_runtime_component"
    )
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    component = manager_module.KubernetesProtectedStagingCapacityManagerRuntimeComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        seed_reader=_seed,
        prerequisite_reader=lambda _plan: prerequisite,
        manager_status_reader=lambda: dict(cluster.status),
    )

    assert component.classify(plan)[0] is ComponentState.READY
    component.apply(plan)

    assert component.classify(plan)[0] is ComponentState.EXACT
    principals = json.loads(
        base64.b64decode(cluster.secret_data["principals.json"], validate=True)
    )["principals"]
    assert principals[-1]["principal_id"] == "staging-demand-reporter"
    manager = cluster.resources[("Deployment", "loom-dev", "loom-capacity-manager")]
    command = manager["spec"]["template"]["spec"]["containers"][0]["readinessProbe"]["exec"][
        "command"
    ]
    assert command[-2:] == ["--required-server-ip-san", "192.168.50.103"]


def test_schema7_manager_runtime_rejects_non_string_server_certificate(
    tmp_path: Path,
) -> None:
    manager_module = importlib.import_module(
        "loom_cli.rollout.operator.protected_staging_capacity_manager_runtime_component"
    )
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    cluster.secret_data["server-certificate.pem"] = None
    component = manager_module.KubernetesProtectedStagingCapacityManagerRuntimeComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        seed_reader=_seed,
        prerequisite_reader=lambda _plan: prerequisite,
        manager_status_reader=lambda: dict(cluster.status),
    )

    assert component.classify(plan)[0] is ComponentState.DRIFTED
    with pytest.raises(ValueError, match="capacity manager Secret is invalid"):
        component.apply(plan)


def test_policy_authority_requires_manager_router_ip_san(tmp_path: Path) -> None:
    module = importlib.import_module(MODULE)

    with pytest.raises(ValueError, match="server certificate"):
        module.ManagerPolicyRuntimeAuthority(
            authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
            principal_registry=b'{"principals":[],"schema_version":1}\n',
            server_certificate=_server_certificate(include_router_ip=False),
        )


def test_policy_component_rejects_unexpected_labeled_resource(tmp_path: Path) -> None:
    module = importlib.import_module(MODULE)
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    cluster.resources[("ConfigMap", "loom-dev", "foreign-policy")] = {
        "apiVersion": "v1",
        "data": {"foreign": "true"},
        "kind": "ConfigMap",
        "metadata": {
            "labels": {_COMPONENT_LABEL: _COMPONENT_VALUE},
            "managedFields": [
                {
                    "apiVersion": "v1",
                    "fieldsType": "FieldsV1",
                    "fieldsV1": {"f:data": {}},
                    "manager": "foreign-controller",
                    "operation": "Update",
                }
            ],
            "name": "foreign-policy",
            "namespace": "loom-dev",
            "resourceVersion": "88",
            "uid": "99999999-9999-4999-8999-999999999999",
        },
    }
    authority = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[],"schema_version":1}\n',
        server_certificate=_server_certificate(),
    )
    component = module.KubernetesProtectedStagingCapacityManagerPolicyComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        prerequisite_reader=lambda _plan: prerequisite,
        runtime_authority_reader=lambda: authority,
        manager_status_reader=lambda: dict(cluster.status),
    )

    assert component.classify(plan)[0] is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
    assert not any(
        payload is not None and ("create" in command or "replace" in command)
        for command, payload in cluster.calls
    )


def test_policy_component_rejects_foreign_ownership_on_expected_resource(
    tmp_path: Path,
) -> None:
    module = importlib.import_module(MODULE)
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    manager = cluster.resources[("Deployment", "loom-dev", "loom-capacity-manager")]
    manager["metadata"]["managedFields"].insert(
        1,
        {
            "apiVersion": "apps/v1",
            "fieldsType": "FieldsV1",
            "fieldsV1": {"f:spec": {"f:replicas": {}}},
            "manager": "foreign-controller",
            "operation": "Update",
        },
    )
    authority = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[],"schema_version":1}\n',
        server_certificate=_server_certificate(),
    )
    component = module.KubernetesProtectedStagingCapacityManagerPolicyComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        prerequisite_reader=lambda _plan: prerequisite,
        runtime_authority_reader=lambda: authority,
        manager_status_reader=lambda: dict(cluster.status),
    )

    assert component.classify(plan)[0] is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
    assert not any(
        payload is not None
        and ("create" in command or "replace" in command)
        and "--dry-run=server" not in command
        for command, payload in cluster.calls
    )


def test_policy_component_rejects_unexpected_server_diff_status(tmp_path: Path) -> None:
    module = importlib.import_module(MODULE)
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    cluster.diff_status = 2
    authority = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[],"schema_version":1}\n',
        server_certificate=_server_certificate(),
    )
    component = module.KubernetesProtectedStagingCapacityManagerPolicyComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        prerequisite_reader=lambda _plan: prerequisite,
        runtime_authority_reader=lambda: authority,
        manager_status_reader=lambda: dict(cluster.status),
    )

    assert component.classify(plan)[0] is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="diff failed"):
        component.apply(plan)
    assert not any(
        payload is not None
        and ("create" in command or "replace" in command)
        and "--dry-run=server" not in command
        for command, payload in cluster.calls
    )


def test_policy_component_keeps_registry_and_certificate_values_out_of_diagnostics(
    tmp_path: Path,
) -> None:
    module = importlib.import_module(MODULE)
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    cluster.diff_status = 2
    registry = b'{"private-value":"registry-secret-marker","schema_version":1}\n'
    certificate = _server_certificate()
    authority = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=registry,
        server_certificate=certificate,
    )
    component = module.KubernetesProtectedStagingCapacityManagerPolicyComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        prerequisite_reader=lambda _plan: prerequisite,
        runtime_authority_reader=lambda: authority,
        manager_status_reader=lambda: dict(cluster.status),
    )

    state, evidence = component.classify(plan)
    assert state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="diff failed") as failure:
        component.apply(plan)

    diagnostic_surface = (
        repr(cluster.calls).encode("utf-8")
        + evidence.encode("ascii")
        + str(failure.value).encode("utf-8")
    )
    assert b"registry-secret-marker" not in diagnostic_surface
    assert registry not in diagnostic_surface
    assert certificate not in diagnostic_surface
    assert base64.b64encode(certificate) not in diagnostic_surface


def test_policy_component_rejects_immutable_policy_drift(tmp_path: Path) -> None:
    module = importlib.import_module(MODULE)
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    authority = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[],"schema_version":1}\n',
        server_certificate=_server_certificate(),
    )
    component = module.KubernetesProtectedStagingCapacityManagerPolicyComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        prerequisite_reader=lambda _plan: prerequisite,
        runtime_authority_reader=lambda: authority,
        manager_status_reader=lambda: dict(cluster.status),
    )
    component.apply(plan)
    policy_key = next(key for key in cluster.resources if key[0] == "ConfigMap")
    cluster.resources[policy_key]["data"] = {"execution-policy.json": "{}"}
    cluster.calls.clear()

    assert component.classify(plan)[0] is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
    assert not any(
        payload is not None and ("create" in command or "replace" in command)
        for command, payload in cluster.calls
    )


def test_policy_component_accepts_trusted_manager_ownership_history(
    tmp_path: Path,
) -> None:
    module = importlib.import_module(MODULE)
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    manager = cluster.resources[("Deployment", "loom-dev", "loom-capacity-manager")]
    managed = manager["metadata"]["managedFields"]
    managed[1:1] = [
        {
            "apiVersion": "apps/v1",
            "fieldsType": "FieldsV1",
            "fieldsV1": {"f:metadata": {"f:annotations": {}}},
            "manager": "kubectl-client-side-apply",
            "operation": "Update",
        },
        {
            "apiVersion": "apps/v1",
            "fieldsType": "FieldsV1",
            "fieldsV1": {"f:spec": {"f:template": {}}},
            "manager": "kubectl-rollout",
            "operation": "Update",
        },
    ]
    authority = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[],"schema_version":1}\n',
        server_certificate=_server_certificate(),
    )
    component = module.KubernetesProtectedStagingCapacityManagerPolicyComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        prerequisite_reader=lambda _plan: prerequisite,
        runtime_authority_reader=lambda: authority,
        manager_status_reader=lambda: dict(cluster.status),
    )

    assert component.classify(plan)[0] is ComponentState.READY
    component.apply(plan)
    assert component.classify(plan)[0] is ComponentState.EXACT


def test_policy_component_detects_source_rotation_between_mutations(tmp_path: Path) -> None:
    module = importlib.import_module(MODULE)
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    certificate = _server_certificate()
    original = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[],"schema_version":1}\n',
        server_certificate=certificate,
    )
    rotated = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[{"principal_id":"rotated"}],"schema_version":1}\n',
        server_certificate=certificate,
    )
    reads = 0

    def authority_reader():  # type: ignore[no-untyped-def]
        nonlocal reads
        reads += 1
        return rotated if reads >= 3 else original

    component = module.KubernetesProtectedStagingCapacityManagerPolicyComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        prerequisite_reader=lambda _plan: prerequisite,
        runtime_authority_reader=authority_reader,
        manager_status_reader=lambda: dict(cluster.status),
    )

    with pytest.raises(RuntimeError, match="source changed"):
        component.apply(plan)

    assert ("Namespace", "", "loom-capacity-router") in cluster.resources
    assert (
        "Deployment",
        "loom-capacity-router",
        "loom-capacity-manager-router",
    ) not in cluster.resources


def test_policy_component_disables_router_when_source_rotates_after_creation(
    tmp_path: Path,
) -> None:
    module = importlib.import_module(MODULE)
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    certificate = _server_certificate()
    original = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[],"schema_version":1}\n',
        server_certificate=certificate,
    )
    rotated = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[{"principal_id":"rotated"}],"schema_version":1}\n',
        server_certificate=certificate,
    )

    def authority_reader():  # type: ignore[no-untyped-def]
        return rotated if _ROUTER_IDENTITY in cluster.resources else original

    component = module.KubernetesProtectedStagingCapacityManagerPolicyComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        prerequisite_reader=lambda _plan: prerequisite,
        runtime_authority_reader=authority_reader,
        manager_status_reader=lambda: dict(cluster.status),
    )

    with pytest.raises(RuntimeError, match="router was disabled"):
        component.apply(plan)

    assert cluster.resources[_ROUTER_IDENTITY]["spec"]["replicas"] == 0


def test_policy_component_requires_frozen_manager_status_after_rollout(
    tmp_path: Path,
) -> None:
    module = importlib.import_module(MODULE)
    candidate = _candidate(tmp_path)
    plan, prerequisite = _plan_and_prerequisite(tmp_path)
    cluster = _PolicyCluster(candidate)
    cluster.status["increase_freeze"] = False
    authority = module.ManagerPolicyRuntimeAuthority(
        authority_incarnation=UUID(str(_seed()["authority_incarnation"])),
        principal_registry=b'{"principals":[],"schema_version":1}\n',
        server_certificate=_server_certificate(),
    )
    component = module.KubernetesProtectedStagingCapacityManagerPolicyComponent(
        runner=cluster,
        candidate_root=candidate,
        container_registry="registry.example.test/loom",
        prerequisite_reader=lambda _plan: prerequisite,
        runtime_authority_reader=lambda: authority,
        manager_status_reader=lambda: dict(cluster.status),
    )

    with pytest.raises(RuntimeError, match="router was disabled"):
        component.apply(plan)
    assert (
        cluster.resources[("Deployment", "loom-capacity-router", "loom-capacity-manager-router")][
            "spec"
        ]["replicas"]
        == 0
    )

    cluster.status["increase_freeze"] = True
    component.apply(plan)
    assert component.classify(plan)[0] is ComponentState.EXACT
