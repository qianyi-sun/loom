from __future__ import annotations

import base64
import hashlib
import json
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from typing import ClassVar

import pytest
import yaml
from sqlalchemy.engine import make_url

from loom_capacity_agent.contracts import ReporterConfigurationV1
from loom_capacity_agent.secret_init import copy_projected_credentials
from loom_capacity_guard.schema_startup import capacity_guard_schema_head
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
)
from loom_cli.rollout.operator.protected_staging_capacity_database_component import (
    KubernetesProtectedStagingCapacityDatabaseComponent,
)
from loom_cli.rollout.operator.protected_staging_capacity_runtime import (
    KubernetesProtectedStagingCapacityRuntime,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan

MODULE = "loom_cli.rollout.operator.protected_staging_capacity_agent_component"


def _component_type():
    import importlib

    module = importlib.import_module(MODULE)
    return module.KubernetesProtectedStagingCapacityAgentComponent


def _seed() -> dict[str, object]:
    return {
        "agent_database_password": "agent-password-" + "a" * 48,
        "agent_incarnation": "00000000-0000-4000-8000-000000000401",
        "authority_incarnation": "00000000-0000-4000-8000-000000000402",
        "migrator_database_password": "migrator-password-" + "m" * 48,
        "observer_database_password": "observer-password-" + "o" * 48,
        "reporter_incarnation": "00000000-0000-4000-8000-000000000403",
        "reporter_token": "reporter-token-" + "t" * 48,
        "runtime_database_password": "runtime-password-" + "r" * 48,
        "schema_version": 1,
        "subject_id": "00000000-0000-4000-8000-000000000404",
        "subject_incarnation": "00000000-0000-4000-8000-000000000405",
    }


_DEFAULT_REPORTER_TLS = {
    "manager-ca.pem": b"manager-ca-public",
    "certificate.pem": b"client-certificate",
    "private-key.pem": b"private-client-key",
}
_DEFAULT_POSTGRES_CA = b"postgres-ca-public"


def _runtime_secret_sources(*, tls: dict[str, bytes], postgres_ca: bytes) -> dict[str, str]:
    return {
        "agent database password": str(_seed()["agent_database_password"]),
        "reporter token": str(_seed()["reporter_token"]),
        "reporter manager CA": tls["manager-ca.pem"].decode("ascii"),
        "reporter certificate": tls["certificate.pem"].decode("ascii"),
        "reporter private key": tls["private-key.pem"].decode("ascii"),
        "Postgres CA": postgres_ca.decode("ascii"),
    }


def _assert_runtime_secret_redaction(
    *,
    argv: list[tuple[str, ...]],
    errors: list[str],
    evidence: list[str],
    sources: dict[str, str],
) -> None:
    observables = {
        "argv": "\n".join(" ".join(command) for command in argv),
        "errors": "\n".join(errors),
        "evidence": "\n".join(evidence),
    }
    for source, secret in sources.items():
        for location, value in observables.items():
            assert secret not in value, f"{source} leaked through {location}"


def test_redaction_assertion_covers_authority_change_tls_values() -> None:
    """Break caught: a TLS value actually used during authority change is omitted from proof."""
    with pytest.raises(AssertionError):
        _assert_runtime_secret_redaction(
            argv=[("kubectl", "manager-ca-one")],
            errors=[],
            evidence=[],
            sources=_runtime_secret_sources(
                tls={
                    "manager-ca.pem": b"manager-ca-one",
                    "certificate.pem": b"certificate-one",
                    "private-key.pem": b"key-one",
                },
                postgres_ca=b"postgres-ca-public",
            ),
        )


class _Cluster:
    environment: ClassVar[dict[str, str]] = {"KUBECONFIG": "/fixed"}

    def __init__(
        self,
        *,
        runtime_status_image: str | None = None,
        runtime_image_id: str | None = None,
    ) -> None:
        self.objects: dict[tuple[str, str], dict[str, object]] = {}
        self.calls: list[tuple[tuple[str, ...], bytes | None]] = []
        self.runtime_status_image = runtime_status_image
        self.runtime_image_id = runtime_image_id

    def capture_stdout(self, argv, *, env, timeout_seconds):
        assert env == self.environment
        command = tuple(argv)
        self.calls.append((command, None))
        assert timeout_seconds in {30.0, 60.0}
        if "secret,configmaps,deployments,networkpolicies" in command:
            return json.dumps(
                {"apiVersion": "v1", "kind": "List", "items": list(self.objects.values())},
                sort_keys=True,
            ).encode("ascii")
        if "--ignore-not-found=true" in command:
            requested = next(
                item
                for item in command
                if "/" in item
                and item.split("/", 1)[0] in {"secret", "configmap", "deployment", "networkpolicy"}
            )
            kind_text, name = requested.split("/", 1)
            kind = {
                "secret": "Secret",
                "configmap": "ConfigMap",
                "deployment": "Deployment",
                "networkpolicy": "NetworkPolicy",
            }[kind_text]
            value = self.objects.get((kind, name))
            return b"" if value is None else json.dumps(value, sort_keys=True).encode("ascii")
        if "pods" in command:
            deployment = self.objects.get(("Deployment", "loom-capacity-agent"))
            if deployment is None:
                return b'{"apiVersion":"v1","kind":"List","items":[]}'
            image = deployment["spec"]["template"]["spec"]["containers"][0]["image"]
            status = {
                "name": "capacity-agent",
                "image": self.runtime_status_image or image,
                "ready": True,
            }
            if self.runtime_image_id is not None:
                status["imageID"] = self.runtime_image_id
            return json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "List",
                    "items": [
                        {
                            "metadata": {
                                "name": "loom-capacity-agent-1",
                                "namespace": "loom-staging",
                                "annotations": deepcopy(
                                    deployment["spec"]["template"]["metadata"].get(
                                        "annotations", {}
                                    )
                                ),
                            },
                            "spec": {"containers": [{"name": "capacity-agent", "image": image}]},
                            "status": {
                                "phase": "Running",
                                "containerStatuses": [status],
                            },
                        }
                    ],
                },
                sort_keys=True,
            ).encode("ascii")
        raise AssertionError(command)

    def run_status(self, argv, *, env, input_payload, timeout_seconds):
        assert env == self.environment
        assert timeout_seconds == 60.0
        assert input_payload is not None
        command = tuple(argv)
        self.calls.append((command, input_payload))
        assert "--server-side=true" in command
        desired = {
            (item["kind"], item["metadata"]["name"]): item
            for item in yaml.safe_load_all(input_payload)
            if item is not None
        }
        current = {key: deepcopy(self.objects.get(key)) for key in desired}
        if any(item is None for item in current.values()):
            return 1
        for item in current.values():
            assert item is not None
            item.pop("status", None)
            metadata = item["metadata"]
            for key in ("managedFields", "resourceVersion", "uid", "generation"):
                metadata.pop(key, None)
        return 0 if desired == current else 1

    def run_checked(self, argv, *, env, input_payload, timeout_seconds):
        assert env == self.environment
        command = tuple(argv)
        self.calls.append((command, input_payload))
        if "apply" in command:
            assert timeout_seconds == 60.0
            assert input_payload is not None
            for document in yaml.safe_load_all(input_payload):
                if document is None:
                    continue
                metadata = document["metadata"]
                prior = self.objects.get((document["kind"], metadata["name"]))
                if prior is not None and prior.get("immutable") is True:
                    if prior.get("data") != document.get("data"):
                        raise RuntimeError("immutable data cannot be changed")
                metadata.update(
                    {
                        "uid": "18fbc43f-2113-4fd7-8c7d-4cfc6c85d800",
                        "resourceVersion": "7",
                        "managedFields": [
                            {
                                "apiVersion": document["apiVersion"],
                                "fieldsType": "FieldsV1",
                                "fieldsV1": {"f:spec": {}},
                                "manager": "loom-staging-capacity-agent",
                                "operation": "Apply",
                            }
                        ],
                    }
                )
                self.objects[(document["kind"], metadata["name"])] = document
            return
        assert timeout_seconds == 660.0
        assert "rollout" in command and "status" in command


def _component(
    cluster: _Cluster,
    *,
    tls: dict[str, bytes] | None = None,
    postgres_ca: bytes = _DEFAULT_POSTGRES_CA,
):
    reporter_tls = _DEFAULT_REPORTER_TLS if tls is None else tls
    return _component_type()(
        runner=cluster,
        container_registry="registry.example.test/loom",
        seed_reader=_seed,
        reporter_tls_reader=lambda: reporter_tls,
        postgres_ca_reader=lambda: postgres_ca,
    )


def test_new_generation_updates_configuration_without_changing_immutable_credentials(
    tmp_path: Path,
) -> None:
    """Break caught: a new rollout rewrites the immutable credential Secret."""
    cluster = _Cluster()
    component = _component(cluster)
    old = _plan(tmp_path)
    component.apply(old)
    credentials = deepcopy(cluster.objects[("Secret", "loom-capacity-agent")]["data"])
    old_template = deepcopy(
        cluster.objects[("Deployment", "loom-capacity-agent")]["spec"]["template"]
    )
    new = replace(old, starting_mutation_epoch=old.starting_mutation_epoch + 1)

    assert component.classify(new)[0] is ComponentState.READY
    component.apply(new)
    assert component.classify(new)[0] is ComponentState.EXACT
    assert cluster.objects[("Secret", "loom-capacity-agent")]["data"] == credentials
    configuration = cluster.objects[("ConfigMap", "loom-capacity-agent")]["data"]
    assert json.loads(configuration["reporter-configuration.json"])["configuration_generation"] == 9
    assert (
        cluster.objects[("Deployment", "loom-capacity-agent")]["spec"]["template"] != old_template
    )


def test_legacy_combined_secret_migrates_without_deletion_or_projecting_stale_configuration(
    tmp_path: Path,
) -> None:
    """Break caught: legacy immutable Secrets cannot roll over or still supply old config."""
    cluster = _Cluster()
    component = _component(cluster)
    old = _plan(tmp_path)
    component.apply(old)
    legacy = cluster.objects[("Secret", "loom-capacity-agent")]
    configuration = cluster.objects.pop(("ConfigMap", "loom-capacity-agent"), None)
    if configuration is not None:
        legacy["data"]["reporter-configuration.json"] = base64.b64encode(
            configuration["data"]["reporter-configuration.json"].encode("ascii")
        ).decode("ascii")
    original_data = deepcopy(legacy["data"])
    deployment = cluster.objects[("Deployment", "loom-capacity-agent")]
    deployment["spec"]["template"]["spec"]["volumes"][0] = {
        "name": "projected",
        "secret": {"secretName": "loom-capacity-agent", "defaultMode": 0o440},
    }
    new = replace(old, starting_mutation_epoch=old.starting_mutation_epoch + 1)

    assert component.classify(new)[0] is ComponentState.READY
    component.apply(new)
    assert component.classify(new)[0] is ComponentState.EXACT
    assert legacy["data"] == original_data
    assert cluster.objects[("Secret", "loom-capacity-agent")]["data"] == original_data
    projected = cluster.objects[("Deployment", "loom-capacity-agent")]["spec"]["template"]["spec"][
        "volumes"
    ][0]["projected"]
    secret_keys = {item["key"] for item in projected["sources"][0]["secret"]["items"]}
    assert "reporter-configuration.json" not in secret_keys
    assert projected["sources"][1]["configMap"]["items"] == [
        {"key": "reporter-configuration.json", "path": "reporter-configuration.json"}
    ]
    assert not any("delete" in argv for argv, _ in cluster.calls)

    # Exercise the real initializer with exactly the rendered projection. Old
    # configuration remains in the immutable Secret but cannot reach runtime.
    source = tmp_path / "projection"
    source.mkdir()
    for projection in projected["sources"]:
        kind, entry = next(iter(projection.items()))
        object_kind = "Secret" if kind == "secret" else "ConfigMap"
        data = cluster.objects[(object_kind, entry["name"])]["data"]
        for item in entry["items"]:
            value = data[item["key"]]
            payload = base64.b64decode(value) if kind == "secret" else value.encode("ascii")
            (source / item["path"]).write_bytes(payload)
    destination = tmp_path / "runtime-files"
    init_command = cluster.objects[("Deployment", "loom-capacity-agent")]["spec"]["template"][
        "spec"
    ]["initContainers"][0]["command"]
    expected_digest = init_command[init_command.index("--configuration-sha256") + 1]
    copy_projected_credentials(source, destination, configuration_sha256=expected_digest)
    loaded = json.loads((destination / "reporter-configuration.json").read_bytes())
    assert loaded["configuration_generation"] == 9
    assert loaded != json.loads(base64.b64decode(original_data["reporter-configuration.json"]))
    assert (destination / "reporter-token").read_bytes() == str(_seed()["reporter_token"]).encode()
    assert (destination / "reporter-token").stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("legacy", [False, True])
def test_rollover_keeps_credential_and_owner_drift_closed(tmp_path: Path, legacy: bool) -> None:
    """Break caught: ignoring stale config also permits changed credentials or foreign config owners."""
    cluster = _Cluster()
    component = _component(cluster)
    old = _plan(tmp_path)
    component.apply(old)
    secret = cluster.objects[("Secret", "loom-capacity-agent")]
    if legacy:
        secret["data"]["reporter-configuration.json"] = base64.b64encode(b"unused").decode()
    original_token = secret["data"]["reporter-token"]
    secret["data"]["reporter-token"] = base64.b64encode(b"foreign-token").decode()
    new = replace(old, starting_mutation_epoch=old.starting_mutation_epoch + 1)
    cluster.calls.clear()
    assert component.classify(new)[0] is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="drifted"):
        component.apply(new)
    assert not any("apply" in command for command, _ in cluster.calls)
    secret["data"]["reporter-token"] = original_token
    configuration = cluster.objects[("ConfigMap", "loom-capacity-agent")]
    configuration["metadata"]["managedFields"][0]["manager"] = "foreign-manager"
    assert component.classify(new)[0] is ComponentState.DRIFTED


def test_interrupted_rollover_recovers_after_configuration_applied(tmp_path: Path) -> None:
    """Break caught: an interruption between config and deployment leaves an unrecoverable mix."""

    class InterruptedCluster(_Cluster):
        fail_deployment = False

        def run_checked(self, argv, *, env, input_payload, timeout_seconds):
            if self.fail_deployment and "apply" in argv and input_payload is not None:
                documents = list(yaml.safe_load_all(input_payload))
                if any(item["kind"] == "Deployment" for item in documents):
                    raise RuntimeError("interrupted deployment")
            return super().run_checked(
                argv, env=env, input_payload=input_payload, timeout_seconds=timeout_seconds
            )

    cluster = InterruptedCluster()
    component = _component(cluster)
    old = _plan(tmp_path)
    component.apply(old)
    original_data = deepcopy(cluster.objects[("Secret", "loom-capacity-agent")]["data"])
    new = replace(old, starting_mutation_epoch=old.starting_mutation_epoch + 1)
    cluster.fail_deployment = True
    with pytest.raises(RuntimeError, match="interrupted deployment"):
        component.apply(new)
    assert component.classify(new)[0] is ComponentState.READY
    cluster.fail_deployment = False
    component.apply(new)
    assert component.classify(new)[0] is ComponentState.EXACT
    assert cluster.objects[("Secret", "loom-capacity-agent")]["data"] == original_data


@pytest.mark.parametrize(
    "annotations", [{}, {"loom.carin.dev/reporter-configuration-sha256": "0" * 64}]
)
def test_ready_same_image_pod_must_load_current_configuration(tmp_path: Path, annotations) -> None:
    """Break caught: image equality alone certifies a pod from the previous configuration."""

    class StalePodCluster(_Cluster):
        def capture_stdout(self, argv, *, env, timeout_seconds):
            payload = super().capture_stdout(argv, env=env, timeout_seconds=timeout_seconds)
            if "pods" in argv:
                value = json.loads(payload)
                value["items"][0]["metadata"]["annotations"] = annotations
                return json.dumps(value).encode()
            return payload

    with pytest.raises(RuntimeError, match="did not converge"):
        _component(StalePodCluster()).apply(_plan(tmp_path))


@pytest.mark.parametrize("pod_state", ["stale-configuration", "absent", "not-ready"])
def test_resume_cannot_certify_exact_resources_with_a_stale_configuration_pod(
    tmp_path: Path, pod_state: str
) -> None:
    """Break caught: failed final readback is bypassed by EXACT resource-only classification."""

    class StalePodCluster(_Cluster):
        stale = True

        def capture_stdout(self, argv, *, env, timeout_seconds):
            payload = super().capture_stdout(argv, env=env, timeout_seconds=timeout_seconds)
            if self.stale and "pods" in argv:
                value = json.loads(payload)
                if pod_state == "absent":
                    value["items"] = []
                elif pod_state == "not-ready":
                    value["items"][0]["status"]["containerStatuses"][0]["ready"] = False
                else:
                    value["items"][0]["metadata"]["annotations"] = {}
                return json.dumps(value).encode()
            return payload

    cluster = StalePodCluster()
    component = _component(cluster)
    plan = _plan(tmp_path)
    with pytest.raises(RuntimeError, match="did not converge"):
        component.apply(plan)
    assert component.classify(plan)[0] is ComponentState.READY
    cluster.stale = False
    assert component.classify(plan)[0] is ComponentState.EXACT


def test_absent_agent_set_converges_to_exact_hardened_candidate_only_resources(
    tmp_path: Path,
) -> None:
    """Break caught: a permissive, incomplete, or non-candidate agent would be applied."""
    cluster = _Cluster()
    component = _component(cluster)
    plan = _plan(tmp_path)

    assert component.classify(plan)[0] is ComponentState.READY
    component.apply(plan)
    assert component.classify(plan)[0] is ComponentState.EXACT

    assert set(cluster.objects) == {
        ("Secret", "loom-capacity-agent"),
        ("ConfigMap", "loom-capacity-agent"),
        ("Deployment", "loom-capacity-agent"),
        ("NetworkPolicy", "loom-capacity-agent-egress"),
        ("NetworkPolicy", "loom-capacity-agent-postgres-ingress"),
    }
    secret = cluster.objects[("Secret", "loom-capacity-agent")]
    assert secret["immutable"] is True
    assert set(secret["data"]) == {
        "ca.pem",
        "certificate.pem",
        "database-url",
        "private-key.pem",
        "reporter-token",
    }
    configuration_map = cluster.objects[("ConfigMap", "loom-capacity-agent")]
    assert set(configuration_map["data"]) == {"reporter-configuration.json"}
    public_configuration = json.dumps(configuration_map)
    for value in _runtime_secret_sources(
        tls=_DEFAULT_REPORTER_TLS, postgres_ca=_DEFAULT_POSTGRES_CA
    ).values():
        assert value not in public_configuration
        assert base64.b64encode(value.encode()).decode() not in public_configuration
    database_url = base64.b64decode(secret["data"]["database-url"]).decode("ascii")
    parsed_url = make_url(database_url)
    assert (parsed_url.username, parsed_url.host, parsed_url.port, parsed_url.database) == (
        "loom_cap_staging_agent",
        "loom-postgres-rw.loom-staging.svc.cluster.local",
        5432,
        "loom",
    )
    assert parsed_url.query == {
        "sslmode": "verify-full",
        "sslrootcert": "/run/loom-postgres-ca/ca.crt",
    }
    deployment = cluster.objects[("Deployment", "loom-capacity-agent")]
    pod_spec = deployment["spec"]["template"]["spec"]
    expected_image = (
        "registry.example.test/loom/loom-control-plane@" + plan.image_digests["loom-control-plane"]
    )
    assert [item["image"] for item in pod_spec["initContainers"] + pod_spec["containers"]] == [
        expected_image,
        expected_image,
    ]
    assert pod_spec["automountServiceAccountToken"] is False
    assert pod_spec["enableServiceLinks"] is False
    assert deployment["spec"]["replicas"] == 1
    assert deployment["spec"]["strategy"] == {"type": "Recreate"}
    assert pod_spec["securityContext"] == {
        "fsGroup": 65532,
        "fsGroupChangePolicy": "OnRootMismatch",
        "runAsGroup": 65532,
        "runAsNonRoot": True,
        "runAsUser": 65532,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    assert pod_spec["initContainers"][0]["command"] == [
        "python",
        "-m",
        "loom_capacity_agent.secret_init",
        "--source",
        "/var/run/loom-capacity-projected",
        "--destination",
        "/run/loom-capacity/files",
        "--configuration-sha256",
        hashlib.sha256(
            configuration_map["data"]["reporter-configuration.json"].encode()
        ).hexdigest(),
    ]
    assert pod_spec["containers"][0]["readinessProbe"]["httpGet"] == {
        "path": "/ready",
        "port": "health",
    }
    expected_restricted = {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
        "runAsGroup": 65532,
        "runAsNonRoot": True,
        "runAsUser": 65532,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    assert pod_spec["initContainers"][0]["securityContext"] == expected_restricted
    assert pod_spec["containers"][0]["securityContext"] == expected_restricted
    assert pod_spec["containers"][0]["resources"] == {
        "requests": {"cpu": "25m", "memory": "64Mi"},
        "limits": {"cpu": "500m", "memory": "256Mi"},
    }
    assert pod_spec["volumes"] == [
        {
            "name": "projected",
            "projected": {
                "defaultMode": 0o440,
                "sources": [
                    {
                        "secret": {
                            "name": "loom-capacity-agent",
                            "items": [
                                {"key": key, "path": key}
                                for key in (
                                    "ca.pem",
                                    "certificate.pem",
                                    "database-url",
                                    "private-key.pem",
                                    "reporter-token",
                                )
                            ],
                        }
                    },
                    {
                        "configMap": {
                            "name": "loom-capacity-agent",
                            "items": [
                                {
                                    "key": "reporter-configuration.json",
                                    "path": "reporter-configuration.json",
                                }
                            ],
                        }
                    },
                ],
            },
        },
        {"name": "runtime", "emptyDir": {"medium": "Memory"}},
        {
            "name": "postgres-ca",
            "secret": {
                "secretName": "loom-postgres-ca",
                "defaultMode": 0o440,
                "items": [{"key": "ca.crt", "path": "ca.crt"}],
            },
        },
    ]
    assert "loom-protected-worker-runtime" not in json.dumps(deployment)
    policies = [value for key, value in cluster.objects.items() if key[0] == "NetworkPolicy"]
    assert all(value["spec"].get("policyTypes") for value in policies)
    apply_positions = [
        index for index, (argv, _payload) in enumerate(cluster.calls) if "apply" in argv
    ]
    rollout_positions = [
        index for index, (argv, _payload) in enumerate(cluster.calls) if "rollout" in argv
    ]
    assert apply_positions and rollout_positions and max(apply_positions) < min(rollout_positions)
    forbidden = str(_seed()["agent_database_password"])
    assert all(forbidden not in " ".join(argv) for argv, _payload in cluster.calls)
    evidence = component.classify(plan)[1]
    for credential in (
        _seed()["agent_database_password"],
        _seed()["reporter_token"],
        b"manager-ca-public".decode("ascii"),
        b"private-client-key".decode("ascii"),
    ):
        assert str(credential) not in evidence


def test_ready_pod_accepts_containerd_local_status_image_with_exact_image_id(
    tmp_path: Path,
) -> None:
    """Break caught: containerd image normalization falsely fails exact pod readback."""
    plan = _plan(tmp_path)
    expected_image = (
        f"registry.example.test/loom/loom-control-plane@{plan.image_digests['loom-control-plane']}"
    )
    cluster = _Cluster(
        runtime_status_image="sha256:" + "1" * 64,
        runtime_image_id=expected_image,
    )
    component = _component(cluster)

    component.apply(plan)

    assert component.classify(plan)[0] is ComponentState.EXACT


def test_agent_diff_uses_installed_kubectl_flags(tmp_path: Path) -> None:
    """Break caught: adding apply-only validation flags makes kubectl diff exit 2."""
    cluster = _Cluster()
    component = _component(cluster)
    plan = _plan(tmp_path)
    component.apply(plan)

    assert component.classify(plan)[0] is ComponentState.EXACT

    diff_calls = [argv for argv, payload in cluster.calls if payload is not None and "diff" in argv]
    assert diff_calls
    assert all("--validate=strict" not in argv for argv in diff_calls)
    apply_calls = [
        argv for argv, payload in cluster.calls if payload is not None and "apply" in argv
    ]
    assert apply_calls
    assert all("--validate=strict" in argv for argv in apply_calls)


def test_runtime_dispatches_agent_only_after_manager_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Break caught: the journal slot remains a placeholder or runs before its manager binding."""
    called: list[str] = []

    class _Agent:
        def classify(self, plan):
            called.append("classify")
            return ComponentState.READY, "a" * 64

        def apply(self, plan):
            called.append("apply")

    runtime = KubernetesProtectedStagingCapacityRuntime(
        runner=object(),  # type: ignore[arg-type]
        state_root=tmp_path / "state",
        candidate_root=tmp_path / "candidate",
        service_uid=1,
        service_gid=1,
        container_registry="registry.example.test/loom",
    )
    monkeypatch.setattr(
        KubernetesProtectedStagingCapacityRuntime,
        "_agent_component",
        lambda _self, _plan: _Agent(),
    )
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    agent = runtime.components(plan, epoch_guard=lambda _: epoch)[-1]

    assert agent.component_id == "staging-capacity-agent"
    assert agent.classify(plan).state is ComponentState.READY
    agent.apply(plan)
    assert called == ["classify", "classify", "apply"]


def test_immutable_secret_drift_and_foreign_owner_fail_closed_without_apply(tmp_path: Path) -> None:
    """Break caught: an immutable credential replacement or foreign object is adopted."""
    cluster = _Cluster()
    component = _component(cluster)
    plan = _plan(tmp_path)
    component.apply(plan)
    secret = cluster.objects[("Secret", "loom-capacity-agent")]
    secret["data"]["reporter-token"] = base64.b64encode(b"replacement").decode("ascii")
    before = len([call for call in cluster.calls if "apply" in call[0]])

    assert component.classify(plan)[0] is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state drifted"):
        component.apply(plan)
    assert len([call for call in cluster.calls if "apply" in call[0]]) == before

    secret["data"]["reporter-token"] = base64.b64encode(
        str(_seed()["reporter_token"]).encode("ascii")
    ).decode("ascii")
    secret["metadata"]["managedFields"][0]["manager"] = "foreign-manager"
    assert component.classify(plan)[0] is ComponentState.DRIFTED


def test_exact_immutable_secret_allows_safe_partial_mutable_recovery(tmp_path: Path) -> None:
    """Break caught: a missing safely owned policy or Deployment blocks recovery."""
    cluster = _Cluster()
    component = _component(cluster)
    plan = _plan(tmp_path)
    component.apply(plan)
    cluster.objects.pop(("Deployment", "loom-capacity-agent"))
    cluster.objects.pop(("NetworkPolicy", "loom-capacity-agent-egress"))

    assert component.classify(plan)[0] is ComponentState.READY
    component.apply(plan)
    assert component.classify(plan)[0] is ComponentState.EXACT


def test_authority_change_before_mutation_and_diff_failure_are_closed(tmp_path: Path) -> None:
    """Break caught: a changed credential source or indeterminate server diff still mutates."""
    cluster = _Cluster()
    seed = _seed()
    reads = 0

    def changing_tls() -> dict[str, bytes]:
        nonlocal reads
        reads += 1
        suffix = b"one" if reads == 1 else b"two"
        return {
            "manager-ca.pem": b"manager-ca-" + suffix,
            "certificate.pem": b"certificate-" + suffix,
            "private-key.pem": b"key-" + suffix,
        }

    component = _component_type()(
        runner=cluster,
        container_registry="registry.example.test/loom",
        seed_reader=lambda: seed,
        reporter_tls_reader=changing_tls,
        postgres_ca_reader=lambda: b"postgres-ca",
    )
    with pytest.raises(RuntimeError, match="authority changed before apply"):
        component.apply(_plan(tmp_path))
    assert not [call for call in cluster.calls if "apply" in call[0]]

    class _DiffFailureCluster(_Cluster):
        def run_status(self, argv, *, env, input_payload, timeout_seconds):
            return 2

    normal = _Cluster()
    _component(normal).apply(_plan(tmp_path))
    failing = _DiffFailureCluster()
    failing.objects = deepcopy(normal.objects)
    assert _component(failing).classify(_plan(tmp_path))[0] is ComponentState.DRIFTED


def test_controller_status_and_ready_pod_failures_do_not_relax_spec_ownership(
    tmp_path: Path,
) -> None:
    """Break caught: status ownership is mistaken for spec ownership, or a bad pod passes readback."""
    cluster = _Cluster()
    component = _component(cluster)
    plan = _plan(tmp_path)
    component.apply(plan)
    deployment = cluster.objects[("Deployment", "loom-capacity-agent")]
    deployment["metadata"]["managedFields"].append(
        {
            "apiVersion": "apps/v1",
            "fieldsType": "FieldsV1",
            "fieldsV1": {"f:status": {}},
            "manager": "k3s",
            "operation": "Update",
            "subresource": "status",
        }
    )

    assert component.classify(plan)[0] is ComponentState.EXACT

    class _UnreadyCluster(_Cluster):
        def capture_stdout(self, argv, *, env, timeout_seconds):
            payload = super().capture_stdout(argv, env=env, timeout_seconds=timeout_seconds)
            if "pods" in argv and payload:
                value = json.loads(payload)
                value["items"][0]["status"]["containerStatuses"][0]["ready"] = False
                return json.dumps(value).encode("ascii")
            return payload

    unready = _UnreadyCluster()
    with pytest.raises(RuntimeError, match="did not converge"):
        _component(unready).apply(plan)

    wrong_runtime_identity = _Cluster(
        runtime_status_image="sha256:" + "2" * 64,
        runtime_image_id="registry.example.test/loom/loom-control-plane@sha256:" + "3" * 64,
    )
    with pytest.raises(RuntimeError, match="did not converge"):
        _component(wrong_runtime_identity).apply(plan)

    missing_runtime_image_id = _Cluster(runtime_status_image="sha256:" + "4" * 64)
    with pytest.raises(RuntimeError, match="did not converge"):
        _component(missing_runtime_image_id).apply(plan)

    class _MalformedStatusCluster(_Cluster):
        def capture_stdout(self, argv, *, env, timeout_seconds):
            payload = super().capture_stdout(argv, env=env, timeout_seconds=timeout_seconds)
            if "pods" in argv and payload:
                value = json.loads(payload)
                value["items"][0]["status"]["containerStatuses"][0] = []
                return json.dumps(value).encode("ascii")
            return payload

    malformed = _MalformedStatusCluster()
    with pytest.raises(RuntimeError, match="did not converge"):
        _component(malformed).apply(plan)


def test_k3s_status_ownership_is_limited_to_deployment_revision_metadata(
    tmp_path: Path,
) -> None:
    """Break caught: live K3s revision ownership is rejected or foreign metadata is trusted."""

    class _ExactDiffCluster(_Cluster):
        def run_status(self, argv, *, env, input_payload, timeout_seconds):
            return 0

    cluster = _ExactDiffCluster()
    component = _component(cluster)
    plan = _plan(tmp_path)
    component.apply(plan)
    deployment = cluster.objects[("Deployment", "loom-capacity-agent")]
    deployment["metadata"]["annotations"] = {"deployment.kubernetes.io/revision": "1"}
    status_owner = {
        "apiVersion": "apps/v1",
        "fieldsType": "FieldsV1",
        "fieldsV1": {
            "f:metadata": {
                "f:annotations": {
                    ".": {},
                    "f:deployment.kubernetes.io/revision": {},
                }
            },
            "f:status": {"f:availableReplicas": {}},
        },
        "manager": "k3s",
        "operation": "Update",
        "subresource": "status",
    }
    deployment["metadata"]["managedFields"].append(status_owner)

    assert component.classify(plan)[0] is ComponentState.EXACT

    status_owner["fieldsV1"]["f:metadata"]["f:labels"] = {"f:foreign": {}}
    assert component.classify(plan)[0] is ComponentState.DRIFTED
    status_owner["fieldsV1"]["f:metadata"].pop("f:labels")
    status_owner["fieldsV1"].pop("f:status")
    assert component.classify(plan)[0] is ComponentState.DRIFTED
    status_owner["fieldsV1"]["f:status"] = {}
    status_owner["apiVersion"] = "v1"
    assert component.classify(plan)[0] is ComponentState.DRIFTED
    status_owner["apiVersion"] = "apps/v1"
    status_owner["fieldsV1"]["f:metadata"] = None
    assert component.classify(plan)[0] is ComponentState.DRIFTED


def test_rollout_and_readback_failures_are_closed_without_secret_disclosure(tmp_path: Path) -> None:
    """Break caught: failed rollout, malformed readback, or readback command failure is accepted."""

    class _RolloutFailureCluster(_Cluster):
        def run_checked(self, argv, *, env, input_payload, timeout_seconds):
            if "rollout" in argv:
                raise RuntimeError("rollout failed")
            return super().run_checked(
                argv,
                env=env,
                input_payload=input_payload,
                timeout_seconds=timeout_seconds,
            )

    with pytest.raises(RuntimeError, match="rollout failed") as rollout_error:
        _component(_RolloutFailureCluster()).apply(_plan(tmp_path))
    assert str(_seed()["agent_database_password"]) not in str(rollout_error.value)

    class _MalformedReadbackCluster(_Cluster):
        def capture_stdout(self, argv, *, env, timeout_seconds):
            if "pods" in argv:
                return b"not-json"
            return super().capture_stdout(argv, env=env, timeout_seconds=timeout_seconds)

    with pytest.raises(RuntimeError, match="did not converge"):
        _component(_MalformedReadbackCluster()).apply(_plan(tmp_path))

    class _ReadbackCommandFailureCluster(_Cluster):
        def __init__(self) -> None:
            super().__init__()
            self.applied = False

        def run_checked(self, argv, *, env, input_payload, timeout_seconds):
            if "apply" in argv:
                self.applied = True
            return super().run_checked(
                argv,
                env=env,
                input_payload=input_payload,
                timeout_seconds=timeout_seconds,
            )

        def capture_stdout(self, argv, *, env, timeout_seconds):
            if self.applied and "secret,configmaps,deployments,networkpolicies" in argv:
                raise RuntimeError("readback unavailable")
            return super().capture_stdout(argv, env=env, timeout_seconds=timeout_seconds)

    with pytest.raises(RuntimeError, match="readback unavailable"):
        _component(_ReadbackCommandFailureCluster()).apply(_plan(tmp_path))


def test_all_runtime_secret_sources_are_redacted_from_argv_errors_and_evidence(
    tmp_path: Path,
) -> None:
    """Break caught: any projected runtime secret becomes observable outside Secret data."""
    plan = _plan(tmp_path)

    def assert_observed(
        cluster: _Cluster,
        *,
        errors: list[str],
        evidence: list[str],
        sources: dict[str, str],
    ) -> None:
        _assert_runtime_secret_redaction(
            argv=[command for command, _payload in cluster.calls],
            errors=errors,
            evidence=evidence,
            sources=sources,
        )

    def observe(cluster: _Cluster, component, *, sources: dict[str, str]) -> None:
        evidence = [component.classify(plan)[1]]
        assert_observed(cluster, errors=[], evidence=evidence, sources=sources)

    exact_cluster = _Cluster()
    exact_component = _component(exact_cluster)
    exact_component.apply(plan)
    observe(
        exact_cluster,
        exact_component,
        sources=_runtime_secret_sources(
            tls=_DEFAULT_REPORTER_TLS,
            postgres_ca=_DEFAULT_POSTGRES_CA,
        ),
    )

    immutable_cluster = _Cluster()
    immutable_component = _component(immutable_cluster)
    immutable_component.apply(plan)
    immutable_cluster.objects[("Secret", "loom-capacity-agent")]["data"]["reporter-token"] = (
        base64.b64encode(b"replacement").decode("ascii")
    )
    with pytest.raises(RuntimeError) as immutable_error:
        immutable_component.apply(plan)
    immutable_evidence = [immutable_component.classify(plan)[1]]
    assert_observed(
        immutable_cluster,
        errors=[str(immutable_error.value)],
        evidence=immutable_evidence,
        sources=_runtime_secret_sources(
            tls=_DEFAULT_REPORTER_TLS,
            postgres_ca=_DEFAULT_POSTGRES_CA,
        ),
    )

    class _RolloutFailureCluster(_Cluster):
        def run_checked(self, command, *, env, input_payload, timeout_seconds):
            if "rollout" in command:
                raise RuntimeError("rollout failed")
            return super().run_checked(
                command,
                env=env,
                input_payload=input_payload,
                timeout_seconds=timeout_seconds,
            )

    rollout_cluster = _RolloutFailureCluster()
    rollout_component = _component(rollout_cluster)
    with pytest.raises(RuntimeError) as rollout_error:
        rollout_component.apply(plan)
    rollout_evidence = [rollout_component.classify(plan)[1]]
    assert_observed(
        rollout_cluster,
        errors=[str(rollout_error.value)],
        evidence=rollout_evidence,
        sources=_runtime_secret_sources(
            tls=_DEFAULT_REPORTER_TLS,
            postgres_ca=_DEFAULT_POSTGRES_CA,
        ),
    )

    class _MalformedReadbackCluster(_Cluster):
        def capture_stdout(self, command, *, env, timeout_seconds):
            if "pods" in command:
                return b"not-json"
            return super().capture_stdout(command, env=env, timeout_seconds=timeout_seconds)

    malformed_cluster = _MalformedReadbackCluster()
    malformed_component = _component(malformed_cluster)
    with pytest.raises(RuntimeError) as malformed_error:
        malformed_component.apply(plan)
    malformed_evidence = [malformed_component.classify(plan)[1]]
    assert_observed(
        malformed_cluster,
        errors=[str(malformed_error.value)],
        evidence=malformed_evidence,
        sources=_runtime_secret_sources(
            tls=_DEFAULT_REPORTER_TLS,
            postgres_ca=_DEFAULT_POSTGRES_CA,
        ),
    )

    class _ReadbackCommandFailureCluster(_Cluster):
        def __init__(self) -> None:
            super().__init__()
            self.applied = False

        def run_checked(self, command, *, env, input_payload, timeout_seconds):
            if "apply" in command:
                self.applied = True
            return super().run_checked(
                command,
                env=env,
                input_payload=input_payload,
                timeout_seconds=timeout_seconds,
            )

        def capture_stdout(self, command, *, env, timeout_seconds):
            if self.applied and "secret,configmaps,deployments,networkpolicies" in command:
                raise RuntimeError("readback unavailable")
            return super().capture_stdout(command, env=env, timeout_seconds=timeout_seconds)

    readback_cluster = _ReadbackCommandFailureCluster()
    readback_component = _component(readback_cluster)
    readback_evidence = [readback_component.classify(plan)[1]]
    with pytest.raises(RuntimeError) as readback_error:
        readback_component.apply(plan)
    assert_observed(
        readback_cluster,
        errors=[str(readback_error.value)],
        evidence=readback_evidence,
        sources=_runtime_secret_sources(
            tls=_DEFAULT_REPORTER_TLS,
            postgres_ca=_DEFAULT_POSTGRES_CA,
        ),
    )

    source_reads = 0

    def changing_tls() -> dict[str, bytes]:
        nonlocal source_reads
        source_reads += 1
        suffix = b"one" if source_reads == 1 else b"two"
        return {
            "manager-ca.pem": b"manager-ca-" + suffix,
            "certificate.pem": b"certificate-" + suffix,
            "private-key.pem": b"key-" + suffix,
        }

    authority_cluster = _Cluster()
    authority_component = _component_type()(
        runner=authority_cluster,
        container_registry="registry.example.test/loom",
        seed_reader=_seed,
        reporter_tls_reader=changing_tls,
        postgres_ca_reader=lambda: b"postgres-ca",
    )
    with pytest.raises(RuntimeError) as authority_error:
        authority_component.apply(plan)
    authority_sources = _runtime_secret_sources(
        tls={
            "manager-ca.pem": b"manager-ca-one",
            "certificate.pem": b"certificate-one",
            "private-key.pem": b"key-one",
        },
        postgres_ca=b"postgres-ca",
    )
    authority_sources.update(
        {
            "reporter manager CA after change": "manager-ca-two",
            "reporter certificate after change": "certificate-two",
            "reporter private key after change": "key-two",
        }
    )
    authority_evidence = [authority_component.classify(plan)[1]]
    assert_observed(
        authority_cluster,
        errors=[str(authority_error.value)],
        evidence=authority_evidence,
        sources=authority_sources,
    )

    normal_cluster = _Cluster()
    _component(normal_cluster).apply(plan)

    class _DiffFailureCluster(_Cluster):
        def run_status(self, command, *, env, input_payload, timeout_seconds):
            return 2

    diff_cluster = _DiffFailureCluster()
    diff_cluster.objects = deepcopy(normal_cluster.objects)
    diff_component = _component(diff_cluster)
    diff_evidence = [diff_component.classify(plan)[1]]
    assert_observed(
        diff_cluster,
        errors=[],
        evidence=diff_evidence,
        sources=_runtime_secret_sources(
            tls=_DEFAULT_REPORTER_TLS,
            postgres_ca=_DEFAULT_POSTGRES_CA,
        ),
    )


def test_policies_have_only_the_three_approved_egress_destinations(tmp_path: Path) -> None:
    """Break caught: a shared or broad rule admits a fourth endpoint or wrong Postgres pods."""
    cluster = _Cluster()
    component = _component(cluster)
    component.apply(_plan(tmp_path))
    egress = cluster.objects[("NetworkPolicy", "loom-capacity-agent-egress")]["spec"]
    assert egress["podSelector"] == {
        "matchLabels": {"app.kubernetes.io/name": "loom-capacity-agent"}
    }
    assert egress["egress"] == [
        {
            "to": [
                {
                    "namespaceSelector": {
                        "matchLabels": {"kubernetes.io/metadata.name": "loom-dev"}
                    },
                    "podSelector": {
                        "matchLabels": {"app.kubernetes.io/name": "loom-capacity-manager"}
                    },
                }
            ],
            "ports": [{"protocol": "TCP", "port": 8443}],
        },
        {
            "to": [
                {
                    "namespaceSelector": {
                        "matchLabels": {"kubernetes.io/metadata.name": "loom-staging"}
                    },
                    "podSelector": {
                        "matchLabels": {
                            "cnpg.io/cluster": "loom-postgres",
                            "cnpg.io/instanceRole": "primary",
                        }
                    },
                }
            ],
            "ports": [{"protocol": "TCP", "port": 5432}],
        },
        {
            "to": [
                {
                    "namespaceSelector": {
                        "matchLabels": {"kubernetes.io/metadata.name": "kube-system"}
                    },
                    "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}},
                }
            ],
            "ports": [{"protocol": "TCP", "port": 53}, {"protocol": "UDP", "port": 53}],
        },
    ]
    ingress = cluster.objects[("NetworkPolicy", "loom-capacity-agent-postgres-ingress")]["spec"]
    assert ingress["podSelector"] == {
        "matchLabels": {"cnpg.io/cluster": "loom-postgres", "cnpg.io/instanceRole": "primary"}
    }
    assert ingress["ingress"][0]["ports"] == [{"protocol": "TCP", "port": 5432}]


def test_agent_reuses_database_bootstrap_configuration_with_runtime_admission_variant(
    tmp_path: Path,
) -> None:
    """Break caught: bootstrap and agent diverge beyond the database admission variant."""
    cluster = _Cluster()
    plan = _plan(tmp_path)
    agent_manifest = _component(cluster)._sources(plan).manifest
    database_manifest = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=cluster,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=_seed,
    )._manifest(plan, _seed())
    agent_configuration_map = next(
        item for item in yaml.safe_load_all(agent_manifest) if item and item["kind"] == "ConfigMap"
    )
    database_secret = next(
        item for item in yaml.safe_load_all(database_manifest) if item and item["kind"] == "Secret"
    )
    agent_configuration = ReporterConfigurationV1.model_validate_json(
        agent_configuration_map["data"]["reporter-configuration.json"]
    )
    database_configuration = ReporterConfigurationV1.model_validate_json(
        base64.b64decode(database_secret["data"]["reporter-configuration.json"])
    )
    assert database_configuration.protected_admission_sha256 is None
    assert agent_configuration.protected_admission_sha256 is not None
    assert agent_configuration.model_dump(exclude={"protected_admission_sha256"}) == (
        database_configuration.model_dump(exclude={"protected_admission_sha256"})
    )


def test_agent_runtime_configuration_seals_the_exact_database_admission_digest(
    tmp_path: Path,
) -> None:
    """Break caught: the runtime serializes the bootstrap's null admission value."""
    manifest = _component(_Cluster())._sources(_plan(tmp_path)).manifest
    configuration_map = next(
        item for item in yaml.safe_load_all(manifest) if item and item["kind"] == "ConfigMap"
    )
    configuration = ReporterConfigurationV1.model_validate_json(
        configuration_map["data"]["reporter-configuration.json"]
    )

    assert capacity_guard_schema_head() == ("guard_0031", 31)
    assert configuration.protected_admission_sha256 == (
        "8197619e92fde22cc2c2b99d65db3136f84b8bc149b0a33942d352a883debd6b"
    )


def test_agent_admission_digest_does_not_reuse_the_previous_guard_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The sealed digest includes the schema generation, not only reporter identity."""
    monkeypatch.setattr(
        "loom.personal_dev_capacity_runtime.capacity_guard_schema_head",
        lambda: ("guard_0029", 29),
    )
    manifest = _component(_Cluster())._sources(_plan(tmp_path)).manifest
    configuration_map = next(
        item for item in yaml.safe_load_all(manifest) if item and item["kind"] == "ConfigMap"
    )
    configuration = ReporterConfigurationV1.model_validate_json(
        configuration_map["data"]["reporter-configuration.json"]
    )

    assert configuration.protected_admission_sha256 == (
        "51b50234edf19102baf749776995846d5bc73ce0a9e3b1b4fe0d93bcced98fc9"
    )


def test_classify_and_final_readback_bracket_source_authority(tmp_path: Path) -> None:
    """Break caught: a source replacement during observation returns a stale READY/EXACT state."""
    cluster = _Cluster()
    reads = 0

    def changing_tls() -> dict[str, bytes]:
        nonlocal reads
        reads += 1
        suffix = b"one" if reads < 2 else b"two"
        return {
            "manager-ca.pem": b"manager-ca-" + suffix,
            "certificate.pem": b"certificate-" + suffix,
            "private-key.pem": b"key-" + suffix,
        }

    component = _component_type()(
        runner=cluster,
        container_registry="registry.example.test/loom",
        seed_reader=_seed,
        reporter_tls_reader=changing_tls,
        postgres_ca_reader=lambda: b"postgres-ca",
    )
    plan = _plan(tmp_path)
    assert component.classify(plan)[0] is ComponentState.DRIFTED

    stable_reads = 0

    def late_changing_tls() -> dict[str, bytes]:
        nonlocal stable_reads
        stable_reads += 1
        suffix = b"one" if stable_reads <= 3 else b"two"
        return {
            "manager-ca.pem": b"manager-ca-" + suffix,
            "certificate.pem": b"certificate-" + suffix,
            "private-key.pem": b"key-" + suffix,
        }

    final_component = _component_type()(
        runner=_Cluster(),
        container_registry="registry.example.test/loom",
        seed_reader=_seed,
        reporter_tls_reader=late_changing_tls,
        postgres_ca_reader=lambda: b"postgres-ca",
    )
    with pytest.raises(RuntimeError, match="authority changed after readback"):
        final_component.apply(plan)
