from __future__ import annotations

import copy
import hashlib
import importlib
import json
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.personal_dev_control_plane_config import (
    load_personal_dev_control_plane_profile,
    load_personal_dev_trusted_release,
)
from loom.personal_dev_control_plane_render import (
    RenderedPersonalDevControlPlane,
    render_shadow_personal_dev_control_plane,
)
from loom.personal_dev_control_plane_status import (
    observe_personal_dev_shadow_status,
)

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _ROOT / "deploy/dev-fleet/personal-dev-control-plane.toml"
_MANAGED_BY = "loom-personal-dev-control-plane"

_CONTEXT = ("config", "current-context")
_NAMESPACES = ("get", "namespaces", "--output=json")
_RUNTIME_CLASS = (
    "get",
    "runtimeclass.node.k8s.io/loom-personal-dev-builder",
    "--output=json",
)
_NAMESPACED = (
    "get",
    (
        "deployments.apps,statefulsets.apps,jobs.batch,persistentvolumeclaims,"
        "serviceaccounts,roles.rbac.authorization.k8s.io,"
        "rolebindings.rbac.authorization.k8s.io,services,pods,"
        "ingresses.networking.k8s.io,networkpolicies.networking.k8s.io"
    ),
    "--namespace",
    "loom-dev",
    "--selector",
    f"app.kubernetes.io/managed-by={_MANAGED_BY}",
    "--output=json",
)
_CLUSTER = (
    "get",
    (
        "clusterroles.rbac.authorization.k8s.io,"
        "clusterrolebindings.rbac.authorization.k8s.io,"
        "validatingadmissionpolicies.admissionregistration.k8s.io,"
        "validatingadmissionpolicybindings.admissionregistration.k8s.io"
    ),
    "--selector",
    f"app.kubernetes.io/managed-by={_MANAGED_BY}",
    "--output=json",
)
_MANAGER = (
    "--request-timeout=10s",
    "--namespace",
    "loom-dev",
    "exec",
    "deployment/loom-capacity-manager",
    "-c",
    "manager",
    "--",
    "python",
    "-m",
    "loom_capacity_manager.health_probe",
    "--url",
    "https://127.0.0.1:8443/healthz",
    "--ca-file",
    "/var/run/loom-capacity-manager/runtime/credentials/server-ca.pem",
    "--certificate-file",
    "/var/run/loom-capacity-manager/runtime/credentials/health-certificate.pem",
    "--private-key-file",
    "/var/run/loom-capacity-manager/runtime/credentials/health-private-key.pem",
    "--server-certificate-file",
    "/var/run/loom-capacity-manager/runtime/credentials/server-certificate.pem",
    "--observe",
)


def _release_value() -> dict[str, object]:
    return {
        "schema_version": 1,
        "source_sha": "1" * 40,
        "source_tree": "2" * 40,
        "images": {
            "loom_service": "ghcr.io/qianyi-sun/loom-service@sha256:" + "3" * 64,
            "personal_dev_builder": (
                "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "4" * 64
            ),
            "personal_dev_activation_agent": (
                "ghcr.io/qianyi-sun/loom-personal-dev-activation-agent@sha256:" + "5" * 64
            ),
            "postgres": "docker.io/library/postgres@sha256:" + "6" * 64,
            "minio": "quay.io/minio/minio@sha256:" + "7" * 64,
            "minio_client": "quay.io/minio/mc@sha256:" + "9" * 64,
        },
        "release_evidence_sha256": "8" * 64,
    }


def _expected_render(tmp_path: Path) -> RenderedPersonalDevControlPlane:
    payload = json.dumps(
        _release_value(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    release_path = tmp_path / "trusted-release.json"
    release_path.write_bytes(payload)
    release_path.chmod(0o600)
    release = load_personal_dev_trusted_release(
        release_path,
        hashlib.sha256(payload).hexdigest(),
    )
    return render_shadow_personal_dev_control_plane(
        load_personal_dev_control_plane_profile(_PROFILE),
        release,
    )


def _identity(item: dict[str, Any]) -> tuple[str, str]:
    return item["kind"], item["metadata"]["name"]


class _FakeRunner:
    def __init__(self, responses: dict[tuple[str, ...], object]) -> None:
        self.responses = responses
        self.calls: list[tuple[tuple[str, ...], int]] = []

    def run(
        self,
        argv: Sequence[str],
        *,
        timeout_seconds: int,
    ) -> subprocess.CompletedProcess[str]:
        command = tuple(argv)
        self.calls.append((command, timeout_seconds))
        if command not in self.responses:
            raise AssertionError(f"unexpected command: {command!r}")
        configured = self.responses[command]
        if isinstance(configured, subprocess.CompletedProcess):
            return configured
        stdout = (
            configured
            if isinstance(configured, str)
            else json.dumps(
                configured,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return subprocess.CompletedProcess(list(argv), 0, stdout, "")


def _pod_for(item: dict[str, Any], suffix: str, *, phase: str) -> dict[str, Any]:
    template = item["spec"]["template"]
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"{item['metadata']['name']}-{suffix}",
            "namespace": "loom-dev",
            "labels": copy.deepcopy(template["metadata"]["labels"]),
            "annotations": copy.deepcopy(template["metadata"]["annotations"]),
        },
        "spec": copy.deepcopy(template["spec"]),
        "status": {
            "phase": phase,
            "initContainerStatuses": [],
        },
    }
    if item["kind"] == "Job":
        pod["metadata"]["labels"]["job-name"] = item["metadata"]["name"]
    return pod


def _healthy_fixture(
    tmp_path: Path,
) -> tuple[RenderedPersonalDevControlPlane, _FakeRunner]:
    expected = _expected_render(tmp_path)
    documents = [copy.deepcopy(item) for item in yaml.safe_load_all(expected.yaml_text)]
    namespace = next(item for item in documents if item["kind"] == "Namespace")
    cluster = [item for item in documents if "namespace" not in item["metadata"]]
    cluster.remove(namespace)
    namespaced = [item for item in documents if item["metadata"].get("namespace")]

    generated: list[dict[str, Any]] = []
    for item in namespaced:
        metadata = item["metadata"]
        metadata["generation"] = 1
        kind, name = _identity(item)
        if kind == "StatefulSet":
            item["status"] = {
                "observedGeneration": 1,
                "replicas": 1,
                "currentReplicas": 1,
                "readyReplicas": 1,
                "updatedReplicas": 1,
                "currentRevision": "revision-1",
                "updateRevision": "revision-1",
            }
            template = item["spec"]["volumeClaimTemplates"][0]
            generated.append(
                {
                    "apiVersion": "v1",
                    "kind": "PersistentVolumeClaim",
                    "metadata": {
                        "name": f"{template['metadata']['name']}-{name}-0",
                        "namespace": "loom-dev",
                        "labels": copy.deepcopy(template["metadata"]["labels"]),
                        "annotations": copy.deepcopy(template["metadata"]["annotations"]),
                    },
                    "spec": copy.deepcopy(template["spec"]),
                    "status": {"phase": "Bound"},
                }
            )
            generated.append(_pod_for(item, "0", phase="Running"))
        elif kind == "Deployment" and name == "loom-personal-dev-management":
            item["status"] = {
                "observedGeneration": 1,
                "replicas": 1,
                "readyReplicas": 1,
                "availableReplicas": 1,
                "updatedReplicas": 1,
            }
            generated.append(_pod_for(item, "abcde", phase="Running"))
        elif kind == "Deployment":
            item["status"] = {
                "observedGeneration": 1,
                "replicas": 0,
                "readyReplicas": 0,
                "availableReplicas": 0,
                "updatedReplicas": 0,
            }
        elif kind == "Job":
            item["status"] = {
                "active": 0,
                "failed": 0,
                "succeeded": 1,
                "conditions": [{"type": "Complete", "status": "True"}],
            }
            generated.append(_pod_for(item, "abcde", phase="Succeeded"))
        elif kind == "PersistentVolumeClaim":
            item["status"] = {"phase": "Bound"}

    responses: dict[tuple[str, ...], object] = {
        _CONTEXT: "reviewed-loom-dev\n",
        _NAMESPACES: {
            "apiVersion": "v1",
            "kind": "NamespaceList",
            "items": [
                namespace,
                {
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": "kube-system"},
                },
            ],
        },
        _RUNTIME_CLASS: {
            "apiVersion": "node.k8s.io/v1",
            "kind": "RuntimeClass",
            "metadata": {"name": "loom-personal-dev-builder"},
            "handler": "loom-personal-dev-builder",
        },
        _NAMESPACED: {
            "apiVersion": "v1",
            "kind": "List",
            "items": [*namespaced, *generated],
        },
        _CLUSTER: {
            "apiVersion": "v1",
            "kind": "List",
            "items": cluster,
        },
        _MANAGER: '{"executable_new_capacity_ceiling":0,"status":"ready"}\n',
    }
    return expected, _FakeRunner(responses)


def _items(runner: _FakeRunner, command: tuple[str, ...]) -> list[dict[str, Any]]:
    document = runner.responses[command]
    assert isinstance(document, dict)
    items = document["items"]
    assert isinstance(items, list)
    return items


def _item(
    runner: _FakeRunner,
    command: tuple[str, ...],
    kind: str,
    name: str,
) -> dict[str, Any]:
    return next(value for value in _items(runner, command) if _identity(value) == (kind, name))


def _observe(
    expected: RenderedPersonalDevControlPlane,
    runner: _FakeRunner,
):
    return observe_personal_dev_shadow_status(
        runner,
        expected=expected,
        namespace="loom-dev",
    )


def test_healthy_shadow_returns_canonical_bounded_status_and_safe_commands(
    tmp_path: Path,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)

    result = _observe(expected, runner)

    assert result.to_dict() == {
        "blockers": [],
        "components": [
            {"name": "cluster-resources", "observed": 10, "ready": True},
            {"name": "manager", "observed": 1, "ready": True},
            {"name": "namespaced-resources", "observed": 28, "ready": True},
            {"name": "namespaces", "observed": 1, "ready": True},
            {"name": "runtime-class", "observed": 1, "ready": True},
        ],
        "input_sha256": expected.input_sha256,
        "manager_ceiling": 0,
        "mode": "shadow",
        "ready": True,
        "release_sha256": expected.release_sha256,
        "schema": "loom-personal-dev-control-plane-status-v1",
    }
    assert [call for call, _timeout in runner.calls] == [
        _CONTEXT,
        _NAMESPACES,
        _RUNTIME_CLASS,
        _NAMESPACED,
        _CLUSTER,
        _MANAGER,
    ]
    assert all(1 <= timeout <= 10 for _call, timeout in runner.calls)
    assert sum(call == _NAMESPACES for call, _timeout in runner.calls) == 1
    for command, _timeout in runner.calls:
        assert "secret" not in " ".join(command).casefold()
        assert command[0] in {"config", "get", "--request-timeout=10s"}
    assert runner.calls[-1][0] == _MANAGER


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        ("namespace-missing", "namespace_missing"),
        ("namespace-wrong-kind", "namespace_inventory_invalid"),
        ("shared-object-missing", "resource_inventory_drift"),
        ("statefulset-not-ready", "storage_not_ready"),
        ("deployment-not-ready", "management_not_ready"),
        ("migration-missing", "migration_missing"),
        ("migration-failed", "migration_failed"),
        ("migration-running", "migration_incomplete"),
        ("init-failed", "init_container_failed"),
        ("mutable-image", "workload_image_drift"),
        ("changed-image", "workload_image_drift"),
        ("render-digest-mismatch", "resource_digest_drift"),
        ("release-digest-mismatch", "resource_digest_drift"),
        ("flag-missing", "management_shadow_flags_invalid"),
        ("flag-malformed", "management_shadow_flags_invalid"),
        ("flag-true", "management_shadow_flags_invalid"),
        ("activation-nonzero", "activation_replicas_nonzero"),
        ("runtime-class-missing", "runtime_class_missing"),
        ("scanner-pvc-missing", "storage_not_ready"),
        ("unexpected-personal-namespace", "unexpected_personal_namespace"),
        ("unexpected-builder-namespace", "unexpected_builder_namespace"),
        ("cluster-binding-drift", "cluster_resource_drift"),
        ("manager-unavailable", "manager_probe_unavailable"),
        ("manager-not-ready", "manager_probe_unavailable"),
        ("manager-malformed", "manager_probe_invalid"),
        ("manager-nonzero", "manager_ceiling_nonzero"),
    ],
)
def test_shadow_status_matrix_reports_stable_sorted_blockers(
    tmp_path: Path,
    mutation: str,
    blocker: str,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    namespaced = _items(runner, _NAMESPACED)
    namespaces = _items(runner, _NAMESPACES)

    if mutation == "namespace-missing":
        namespaces[:] = [item for item in namespaces if item["metadata"]["name"] != "loom-dev"]
    elif mutation == "namespace-wrong-kind":
        shared = next(item for item in namespaces if item["metadata"]["name"] == "loom-dev")
        shared.update({"apiVersion": "v1", "kind": "ConfigMap"})
    elif mutation == "shared-object-missing":
        namespaced.remove(_item(runner, _NAMESPACED, "Service", "loom-dev-minio"))
    elif mutation == "statefulset-not-ready":
        _item(runner, _NAMESPACED, "StatefulSet", "loom-dev-minio")["status"]["readyReplicas"] = 0
    elif mutation == "deployment-not-ready":
        _item(
            runner,
            _NAMESPACED,
            "Deployment",
            "loom-personal-dev-management",
        )["status"]["availableReplicas"] = 0
    elif mutation == "migration-missing":
        namespaced[:] = [item for item in namespaced if item["kind"] != "Job"]
    elif mutation in {"migration-failed", "migration-running"}:
        migration = next(item for item in namespaced if item["kind"] == "Job")
        if mutation == "migration-failed":
            migration["status"] = {"active": 0, "failed": 1, "succeeded": 0}
        else:
            migration["status"] = {"active": 1, "failed": 0, "succeeded": 0}
    elif mutation == "init-failed":
        pod = next(item for item in namespaced if item["kind"] == "Pod")
        pod["status"]["initContainerStatuses"] = [
            {
                "name": "credentials",
                "state": {"terminated": {"exitCode": 1, "reason": "Error"}},
            }
        ]
    elif mutation in {"mutable-image", "changed-image"}:
        deployment = _item(
            runner,
            _NAMESPACED,
            "Deployment",
            "loom-personal-dev-management",
        )
        deployment["spec"]["template"]["spec"]["containers"][0]["image"] = (
            "ghcr.io/qianyi-sun/loom-service:dev"
            if mutation == "mutable-image"
            else "ghcr.io/qianyi-sun/loom-service@sha256:" + "a" * 64
        )
    elif mutation in {"render-digest-mismatch", "release-digest-mismatch"}:
        service = _item(
            runner,
            _NAMESPACED,
            "Service",
            "loom-personal-dev-management",
        )
        annotation = (
            "loom.dev/render-input-sha256"
            if mutation == "render-digest-mismatch"
            else "loom.dev/trusted-release-sha256"
        )
        service["metadata"]["annotations"][annotation] = "a" * 64
    elif mutation.startswith("flag-"):
        deployment = _item(
            runner,
            _NAMESPACED,
            "Deployment",
            "loom-personal-dev-management",
        )
        environment = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
        flag = next(
            item for item in environment if item["name"] == "LOOM_SVC_DEV_INSTANCES_ENABLED"
        )
        if mutation == "flag-missing":
            environment.remove(flag)
        elif mutation == "flag-malformed":
            flag["value"] = "FALSE"
        else:
            flag["value"] = "true"
    elif mutation == "activation-nonzero":
        _item(
            runner,
            _NAMESPACED,
            "Deployment",
            "loom-personal-dev-activation-agent",
        )["spec"]["replicas"] = 1
    elif mutation == "runtime-class-missing":
        runner.responses[_RUNTIME_CLASS] = subprocess.CompletedProcess(
            list(_RUNTIME_CLASS), 1, "", "not found"
        )
    elif mutation == "scanner-pvc-missing":
        namespaced.remove(
            _item(
                runner,
                _NAMESPACED,
                "PersistentVolumeClaim",
                "loom-personal-dev-scanner-cache",
            )
        )
    elif mutation == "unexpected-personal-namespace":
        namespaces.append(
            {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "loom-dev-alice"}}
        )
    elif mutation == "unexpected-builder-namespace":
        namespaces.append(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {"name": "loom-build-attempt"},
            }
        )
    elif mutation == "cluster-binding-drift":
        binding = _item(
            runner,
            _CLUSTER,
            "ClusterRoleBinding",
            "loom-personal-dev-management-mutation",
        )
        binding["roleRef"]["name"] = "cluster-admin"
    elif mutation == "manager-unavailable":
        runner.responses[_MANAGER] = subprocess.CompletedProcess(
            list(_MANAGER), 1, "", "probe failed"
        )
    elif mutation == "manager-not-ready":
        runner.responses[_MANAGER] = '{"executable_new_capacity_ceiling":0,"status":"not-ready"}\n'
    elif mutation == "manager-malformed":
        runner.responses[_MANAGER] = '{"status":"ready"}\n'
    elif mutation == "manager-nonzero":
        runner.responses[_MANAGER] = '{"executable_new_capacity_ceiling":1,"status":"ready"}\n'
    else:  # pragma: no cover - parameter table is exhaustive
        raise AssertionError(mutation)

    first = _observe(expected, runner)
    second = _observe(expected, runner)

    assert first.ready is False
    assert blocker in first.blockers
    assert first.blockers == tuple(sorted(set(first.blockers)))
    assert second.to_dict() == first.to_dict()
    if blocker == "resource_digest_drift":
        assert first.input_sha256 is None
        assert first.release_sha256 is None
    components = {component.name: component for component in first.components}
    if mutation in {"unexpected-personal-namespace", "unexpected-builder-namespace"}:
        assert components["namespaces"].ready is False
    if mutation in {"manager-nonzero", "manager-not-ready"}:
        assert components["manager"].observed == 1


@pytest.mark.parametrize("drift", ["rogue-pod", "generated-pvc-spec"])
def test_observer_rejects_untrusted_generated_resource_drift(
    tmp_path: Path,
    drift: str,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    items = _items(runner, _NAMESPACED)
    if drift == "rogue-pod":
        rogue = copy.deepcopy(next(item for item in items if item["kind"] == "Pod"))
        rogue["metadata"]["name"] = "rogue-managed-pod"
        rogue["metadata"]["labels"]["app"] = "rogue-managed-pod"
        items.append(rogue)
    else:
        generated = next(
            item
            for item in items
            if item["kind"] == "PersistentVolumeClaim"
            and item["metadata"]["name"].startswith("data-")
        )
        generated["spec"]["resources"]["requests"]["storage"] = "999Gi"

    result = _observe(expected, runner)

    assert result.ready is False
    assert "resource_inventory_drift" in result.blockers


def test_observer_accepts_bounded_successful_migration_history(tmp_path: Path) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    items = _items(runner, _NAMESPACED)
    current_job = next(item for item in items if item["kind"] == "Job")
    current_pod = next(
        item
        for item in items
        if item["kind"] == "Pod"
        and item["metadata"]["labels"].get("app") == "loom-personal-dev-migration"
    )
    historical_job = copy.deepcopy(current_job)
    historical_pod = copy.deepcopy(current_pod)
    historical_input = "a" * 64
    historical_release = "b" * 64
    historical_name = f"loom-personal-dev-migrate-{historical_input[:16]}-{historical_release[:16]}"

    historical_job["metadata"]["name"] = historical_name
    historical_pod["metadata"]["name"] = f"{historical_name}-abcde"
    historical_pod["metadata"]["labels"]["job-name"] = historical_name
    for metadata in (
        historical_job["metadata"],
        historical_job["spec"]["template"]["metadata"],
        historical_pod["metadata"],
    ):
        metadata["labels"]["loom.dev/render-input"] = historical_input[:32]
        metadata["labels"]["loom.dev/trusted-release"] = historical_release[:32]
        metadata["annotations"]["loom.dev/render-input-sha256"] = historical_input
        metadata["annotations"]["loom.dev/trusted-release-sha256"] = historical_release
    historical_pod["spec"] = copy.deepcopy(historical_job["spec"]["template"]["spec"])
    items.extend([historical_job, historical_pod])

    result = _observe(expected, runner)

    assert result.ready is True
    assert result.blockers == ()
    components = {component.name: component for component in result.components}
    assert components["namespaced-resources"].observed == 30


@pytest.mark.parametrize(
    "invalid",
    [
        "duplicate",
        "unknown-shape",
        "missing-api-version",
        "deep-json",
        "oversized",
        "combined-output",
        "nonfinite",
    ],
)
def test_observer_rejects_duplicate_unknown_or_oversized_inventory(
    tmp_path: Path,
    invalid: str,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    if invalid == "duplicate":
        items = _items(runner, _NAMESPACED)
        items.append(copy.deepcopy(items[0]))
    elif invalid == "unknown-shape":
        document = runner.responses[_NAMESPACED]
        assert isinstance(document, dict)
        document["unexpected"] = True
    elif invalid == "missing-api-version":
        document = runner.responses[_NAMESPACED]
        assert isinstance(document, dict)
        del document["apiVersion"]
    elif invalid == "deep-json":
        runner.responses[_NAMESPACED] = (
            '{"apiVersion":"v1","items":' + "[" * 1100 + "0" + "]" * 1100 + ',"kind":"List"}'
        )
    elif invalid == "oversized":
        runner.responses[_NAMESPACED] = "x" * (4 * 1024 * 1024 + 1)
    elif invalid == "combined-output":
        document = runner.responses[_NAMESPACED]
        assert isinstance(document, dict)
        runner.responses[_NAMESPACED] = subprocess.CompletedProcess(
            list(_NAMESPACED),
            0,
            json.dumps(document, sort_keys=True, separators=(",", ":")),
            "x" * (4 * 1024 * 1024),
        )
    else:
        pod = next(item for item in _items(runner, _NAMESPACED) if item["kind"] == "Pod")
        pod["status"]["ignored"] = float("nan")

    result = _observe(expected, runner)

    assert result.ready is False
    assert "resource_inventory_invalid" in result.blockers


def test_observer_rejects_invalid_namespace_and_expected_render_inputs(
    tmp_path: Path,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)

    with pytest.raises(ValueError, match="namespace"):
        observe_personal_dev_shadow_status(
            runner,
            expected=expected,
            namespace="loom-dev-alice",
        )
    with pytest.raises(TypeError, match="expected render"):
        observe_personal_dev_shadow_status(
            runner,
            expected=object(),  # type: ignore[arg-type]
            namespace="loom-dev",
        )


def test_observer_starts_no_call_when_less_than_one_second_remains(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected, runner = _healthy_fixture(tmp_path)
    module = importlib.import_module("loom.personal_dev_control_plane_status")
    ticks = iter([100.0, 159.5, 160.0, 160.0, 160.0, 160.0, 160.0])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(ticks))

    result = _observe(expected, runner)

    assert runner.calls == []
    assert result.ready is False
    assert "kube_context_invalid" in result.blockers
