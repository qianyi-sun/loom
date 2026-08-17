"""Bounded read-only status for the personal-development shadow package."""

from __future__ import annotations

import json
import re
import subprocess
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import yaml  # type: ignore[import-untyped]

from loom.personal_dev_control_plane_render import RenderedPersonalDevControlPlane
from loom_capacity_manager.health_probe import capacity_health_probe_argv

_NAMESPACE = "loom-dev"
_MANAGED_BY = "loom-personal-dev-control-plane"
_RUNTIME_CLASS = "loom-personal-dev-builder"
MAX_PERSONAL_DEV_STATUS_RESPONSE_BYTES = 4 * 1024 * 1024
_TOTAL_TIMEOUT_SECONDS = 60.0
_CALL_TIMEOUT_SECONDS = 10
_MAX_INVENTORY_ITEMS = 4096
_DIGEST = re.compile(r"[0-9a-f]{64}")
_IMAGE = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}")
_CONTEXT = re.compile(r"[A-Za-z0-9_.:@/-]{1,253}")

_CONTEXT_COMMAND = ("config", "current-context")
_NAMESPACE_COMMAND = ("get", "namespaces", "--output=json")
_RUNTIME_CLASS_COMMAND = (
    "get",
    f"runtimeclass.node.k8s.io/{_RUNTIME_CLASS}",
    "--output=json",
)
_NAMESPACED_COMMAND = (
    "get",
    (
        "deployments.apps,statefulsets.apps,jobs.batch,persistentvolumeclaims,"
        "serviceaccounts,roles.rbac.authorization.k8s.io,"
        "rolebindings.rbac.authorization.k8s.io,services,pods,"
        "ingresses.networking.k8s.io,networkpolicies.networking.k8s.io"
    ),
    "--namespace",
    _NAMESPACE,
    "--selector",
    f"app.kubernetes.io/managed-by={_MANAGED_BY}",
    "--output=json",
)
_CLUSTER_COMMAND = (
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
_MANAGER_COMMAND = (
    "--request-timeout=10s",
    "--namespace",
    _NAMESPACE,
    "exec",
    "deployment/loom-capacity-manager",
    "-c",
    "manager",
    "--",
    *capacity_health_probe_argv(observe=True),
)


class KubectlRunner(Protocol):
    def run(
        self,
        argv: Sequence[str],
        *,
        timeout_seconds: int,
    ) -> subprocess.CompletedProcess[str]: ...


@dataclass(frozen=True, slots=True)
class PersonalDevShadowComponent:
    name: str
    observed: int
    ready: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "observed": self.observed,
            "ready": self.ready,
        }


@dataclass(frozen=True, slots=True)
class PersonalDevShadowStatus:
    ready: bool
    blockers: tuple[str, ...]
    input_sha256: str | None
    release_sha256: str | None
    manager_ceiling: int | None
    components: tuple[PersonalDevShadowComponent, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "blockers": list(self.blockers),
            "components": [component.to_dict() for component in self.components],
            "input_sha256": self.input_sha256,
            "manager_ceiling": self.manager_ceiling,
            "mode": "shadow",
            "ready": self.ready,
            "release_sha256": self.release_sha256,
            "schema": "loom-personal-dev-control-plane-status-v1",
        }


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON field")
        value[key] = item
    return value


def _remaining_timeout(deadline: float) -> int:
    remaining = deadline - time.monotonic()
    whole_seconds = int(remaining)
    if whole_seconds < 1:
        raise TimeoutError("personal-dev shadow observation timed out")
    return min(_CALL_TIMEOUT_SECONDS, whole_seconds)


def _run(
    runner: KubectlRunner,
    command: tuple[str, ...],
    deadline: float,
) -> subprocess.CompletedProcess[str] | None:
    try:
        result = runner.run(command, timeout_seconds=_remaining_timeout(deadline))
    except (OSError, subprocess.SubprocessError, TimeoutError, ValueError):
        return None
    if (
        not isinstance(result, subprocess.CompletedProcess)
        or not isinstance(result.returncode, int)
        or not isinstance(result.stdout, str)
        or not isinstance(result.stderr, str)
        or len(result.stdout.encode("utf-8")) + len(result.stderr.encode("utf-8"))
        > MAX_PERSONAL_DEV_STATUS_RESPONSE_BYTES
    ):
        return None
    return result


def _json_document(payload: str) -> dict[str, Any]:
    value = json.loads(
        payload,
        object_pairs_hook=_unique_object,
        parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
    )
    if not isinstance(value, dict):
        raise ValueError("JSON document is not an object")
    return value


def _list_items(result: subprocess.CompletedProcess[str]) -> list[dict[str, Any]]:
    if result.returncode != 0:
        raise OSError("kubectl inventory is unavailable")
    document = _json_document(result.stdout)
    items = document.get("items")
    if (
        not set(document).issubset({"apiVersion", "kind", "metadata", "items"})
        or document.get("kind") != "List"
        or not isinstance(items, list)
        or len(items) > _MAX_INVENTORY_ITEMS
        or any(not isinstance(item, dict) for item in items)
    ):
        raise ValueError("kubectl inventory has an invalid shape")
    return [item for item in items if isinstance(item, dict)]


def _metadata(item: Mapping[str, Any]) -> Mapping[str, Any] | None:
    value = item.get("metadata")
    return value if isinstance(value, Mapping) else None


def _identity(item: Mapping[str, Any]) -> tuple[str, str, str, str] | None:
    metadata = _metadata(item)
    if metadata is None:
        return None
    api_version = item.get("apiVersion")
    kind = item.get("kind")
    name = metadata.get("name")
    namespace = metadata.get("namespace", "")
    if (
        not isinstance(api_version, str)
        or not isinstance(kind, str)
        or not isinstance(name, str)
        or not isinstance(namespace, str)
    ):
        return None
    return api_version, kind, namespace, name


def _expected_subset(expected: object, actual: object) -> bool:
    if isinstance(expected, Mapping):
        return isinstance(actual, Mapping) and all(
            key in actual and _expected_subset(value, actual[key])
            for key, value in expected.items()
        )
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(expected) == len(actual)
            and all(
                _expected_subset(left, right) for left, right in zip(expected, actual, strict=True)
            )
        )
    return type(expected) is type(actual) and expected == actual


def _expected_documents(
    expected: RenderedPersonalDevControlPlane,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    try:
        values = list(yaml.safe_load_all(expected.yaml_text))
    except yaml.YAMLError as exc:
        raise ValueError("expected render is invalid") from exc
    if (
        len(values) != expected.resource_count
        or any(not isinstance(value, dict) or _identity(value) is None for value in values)
        or _DIGEST.fullmatch(expected.input_sha256) is None
        or _DIGEST.fullmatch(expected.release_sha256) is None
    ):
        raise ValueError("expected render is invalid")
    namespace = next(
        (
            value
            for value in values
            if value.get("kind") == "Namespace"
            and _metadata(value)
            and _metadata(value).get("name") == _NAMESPACE  # type: ignore[union-attr]
        ),
        None,
    )
    if namespace is None:
        raise ValueError("expected render is invalid")
    namespaced = [value for value in values if _metadata(value).get("namespace") == _NAMESPACE]  # type: ignore[union-attr]
    cluster = [
        value
        for value in values
        if not _metadata(value).get("namespace") and value is not namespace  # type: ignore[union-attr]
    ]
    return namespaced, cluster, namespace


def _index_unique(
    items: Sequence[dict[str, Any]],
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    indexed: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for item in items:
        identity = _identity(item)
        if identity is None or identity in indexed:
            raise ValueError("resource identity is invalid or duplicated")
        indexed[identity] = item
    return indexed


def _digest_matches(
    item: Mapping[str, Any],
    *,
    input_sha256: str,
    release_sha256: str,
) -> bool:
    metadata = _metadata(item)
    if metadata is None:
        return False
    labels = metadata.get("labels")
    annotations = metadata.get("annotations")
    return (
        isinstance(labels, Mapping)
        and isinstance(annotations, Mapping)
        and labels.get("app.kubernetes.io/managed-by") == _MANAGED_BY
        and labels.get("loom.dev/render-input") == input_sha256[:32]
        and labels.get("loom.dev/trusted-release") == release_sha256[:32]
        and annotations.get("loom.dev/render-input-sha256") == input_sha256
        and annotations.get("loom.dev/trusted-release-sha256") == release_sha256
    )


def _containers(item: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    spec = item.get("spec")
    if not isinstance(spec, Mapping):
        return []
    if item.get("kind") in {"Deployment", "StatefulSet", "Job"}:
        template = spec.get("template")
        if not isinstance(template, Mapping):
            return []
        spec = template.get("spec")
        if not isinstance(spec, Mapping):
            return []
    values: list[Mapping[str, Any]] = []
    for field in ("initContainers", "containers"):
        entries = spec.get(field, [])
        if isinstance(entries, list):
            values.extend(entry for entry in entries if isinstance(entry, Mapping))
    return values


def _images_match(expected: Mapping[str, Any], actual: Mapping[str, Any]) -> bool:
    expected_images = {
        value.get("name"): value.get("image")
        for value in _containers(expected)
        if isinstance(value.get("name"), str) and isinstance(value.get("image"), str)
    }
    actual_images = {
        value.get("name"): value.get("image")
        for value in _containers(actual)
        if isinstance(value.get("name"), str) and isinstance(value.get("image"), str)
    }
    return expected_images == actual_images and all(
        isinstance(image, str) and _IMAGE.fullmatch(image) is not None
        for image in actual_images.values()
    )


def _shadow_flags_valid(deployment: Mapping[str, Any]) -> bool:
    containers = _containers(deployment)
    management = next(
        (container for container in containers if container.get("name") == "management"),
        None,
    )
    if management is None or not isinstance(management.get("env"), list):
        return False
    environment: dict[str, str] = {}
    for entry in management["env"]:
        if not isinstance(entry, Mapping):
            return False
        name = entry.get("name")
        value = entry.get("value")
        if not isinstance(name, str) or name in environment or not isinstance(value, str):
            continue
        environment[name] = value
    return all(
        environment.get(name) == "false"
        for name in (
            "LOOM_SVC_DEV_INSTANCES_ENABLED",
            "LOOM_SVC_PERSONAL_DEV_BUILDER_ENABLED",
            "LOOM_SVC_K8S_WORKER_ENABLED",
        )
    )


def _integer(value: object) -> int | None:
    return value if type(value) is int else None


def _stateful_ready(item: Mapping[str, Any]) -> bool:
    metadata = _metadata(item)
    spec = item.get("spec")
    status = item.get("status")
    if metadata is None or not isinstance(spec, Mapping) or not isinstance(status, Mapping):
        return False
    replicas = _integer(spec.get("replicas"))
    generation = _integer(metadata.get("generation"))
    observed_generation = _integer(status.get("observedGeneration"))
    return (
        replicas is not None
        and generation is not None
        and observed_generation is not None
        and observed_generation >= generation
        and all(
            _integer(status.get(field)) == replicas
            for field in ("replicas", "currentReplicas", "readyReplicas", "updatedReplicas")
        )
        and isinstance(status.get("currentRevision"), str)
        and status.get("currentRevision") == status.get("updateRevision")
    )


def _management_ready(item: Mapping[str, Any]) -> bool:
    metadata = _metadata(item)
    spec = item.get("spec")
    status = item.get("status")
    if metadata is None or not isinstance(spec, Mapping) or not isinstance(status, Mapping):
        return False
    replicas = _integer(spec.get("replicas"))
    generation = _integer(metadata.get("generation"))
    observed_generation = _integer(status.get("observedGeneration"))
    return (
        replicas == 1
        and generation is not None
        and observed_generation is not None
        and observed_generation >= generation
        and all(
            _integer(status.get(field)) == replicas
            for field in ("replicas", "readyReplicas", "availableReplicas", "updatedReplicas")
        )
    )


def _migration_state(item: Mapping[str, Any] | None) -> str:
    if item is None:
        return "missing"
    status = item.get("status")
    if not isinstance(status, Mapping):
        return "incomplete"
    if (_integer(status.get("failed")) or 0) > 0:
        return "failed"
    conditions = status.get("conditions", [])
    complete = isinstance(conditions, list) and any(
        isinstance(condition, Mapping)
        and condition.get("type") == "Complete"
        and condition.get("status") == "True"
        for condition in conditions
    )
    if (
        _integer(status.get("succeeded")) == 1
        and (_integer(status.get("active")) or 0) == 0
        and complete
    ):
        return "succeeded"
    return "incomplete"


def _init_failed(pods: Sequence[Mapping[str, Any]]) -> bool:
    failed_waiting = {
        "CrashLoopBackOff",
        "CreateContainerConfigError",
        "ErrImagePull",
        "Error",
        "ImagePullBackOff",
    }
    for pod in pods:
        status = pod.get("status")
        if not isinstance(status, Mapping):
            continue
        entries = status.get("initContainerStatuses", [])
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, Mapping) or not isinstance(entry.get("state"), Mapping):
                continue
            state = entry["state"]
            terminated = state.get("terminated")
            waiting = state.get("waiting")
            if isinstance(terminated, Mapping) and _integer(terminated.get("exitCode")) not in {
                None,
                0,
            }:
                return True
            if isinstance(waiting, Mapping) and waiting.get("reason") in failed_waiting:
                return True
    return False


def _manager_status(
    result: subprocess.CompletedProcess[str] | None,
) -> tuple[int | None, str | None]:
    if result is None or result.returncode != 0:
        return None, "manager_probe_unavailable"
    try:
        document = _json_document(result.stdout)
    except (json.JSONDecodeError, ValueError):
        return None, "manager_probe_invalid"
    if (
        set(document) != {"status", "executable_new_capacity_ceiling"}
        or document.get("status") not in {"ready", "not-ready"}
        or type(document.get("executable_new_capacity_ceiling")) is not int
        or document["executable_new_capacity_ceiling"] < 0
    ):
        return None, "manager_probe_invalid"
    ceiling = document["executable_new_capacity_ceiling"]
    if document["status"] != "ready":
        return ceiling, "manager_probe_unavailable"
    if ceiling != 0:
        return ceiling, "manager_ceiling_nonzero"
    return ceiling, None


def observe_personal_dev_shadow_status(
    runner: KubectlRunner,
    *,
    expected: RenderedPersonalDevControlPlane,
    namespace: str = _NAMESPACE,
) -> PersonalDevShadowStatus:
    """Compare bounded live state with one locally trusted shadow render."""

    if namespace != _NAMESPACE:
        raise ValueError("personal-dev shadow namespace must be loom-dev")
    if not isinstance(expected, RenderedPersonalDevControlPlane):
        raise TypeError("personal-dev expected render is invalid")
    expected_namespaced, expected_cluster, expected_namespace = _expected_documents(expected)
    deadline = time.monotonic() + _TOTAL_TIMEOUT_SECONDS
    results = {
        command: _run(runner, command, deadline)
        for command in (
            _CONTEXT_COMMAND,
            _NAMESPACE_COMMAND,
            _RUNTIME_CLASS_COMMAND,
            _NAMESPACED_COMMAND,
            _CLUSTER_COMMAND,
            _MANAGER_COMMAND,
        )
    }
    blockers: set[str] = set()

    context_result = results[_CONTEXT_COMMAND]
    context_ok = (
        context_result is not None
        and context_result.returncode == 0
        and _CONTEXT.fullmatch(context_result.stdout.strip()) is not None
    )
    if not context_ok:
        blockers.add("kube_context_invalid")

    namespace_ok = False
    namespace_observed = 0
    try:
        namespace_result = results[_NAMESPACE_COMMAND]
        if namespace_result is None:
            raise ValueError("namespace inventory response is invalid")
        namespace_items = _list_items(namespace_result)
        names: dict[str, dict[str, Any]] = {}
        for item in namespace_items:
            metadata = _metadata(item)
            name = metadata.get("name") if metadata else None
            if not isinstance(name, str) or name in names:
                raise ValueError("namespace identity is invalid or duplicated")
            names[name] = item
        personal_names = sorted(name for name in names if name.startswith("loom-dev-"))
        namespace_observed = int(_NAMESPACE in names) + len(personal_names)
        if personal_names:
            blockers.add("unexpected_personal_namespace")
        shared_namespace_ok = _NAMESPACE in names and _expected_subset(
            expected_namespace,
            names[_NAMESPACE],
        )
        namespace_ok = shared_namespace_ok and not personal_names
        if not shared_namespace_ok:
            blockers.add("namespace_missing")
    except (OSError, json.JSONDecodeError, UnicodeError, ValueError):
        blockers.add("namespace_inventory_invalid")

    runtime_ok = False
    runtime_observed = 0
    runtime_result = results[_RUNTIME_CLASS_COMMAND]
    if runtime_result is not None and runtime_result.returncode == 0:
        try:
            runtime = _json_document(runtime_result.stdout)
            runtime_ok = (
                runtime.get("apiVersion") == "node.k8s.io/v1"
                and runtime.get("kind") == "RuntimeClass"
                and _metadata(runtime) is not None
                and _metadata(runtime).get("name") == _RUNTIME_CLASS  # type: ignore[union-attr]
                and isinstance(runtime.get("handler"), str)
                and bool(runtime["handler"])
            )
            runtime_observed = int(runtime_ok)
        except (json.JSONDecodeError, ValueError):
            runtime_ok = False
    if not runtime_ok:
        blockers.add("runtime_class_missing")

    namespaced_ok = False
    namespaced_observed = 0
    live_namespaced: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    expected_namespaced_index = _index_unique(expected_namespaced)
    try:
        namespaced_result = results[_NAMESPACED_COMMAND]
        if namespaced_result is None:
            raise ValueError("namespaced inventory response is invalid")
        namespaced_items = _list_items(namespaced_result)
        live_namespaced = _index_unique(namespaced_items)
        namespaced_observed = len(live_namespaced)
        pods = [item for item in namespaced_items if item.get("kind") == "Pod"]
        expected_generated_pvcs = {
            (
                "v1",
                "PersistentVolumeClaim",
                _NAMESPACE,
                f"{template['metadata']['name']}-{item['metadata']['name']}-0",
            ): {
                "metadata": {
                    "labels": template["metadata"]["labels"],
                    "annotations": template["metadata"]["annotations"],
                },
                "spec": template["spec"],
            }
            for item in expected_namespaced
            if item.get("kind") == "StatefulSet"
            for template in item["spec"].get("volumeClaimTemplates", [])
        }
        generated_pvcs = set(expected_generated_pvcs)
        allowed_extra = generated_pvcs | {
            identity for identity, item in live_namespaced.items() if item.get("kind") == "Pod"
        }
        live_expected = {
            identity: live_namespaced[identity]
            for identity in expected_namespaced_index
            if identity in live_namespaced
        }
        missing = set(expected_namespaced_index) - set(live_expected)
        unexpected = set(live_namespaced) - set(expected_namespaced_index) - allowed_extra
        drifted = {
            identity
            for identity, actual in live_expected.items()
            if not _expected_subset(expected_namespaced_index[identity], actual)
        }
        generated_pvc_drift = any(
            identity not in live_namespaced
            or not _expected_subset(expected_pvc, live_namespaced[identity])
            for identity, expected_pvc in expected_generated_pvcs.items()
        )
        expected_pod_templates: dict[str, tuple[dict[str, Any], int]] = {}
        for item in expected_namespaced:
            if item.get("kind") not in {"Deployment", "StatefulSet", "Job"}:
                continue
            template = item["spec"]["template"]
            app = template["metadata"]["labels"].get("app")
            replicas = 1 if item.get("kind") == "Job" else item["spec"].get("replicas")
            if (
                not isinstance(app, str)
                or app in expected_pod_templates
                or type(replicas) is not int
            ):
                raise ValueError("expected pod template is invalid")
            expected_pod_templates[app] = (
                {
                    "metadata": template["metadata"],
                    "spec": template["spec"],
                },
                replicas,
            )
        observed_pods: dict[str, list[dict[str, Any]]] = {app: [] for app in expected_pod_templates}
        pod_drift = False
        for pod in pods:
            metadata = _metadata(pod)
            labels = metadata.get("labels") if metadata else None
            app = labels.get("app") if isinstance(labels, Mapping) else None
            if not isinstance(app, str) or app not in expected_pod_templates:
                pod_drift = True
                continue
            observed_pods[app].append(pod)
            if not _expected_subset(expected_pod_templates[app][0], pod):
                pod_drift = True
        pod_drift = pod_drift or any(
            len(observed_pods[app]) != expected_count
            for app, (_template, expected_count) in expected_pod_templates.items()
        )
        if missing or unexpected or drifted or generated_pvc_drift or pod_drift:
            blockers.add("resource_inventory_drift")

        digest_ok = all(
            _digest_matches(
                item,
                input_sha256=expected.input_sha256,
                release_sha256=expected.release_sha256,
            )
            for item in namespaced_items
        )
        if not digest_ok:
            blockers.add("resource_digest_drift")

        workload_image_ok = all(
            identity not in live_expected or _images_match(item, live_expected[identity])
            for identity, item in expected_namespaced_index.items()
            if item.get("kind") in {"Deployment", "StatefulSet", "Job"}
        )
        if not workload_image_ok:
            blockers.add("workload_image_drift")

        management_identity = next(
            identity
            for identity, item in expected_namespaced_index.items()
            if item.get("kind") == "Deployment"
            and _metadata(item).get("name") == "loom-personal-dev-management"  # type: ignore[union-attr]
        )
        management = live_expected.get(management_identity)
        management_ok = management is not None and _management_ready(management)
        if not management_ok:
            blockers.add("management_not_ready")
        if management is None or not _shadow_flags_valid(management):
            blockers.add("management_shadow_flags_invalid")

        activation = next(
            (
                item
                for item in live_expected.values()
                if item.get("kind") == "Deployment"
                and _metadata(item).get("name")  # type: ignore[union-attr]
                == "loom-personal-dev-activation-agent"
            ),
            None,
        )
        activation_spec = activation.get("spec") if activation else None
        if (
            not isinstance(activation_spec, Mapping)
            or _integer(activation_spec.get("replicas")) != 0
        ):
            blockers.add("activation_replicas_nonzero")

        migration = next(
            (item for item in live_expected.values() if item.get("kind") == "Job"),
            None,
        )
        migration_state = _migration_state(migration)
        if migration_state != "succeeded":
            blockers.add(f"migration_{migration_state}")

        required_pvcs = generated_pvcs | {
            identity
            for identity, item in expected_namespaced_index.items()
            if item.get("kind") == "PersistentVolumeClaim"
        }
        pvc_ready = all(
            identity in live_namespaced
            and isinstance(live_namespaced[identity].get("status"), Mapping)
            and live_namespaced[identity]["status"].get("phase") == "Bound"
            for identity in required_pvcs
        )
        stateful_ready = all(
            identity in live_expected and _stateful_ready(live_expected[identity])
            for identity, item in expected_namespaced_index.items()
            if item.get("kind") == "StatefulSet"
        )
        if not pvc_ready or not stateful_ready:
            blockers.add("storage_not_ready")
        if _init_failed(pods):
            blockers.add("init_container_failed")
        namespaced_ok = not bool(
            blockers
            & {
                "activation_replicas_nonzero",
                "init_container_failed",
                "management_not_ready",
                "management_shadow_flags_invalid",
                "migration_failed",
                "migration_incomplete",
                "migration_missing",
                "resource_digest_drift",
                "resource_inventory_drift",
                "storage_not_ready",
                "workload_image_drift",
            }
        )
    except (OSError, json.JSONDecodeError, KeyError, TypeError, UnicodeError, ValueError):
        blockers.add("resource_inventory_invalid")

    cluster_ok = False
    cluster_observed = 0
    expected_cluster_index = _index_unique(expected_cluster)
    try:
        cluster_result = results[_CLUSTER_COMMAND]
        if cluster_result is None:
            raise ValueError("cluster inventory response is invalid")
        cluster_items = _list_items(cluster_result)
        live_cluster = _index_unique(cluster_items)
        cluster_observed = len(live_cluster)
        cluster_ok = set(live_cluster) == set(expected_cluster_index) and all(
            _expected_subset(expected_cluster_index[identity], live_cluster[identity])
            for identity in expected_cluster_index
        )
        if not cluster_ok:
            blockers.add("cluster_resource_drift")
        if not all(
            _digest_matches(
                item,
                input_sha256=expected.input_sha256,
                release_sha256=expected.release_sha256,
            )
            for item in cluster_items
        ):
            blockers.add("resource_digest_drift")
            cluster_ok = False
    except (OSError, json.JSONDecodeError, UnicodeError, ValueError):
        blockers.add("resource_inventory_invalid")

    manager_ceiling, manager_blocker = _manager_status(results[_MANAGER_COMMAND])
    if manager_blocker:
        blockers.add(manager_blocker)
    manager_ok = manager_blocker is None

    digest_observed = (
        "resource_digest_drift" not in blockers
        and "resource_inventory_invalid" not in blockers
        and namespaced_observed > 0
        and cluster_observed > 0
    )
    components = tuple(
        sorted(
            (
                PersonalDevShadowComponent("cluster-resources", cluster_observed, cluster_ok),
                PersonalDevShadowComponent("manager", int(manager_ceiling is not None), manager_ok),
                PersonalDevShadowComponent(
                    "namespaced-resources", namespaced_observed, namespaced_ok
                ),
                PersonalDevShadowComponent("namespaces", namespace_observed, namespace_ok),
                PersonalDevShadowComponent("runtime-class", runtime_observed, runtime_ok),
            ),
            key=lambda component: component.name,
        )
    )
    stable_blockers = tuple(sorted(blockers))
    return PersonalDevShadowStatus(
        ready=not stable_blockers,
        blockers=stable_blockers,
        input_sha256=expected.input_sha256 if digest_observed else None,
        release_sha256=expected.release_sha256 if digest_observed else None,
        manager_ceiling=manager_ceiling,
        components=components,
    )


__all__ = [
    "MAX_PERSONAL_DEV_STATUS_RESPONSE_BYTES",
    "KubectlRunner",
    "PersonalDevShadowComponent",
    "PersonalDevShadowStatus",
    "observe_personal_dev_shadow_status",
]
