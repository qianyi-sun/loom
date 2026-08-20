"""Bounded read-only status for personal-development control-plane modes."""

from __future__ import annotations

import json
import re
import subprocess
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import UUID

import yaml  # type: ignore[import-untyped]

from loom.personal_dev_capacity import PersonalDevCapacityManagerBinding
from loom.personal_dev_control_plane_config import PersonalDevAcceptancePlan
from loom.personal_dev_control_plane_render import RenderedPersonalDevControlPlane
from loom_capacity_manager.health_probe import capacity_health_probe_argv

_NAMESPACE = "loom-dev"
_MANAGED_BY = "loom-personal-dev-control-plane"
MAX_PERSONAL_DEV_STATUS_RESPONSE_BYTES = 4 * 1024 * 1024
_TOTAL_TIMEOUT_SECONDS = 60.0
_CALL_TIMEOUT_SECONDS = 10
_MAX_INVENTORY_ITEMS = 4096
_DIGEST = re.compile(r"[0-9a-f]{64}")
_IMAGE = re.compile(r"[^\s@]+@sha256:[0-9a-f]{64}")
_CONTEXT = re.compile(r"[A-Za-z0-9_.:@/-]{1,253}")
_MIGRATION_JOB = re.compile(r"loom-personal-dev-migrate-([0-9a-f]{16})-([0-9a-f]{16})")
_PERSONAL_NAMESPACE = re.compile(r"loom-dev-[a-z]([-a-z0-9]{0,18}[a-z0-9])?")
_RESERVED_PERSONAL_NAMESPACE = re.compile(
    r"loom-dev-(dev|development|staging|production|prod|local|loom|shared|default)"
)
_BUILDER_NAMESPACE = re.compile(r"loom-build-[0-9a-f]{32}-l[0-9a-f]{16}")
_BUILDER_POD_SECURITY_LABELS = (
    ("pod-security.kubernetes.io/enforce", "privileged"),
    ("pod-security.kubernetes.io/enforce-version", "v1.36"),
    ("pod-security.kubernetes.io/audit", "restricted"),
    ("pod-security.kubernetes.io/audit-version", "v1.36"),
    ("pod-security.kubernetes.io/warn", "restricted"),
    ("pod-security.kubernetes.io/warn-version", "v1.36"),
)
_MAX_MIGRATION_HISTORY = 8

_CONTEXT_COMMAND = ("config", "current-context")
_NAMESPACE_COMMAND = ("get", "namespaces", "--output=json")
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
_ACCEPTANCE_MANAGER_COMMAND = (
    "--request-timeout=10s",
    "--namespace",
    _NAMESPACE,
    "exec",
    "deployment/loom-personal-dev-management",
    "-c",
    "management",
    "--",
    *capacity_health_probe_argv(
        "/run/loom-personal-dev/management/files",
        observe_identity=True,
    ),
)
_DEPLOYMENTS_COMMAND = (
    "get",
    "deployments.apps",
    "--all-namespaces",
    "--output=json",
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


@dataclass(frozen=True, slots=True)
class PersonalDevAcceptanceStatus:
    ready: bool
    blockers: tuple[str, ...]
    input_sha256: str | None
    release_sha256: str | None
    acceptance_plan_sha256: str
    manager_ceiling: int | None
    components: tuple[PersonalDevShadowComponent, ...]
    application_ready: bool
    capacity_publication_ready: bool
    worker_available: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "acceptance_plan_sha256": self.acceptance_plan_sha256,
            "application_ready": self.application_ready,
            "blockers": list(self.blockers),
            "capacity_publication_ready": self.capacity_publication_ready,
            "components": [component.to_dict() for component in self.components],
            "input_sha256": self.input_sha256,
            "manager_ceiling": self.manager_ceiling,
            "mode": "acceptance",
            "ready": self.ready,
            "release_sha256": self.release_sha256,
            "schema": "loom-personal-dev-control-plane-status-v1",
            "worker_available": self.worker_available,
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
        or type(result.returncode) is not int
        or not isinstance(result.stdout, str)
        or not isinstance(result.stderr, str)
        or len(result.stdout.encode("utf-8")) + len(result.stderr.encode("utf-8"))
        > MAX_PERSONAL_DEV_STATUS_RESPONSE_BYTES
    ):
        return None
    return result


def _json_document(payload: str) -> dict[str, Any]:
    try:
        value = json.loads(
            payload,
            object_pairs_hook=_unique_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
    except RecursionError:
        raise ValueError("JSON document is too deeply nested") from None
    if not isinstance(value, dict):
        raise ValueError("JSON document is not an object")
    return value


def _list_items(
    result: subprocess.CompletedProcess[str],
    *,
    expected_kind: str,
    expected_api_version: str = "v1",
) -> list[dict[str, Any]]:
    if result.returncode != 0:
        raise OSError("kubectl inventory is unavailable")
    document = _json_document(result.stdout)
    items = document.get("items")
    if (
        not set(document).issubset({"apiVersion", "kind", "metadata", "items"})
        or document.get("apiVersion") != expected_api_version
        or document.get("kind") != expected_kind
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


def _expected_subset(
    expected: object,
    actual: object,
    path: tuple[str, ...] = (),
) -> bool:
    if isinstance(expected, Mapping):
        # The API server omits EnvVar.value when its intended literal value is empty.
        # Accept only that exact, context-bound zero-value normalization.
        if (
            isinstance(actual, Mapping)
            and len(path) >= 4
            and path[-4] in {"containers", "initContainers"}
            and path[-2] == "env"
            and set(expected) == {"name", "value"}
            and expected.get("value") == ""
            and set(actual) == {"name"}
            and actual.get("name") == expected.get("name")
        ):
            return True
        return isinstance(actual, Mapping) and all(
            key in actual and _expected_subset(value, actual[key], (*path, str(key)))
            for key, value in expected.items()
        )
    if isinstance(expected, list):
        return (
            isinstance(actual, list)
            and len(expected) == len(actual)
            and all(
                _expected_subset(left, right, (*path, str(index)))
                for index, (left, right) in enumerate(zip(expected, actual, strict=True))
            )
        )
    return type(expected) is type(actual) and expected == actual


def _security_boundary_matches(
    expected: Mapping[str, Any],
    actual: Mapping[str, Any],
) -> bool:
    """Reject live additions that can widen authority or network exposure."""

    kind = expected.get("kind")
    if kind == "ClusterRole" and actual.get("aggregationRule") != expected.get(
        "aggregationRule"
    ):
        return False
    if kind == "ValidatingAdmissionPolicyBinding":
        expected_spec = expected.get("spec")
        actual_spec = actual.get("spec")
        if not isinstance(expected_spec, Mapping) or not isinstance(actual_spec, Mapping):
            return False
        if set(actual_spec) != set(expected_spec):
            return False
    if kind == "ValidatingAdmissionPolicy":
        expected_spec = expected.get("spec")
        actual_spec = actual.get("spec")
        if not isinstance(expected_spec, Mapping) or not isinstance(actual_spec, Mapping):
            return False
        expected_constraints = expected_spec.get("matchConstraints")
        actual_constraints = actual_spec.get("matchConstraints")
        if not isinstance(expected_constraints, Mapping) or not isinstance(
            actual_constraints, Mapping
        ):
            return False
        if set(actual_constraints) - {
            "resourceRules",
            "matchPolicy",
            "namespaceSelector",
            "objectSelector",
        }:
            return False
        if actual_constraints.get("matchPolicy", "Equivalent") != expected_constraints.get(
            "matchPolicy", "Equivalent"
        ):
            return False
        # The API server materializes omitted admission selectors as empty objects.
        for selector in ("namespaceSelector", "objectSelector"):
            if actual_constraints.get(selector, {}) != expected_constraints.get(selector, {}):
                return False
    if kind == "NetworkPolicy":
        expected_spec = expected.get("spec")
        actual_spec = actual.get("spec")
        if (
            not isinstance(expected_spec, Mapping)
            or not isinstance(actual_spec, Mapping)
            or set(actual_spec) != set(expected_spec)
        ):
            return False
    if kind == "Service":
        expected_spec = expected.get("spec")
        actual_spec = actual.get("spec")
        if not isinstance(expected_spec, Mapping) or not isinstance(actual_spec, Mapping):
            return False
        expected_type = expected_spec.get("type", "ClusterIP")
        if actual_spec.get("type", "ClusterIP") != expected_type:
            return False
        if (
            actual_spec.get("externalName") not in {None, ""}
            or actual_spec.get("loadBalancerClass") not in {None, ""}
            or actual_spec.get("loadBalancerIP") not in {None, ""}
        ):
            return False
        for field in ("externalIPs", "loadBalancerSourceRanges"):
            value = actual_spec.get(field, [])
            if not isinstance(value, list) or value:
                return False
        ports = actual_spec.get("ports")
        if not isinstance(ports, list) or any(
            isinstance(port, Mapping)
            and type(port.get("nodePort")) is int
            and port["nodePort"] > 0
            for port in ports
        ):
            return False
    if kind in {"Deployment", "StatefulSet", "Job", "Pod"}:
        spec = actual.get("spec")
        if kind == "Pod":
            pod_spec = spec
        else:
            template = spec.get("template") if isinstance(spec, Mapping) else None
            pod_spec = template.get("spec") if isinstance(template, Mapping) else None
        if not isinstance(pod_spec, Mapping):
            return False
        if any(
            pod_spec.get(field) is True
            for field in ("hostIPC", "hostNetwork", "hostPID", "shareProcessNamespace")
        ) or pod_spec.get("hostUsers") is False:
            return False
    return True


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
    return _observed_digests(item) == (input_sha256, release_sha256)


def _observed_digests(item: Mapping[str, Any]) -> tuple[str, str] | None:
    metadata = _metadata(item)
    if metadata is None:
        return None
    labels = metadata.get("labels")
    annotations = metadata.get("annotations")
    if not isinstance(labels, Mapping) or not isinstance(annotations, Mapping):
        return None
    input_sha256 = annotations.get("loom.dev/render-input-sha256")
    release_sha256 = annotations.get("loom.dev/trusted-release-sha256")
    if (
        not isinstance(input_sha256, str)
        or not isinstance(release_sha256, str)
        or _DIGEST.fullmatch(input_sha256) is None
        or _DIGEST.fullmatch(release_sha256) is None
        or labels.get("app.kubernetes.io/managed-by") != _MANAGED_BY
        or labels.get("loom.dev/render-input") != input_sha256[:32]
        or labels.get("loom.dev/trusted-release") != release_sha256[:32]
    ):
        return None
    return input_sha256, release_sha256


def _acceptance_plan_digest_matches(
    item: Mapping[str, Any],
    *,
    acceptance_plan_sha256: str,
) -> bool:
    metadata = _metadata(item)
    if metadata is None:
        return False
    labels = metadata.get("labels")
    annotations = metadata.get("annotations")
    return (
        isinstance(labels, Mapping)
        and isinstance(annotations, Mapping)
        and annotations.get("loom.dev/acceptance-plan-sha256") == acceptance_plan_sha256
        and labels.get("loom.dev/acceptance-plan-sha256") == acceptance_plan_sha256[:32]
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


def _images_are_immutable(item: Mapping[str, Any]) -> bool:
    containers = _containers(item)
    return bool(containers) and all(
        isinstance(container.get("image"), str) and _IMAGE.fullmatch(container["image"]) is not None
        for container in containers
    )


def _literal_environment(deployment: Mapping[str, Any]) -> dict[str, str] | None:
    containers = _containers(deployment)
    management = next(
        (container for container in containers if container.get("name") == "management"),
        None,
    )
    if management is None or not isinstance(management.get("env"), list):
        return None
    environment: dict[str, str] = {}
    names: set[str] = set()
    for entry in management["env"]:
        if not isinstance(entry, Mapping):
            return None
        name = entry.get("name")
        if not isinstance(name, str) or name in names:
            return None
        names.add(name)
        value = entry.get("value")
        if value is None and isinstance(entry.get("valueFrom"), Mapping):
            continue
        # EnvVar.value is serialized with omitempty by the Kubernetes API.
        if value is None and set(entry) == {"name"}:
            environment[name] = ""
            continue
        if not isinstance(value, str):
            return None
        environment[name] = value
    return environment


def _shadow_flags_valid(deployment: Mapping[str, Any]) -> bool:
    environment = _literal_environment(deployment)
    if environment is None:
        return False
    return all(
        environment.get(name) == "false"
        for name in (
            "LOOM_SVC_DEV_INSTANCES_ENABLED",
            "LOOM_SVC_PERSONAL_DEV_BUILDER_ENABLED",
            "LOOM_SVC_K8S_WORKER_ENABLED",
        )
    )


def _acceptance_management_binding_valid(
    deployment: Mapping[str, Any],
    plan: PersonalDevAcceptancePlan,
) -> bool:
    environment = _literal_environment(deployment)
    if environment is None:
        return False
    scanner_identity = (
        f"trivy-bin-sha256:{plan.builder.scanner_binary_sha256}:"
        f"db-sha256:{plan.builder.scanner_database_sha256}:"
        f"java-db-sha256:{plan.builder.scanner_java_database_sha256}"
    )
    expected = {
        "LOOM_SVC_DEV_INSTANCES_ENABLED": "true",
        "LOOM_SVC_K8S_WORKER_ENABLED": "false",
        "LOOM_SVC_PERSONAL_DEV_ACCEPTANCE_BINDING_JSON": plan.manager_runtime_json(),
        "LOOM_SVC_PERSONAL_DEV_ACCEPTANCE_PLAN_SHA256": plan.sha256,
        "LOOM_SVC_PERSONAL_DEV_ACTIVATION_PUBLIC_KEY_SHA256": (plan.activation.public_key_sha256),
        "LOOM_SVC_PERSONAL_DEV_BUILDER_ENABLED": "true",
        "LOOM_SVC_PERSONAL_DEV_BUILDER_PUBLISHER_IDENTITY": (plan.builder.publisher_identity),
        "LOOM_SVC_PERSONAL_DEV_BUILDER_REGISTRY_PREFIX": plan.builder.registry_prefix,
        "LOOM_SVC_PERSONAL_DEV_BUILDER_RUNTIME_CLASS_NAME": (plan.builder.runtime_class_name),
        "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_CACHE_DIR": (
            "/var/lib/loom-personal-dev-scanner/generations/"
            + plan.builder.scanner_cache_identity_sha256
        ),
        "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_CACHE_IDENTITY_SHA256": (
            plan.builder.scanner_cache_identity_sha256
        ),
        "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_DATABASE_METADATA_SHA256": (
            plan.builder.scanner_database_metadata_sha256
        ),
        "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_IDENTITY": scanner_identity,
        "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_JAVA_DATABASE_METADATA_SHA256": (
            plan.builder.scanner_java_database_metadata_sha256
        ),
        "LOOM_SVC_PERSONAL_DEV_BUILDER_SCANNER_POLICY_SHA256": (
            plan.builder.scanner_finding_policy_sha256
        ),
        "LOOM_SVC_PERSONAL_DEV_TRUSTED_LAUNCHER_PROFILE_SHA256": (
            plan.builder.trusted_launcher_profile_sha256
        ),
    }
    return all(environment.get(name) == value for name, value in expected.items())


def _acceptance_management_probe_valid(deployment: Mapping[str, Any]) -> bool:
    management = next(
        (
            container
            for container in _containers(deployment)
            if container.get("name") == "management"
        ),
        None,
    )
    if management is None:
        return False
    readiness = management.get("readinessProbe")
    http_get = readiness.get("httpGet") if isinstance(readiness, Mapping) else None
    return (
        isinstance(http_get, Mapping)
        and http_get.get("path") == "/api/v1/health/personal-dev-acceptance"
        and http_get.get("port") == "http"
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


def _runtime_class_matches_binding(
    runtime: Mapping[str, Any],
    *,
    runtime_class_name: str,
    runtime_handler: str,
    runtime_profile_sha256: str,
) -> bool:
    if _DIGEST.fullmatch(runtime_profile_sha256) is None:
        return False
    metadata = _metadata(runtime)
    annotations = metadata.get("annotations") if metadata is not None else None
    scheduling = runtime.get("scheduling")
    selector = {
        "kubernetes.io/arch": "amd64",
        "kubernetes.io/os": "linux",
        "loom.dev/personal-dev-runtime-profile-a": runtime_profile_sha256[:32],
        "loom.dev/personal-dev-runtime-profile-b": runtime_profile_sha256[32:],
    }
    return (
        runtime.get("apiVersion") == "node.k8s.io/v1"
        and runtime.get("kind") == "RuntimeClass"
        and metadata is not None
        and metadata.get("name") == runtime_class_name
        and runtime.get("handler") == runtime_handler
        and isinstance(annotations, Mapping)
        and annotations.get("loom.dev/runtime-profile-sha256")
        == runtime_profile_sha256
        and isinstance(scheduling, Mapping)
        and set(scheduling).issubset({"nodeSelector", "tolerations"})
        and scheduling.get("nodeSelector") == selector
        and scheduling.get("tolerations", []) == []
        and "overhead" not in runtime
    )


def _canonical_nonzero_uuid(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 36:
        return False
    try:
        parsed = UUID(value)
    except ValueError:
        return False
    return parsed.int != 0 and str(parsed) == value


def _dynamic_namespace_valid(
    name: str,
    item: Mapping[str, Any],
    *,
    personal: bool,
) -> bool:
    metadata = _metadata(item)
    labels = metadata.get("labels") if metadata is not None else None
    if not isinstance(labels, Mapping):
        return False
    if personal:
        name_valid = (
            _PERSONAL_NAMESPACE.fullmatch(name) is not None
            and _RESERVED_PERSONAL_NAMESPACE.fullmatch(name) is None
        )
        managed_by = "loom-dev-instance-controller"
        pod_security_valid = (
            labels.get("pod-security.kubernetes.io/enforce") == "restricted"
        )
    else:
        name_valid = _BUILDER_NAMESPACE.fullmatch(name) is not None
        managed_by = "loom-personal-dev-builder-controller"
        pod_security_valid = all(
            labels.get(label) == expected
            for label, expected in _BUILDER_POD_SECURITY_LABELS
        )
    return (
        name_valid
        and labels.get("app.kubernetes.io/managed-by") == managed_by
        and labels.get("app.kubernetes.io/part-of") == "loom"
        and pod_security_valid
        and _canonical_nonzero_uuid(labels.get("loom.dev/subject"))
    )


def _acceptance_manager_status(
    result: subprocess.CompletedProcess[str] | None,
    plan: PersonalDevAcceptancePlan,
) -> tuple[int | None, bool, set[str]]:
    if result is None or result.returncode != 0:
        return None, False, {"manager_probe_unavailable"}
    try:
        document = _json_document(result.stdout)
        if set(document) != {
            "authority_incarnation",
            "configuration_epoch",
            "executable_new_capacity_ceiling",
            "execution_epoch",
            "execution_state",
            "observer_principal_id",
        }:
            raise ValueError("manager identity response fields differ")
        authority = document["authority_incarnation"]
        if not isinstance(authority, str) or str(UUID(authority)) != authority:
            raise ValueError("manager authority is invalid")
        binding = PersonalDevCapacityManagerBinding(
            authority_incarnation=UUID(authority),
            observer_principal_id=document["observer_principal_id"],
            configuration_epoch=document["configuration_epoch"],
            execution_state=document["execution_state"],
            execution_epoch=document["execution_epoch"],
            executable_new_capacity_ceiling=document["executable_new_capacity_ceiling"],
        )
    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None, False, {"manager_probe_invalid"}
    expected = PersonalDevCapacityManagerBinding(
        authority_incarnation=plan.manager.authority_incarnation,
        observer_principal_id=plan.principals.lifecycle_principal_id,
        configuration_epoch=plan.manager.configuration_epoch,
        execution_state=plan.manager.execution_state,
        execution_epoch=plan.manager.execution_epoch,
        executable_new_capacity_ceiling=plan.manager.executable_new_capacity_ceiling,
    )
    blockers: set[str] = set()
    if not binding.satisfies_acceptance_boundary(expected):
        blockers.add("manager_binding_drift")
    if binding.executable_new_capacity_ceiling != 0:
        blockers.add("manager_ceiling_nonzero")
    return binding.executable_new_capacity_ceiling, not blockers, blockers


def _personal_worker_signature(item: Mapping[str, Any]) -> bool:
    metadata = _metadata(item)
    if metadata is None:
        return False
    name = metadata.get("name")
    labels = metadata.get("labels")
    spec = item.get("spec")
    template = spec.get("template") if isinstance(spec, Mapping) else None
    template_metadata = template.get("metadata") if isinstance(template, Mapping) else None
    template_labels = (
        template_metadata.get("labels") if isinstance(template_metadata, Mapping) else None
    )
    if (
        name == "loom-worker"
        or (isinstance(name, str) and re.fullmatch(r"loom-worker-g[1-9][0-9]*", name))
        or (isinstance(labels, Mapping) and labels.get("app") == "loom-worker")
        or (isinstance(template_labels, Mapping) and template_labels.get("app") == "loom-worker")
    ):
        return True
    for container in _containers(item):
        if container.get("name") == "worker":
            return True
        image = container.get("image")
        if isinstance(image, str) and image.partition("@sha256:")[0].endswith("/loom-worker"):
            return True
        environment = container.get("env")
        if isinstance(environment, list) and any(
            isinstance(entry, Mapping)
            and entry.get("name") == "LOOM_SVC_K8S_WORKER_ENABLED"
            and entry.get("value") == "true"
            for entry in environment
        ):
            return True
    return False


def _personal_worker_inventory(
    result: subprocess.CompletedProcess[str] | None,
) -> tuple[int, bool]:
    if result is None:
        raise ValueError("deployment inventory response is invalid")
    deployments = _list_items(
        result,
        expected_kind="DeploymentList",
        expected_api_version="apps/v1",
    )
    workers = 0
    for deployment in deployments:
        metadata = _metadata(deployment)
        namespace = metadata.get("namespace") if metadata is not None else None
        name = metadata.get("name") if metadata is not None else None
        if (
            deployment.get("apiVersion") != "apps/v1"
            or deployment.get("kind") != "Deployment"
            or not isinstance(namespace, str)
            or not namespace
            or not isinstance(name, str)
            or not name
        ):
            raise ValueError("deployment inventory item has an invalid shape")
        in_development = (
            namespace == _NAMESPACE
            or namespace.startswith("loom-dev-")
            or namespace.startswith("loom-build-")
        )
        if in_development and _personal_worker_signature(deployment):
            workers += 1
    return workers, workers == 0


def _acceptance_window_blocker(plan: PersonalDevAcceptancePlan) -> str | None:
    now = datetime.now(UTC)
    if now < plan.window.started_at:
        return "acceptance_window_not_open"
    if now >= plan.window.expires_at:
        return "acceptance_window_expired"
    return None


def _observe_personal_dev_status(
    runner: KubectlRunner,
    *,
    expected: RenderedPersonalDevControlPlane,
    plan: PersonalDevAcceptancePlan | None,
    namespace: str = _NAMESPACE,
) -> PersonalDevShadowStatus | PersonalDevAcceptanceStatus:
    """Compare bounded live state with one locally trusted mode render."""

    if namespace != _NAMESPACE:
        raise ValueError("personal-dev control-plane namespace must be loom-dev")
    if not isinstance(expected, RenderedPersonalDevControlPlane):
        raise TypeError("personal-dev expected render is invalid")
    if plan is not None and not isinstance(plan, PersonalDevAcceptancePlan):
        raise TypeError("personal-dev acceptance plan is invalid")
    acceptance = plan is not None
    expected_namespaced, expected_cluster, expected_namespace = _expected_documents(expected)
    deadline = time.monotonic() + _TOTAL_TIMEOUT_SECONDS
    runtime_binding = plan.builder if plan is not None else expected
    runtime_class_command = (
        "get",
        f"runtimeclass.node.k8s.io/{runtime_binding.runtime_class_name}",
        "--output=json",
    )
    mode_commands = (
        (_ACCEPTANCE_MANAGER_COMMAND, _DEPLOYMENTS_COMMAND) if acceptance else (_MANAGER_COMMAND,)
    )
    results = {
        command: _run(runner, command, deadline)
        for command in (
            _CONTEXT_COMMAND,
            _NAMESPACE_COMMAND,
            runtime_class_command,
            _NAMESPACED_COMMAND,
            _CLUSTER_COMMAND,
            *mode_commands,
        )
    }
    blockers: set[str] = set()
    window_blocker = _acceptance_window_blocker(plan) if plan is not None else None
    if window_blocker is not None:
        blockers.add(window_blocker)

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
        namespace_items = _list_items(namespace_result, expected_kind="NamespaceList")
        names: dict[str, dict[str, Any]] = {}
        for item in namespace_items:
            if item.get("apiVersion") != "v1" or item.get("kind") != "Namespace":
                raise ValueError("namespace inventory item has an invalid shape")
            metadata = _metadata(item)
            name = metadata.get("name") if metadata else None
            if not isinstance(name, str) or name in names:
                raise ValueError("namespace identity is invalid or duplicated")
            names[name] = item
        personal_names = sorted(name for name in names if name.startswith("loom-dev-"))
        builder_names = sorted(name for name in names if name.startswith("loom-build-"))
        namespace_observed = int(_NAMESPACE in names) + len(personal_names) + len(builder_names)
        shared_namespace_ok = _NAMESPACE in names and _expected_subset(
            expected_namespace,
            names[_NAMESPACE],
        )
        if not shared_namespace_ok:
            blockers.add("namespace_missing")
        if plan is None:
            if personal_names:
                blockers.add("unexpected_personal_namespace")
            if builder_names:
                blockers.add("unexpected_builder_namespace")
            namespace_ok = shared_namespace_ok and not personal_names and not builder_names
        else:
            personal_ok = all(
                _dynamic_namespace_valid(name, names[name], personal=True)
                for name in personal_names
            )
            builder_ok = all(
                _dynamic_namespace_valid(name, names[name], personal=False)
                for name in builder_names
            )
            if not personal_ok:
                blockers.add("personal_namespace_invalid")
            if not builder_ok:
                blockers.add("builder_namespace_invalid")
            namespace_ok = shared_namespace_ok and personal_ok and builder_ok
    except (OSError, json.JSONDecodeError, UnicodeError, ValueError):
        blockers.add("namespace_inventory_invalid")

    runtime_ok = False
    runtime_observed = 0
    runtime_result = results[runtime_class_command]
    if runtime_result is not None and runtime_result.returncode == 0:
        try:
            runtime = _json_document(runtime_result.stdout)
            runtime_ok = _runtime_class_matches_binding(
                runtime,
                runtime_class_name=runtime_binding.runtime_class_name,
                runtime_handler=runtime_binding.runtime_handler,
                runtime_profile_sha256=runtime_binding.runtime_profile_sha256,
            )
            runtime_observed = int(runtime_ok)
        except (json.JSONDecodeError, ValueError):
            runtime_ok = False
    if not runtime_ok:
        blockers.add("runtime_class_binding_invalid" if acceptance else "runtime_class_missing")

    namespaced_ok = False
    namespaced_observed = 0
    activation_ready = not acceptance
    live_namespaced: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    expected_namespaced_index = _index_unique(expected_namespaced)
    try:
        namespaced_result = results[_NAMESPACED_COMMAND]
        if namespaced_result is None:
            raise ValueError("namespaced inventory response is invalid")
        namespaced_items = _list_items(namespaced_result, expected_kind="List")
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
        current_migration_name = next(
            _metadata(item).get("name")  # type: ignore[union-attr]
            for item in expected_namespaced
            if item.get("kind") == "Job"
        )
        historical_jobs = {
            identity: item
            for identity, item in live_namespaced.items()
            if item.get("kind") == "Job"
            and _metadata(item).get("name") != current_migration_name  # type: ignore[union-attr]
            and isinstance(_metadata(item).get("name"), str)  # type: ignore[union-attr]
            and _MIGRATION_JOB.fullmatch(_metadata(item)["name"]) is not None  # type: ignore[index]
        }
        historical_job_names = {
            _metadata(item)["name"]: (identity, item)  # type: ignore[index]
            for identity, item in historical_jobs.items()
        }
        historical_pods: dict[
            str,
            list[tuple[tuple[str, str, str, str], dict[str, Any]]],
        ] = {name: [] for name in historical_job_names}
        for identity, pod in live_namespaced.items():
            if pod.get("kind") != "Pod":
                continue
            metadata = _metadata(pod)
            labels = metadata.get("labels") if metadata else None
            job_name = labels.get("job-name") if isinstance(labels, Mapping) else None
            if isinstance(job_name, str) and job_name in historical_pods:
                historical_pods[job_name].append((identity, pod))
        history_drift = len(historical_jobs) > _MAX_MIGRATION_HISTORY
        for name, (_identity_value, job) in historical_job_names.items():
            match = _MIGRATION_JOB.fullmatch(name)
            digests = _observed_digests(job)
            pod_entries = historical_pods[name]
            if (
                match is None
                or digests is None
                or match.groups() != (digests[0][:16], digests[1][:16])
                or _migration_state(job) != "succeeded"
                or not _images_are_immutable(job)
                or len(pod_entries) != 1
            ):
                history_drift = True
                continue
            _pod_identity, pod = pod_entries[0]
            job_spec = job.get("spec")
            template = job_spec.get("template") if isinstance(job_spec, Mapping) else None
            pod_status = pod.get("status")
            if (
                not isinstance(template, Mapping)
                or _observed_digests(pod) != digests
                or not _expected_subset(
                    {
                        "metadata": template.get("metadata"),
                        "spec": template.get("spec"),
                    },
                    pod,
                )
                or not isinstance(pod_status, Mapping)
                or pod_status.get("phase") != "Succeeded"
            ):
                history_drift = True
        historical_identities = set(historical_jobs) | {
            identity for entries in historical_pods.values() for identity, _pod in entries
        }
        current_pods = [pod for pod in pods if _identity(pod) not in historical_identities]
        allowed_extra = (
            generated_pvcs
            | historical_identities
            | {identity for identity, item in live_namespaced.items() if item.get("kind") == "Pod"}
        )
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
            or not _security_boundary_matches(
                expected_namespaced_index[identity],
                actual,
            )
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
        for pod in current_pods:
            metadata = _metadata(pod)
            labels = metadata.get("labels") if metadata else None
            app = labels.get("app") if isinstance(labels, Mapping) else None
            if not isinstance(app, str) or app not in expected_pod_templates:
                pod_drift = True
                continue
            observed_pods[app].append(pod)
            if not _expected_subset(
                expected_pod_templates[app][0],
                pod,
            ) or not _security_boundary_matches({"kind": "Pod"}, pod):
                pod_drift = True
        pod_drift = pod_drift or any(
            len(observed_pods[app]) != expected_count
            for app, (_template, expected_count) in expected_pod_templates.items()
        )
        if missing or unexpected or drifted or generated_pvc_drift or pod_drift or history_drift:
            blockers.add("resource_inventory_drift")

        digest_ok = all(
            _digest_matches(
                item,
                input_sha256=expected.input_sha256,
                release_sha256=expected.release_sha256,
            )
            for item in namespaced_items
            if _identity(item) not in historical_identities
        )
        if not digest_ok:
            blockers.add("resource_digest_drift")
        if plan is not None and not all(
            _acceptance_plan_digest_matches(
                item,
                acceptance_plan_sha256=plan.sha256,
            )
            for item in namespaced_items
            if _identity(item) not in historical_identities
        ):
            blockers.add("acceptance_plan_digest_drift")

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
        if plan is None:
            if management is None or not _shadow_flags_valid(management):
                blockers.add("management_shadow_flags_invalid")
        else:
            if management is None or not _acceptance_management_binding_valid(
                management,
                plan,
            ):
                blockers.add("management_acceptance_binding_invalid")
            if management is None or not _acceptance_management_probe_valid(management):
                blockers.add("management_acceptance_probe_invalid")

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
        if plan is None:
            if (
                not isinstance(activation_spec, Mapping)
                or _integer(activation_spec.get("replicas")) != 0
            ):
                blockers.add("activation_replicas_nonzero")
        else:
            activation_replicas_valid = (
                isinstance(activation_spec, Mapping)
                and _integer(activation_spec.get("replicas")) == 1
            )
            if not activation_replicas_valid:
                blockers.add("activation_replicas_invalid")
            activation_ready = (
                activation_replicas_valid
                and activation is not None
                and _management_ready(activation)
            )
            if not activation_ready:
                blockers.add("activation_not_ready")

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
        namespaced_blockers = {
            "acceptance_plan_digest_drift",
            "activation_not_ready",
            "activation_replicas_invalid",
            "activation_replicas_nonzero",
            "init_container_failed",
            "management_acceptance_binding_invalid",
            "management_acceptance_probe_invalid",
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
        namespaced_ok = not bool(blockers & namespaced_blockers)
    except (OSError, json.JSONDecodeError, KeyError, TypeError, UnicodeError, ValueError):
        blockers.add("resource_inventory_invalid")

    cluster_ok = False
    cluster_observed = 0
    expected_cluster_index = _index_unique(expected_cluster)
    try:
        cluster_result = results[_CLUSTER_COMMAND]
        if cluster_result is None:
            raise ValueError("cluster inventory response is invalid")
        cluster_items = _list_items(cluster_result, expected_kind="List")
        live_cluster = _index_unique(cluster_items)
        cluster_observed = len(live_cluster)
        cluster_ok = set(live_cluster) == set(expected_cluster_index) and all(
            _expected_subset(expected_cluster_index[identity], live_cluster[identity])
            and _security_boundary_matches(
                expected_cluster_index[identity],
                live_cluster[identity],
            )
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
        if plan is not None and not all(
            _acceptance_plan_digest_matches(
                item,
                acceptance_plan_sha256=plan.sha256,
            )
            for item in cluster_items
        ):
            blockers.add("acceptance_plan_digest_drift")
            cluster_ok = False
    except (OSError, json.JSONDecodeError, UnicodeError, ValueError):
        blockers.add("resource_inventory_invalid")

    personal_workers = 0
    worker_inventory_ok = True
    if plan is None:
        manager_ceiling, manager_blocker = _manager_status(results[_MANAGER_COMMAND])
        if manager_blocker:
            blockers.add(manager_blocker)
        manager_ok = manager_blocker is None
    else:
        manager_ceiling, manager_ok, manager_blockers = _acceptance_manager_status(
            results[_ACCEPTANCE_MANAGER_COMMAND],
            plan,
        )
        blockers.update(manager_blockers)
        try:
            personal_workers, worker_inventory_ok = _personal_worker_inventory(
                results[_DEPLOYMENTS_COMMAND]
            )
        except (OSError, json.JSONDecodeError, UnicodeError, ValueError):
            worker_inventory_ok = False
            blockers.add("deployment_inventory_invalid")
        if not worker_inventory_ok and "deployment_inventory_invalid" not in blockers:
            blockers.add("unexpected_personal_worker")

    digest_observed = (
        "resource_digest_drift" not in blockers
        and "resource_inventory_invalid" not in blockers
        and namespaced_observed > 0
        and cluster_observed > 0
    )
    component_values = [
        PersonalDevShadowComponent("cluster-resources", cluster_observed, cluster_ok),
        PersonalDevShadowComponent("manager", int(manager_ceiling is not None), manager_ok),
        PersonalDevShadowComponent("namespaced-resources", namespaced_observed, namespaced_ok),
        PersonalDevShadowComponent("namespaces", namespace_observed, namespace_ok),
        PersonalDevShadowComponent("runtime-class", runtime_observed, runtime_ok),
    ]
    if plan is not None:
        component_values.append(
            PersonalDevShadowComponent(
                "personal-workers",
                personal_workers,
                worker_inventory_ok,
            )
        )
    components = tuple(sorted(component_values, key=lambda component: component.name))
    stable_blockers = tuple(sorted(blockers))
    if plan is not None:
        shared_ready = namespace_ok and runtime_ok and namespaced_ok and cluster_ok
        application_ready = shared_ready and activation_ready
        capacity_publication_ready = manager_ok and manager_ceiling == 0
        return PersonalDevAcceptanceStatus(
            ready=(application_ready and capacity_publication_ready and not stable_blockers),
            blockers=stable_blockers,
            input_sha256=expected.input_sha256 if digest_observed else None,
            release_sha256=expected.release_sha256 if digest_observed else None,
            acceptance_plan_sha256=plan.sha256,
            manager_ceiling=manager_ceiling,
            components=components,
            application_ready=application_ready,
            capacity_publication_ready=capacity_publication_ready,
            worker_available=False,
        )
    return PersonalDevShadowStatus(
        ready=not stable_blockers,
        blockers=stable_blockers,
        input_sha256=expected.input_sha256 if digest_observed else None,
        release_sha256=expected.release_sha256 if digest_observed else None,
        manager_ceiling=manager_ceiling,
        components=components,
    )


def observe_personal_dev_shadow_status(
    runner: KubectlRunner,
    *,
    expected: RenderedPersonalDevControlPlane,
    namespace: str = _NAMESPACE,
) -> PersonalDevShadowStatus:
    """Compare bounded live state with one locally trusted shadow render."""

    result = _observe_personal_dev_status(
        runner,
        expected=expected,
        plan=None,
        namespace=namespace,
    )
    assert isinstance(result, PersonalDevShadowStatus)
    return result


def observe_personal_dev_acceptance_status(
    runner: KubectlRunner,
    *,
    expected: RenderedPersonalDevControlPlane,
    plan: PersonalDevAcceptancePlan,
    namespace: str = _NAMESPACE,
) -> PersonalDevAcceptanceStatus:
    """Observe one read-only zero-capacity personal acceptance binding."""

    if not isinstance(plan, PersonalDevAcceptancePlan):
        raise TypeError("personal-dev acceptance plan is invalid")
    result = _observe_personal_dev_status(
        runner,
        expected=expected,
        plan=plan,
        namespace=namespace,
    )
    assert isinstance(result, PersonalDevAcceptanceStatus)
    return result


__all__ = [
    "MAX_PERSONAL_DEV_STATUS_RESPONSE_BYTES",
    "KubectlRunner",
    "PersonalDevAcceptanceStatus",
    "PersonalDevShadowComponent",
    "PersonalDevShadowStatus",
    "observe_personal_dev_acceptance_status",
    "observe_personal_dev_shadow_status",
]
