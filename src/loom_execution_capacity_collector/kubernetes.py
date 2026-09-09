"""Read-only Kubernetes node and Pod resource accounting."""

from __future__ import annotations

import asyncio
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from types import SimpleNamespace
from typing import Any

from loom.nebius_kubernetes import (
    NebiusKubernetesConnection,
    NebiusKubernetesCredentials,
    create_api_client,
)
from loom_execution_capacity_collector.contracts import (
    DaemonSetPlacement,
    KubernetesCapacitySnapshot,
    ManagedPodPlacement,
    NodePlacement,
    NodeTemplateSample,
    ResourceTotals,
)

_MIB = Decimal(1024 * 1024)
_TARGET_ANNOTATION = "loom.openai.com/target-id"
_MANAGED_BY = "loom-execution-actuator"
_IMAGE_PULL_REASONS = {"ErrImagePull", "ImagePullBackOff"}


class KubernetesObservationError(RuntimeError):
    """Kubernetes did not return a complete, safely scoped inventory."""


def _quantity(value: object, *, kind: str, capacity: bool = False) -> int:
    if value is None:
        return 0
    try:
        from kubernetes.utils.quantity import parse_quantity

        parsed = Decimal(parse_quantity(str(value)))
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise KubernetesObservationError(f"invalid Kubernetes {kind} quantity") from exc
    if parsed < 0:
        raise KubernetesObservationError(f"negative Kubernetes {kind} quantity")
    if kind == "cpu":
        scaled = parsed * 1000
    else:
        scaled = parsed / _MIB
    rounding = ROUND_FLOOR if capacity else ROUND_CEILING
    return int(scaled.to_integral_value(rounding=rounding))


def _resources(values: object, *, capacity: bool = False) -> ResourceTotals:
    mapping = values if isinstance(values, dict) else {}
    return ResourceTotals(
        cpu_millis=_quantity(mapping.get("cpu"), kind="cpu", capacity=capacity),
        memory_mib=_quantity(mapping.get("memory"), kind="memory", capacity=capacity),
        storage_mib=_quantity(mapping.get("ephemeral-storage"), kind="storage", capacity=capacity),
    )


def _required_node_resources(values: object, *, name: str) -> ResourceTotals:
    result = _resources(values, capacity=True)
    if min(result.cpu_millis, result.memory_mib, result.storage_mib) <= 0:
        raise KubernetesObservationError(f"Kubernetes node {name} resources are incomplete")
    return result


def _add(*values: ResourceTotals) -> ResourceTotals:
    return ResourceTotals(
        cpu_millis=sum(value.cpu_millis for value in values),
        memory_mib=sum(value.memory_mib for value in values),
        storage_mib=sum(value.storage_mib for value in values),
    )


def _maximum(*values: ResourceTotals) -> ResourceTotals:
    return ResourceTotals(
        cpu_millis=max((value.cpu_millis for value in values), default=0),
        memory_mib=max((value.memory_mib for value in values), default=0),
        storage_mib=max((value.storage_mib for value in values), default=0),
    )


def _container_request(container: Any) -> ResourceTotals:
    resources = getattr(container, "resources", None)
    return _resources(getattr(resources, "requests", None) or {})


def _pod_request(pod: Any) -> ResourceTotals:
    """Mirror scheduler accounting, including restartable init sidecars."""

    regular = _add(*[_container_request(row) for row in list(pod.spec.containers or [])])
    restartable = ResourceTotals(cpu_millis=0, memory_mib=0, storage_mib=0)
    init_peaks: list[ResourceTotals] = []
    for container in list(getattr(pod.spec, "init_containers", None) or []):
        request = _container_request(container)
        if getattr(container, "restart_policy", None) == "Always":
            restartable = _add(restartable, request)
        else:
            init_peaks.append(_add(restartable, request))
    effective = _maximum(_add(regular, restartable), *init_peaks)
    return _add(effective, _resources(getattr(pod.spec, "overhead", None) or {}))


def _condition(conditions: list[Any] | None, condition_type: str) -> Any | None:
    return next(
        (row for row in conditions or [] if getattr(row, "type", None) == condition_type),
        None,
    )


def _node_ready(node: Any) -> bool:
    ready = _condition(getattr(node.status, "conditions", None), "Ready")
    return (
        ready is not None
        and getattr(ready, "status", None) == "True"
        and getattr(node.metadata, "deletion_timestamp", None) is None
        and not bool(getattr(node.spec, "unschedulable", False))
    )


def _identity(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise KubernetesObservationError(f"Kubernetes {name} is unavailable")
    return value


def _positive_int(value: object, *, name: str) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise KubernetesObservationError(f"Kubernetes {name} is invalid") from exc
    if isinstance(value, bool) or parsed <= 0:
        raise KubernetesObservationError(f"Kubernetes {name} is invalid")
    return parsed


def _plain(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        return _plain(value.to_dict())
    if isinstance(value, SimpleNamespace):
        return _plain(vars(value))
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items() if item is not None}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _scheduling(spec: Any) -> dict[str, Any]:
    # Only scheduling fields, never commands, environment, volumes or credentials.
    return {
        field: _plain(getattr(spec, field))
        for field in (
            "node_selector",
            "tolerations",
            "affinity",
            "topology_spread_constraints",
            "scheduler_name",
            "priority_class_name",
            "runtime_class_name",
            "overhead",
        )
        if getattr(spec, field, None) is not None
    }


def _managed_placement(pod: Any) -> ManagedPodPlacement:
    labels = dict(getattr(pod.metadata, "labels", None) or {})
    return ManagedPodPlacement(
        uid=_identity(getattr(pod.metadata, "uid", None), name="Pod UID"),
        lease_id=_identity(labels.get("loom.openai.com/lease-id"), name="Pod lease ID"),
        generation=_positive_int(labels.get("loom.openai.com/generation"), name="Pod generation"),
        requests=_pod_request(pod),
    )


def _matches_expression(expression: dict[str, Any], labels: dict[str, str]) -> bool:
    key, op = expression.get("key"), expression.get("operator")
    if not isinstance(key, str):
        raise KubernetesObservationError("DaemonSet node selector key is invalid")
    values = expression.get("values", [])
    if op == "In":
        return key in labels and labels[key] in values
    if op == "NotIn":
        return key not in labels or labels[key] not in values
    if op == "Exists":
        return key in labels
    if op == "DoesNotExist":
        return key not in labels
    if op in {"Gt", "Lt"}:
        try:
            left, right = int(labels[key]), int(values[0])
            return left > right if op == "Gt" else left < right
        except (KeyError, IndexError, TypeError, ValueError):
            return False
    raise KubernetesObservationError("unsupported DaemonSet node selector operator")


def _daemonset_matches_node(daemon: Any, node: Any) -> bool:
    """Only hard node constraints; Pod affinity is established by observed Pods."""
    spec = daemon.spec.template.spec
    labels = dict(getattr(node.metadata, "labels", None) or {})
    if any(
        labels.get(key) != value
        for key, value in (getattr(spec, "node_selector", None) or {}).items()
    ):
        return False
    affinity = _plain(getattr(spec, "affinity", None) or {})
    required = affinity.get("node_affinity", {}).get(
        "required_during_scheduling_ignored_during_execution"
    )
    if required is not None:
        terms = required.get("node_selector_terms", [])
        if not any(
            (term.get("match_expressions") or term.get("match_fields"))
            and all(_matches_expression(item, labels) for item in term.get("match_expressions", []))
            and all(
                _matches_expression(item, {"metadata.name": node.metadata.name})
                for item in term.get("match_fields", [])
            )
            for term in terms
        ):
            return False
    tolerations = _plain(getattr(spec, "tolerations", None) or [])
    for taint in _plain(getattr(node.spec, "taints", None) or []):
        if taint.get("effect") not in {"NoSchedule", "NoExecute"}:
            continue
        if not any(
            (not tol.get("effect") or tol["effect"] == taint["effect"])
            and (
                (
                    tol.get("operator") == "Exists"
                    and (not tol.get("key") or tol["key"] == taint["key"])
                )
                or (
                    tol.get("operator", "Equal") == "Equal"
                    and tol.get("key") == taint["key"]
                    and tol.get("value", "") == taint.get("value", "")
                )
            )
            for tol in tolerations
        ):
            return False
    return True


def _template_sample(
    node: Any, placement: NodePlacement, pods: list[Any], daemons: list[Any]
) -> NodeTemplateSample | None:
    if not placement.ready:
        return None
    daemon_requests: list[ResourceTotals] = []
    revisions: dict[str, int] = {}
    for daemon in daemons:
        if not _daemonset_matches_node(daemon, node):
            continue
        uid = _identity(getattr(daemon.metadata, "uid", None), name="DaemonSet UID")
        generation = _positive_int(daemon.metadata.generation, name="DaemonSet generation")
        status = daemon.status
        if (
            getattr(status, "observed_generation", None) != generation
            or getattr(status, "updated_number_scheduled", None)
            != getattr(status, "desired_number_scheduled", None)
            or getattr(daemon.spec.template.spec, "runtime_class_name", None)
        ):
            return None
        matches = [
            pod
            for pod in pods
            if any(
                getattr(owner, "kind", None) == "DaemonSet" and getattr(owner, "uid", None) == uid
                for owner in getattr(pod.metadata, "owner_references", None) or []
            )
        ]
        if (
            len(matches) != 1
            or matches[0].status.phase != "Running"
            or getattr(matches[0].metadata, "deletion_timestamp", None)
        ):
            return None
        actual = _pod_request(matches[0])
        declared = _pod_request(SimpleNamespace(spec=daemon.spec.template.spec))
        if actual != declared:
            return None
        daemon_requests.append(actual)
        revisions[uid] = generation
    # A disappearing controller or a still-running old DaemonSet must not vanish
    # from the next-node overhead estimate.
    observed_daemons = {
        getattr(owner, "uid", None)
        for pod in pods
        for owner in getattr(pod.metadata, "owner_references", None) or []
        if getattr(owner, "kind", None) == "DaemonSet"
    }
    if observed_daemons != set(revisions):
        return None
    version = getattr(getattr(node.status, "node_info", None), "kubelet_version", None)
    if not isinstance(version, str) or not version:
        return None
    return NodeTemplateSample(
        node_uid=placement.uid,
        allocatable=placement.allocatable,
        pod_slots=placement.pod_slots,
        daemonset_requests=_add(*daemon_requests),
        daemonset_slots=len(daemon_requests),
        kubelet_version=version,
        daemonsets=revisions,
    )


def _target_pod(pod: Any, *, namespace: str, target_id: str) -> bool:
    metadata = pod.metadata
    labels = dict(getattr(metadata, "labels", None) or {})
    annotations = dict(getattr(metadata, "annotations", None) or {})
    return (
        metadata.namespace == namespace
        and labels.get("app.kubernetes.io/managed-by") == _MANAGED_BY
        and annotations.get(_TARGET_ANNOTATION) == target_id
    )


def _pending_state(pod: Any) -> tuple[bool, bool, bool, str | None]:
    if getattr(pod.status, "phase", None) != "Pending":
        return False, False, False, None
    scheduled = _condition(getattr(pod.status, "conditions", None), "PodScheduled")
    unschedulable = bool(
        scheduled is not None
        and getattr(scheduled, "status", None) == "False"
        and getattr(scheduled, "reason", None) == "Unschedulable"
    )
    statuses = [
        *list(getattr(pod.status, "init_container_statuses", None) or []),
        *list(getattr(pod.status, "container_statuses", None) or []),
    ]
    waiting_reasons = [
        getattr(getattr(getattr(row, "state", None), "waiting", None), "reason", None)
        for row in statuses
    ]
    image_pull = any(reason in _IMAGE_PULL_REASONS for reason in waiting_reasons)
    if image_pull:
        reason = next(str(reason) for reason in waiting_reasons if reason in _IMAGE_PULL_REASONS)
    elif unschedulable:
        reason = "Unschedulable"
    else:
        raw = getattr(pod.status, "reason", None)
        reason = str(raw)[:120] if isinstance(raw, str) and raw else "Pending"
    return True, unschedulable, image_pull, reason


class InClusterKubernetesCapacityReader:
    def __init__(
        self,
        *,
        connection: NebiusKubernetesConnection | None = None,
        core_api: Any | None = None,
        apps_api: Any | None = None,
        request_timeout_seconds: float = 15.0,
    ) -> None:
        if not 1.0 <= request_timeout_seconds <= 60.0:
            raise ValueError("Kubernetes request timeout must be between 1 and 60 seconds")
        self._api_client: Any | None = None
        self._credentials: NebiusKubernetesCredentials | None = None
        if connection is not None:
            if core_api is not None or apps_api is not None:
                raise ValueError("remote connection cannot be combined with injected clients")
            from kubernetes import client

            self._api_client, self._credentials = create_api_client(connection)
            core_api = client.CoreV1Api(self._api_client)
            apps_api = client.AppsV1Api(self._api_client)
        if core_api is None:
            try:
                from kubernetes import client, config
            except ModuleNotFoundError as exc:
                raise RuntimeError("install Loom with the cluster extra") from exc
            config.load_incluster_config()
            core_api = client.CoreV1Api()
        self._core = core_api
        if apps_api is None:
            from kubernetes import client

            apps_api = client.AppsV1Api()
        self._apps = apps_api
        self._request_timeout = request_timeout_seconds

    async def close(self) -> None:
        try:
            if self._api_client is not None:
                await asyncio.to_thread(self._api_client.close)
        finally:
            if self._credentials is not None:
                await self._credentials.close()

    def _list_all(
        self,
        method: Any,
        *,
        maximum_items: int,
        page_size: int,
        **kwargs: object,
    ) -> tuple[list[Any], str]:
        items: list[Any] = []
        token = ""
        seen_tokens: set[str] = set()
        resource_version: str | None = None
        while True:
            response = method(
                **kwargs,
                limit=page_size,
                _continue=token or None,
                _request_timeout=(self._request_timeout, self._request_timeout),
            )
            metadata = getattr(response, "metadata", None)
            version = getattr(metadata, "resource_version", None)
            if not version or (resource_version is not None and str(version) != resource_version):
                raise KubernetesObservationError(
                    "Kubernetes paginated list resource version is unavailable or changed"
                )
            resource_version = str(version)
            items.extend(list(response.items))
            if len(items) > maximum_items:
                raise KubernetesObservationError("Kubernetes capacity inventory exceeded its bound")
            next_token = str(getattr(metadata, "_continue", None) or "")
            if not next_token:
                return items, resource_version
            if next_token in seen_tokens:
                raise KubernetesObservationError("Kubernetes pagination repeated a token")
            seen_tokens.add(next_token)
            token = next_token

    def _capture_sync(
        self,
        *,
        namespace: str,
        target_id: str,
        node_label_selector: str,
    ) -> KubernetesCapacitySnapshot:
        try:
            nodes, node_version = self._list_all(
                self._core.list_node,
                maximum_items=10_000,
                page_size=500,
                label_selector=node_label_selector,
            )
            pods, pod_version = self._list_all(
                self._core.list_pod_for_all_namespaces,
                maximum_items=200_000,
                page_size=1000,
                watch=False,
            )
            daemons, daemon_version = self._list_all(
                self._apps.list_daemon_set_for_all_namespaces,
                maximum_items=10_000,
                page_size=500,
                watch=False,
            )
        except Exception as exc:
            if isinstance(exc, KubernetesObservationError):
                raise
            raise KubernetesObservationError("Kubernetes capacity list failed") from exc
        node_names = {
            str(node.metadata.name)
            for node in nodes
            if isinstance(getattr(node.metadata, "name", None), str)
        }
        if len(node_names) != len(nodes):
            raise KubernetesObservationError("Kubernetes node identity is missing or duplicated")
        provisioned = _add(
            *[
                _required_node_resources(
                    getattr(node.status, "capacity", None) or {}, name="capacity"
                )
                for node in nodes
            ]
        )
        allocatable = _add(
            *[
                _required_node_resources(
                    getattr(node.status, "allocatable", None) or {}, name="allocatable"
                )
                for node in nodes
                if _node_ready(node)
            ]
        )
        requested_rows: list[ResourceTotals] = []
        pending_jobs = 0
        unschedulable_jobs = 0
        image_pull_jobs = 0
        reasons: dict[str, int] = {}
        by_node: dict[str, list[Any]] = {name: [] for name in node_names}
        pending_pods: list[ManagedPodPlacement] = []
        for pod in pods:
            phase = getattr(pod.status, "phase", None)
            if phase in {"Succeeded", "Failed"}:
                continue
            node_name = getattr(pod.spec, "node_name", None)
            target = _target_pod(pod, namespace=namespace, target_id=target_id)
            if target and node_name is not None and node_name not in node_names:
                raise KubernetesObservationError(
                    "managed target Pod is scheduled outside the selected node group"
                )
            if node_name in node_names or (target and node_name is None and phase == "Pending"):
                requested_rows.append(_pod_request(pod))
            if node_name in node_names:
                by_node[node_name].append(pod)
            elif target and node_name is None and phase == "Pending":
                pending_pods.append(_managed_placement(pod))
            if not target:
                continue
            pending, unschedulable, image_pull, reason = _pending_state(pod)
            pending_jobs += int(pending)
            unschedulable_jobs += int(unschedulable)
            image_pull_jobs += int(image_pull)
            if reason is not None:
                reasons[reason] = reasons.get(reason, 0) + 1
        placements: list[NodePlacement] = []
        samples: list[NodeTemplateSample] = []
        for node in nodes:
            assigned = by_node[node.metadata.name]
            placement = NodePlacement(
                uid=_identity(getattr(node.metadata, "uid", None), name="Node UID"),
                provider_id=_identity(
                    getattr(node.spec, "provider_id", None), name="Node provider ID"
                ),
                ready=_node_ready(node),
                unschedulable=bool(getattr(node.spec, "unschedulable", False)),
                deleting=getattr(node.metadata, "deletion_timestamp", None) is not None,
                allocatable=_required_node_resources(node.status.allocatable, name="allocatable"),
                requested=_add(*[_pod_request(pod) for pod in assigned]),
                pod_slots=_positive_int(node.status.allocatable.get("pods"), name="node Pod slots"),
                used_pod_slots=len(assigned),
                managed_pods=[
                    _managed_placement(pod)
                    for pod in assigned
                    if _target_pod(pod, namespace=namespace, target_id=target_id)
                ],
            )
            placements.append(placement)
            sample = _template_sample(node, placement, assigned, daemons)
            if sample is not None:
                samples.append(sample)
        return KubernetesCapacitySnapshot(
            source_versions={
                "nodes": str(node_version),
                "pods": str(pod_version),
                "daemonsets": str(daemon_version),
            },
            active_nodes=len(nodes),
            ready_nodes=sum(_node_ready(node) for node in nodes),
            provisioned=provisioned,
            allocatable=allocatable,
            requested=_add(*requested_rows),
            pending_jobs=pending_jobs,
            unschedulable_jobs=unschedulable_jobs,
            image_pull_backoff_jobs=image_pull_jobs,
            pending_reasons=dict(sorted(reasons.items())),
            nodes=sorted(placements, key=lambda row: row.uid),
            pending_pods=sorted(pending_pods, key=lambda row: row.uid),
            daemonsets=sorted(
                [
                    DaemonSetPlacement(
                        uid=_identity(getattr(daemon.metadata, "uid", None), name="DaemonSet UID"),
                        generation=_positive_int(
                            daemon.metadata.generation, name="DaemonSet generation"
                        ),
                        requests=_pod_request(SimpleNamespace(spec=daemon.spec.template.spec)),
                        scheduling=_scheduling(daemon.spec.template.spec),
                    )
                    for daemon in daemons
                ],
                key=lambda row: row.uid,
            ),
            template_samples=sorted(samples, key=lambda row: row.node_uid),
            node_templates={
                node.metadata.uid: {
                    "labels": dict(getattr(node.metadata, "labels", None) or {}),
                    "taints": _plain(getattr(node.spec, "taints", None) or []),
                    "capacity": _required_node_resources(
                        node.status.capacity, name="capacity"
                    ).model_dump(),
                    "os_image": str(
                        getattr(getattr(node.status, "node_info", None), "os_image", None) or ""
                    ),
                }
                for node in nodes
            },
        )

    async def capture(
        self,
        *,
        namespace: str,
        target_id: str,
        node_label_selector: str,
    ) -> KubernetesCapacitySnapshot:
        return await asyncio.to_thread(
            self._capture_sync,
            namespace=namespace,
            target_id=target_id,
            node_label_selector=node_label_selector,
        )


__all__ = ["InClusterKubernetesCapacityReader", "KubernetesObservationError"]
