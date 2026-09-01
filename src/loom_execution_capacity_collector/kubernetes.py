"""Read-only Kubernetes node and Pod resource accounting."""

from __future__ import annotations

import asyncio
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any

from loom_execution_capacity_collector.contracts import (
    KubernetesCapacitySnapshot,
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
        core_api: Any | None = None,
        request_timeout_seconds: float = 15.0,
    ) -> None:
        if not 1.0 <= request_timeout_seconds <= 60.0:
            raise ValueError("Kubernetes request timeout must be between 1 and 60 seconds")
        if core_api is None:
            try:
                from kubernetes import client, config
            except ModuleNotFoundError as exc:
                raise RuntimeError("install Loom with the cluster extra") from exc
            config.load_incluster_config()
            core_api = client.CoreV1Api()
        self._core = core_api
        self._request_timeout = request_timeout_seconds

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
        for pod in pods:
            phase = getattr(pod.status, "phase", None)
            if (
                phase in {"Succeeded", "Failed"}
                or getattr(pod.metadata, "deletion_timestamp", None) is not None
            ):
                continue
            node_name = getattr(pod.spec, "node_name", None)
            target = _target_pod(pod, namespace=namespace, target_id=target_id)
            if target and node_name is not None and node_name not in node_names:
                raise KubernetesObservationError(
                    "managed target Pod is scheduled outside the selected node group"
                )
            if node_name in node_names or (target and node_name is None and phase == "Pending"):
                requested_rows.append(_pod_request(pod))
            if not target:
                continue
            pending, unschedulable, image_pull, reason = _pending_state(pod)
            pending_jobs += int(pending)
            unschedulable_jobs += int(unschedulable)
            image_pull_jobs += int(image_pull)
            if reason is not None:
                reasons[reason] = reasons.get(reason, 0) + 1
        return KubernetesCapacitySnapshot(
            source_versions={"nodes": str(node_version), "pods": str(pod_version)},
            active_nodes=len(nodes),
            ready_nodes=sum(_node_ready(node) for node in nodes),
            provisioned=provisioned,
            allocatable=allocatable,
            requested=_add(*requested_rows),
            pending_jobs=pending_jobs,
            unschedulable_jobs=unschedulable_jobs,
            image_pull_backoff_jobs=image_pull_jobs,
            pending_reasons=dict(sorted(reasons.items())),
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
