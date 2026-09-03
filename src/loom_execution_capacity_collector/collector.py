"""One complete fail-closed provider/cluster observation transaction."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from loom.pipeline.keys import canonical_digest
from loom_execution_capacity_collector.config import ExecutionCapacityCollectorSettings
from loom_execution_capacity_collector.contracts import (
    CapacityObservationReceipt,
    CapacityObservationV1,
)
from loom_execution_capacity_collector.control_plane import CapacityControlPlaneClient
from loom_execution_capacity_collector.kubernetes import InClusterKubernetesCapacityReader
from loom_execution_capacity_collector.nebius import NebiusCapacityReader


class CapacityCollectionError(RuntimeError):
    """Complete source snapshots disagree on their shared target identity."""


async def collect_capacity_observation(
    settings: ExecutionCapacityCollectorSettings,
    *,
    control_plane: Any | None = None,
    provider: Any | None = None,
    kubernetes: Any | None = None,
    now: datetime | None = None,
) -> CapacityObservationReceipt:
    """Capture all sources before publishing one immutable observation."""

    owns_control_plane = control_plane is None
    owns_provider = provider is None
    control_plane = control_plane or CapacityControlPlaneClient(
        origin=settings.control_plane_url,
        bearer_token_file=settings.control_plane_bearer_token_file,
        timeout_seconds=settings.request_timeout_seconds,
        attempts=settings.request_attempts,
    )
    provider = provider or NebiusCapacityReader(settings)
    kubernetes = kubernetes or InClusterKubernetesCapacityReader(
        request_timeout_seconds=settings.request_timeout_seconds
    )
    try:
        policy = await control_plane.fetch_policy(
            target_id=settings.target_id,
            pool_id=settings.pool_id,
        )
        provider_snapshot, kubernetes_snapshot = await asyncio.gather(
            provider.capture(policy),
            kubernetes.capture(
                namespace=settings.namespace,
                target_id=settings.target_id,
                node_label_selector=settings.node_label_selector,
            ),
        )
        # Nebius quota usage can lag node-group inventory while autoscaling. Use
        # the complete node-group snapshot as a conservative floor so the
        # observation never overstates headroom during that convergence window.
        provider_used_nodes = max(provider_snapshot.used_nodes, provider_snapshot.node_count)
        provider_used_vcpu_millis = max(
            provider_snapshot.used_vcpu_millis,
            provider_snapshot.node_count * policy.node_cpu_millis,
        )
        provider_used_memory_mib = max(
            provider_snapshot.used_memory_mib,
            provider_snapshot.node_count * policy.node_memory_mib,
        )
        provider_used_storage_mib = max(
            provider_snapshot.used_storage_mib,
            provider_snapshot.node_count * policy.node_storage_mib,
        )
        if provider_snapshot.autoscaler_state == "ready" and (
            kubernetes_snapshot.active_nodes != provider_snapshot.node_count
            or kubernetes_snapshot.ready_nodes != provider_snapshot.ready_node_count
        ):
            raise CapacityCollectionError(
                "ready provider and Kubernetes node inventories do not agree"
            )
        observed_at = (now or datetime.now(UTC)).astimezone(UTC)
        active_nodes = max(
            kubernetes_snapshot.active_nodes,
            provider_snapshot.node_count,
            provider_snapshot.target_node_count,
        )
        missing_nodes = max(0, active_nodes - kubernetes_snapshot.active_nodes)
        provisioned_cpu = (
            kubernetes_snapshot.provisioned.cpu_millis + missing_nodes * policy.node_cpu_millis
        )
        provisioned_memory = (
            kubernetes_snapshot.provisioned.memory_mib + missing_nodes * policy.node_memory_mib
        )
        provisioned_storage = (
            kubernetes_snapshot.provisioned.storage_mib + missing_nodes * policy.node_storage_mib
        )
        identity = {
            "schema_version": "loom.execution-capacity-collector-source.v1",
            "target_id": settings.target_id,
            "policy_version": policy.version,
            "provider": provider_snapshot.source_versions,
            "kubernetes": kubernetes_snapshot.source_versions,
            "observed_at": observed_at.isoformat(),
        }
        observation = CapacityObservationV1(
            target_id=settings.target_id,
            source=settings.source,
            source_version=canonical_digest(identity),
            observed_at=observed_at,
            provider_capacity_state=provider_snapshot.provider_capacity_state,
            provider_capacity_reason=provider_snapshot.provider_capacity_reason,
            autoscaler_state=provider_snapshot.autoscaler_state,
            autoscaler_reason=provider_snapshot.autoscaler_reason,
            provider_quota_nodes=provider_snapshot.quota_nodes,
            provider_quota_vcpu_millis=provider_snapshot.quota_vcpu_millis,
            provider_quota_memory_mib=provider_snapshot.quota_memory_mib,
            provider_quota_storage_mib=provider_snapshot.quota_storage_mib,
            provider_used_nodes=provider_used_nodes,
            provider_used_vcpu_millis=provider_used_vcpu_millis,
            provider_used_memory_mib=provider_used_memory_mib,
            provider_used_storage_mib=provider_used_storage_mib,
            active_nodes=active_nodes,
            provisioned_vcpu_millis=provisioned_cpu,
            provisioned_memory_mib=provisioned_memory,
            provisioned_storage_mib=provisioned_storage,
            allocatable_cpu_millis=kubernetes_snapshot.allocatable.cpu_millis,
            allocatable_memory_mib=kubernetes_snapshot.allocatable.memory_mib,
            allocatable_storage_mib=kubernetes_snapshot.allocatable.storage_mib,
            requested_cpu_millis=kubernetes_snapshot.requested.cpu_millis,
            requested_memory_mib=kubernetes_snapshot.requested.memory_mib,
            requested_storage_mib=kubernetes_snapshot.requested.storage_mib,
            pending_jobs=kubernetes_snapshot.pending_jobs,
            unschedulable_jobs=kubernetes_snapshot.unschedulable_jobs,
            image_pull_backoff_jobs=kubernetes_snapshot.image_pull_backoff_jobs,
            pending_reasons=kubernetes_snapshot.pending_reasons,
        )
        return await control_plane.publish(observation)
    finally:
        if owns_provider:
            await provider.close()
        if owns_control_plane:
            await control_plane.close()


__all__ = ["CapacityCollectionError", "collect_capacity_observation"]
