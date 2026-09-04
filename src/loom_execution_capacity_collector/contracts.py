"""Strict contracts for the Nebius/Kubernetes capacity collector."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ResourceTotals(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    cpu_millis: int = Field(ge=0)
    memory_mib: int = Field(ge=0)
    storage_mib: int = Field(ge=0)


class NodeStateCounts(BaseModel):
    """Provider/Kubernetes node lifecycle counts safe for operator projection."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    desired: int = Field(ge=0)
    creating: int = Field(ge=0)
    ready: int = Field(ge=0)
    failed: int = Field(ge=0)
    deleting: int = Field(ge=0)


class CapacityPolicyBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    target_id: str = Field(min_length=1, max_length=120)
    pool_id: str = Field(min_length=1, max_length=120)
    enabled: bool
    max_nodes: int = Field(gt=0)
    node_cpu_millis: int = Field(gt=0)
    node_memory_mib: int = Field(gt=0)
    node_storage_mib: int = Field(gt=0)
    version: int = Field(gt=0)


class ProviderCapacitySnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source_versions: dict[str, str]
    provider_capacity_state: Literal["available", "insufficient", "unknown"]
    provider_capacity_reason: str | None = Field(default=None, max_length=500)
    autoscaler_state: Literal["ready", "scaling", "stalled", "unknown"]
    autoscaler_reason: str | None = Field(default=None, max_length=500)
    quota_nodes: int = Field(gt=0)
    quota_vcpu_millis: int = Field(gt=0)
    quota_memory_mib: int = Field(gt=0)
    quota_storage_mib: int = Field(gt=0)
    used_nodes: int = Field(ge=0)
    used_vcpu_millis: int = Field(ge=0)
    used_memory_mib: int = Field(ge=0)
    used_storage_mib: int = Field(ge=0)
    node_count: int = Field(ge=0)
    target_node_count: int = Field(ge=0)
    ready_node_count: int = Field(ge=0)


class KubernetesCapacitySnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source_versions: dict[str, str]
    active_nodes: int = Field(ge=0)
    ready_nodes: int = Field(ge=0)
    provisioned: ResourceTotals
    allocatable: ResourceTotals
    requested: ResourceTotals
    pending_jobs: int = Field(ge=0)
    unschedulable_jobs: int = Field(ge=0)
    image_pull_backoff_jobs: int = Field(ge=0)
    pending_reasons: dict[str, int]


class CapacityObservationV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    target_id: str = Field(min_length=1, max_length=120)
    source: str = Field(min_length=1, max_length=120)
    source_version: str = Field(min_length=1, max_length=160)
    observed_at: datetime
    provider_capacity_state: Literal["available", "insufficient", "unknown"]
    provider_capacity_reason: str | None = Field(default=None, max_length=500)
    autoscaler_state: Literal["ready", "scaling", "stalled", "unknown"]
    autoscaler_reason: str | None = Field(default=None, max_length=500)
    provider_quota_nodes: int = Field(gt=0)
    provider_quota_vcpu_millis: int = Field(gt=0)
    provider_quota_memory_mib: int = Field(gt=0)
    provider_quota_storage_mib: int = Field(gt=0)
    provider_used_nodes: int = Field(ge=0)
    provider_used_vcpu_millis: int = Field(ge=0)
    provider_used_memory_mib: int = Field(ge=0)
    provider_used_storage_mib: int = Field(ge=0)
    active_nodes: int = Field(ge=0)
    node_states: NodeStateCounts
    provisioned_vcpu_millis: int = Field(ge=0)
    provisioned_memory_mib: int = Field(ge=0)
    provisioned_storage_mib: int = Field(ge=0)
    allocatable_cpu_millis: int = Field(ge=0)
    allocatable_memory_mib: int = Field(ge=0)
    allocatable_storage_mib: int = Field(ge=0)
    requested_cpu_millis: int = Field(ge=0)
    requested_memory_mib: int = Field(ge=0)
    requested_storage_mib: int = Field(ge=0)
    pending_jobs: int = Field(ge=0)
    unschedulable_jobs: int = Field(ge=0)
    image_pull_backoff_jobs: int = Field(ge=0)
    pending_reasons: dict[str, int]


class CapacityObservationReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    id: str
    created: bool
    target_id: str
    source: str
    source_version: str
    observed_at: datetime
    provider_capacity_state: Literal["available", "insufficient", "unknown"]
    autoscaler_state: Literal["ready", "scaling", "stalled", "unknown"]
    observation_sha256: str


__all__ = [
    "CapacityObservationReceipt",
    "CapacityObservationV1",
    "CapacityPolicyBinding",
    "KubernetesCapacitySnapshot",
    "NodeStateCounts",
    "ProviderCapacitySnapshot",
    "ResourceTotals",
]
