"""Strict injected acceptance-preflight boundaries owned by #1212."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

from pydantic import Field, model_validator

from loom.pipeline.spec import Digest, PipelineModel, PositiveSafeInt


class AcceptanceEvictionEntryV1(PipelineModel):
    manifest_sha256: Digest
    absence_verified: Literal[True]


class AcceptanceEvictionResultV1(PipelineModel):
    schema_version: Literal["loom.acceptance-eviction-result.v1"]
    prerequisite_pipeline_run_id: UUID
    exclusive_fence_id: UUID
    worker_id: UUID
    worker_lease_epoch: PositiveSafeInt
    entries: list[AcceptanceEvictionEntryV1] = Field(min_length=5, max_length=5)

    @model_validator(mode="after")
    def entries_are_exact(self) -> AcceptanceEvictionResultV1:
        digests = [entry.manifest_sha256 for entry in self.entries]
        if digests != sorted(digests, key=lambda value: value.encode()) or len(set(digests)) != 5:
            raise ValueError("eviction entries must be five bytewise-sorted unique manifests")
        return self


ReleaseReason = Literal[
    "warm_cleanup_complete",
    "cold_cleanup_complete",
    "eviction_failed_cleanup_complete",
    "campaign_cancel_cleanup_complete",
    "campaign_failed_cleanup_complete",
]


class AcceptanceWorkerFenceV1(PipelineModel):
    prerequisite_pipeline_run_id: UUID
    authorization_id: UUID
    candidate_sha256: Digest
    exclusive_fence_id: UUID
    worker_id: UUID
    worker_capability_snapshot_digest: Digest
    worker_lease_epoch: PositiveSafeInt
    policy_id: str
    policy_config_sha256: Digest
    policy_activation_epoch: PositiveSafeInt
    state: Literal["active", "released"]
    acquired_at: datetime
    released_at: datetime | None
    release_reason: ReleaseReason | None
    version: PositiveSafeInt

    @model_validator(mode="after")
    def release_fields_match_state(self) -> AcceptanceWorkerFenceV1:
        released = self.released_at is not None and self.release_reason is not None
        if (self.state == "released") != released:
            raise ValueError("release fields must be null exactly while the fence is active")
        return self


class AcceptanceEvictionDispatcherV1(Protocol):
    async def evict_acceptance_entries(
        self,
        *,
        prerequisite_pipeline_run_id: UUID,
        authorization_id: UUID,
        candidate_sha256: str,
        exclusive_fence_id: UUID,
        worker_id: UUID,
        worker_capability_snapshot_digest: str,
        worker_lease_epoch: int,
        policy_id: str,
        policy_config_sha256: str,
        policy_activation_epoch: int,
        ordered_manifest_sha256s: tuple[str, str, str, str, str],
    ) -> AcceptanceEvictionResultV1: ...


class AcceptanceWorkerFenceRepositoryV1(Protocol):
    async def acquire(
        self,
        *,
        prerequisite_pipeline_run_id: UUID,
        authorization_id: UUID,
        candidate_sha256: str,
        worker_id: UUID,
        worker_capability_snapshot_digest: str,
        policy_id: str,
        policy_config_sha256: str,
        policy_activation_epoch: int,
    ) -> AcceptanceWorkerFenceV1: ...

    async def revalidate(
        self,
        *,
        prerequisite_pipeline_run_id: UUID,
        exclusive_fence_id: UUID,
        worker_lease_epoch: int,
    ) -> AcceptanceWorkerFenceV1: ...

    async def release(
        self,
        *,
        prerequisite_pipeline_run_id: UUID,
        exclusive_fence_id: UUID,
        reason: ReleaseReason,
    ) -> None: ...
