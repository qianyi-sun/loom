"""Restart-safe acceptance prerequisite choreography through strict protocols."""

from __future__ import annotations

from typing import Literal, Protocol
from uuid import UUID

from pydantic import Field

from loom.pipeline.acceptance import (
    AcceptanceEvictionDispatcherV1,
    AcceptanceEvictionResultV1,
    AcceptanceWorkerFenceRepositoryV1,
    AcceptanceWorkerFenceV1,
    ReleaseReason,
)
from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.spec import Digest, PipelineModel, PositiveSafeInt


class AcceptancePreflightPlanV1(PipelineModel):
    prerequisite_pipeline_run_id: UUID
    authorization_id: UUID
    candidate_sha256: Digest
    worker_id: UUID
    worker_capability_snapshot_digest: Digest
    worker_lease_epoch: PositiveSafeInt
    policy_id: str
    policy_config_sha256: Digest
    policy_activation_epoch: PositiveSafeInt
    slurm_cluster_id: str
    slurm_cluster_config_sha256: Digest
    slurm_allocation_id: str
    ordered_manifest_sha256s: tuple[Digest, Digest, Digest, Digest, Digest]


class AcceptancePreflightSnapshotV1(PipelineModel):
    pipeline_run_id: UUID
    state: Literal["pending", "satisfied", "consumed"]
    fence_state: Literal["pending", "active", "released"]
    exclusive_fence_id: UUID | None
    worker_lease_epoch: int | None
    eviction_result_sha256: Digest | None
    fence_release_reason: ReleaseReason | None
    version: int = Field(ge=0)


class AcceptancePreflightStateRepositoryV1(Protocol):
    async def load(self, pipeline_run_id: UUID) -> AcceptancePreflightSnapshotV1: ...

    async def record_active(
        self,
        *,
        expected_version: int,
        plan: AcceptancePreflightPlanV1,
        fence: AcceptanceWorkerFenceV1,
    ) -> AcceptancePreflightSnapshotV1: ...

    async def record_satisfied(
        self,
        *,
        expected_version: int,
        pipeline_run_id: UUID,
        eviction_result: AcceptanceEvictionResultV1,
        eviction_result_bytes: bytes,
        eviction_result_sha256: str,
    ) -> AcceptancePreflightSnapshotV1: ...

    async def record_released(
        self,
        *,
        expected_version: int,
        pipeline_run_id: UUID,
        reason: ReleaseReason,
    ) -> AcceptancePreflightSnapshotV1: ...


class AcceptancePreflightCoordinatorV1:
    """Each external call is separated from the following durable CAS."""

    def __init__(
        self,
        *,
        states: AcceptancePreflightStateRepositoryV1,
        fences: AcceptanceWorkerFenceRepositoryV1,
        eviction: AcceptanceEvictionDispatcherV1,
    ) -> None:
        self._states = states
        self._fences = fences
        self._eviction = eviction

    async def reconcile(self, plan: AcceptancePreflightPlanV1) -> AcceptancePreflightSnapshotV1:
        snapshot = await self._states.load(plan.prerequisite_pipeline_run_id)
        if snapshot.fence_state == "pending":
            fence = await self._fences.acquire(
                prerequisite_pipeline_run_id=plan.prerequisite_pipeline_run_id,
                authorization_id=plan.authorization_id,
                candidate_sha256=plan.candidate_sha256,
                worker_id=plan.worker_id,
                worker_capability_snapshot_digest=plan.worker_capability_snapshot_digest,
                policy_id=plan.policy_id,
                policy_config_sha256=plan.policy_config_sha256,
                policy_activation_epoch=plan.policy_activation_epoch,
            )
            self._validate_fence(plan, fence)
            snapshot = await self._states.record_active(
                expected_version=snapshot.version,
                plan=plan,
                fence=fence,
            )
        if snapshot.state == "pending" and snapshot.fence_state == "active":
            assert snapshot.exclusive_fence_id is not None
            assert snapshot.worker_lease_epoch is not None
            fence = await self._fences.revalidate(
                prerequisite_pipeline_run_id=plan.prerequisite_pipeline_run_id,
                exclusive_fence_id=snapshot.exclusive_fence_id,
                worker_lease_epoch=snapshot.worker_lease_epoch,
            )
            self._validate_fence(plan, fence)
            result = await self._eviction.evict_acceptance_entries(
                prerequisite_pipeline_run_id=plan.prerequisite_pipeline_run_id,
                authorization_id=plan.authorization_id,
                candidate_sha256=plan.candidate_sha256,
                exclusive_fence_id=fence.exclusive_fence_id,
                worker_id=plan.worker_id,
                worker_capability_snapshot_digest=plan.worker_capability_snapshot_digest,
                worker_lease_epoch=fence.worker_lease_epoch,
                policy_id=plan.policy_id,
                policy_config_sha256=plan.policy_config_sha256,
                policy_activation_epoch=plan.policy_activation_epoch,
                ordered_manifest_sha256s=plan.ordered_manifest_sha256s,
            )
            if (
                result.prerequisite_pipeline_run_id != plan.prerequisite_pipeline_run_id
                or result.exclusive_fence_id != fence.exclusive_fence_id
                or result.worker_id != plan.worker_id
                or result.worker_lease_epoch != fence.worker_lease_epoch
                or tuple(entry.manifest_sha256 for entry in result.entries)
                != plan.ordered_manifest_sha256s
            ):
                raise ValueError("acceptance eviction result drift")
            result_bytes = canonical_document(result)
            snapshot = await self._states.record_satisfied(
                expected_version=snapshot.version,
                pipeline_run_id=plan.prerequisite_pipeline_run_id,
                eviction_result=result,
                eviction_result_bytes=result_bytes,
                eviction_result_sha256=canonical_digest(result),
            )
        return snapshot

    async def release(
        self, *, pipeline_run_id: UUID, reason: ReleaseReason
    ) -> AcceptancePreflightSnapshotV1:
        snapshot = await self._states.load(pipeline_run_id)
        if snapshot.fence_state == "released":
            if snapshot.fence_release_reason != reason:
                raise ValueError("acceptance fence release replay reason drift")
            return snapshot
        if snapshot.exclusive_fence_id is None:
            raise ValueError("acceptance fence was never acquired")
        await self._fences.release(
            prerequisite_pipeline_run_id=pipeline_run_id,
            exclusive_fence_id=snapshot.exclusive_fence_id,
            reason=reason,
        )
        return await self._states.record_released(
            expected_version=snapshot.version,
            pipeline_run_id=pipeline_run_id,
            reason=reason,
        )

    @staticmethod
    def _validate_fence(plan: AcceptancePreflightPlanV1, fence: AcceptanceWorkerFenceV1) -> None:
        if (
            fence.state != "active"
            or fence.prerequisite_pipeline_run_id != plan.prerequisite_pipeline_run_id
            or fence.authorization_id != plan.authorization_id
            or fence.candidate_sha256 != plan.candidate_sha256
            or fence.worker_id != plan.worker_id
            or fence.worker_capability_snapshot_digest != plan.worker_capability_snapshot_digest
            or fence.policy_id != plan.policy_id
            or fence.policy_config_sha256 != plan.policy_config_sha256
            or fence.policy_activation_epoch != plan.policy_activation_epoch
        ):
            raise ValueError("acceptance worker fence drift")
