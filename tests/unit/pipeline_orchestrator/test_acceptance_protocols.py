from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom.pipeline.acceptance import (
    AcceptanceEvictionEntryV1,
    AcceptanceEvictionResultV1,
    AcceptanceWorkerFenceV1,
)
from loom.pipeline.keys import canonical_digest
from loom_pipeline_orchestrator.acceptance_preflight import (
    AcceptancePreflightCoordinatorV1,
    AcceptancePreflightPlanV1,
    AcceptancePreflightSnapshotV1,
)


def _digest(char: str) -> str:
    return "sha256:" + char * 64


def test_eviction_result_requires_five_sorted_absence_proofs() -> None:
    result = AcceptanceEvictionResultV1(
        schema_version="loom.acceptance-eviction-result.v1",
        prerequisite_pipeline_run_id=uuid4(),
        exclusive_fence_id=uuid4(),
        worker_id=uuid4(),
        worker_lease_epoch=1,
        entries=[
            AcceptanceEvictionEntryV1(manifest_sha256=_digest(char), absence_verified=True)
            for char in "12345"
        ],
    )
    assert len(result.entries) == 5

    with pytest.raises(ValidationError, match="bytewise-sorted"):
        AcceptanceEvictionResultV1.model_validate(
            {
                **result.model_dump(mode="python"),
                "entries": list(reversed(result.model_dump(mode="python")["entries"])),
            }
        )


def test_worker_fence_release_fields_are_exact() -> None:
    base = {
        "prerequisite_pipeline_run_id": uuid4(),
        "authorization_id": uuid4(),
        "candidate_sha256": _digest("a"),
        "exclusive_fence_id": uuid4(),
        "worker_id": uuid4(),
        "worker_capability_snapshot_digest": _digest("b"),
        "worker_lease_epoch": 1,
        "policy_id": "behavior-gpu-oldlab",
        "policy_config_sha256": _digest("c"),
        "policy_activation_epoch": 1,
        "state": "active",
        "acquired_at": datetime.now(UTC),
        "released_at": None,
        "release_reason": None,
        "version": 1,
    }
    assert AcceptanceWorkerFenceV1.model_validate(base).state == "active"
    with pytest.raises(ValidationError, match="release fields"):
        AcceptanceWorkerFenceV1.model_validate(
            {**base, "state": "released", "released_at": datetime.now(UTC)}
        )


class StateFake:
    def __init__(self, pipeline_run_id: Any) -> None:
        self.snapshot = AcceptancePreflightSnapshotV1(
            pipeline_run_id=pipeline_run_id,
            state="pending",
            fence_state="pending",
            exclusive_fence_id=None,
            worker_lease_epoch=None,
            eviction_result_sha256=None,
            fence_release_reason=None,
            version=0,
        )

    async def load(self, _pipeline_run_id: Any) -> AcceptancePreflightSnapshotV1:
        return self.snapshot

    async def record_active(self, *, expected_version: int, plan: Any, fence: Any) -> AcceptancePreflightSnapshotV1:
        assert expected_version == self.snapshot.version
        self.snapshot = AcceptancePreflightSnapshotV1(
            **{
                **self.snapshot.model_dump(mode="python"),
                "fence_state": "active",
                "exclusive_fence_id": fence.exclusive_fence_id,
                "worker_lease_epoch": fence.worker_lease_epoch,
                "version": expected_version + 1,
            }
        )
        return self.snapshot

    async def record_satisfied(
        self, *, expected_version: int, eviction_result_sha256: str, **_kwargs: Any
    ) -> AcceptancePreflightSnapshotV1:
        assert expected_version == self.snapshot.version
        self.snapshot = AcceptancePreflightSnapshotV1(
            **{
                **self.snapshot.model_dump(mode="python"),
                "state": "satisfied",
                "eviction_result_sha256": eviction_result_sha256,
                "version": expected_version + 1,
            }
        )
        return self.snapshot

    async def record_released(
        self, *, expected_version: int, reason: str, **_kwargs: Any
    ) -> AcceptancePreflightSnapshotV1:
        assert expected_version == self.snapshot.version
        self.snapshot = AcceptancePreflightSnapshotV1(
            **{
                **self.snapshot.model_dump(mode="python"),
                "fence_state": "released",
                "fence_release_reason": reason,
                "version": expected_version + 1,
            }
        )
        return self.snapshot


class FenceFake:
    def __init__(self, fence: AcceptanceWorkerFenceV1) -> None:
        self.fence = fence
        self.acquire_calls = 0
        self.release_calls = 0

    async def acquire(self, **_kwargs: Any) -> AcceptanceWorkerFenceV1:
        self.acquire_calls += 1
        return self.fence

    async def revalidate(self, **_kwargs: Any) -> AcceptanceWorkerFenceV1:
        return self.fence

    async def release(self, **_kwargs: Any) -> None:
        self.release_calls += 1


class EvictionFake:
    def __init__(self, result: AcceptanceEvictionResultV1) -> None:
        self.result = result
        self.calls = 0

    async def evict_acceptance_entries(self, **_kwargs: Any) -> AcceptanceEvictionResultV1:
        self.calls += 1
        return self.result


@pytest.mark.asyncio
async def test_acceptance_preflight_replay_and_release_are_exactly_once() -> None:
    run_id, authorization_id, worker_id, fence_id = (uuid4() for _ in range(4))
    manifests = tuple(_digest(char) for char in "12345")
    plan = AcceptancePreflightPlanV1(
        prerequisite_pipeline_run_id=run_id,
        authorization_id=authorization_id,
        candidate_sha256=_digest("a"),
        worker_id=worker_id,
        worker_capability_snapshot_digest=_digest("b"),
        worker_lease_epoch=3,
        policy_id="behavior-gpu-oldlab",
        policy_config_sha256=_digest("c"),
        policy_activation_epoch=2,
        slurm_cluster_id="oldlab",
        slurm_cluster_config_sha256=_digest("d"),
        slurm_allocation_id="job-7",
        ordered_manifest_sha256s=manifests,
    )
    fence = AcceptanceWorkerFenceV1(
        prerequisite_pipeline_run_id=run_id,
        authorization_id=authorization_id,
        candidate_sha256=plan.candidate_sha256,
        exclusive_fence_id=fence_id,
        worker_id=worker_id,
        worker_capability_snapshot_digest=plan.worker_capability_snapshot_digest,
        worker_lease_epoch=3,
        policy_id=plan.policy_id,
        policy_config_sha256=plan.policy_config_sha256,
        policy_activation_epoch=plan.policy_activation_epoch,
        state="active",
        acquired_at=datetime.now(UTC),
        released_at=None,
        release_reason=None,
        version=1,
    )
    result = AcceptanceEvictionResultV1(
        schema_version="loom.acceptance-eviction-result.v1",
        prerequisite_pipeline_run_id=run_id,
        exclusive_fence_id=fence_id,
        worker_id=worker_id,
        worker_lease_epoch=3,
        entries=[
            AcceptanceEvictionEntryV1(manifest_sha256=digest, absence_verified=True)
            for digest in manifests
        ],
    )
    states = StateFake(run_id)
    fences = FenceFake(fence)
    eviction = EvictionFake(result)
    coordinator = AcceptancePreflightCoordinatorV1(
        states=states,  # type: ignore[arg-type]
        fences=fences,
        eviction=eviction,
    )
    satisfied = await coordinator.reconcile(plan)
    replay = await coordinator.reconcile(plan)
    assert satisfied == replay
    assert satisfied.eviction_result_sha256 == canonical_digest(result)
    assert (fences.acquire_calls, eviction.calls) == (1, 1)
    released = await coordinator.release(
        pipeline_run_id=run_id,
        reason="warm_cleanup_complete",
    )
    replay_release = await coordinator.release(
        pipeline_run_id=run_id,
        reason="warm_cleanup_complete",
    )
    assert released == replay_release
    assert fences.release_calls == 1


def test_acceptance_boundary_does_not_import_worker_claim_or_cache_implementation() -> None:
    source = Path(
        "src/loom_pipeline_orchestrator/acceptance_preflight.py"
    ).read_text(encoding="utf-8")
    assert "loom_worker" not in source
    assert "materializer" not in source
    assert "object_store" not in source
