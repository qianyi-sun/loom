from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from loom.pipeline.execution_cancellation import ExecutionCancellationAck
from loom.pipeline.work_protocol import WorkerCleanupProofV1, WorkerLostCleanupAckV1


def _proof() -> WorkerCleanupProofV1:
    return WorkerCleanupProofV1(
        container_absent=True,
        cgroup_empty=True,
        network_absent=True,
        step_jwt_revoked=True,
        runtime_secret_mount_absent=True,
        scratch_absent=True,
        outputs_absent=True,
        input_views_absent=True,
        active_upload_session_ids=[],
    )


def test_worker_lost_cleanup_requires_positive_all_absent_proof() -> None:
    payload = WorkerLostCleanupAckV1(
        schema_version="loom.worker-lost-cleanup-ack.v1",
        observer_kind="worker_journal",
        observed_at=datetime(2026, 8, 12, tzinfo=UTC),
        allocation_id=None,
        allocation_terminal=None,
        resources=_proof(),
    )
    assert payload.resources.active_upload_session_ids == []


def test_cleanup_observation_cannot_precede_the_request() -> None:
    observed = datetime(2026, 8, 12, tzinfo=UTC)
    with pytest.raises(ValueError, match="precedes"):
        ExecutionCancellationAck(
            work_kind="pipeline",
            execution_id=UUID(int=1),
            requested_at=observed + timedelta(seconds=1),
            observed_at=observed,
            outcome="worker_lost_cleanup",
            resources=_proof(),
            version=1,
        )
