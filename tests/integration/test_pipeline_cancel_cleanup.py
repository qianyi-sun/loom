from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from loom.pipeline.execution_cancellation import ExecutionCancellationAck
from loom.pipeline.work_protocol import WorkerCleanupProofV1


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


def test_live_pipeline_cancel_ack_binds_observation_and_positive_cleanup() -> None:
    requested = datetime(2026, 8, 12, 12, tzinfo=UTC)
    ack = ExecutionCancellationAck(
        work_kind="pipeline",
        execution_id=UUID(int=1),
        requested_at=requested,
        observed_at=requested,
        outcome="forced",
        resources=_proof(),
        version=1,
    )
    assert ack.resource_digest.startswith("sha256:")
    assert ack.outcome == "forced"
