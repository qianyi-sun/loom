from __future__ import annotations

from dataclasses import dataclass, field
from uuid import UUID

import pytest

from loom.pipeline.work_protocol import WorkerCleanupProofV1
from loom_worker.pipeline_cancellation import PipelineCancellationCoordinator


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


@dataclass
class _Backend:
    graceful: bool
    calls: list[str] = field(default_factory=list)

    async def term(self, *, attempt_id: UUID) -> None:
        del attempt_id
        self.calls.append("term")

    async def wait_empty(self, *, attempt_id: UUID, timeout_seconds: int) -> bool:
        del attempt_id
        self.calls.append(f"wait:{timeout_seconds}")
        return self.graceful if timeout_seconds else True

    async def kill(self, *, attempt_id: UUID) -> None:
        del attempt_id
        self.calls.append("kill")

    async def teardown(self, *, attempt_id: UUID) -> WorkerCleanupProofV1:
        del attempt_id
        self.calls.append("teardown")
        return _proof()


@pytest.mark.asyncio
async def test_graceful_ack_is_after_positive_teardown() -> None:
    backend = _Backend(graceful=True)
    result = await PipelineCancellationCoordinator(backend).observe_and_teardown(
        attempt_id=UUID(int=1)
    )
    assert result.outcome == "graceful"
    assert backend.calls == ["term", "wait:30", "teardown"]


@pytest.mark.asyncio
async def test_forced_ack_requires_post_kill_empty_check() -> None:
    backend = _Backend(graceful=False)
    result = await PipelineCancellationCoordinator(backend).observe_and_teardown(
        attempt_id=UUID(int=1)
    )
    assert result.outcome == "forced"
    assert backend.calls == ["term", "wait:30", "kill", "wait:0", "teardown"]


def test_cancellation_cadence_is_closed() -> None:
    with pytest.raises(ValueError, match="fixed"):
        PipelineCancellationCoordinator(_Backend(True), grace_seconds=29)
