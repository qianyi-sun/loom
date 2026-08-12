from __future__ import annotations

from uuid import uuid4

from loom_worker.metrics import PIPELINE_GPU_ALLOCATED_IDLE_SECONDS
from loom_worker.pipeline_gpu_lifecycle import PipelineGpuLifecycleTracker


def test_gpu_lifecycle_uses_process_presence_and_clears_disappeared_tuples() -> None:
    now = [10.0]
    tracker = PipelineGpuLifecycleTracker(clock=lambda: now[0])
    attempt = uuid4()

    tracker.mark(attempt, cluster="oldlab", reason="pre_start")
    now[0] = 17.0
    tracker.refresh()
    assert (
        PIPELINE_GPU_ALLOCATED_IDLE_SECONDS.labels(
            slurm_cluster="oldlab", reason="pre_start"
        )._value.get()
        == 7
    )

    tracker.mark(attempt, cluster="oldlab", reason="process_absent")
    now[0] = 20.0
    tracker.refresh()
    assert (
        PIPELINE_GPU_ALLOCATED_IDLE_SECONDS.labels(
            slurm_cluster="oldlab", reason="process_absent"
        )._value.get()
        == 3
    )

    tracker.process_present(attempt)
    assert not PIPELINE_GPU_ALLOCATED_IDLE_SECONDS._metrics


def test_cleanup_pending_age_is_worker_lifecycle_not_gpu_utilization() -> None:
    now = [100.0]
    tracker = PipelineGpuLifecycleTracker(clock=lambda: now[0])
    tracker.mark(uuid4(), cluster="gb10", reason="cleanup_pending")
    now[0] = 106.5
    tracker.refresh()
    assert (
        PIPELINE_GPU_ALLOCATED_IDLE_SECONDS.labels(
            slurm_cluster="gb10", reason="cleanup_pending"
        )._value.get()
        == 6.5
    )
