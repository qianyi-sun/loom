from loom_control_plane.metrics import (
    CLAIM_LATENCY_SEC,
    QUEUE_DEPTH,
    SLURM_WORKER_ACTIVE_SLOTS,
    SLURM_WORKER_CANCELLED_PENDING_JOBS,
    SLURM_WORKER_DESIRED_SLOTS,
    SLURM_WORKER_FAILED_SUBMISSIONS,
    SLURM_WORKER_IDLE_EXITS,
    SLURM_WORKER_PENDING_JOBS,
    SLURM_WORKER_PENDING_SLOTS,
    SLURM_WORKER_RUNNING_JOBS,
    SLURM_WORKER_STALE_JOBS,
    SLURM_WORKER_STALE_SLOTS,
    STATE_PATCH_TOTAL,
    TRIALS_INFLIGHT,
    TRIALS_STATE_TOTAL,
    WORKER_POOL_AUTOSCALER_DECISION,
    WORKER_POOL_AUTOSCALER_ERROR,
    WORKER_POOL_AUTOSCALER_IDLE_SECONDS,
    WORKER_POOL_DESIRED_SLOTS,
    WORKER_POOL_DRAINING_SLOTS,
    WORKER_POOL_DRAINING_WORKERS,
    WORKER_POOL_FREE_SLOTS,
    WORKER_POOL_OCCUPIED_SLOTS,
    WORKER_POOL_PENDING_SLOTS,
    WORKER_POOL_TOTAL_SLOTS,
    WORKER_POOL_WORKERS,
    WORKER_RECLAIM_TOTAL,
    WORKERS_ACTIVE,
)


def test_metrics_have_documented_labels():
    assert TRIALS_STATE_TOTAL._labelnames == ("from_state", "to_state", "team_id")
    assert TRIALS_INFLIGHT._labelnames == ("team_id", "state")
    assert STATE_PATCH_TOTAL._labelnames == ("endpoint", "result")
    assert CLAIM_LATENCY_SEC._labelnames == ("result",)


def test_metrics_exist_unlabeled():
    """QUEUE_DEPTH/WORKERS_ACTIVE/WORKER_RECLAIM_TOTAL exist + have the right cardinality."""
    assert QUEUE_DEPTH._labelnames == ("team_id",)
    assert WORKERS_ACTIVE._labelnames == ()
    assert WORKER_RECLAIM_TOTAL._labelnames == ()


def test_slurm_worker_capacity_metrics_are_bounded_by_pool():
    expected = ("environment", "pool_name")
    assert SLURM_WORKER_DESIRED_SLOTS._labelnames == expected
    assert SLURM_WORKER_ACTIVE_SLOTS._labelnames == expected
    assert SLURM_WORKER_PENDING_SLOTS._labelnames == expected
    assert SLURM_WORKER_STALE_SLOTS._labelnames == expected
    assert SLURM_WORKER_RUNNING_JOBS._labelnames == expected
    assert SLURM_WORKER_PENDING_JOBS._labelnames == expected
    assert SLURM_WORKER_STALE_JOBS._labelnames == expected
    assert SLURM_WORKER_FAILED_SUBMISSIONS._labelnames == expected
    assert SLURM_WORKER_CANCELLED_PENDING_JOBS._labelnames == expected
    assert SLURM_WORKER_IDLE_EXITS._labelnames == expected


def test_worker_pool_slot_metrics_are_bounded_by_pool_backend_and_arch():
    expected = ("pool_name", "backend", "cpu_arch")
    assert WORKER_POOL_TOTAL_SLOTS._labelnames == expected
    assert WORKER_POOL_OCCUPIED_SLOTS._labelnames == expected
    assert WORKER_POOL_FREE_SLOTS._labelnames == expected
    assert WORKER_POOL_WORKERS._labelnames == expected
    assert WORKER_POOL_DESIRED_SLOTS._labelnames == expected
    assert WORKER_POOL_PENDING_SLOTS._labelnames == expected
    assert WORKER_POOL_DRAINING_SLOTS._labelnames == expected
    assert WORKER_POOL_DRAINING_WORKERS._labelnames == expected


def test_worker_pool_autoscaler_metrics_have_bounded_labels():
    expected = ("pool_name", "backend", "cpu_arch", "action", "reason")
    assert WORKER_POOL_AUTOSCALER_DECISION._labelnames == expected
    assert WORKER_POOL_AUTOSCALER_ERROR._labelnames == (
        "pool_name",
        "backend",
        "cpu_arch",
    )
    assert WORKER_POOL_AUTOSCALER_IDLE_SECONDS._labelnames == (
        "pool_name",
        "backend",
        "cpu_arch",
    )
