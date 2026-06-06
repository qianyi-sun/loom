from loom_control_plane.metrics import (
    CLAIM_LATENCY_SEC,
    QUEUE_DEPTH,
    STATE_PATCH_TOTAL,
    TRIALS_INFLIGHT,
    TRIALS_STATE_TOTAL,
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
