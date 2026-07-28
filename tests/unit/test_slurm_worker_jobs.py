from __future__ import annotations

import hashlib
from uuid import uuid4

from loom_control_plane.slurm_worker_jobs import (
    SlurmWorkerJobObservation,
    _normalize_slurm_state,
    is_explicit_live_slurm_state,
    redact_env,
    summarize_jobs,
)


def test_redact_env_removes_secret_like_values() -> None:
    redacted = redact_env(
        {
            "LOOM_WORKER_TOKEN": "loom_w_secret",
            "LOOM_WORKER_MINIO_SECRET_KEY": "minio-secret",
            "LOOM_WORKER_MAX_CONCURRENT": "12",
            "PATH": "/usr/bin",
        }
    )

    assert redacted == {
        "LOOM_WORKER_TOKEN": "<redacted>",
        "LOOM_WORKER_AUTH_FINGERPRINT": (
            f"sha256:{hashlib.sha256(b'loom_w_secret').hexdigest()[:12]} len=13"
        ),
        "LOOM_WORKER_MINIO_SECRET_KEY": "<redacted>",
        "LOOM_WORKER_MAX_CONCURRENT": "12",
        "PATH": "/usr/bin",
    }


def test_normalize_slurm_state_keeps_raw_state_separate() -> None:
    assert _normalize_slurm_state("PENDING") == "pending"
    assert _normalize_slurm_state("CONFIGURING") == "pending"
    assert _normalize_slurm_state("RUNNING") == "running"
    assert _normalize_slurm_state("COMPLETED") == "completed"
    assert _normalize_slurm_state("CANCELLED by 123") == "cancelled"
    assert _normalize_slurm_state("TIMEOUT") == "failed"
    assert _normalize_slurm_state("OUT_OF_MEMORY") == "failed"
    assert _normalize_slurm_state("SUSPENDED") == "running"
    assert _normalize_slurm_state("FUTURE_STATE") == "running"
    assert is_explicit_live_slurm_state("RUNNING") is True
    assert is_explicit_live_slurm_state("SUSPENDED") is True
    assert is_explicit_live_slurm_state("FUTURE_STATE") is False


def test_observation_rejects_secret_environment() -> None:
    worker_id = uuid4()
    obs = SlurmWorkerJobObservation(
        job_id="123",
        slurm_state="RUNNING",
        nodelist="oldlab-1",
        pending_reason=None,
        worker_id=worker_id,
    )

    assert obs.worker_id == worker_id
    assert obs.slurm_state == "RUNNING"


def test_summarize_jobs_counts_capacity_without_secrets() -> None:
    rows = [
        {
            "environment": "production",
            "pool_name": "oldlab",
            "state": "pending",
            "requested_concurrency": 6,
            "job_id": "1",
            "worker_status": None,
        },
        {
            "environment": "production",
            "pool_name": "oldlab",
            "state": "running",
            "requested_concurrency": 12,
            "job_id": "2",
            "worker_status": "active",
        },
        {
            "environment": "production",
            "pool_name": "oldlab",
            "state": "completed",
            "requested_concurrency": 6,
            "job_id": "3",
            "worker_status": "idle-exit",
        },
        {
            "environment": "production",
            "pool_name": "oldlab",
            "state": "stale",
            "requested_concurrency": 6,
            "job_id": "6",
            "worker_status": None,
        },
        {
            "environment": "staging",
            "pool_name": "lux",
            "state": "failed",
            "requested_concurrency": 4,
            "job_id": None,
            "submission_error": "sbatch failed",
            "worker_status": None,
        },
        {
            "environment": "staging",
            "pool_name": "lux",
            "state": "cancelled",
            "requested_concurrency": 4,
            "job_id": "4",
            "started_at": None,
            "worker_status": None,
        },
        {
            "environment": "staging",
            "pool_name": "lux",
            "state": "cancelled",
            "requested_concurrency": 4,
            "job_id": "5",
            "started_at": "2026-06-24T00:00:00Z",
            "worker_status": None,
        },
    ]

    summary = summarize_jobs(rows)

    oldlab = summary.by_pool[("production", "oldlab")]
    assert oldlab.desired_slots == 18
    assert oldlab.pending_slots == 6
    assert oldlab.active_slots == 12
    assert oldlab.pending_jobs == 1
    assert oldlab.running_jobs == 1
    assert oldlab.failed_submissions == 0
    assert oldlab.idle_exits == 1
    assert oldlab.stale_slots == 6
    assert oldlab.stale_jobs == 1
    assert oldlab.as_dict()["stale_slots"] == 6
    assert oldlab.as_dict()["stale_jobs"] == 1

    lux = summary.by_pool[("staging", "lux")]
    assert lux.failed_submissions == 1
    assert lux.cancelled_pending_jobs == 1
