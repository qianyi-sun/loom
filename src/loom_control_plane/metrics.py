"""Prometheus metric definitions for Control Plane (spec §7.3).

Cardinality is bounded by design — `team_id` is the only high-cardinality
label and is only attached where strictly necessary.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

TRIALS_STATE_TOTAL = Counter(
    "loom_trials_state_total",
    "Trial state transitions",
    labelnames=("from_state", "to_state", "team_id"),
)

TRIALS_INFLIGHT = Gauge(
    "loom_trials_inflight",
    "Trials in claimed/running",
    labelnames=("team_id", "state"),
)

QUEUE_DEPTH = Gauge(
    "loom_queue_depth",
    "Queued trials per team",
    labelnames=("team_id",),
)

CLAIM_LATENCY_SEC = Histogram(
    "loom_claim_latency_sec",
    "Time for POST /trials/claim",
    labelnames=("result",),  # 'hit' | 'miss'
)

STATE_PATCH_TOTAL = Counter(
    "loom_state_patch_total",
    "Trial state PATCH outcomes",
    # endpoint: 'state' / 'trajectory' / ...; result: 'ok' / 'fenced' / 'timeout'
    labelnames=("endpoint", "result"),
)

WORKERS_ACTIVE = Gauge(
    "loom_workers_active",
    "Workers with fresh heartbeat",
)

WORKER_POOL_TOTAL_SLOTS = Gauge(
    "loom_worker_pool_total_slots",
    "Fresh active worker execution slots by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_POOL_OCCUPIED_SLOTS = Gauge(
    "loom_worker_pool_occupied_slots",
    "Claimed and running trials assigned to fresh workers by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_POOL_FREE_SLOTS = Gauge(
    "loom_worker_pool_free_slots",
    "Unoccupied fresh active worker execution slots by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_POOL_WORKERS = Gauge(
    "loom_worker_pool_workers",
    "Fresh active worker processes by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_POOL_DESIRED_SLOTS = Gauge(
    "loom_worker_pool_desired_slots",
    "Autoscaler desired execution slots by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_POOL_PENDING_SLOTS = Gauge(
    "loom_worker_pool_pending_slots",
    "Autoscaler pending execution slots by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_POOL_DRAINING_SLOTS = Gauge(
    "loom_worker_pool_draining_slots",
    "Draining worker execution slots by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_POOL_DRAINING_WORKERS = Gauge(
    "loom_worker_pool_draining_workers",
    "Draining worker processes by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_POOL_AUTOSCALER_DECISION = Gauge(
    "loom_worker_pool_autoscaler_decision",
    "Last autoscaler decision by resource pool, action, and reason",
    labelnames=("pool_name", "backend", "cpu_arch", "action", "reason"),
)

WORKER_POOL_AUTOSCALER_ERROR = Gauge(
    "loom_worker_pool_autoscaler_error",
    "Autoscaler actuator error present by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_POOL_AUTOSCALER_IDLE_SECONDS = Gauge(
    "loom_worker_pool_autoscaler_idle_seconds",
    "Autoscaler idle-window age in seconds by resource pool",
    labelnames=("pool_name", "backend", "cpu_arch"),
)

WORKER_RECLAIM_TOTAL = Counter(
    "loom_worker_reclaim_total",
    "Trials reclaimed by crash detector",
)

RETRY_EXHAUSTED_TOTAL = Counter(
    "loom_retry_exhausted_total",
    "Trials transitioned to failed because attempt_count >= max_attempts",
)

PIPELINE_RUNS = Gauge(
    "loom_pipeline_runs",
    "Current PipelineRuns by closed lifecycle state/result",
    labelnames=("state", "result_status"),
)
PIPELINE_STAGE_RUNS = Gauge(
    "loom_pipeline_stage_runs", "Current Pipeline StageRuns", labelnames=("state", "resource_class")
)
PIPELINE_STAGE_DURATION_SECONDS = Histogram(
    "loom_pipeline_stage_duration_seconds",
    "Committed Pipeline StageRun duration",
    labelnames=("resource_class", "result"),
    buckets=(
        1,
        5,
        15,
        30,
        60,
        120,
        300,
        600,
        1800,
        3600,
        7200,
        14400,
        28800,
        86400,
        172800,
        345600,
        864000,
    ),
)
EXECUTION_ATTEMPTS = Gauge(
    "loom_execution_attempts",
    "Current Pipeline execution Attempts",
    labelnames=("state", "resource_class"),
)
PIPELINE_GPU_SECONDS_TOTAL = Counter(
    "loom_pipeline_gpu_seconds_total",
    "Settled Pipeline GPU seconds",
    labelnames=("slurm_cluster", "gpu_count_class"),
)
PIPELINE_ARTIFACT_BYTES_TOTAL = Counter(
    "loom_pipeline_artifact_bytes_total",
    "Newly committed Pipeline Artifact bytes charged to the run budget",
    labelnames=("artifact_class",),
)
PIPELINE_CANCEL_LATENCY_SECONDS = Histogram(
    "loom_pipeline_cancel_latency_seconds",
    "Pipeline cancellation acknowledgement latency",
    labelnames=("outcome",),
    buckets=(1, 2, 5, 10, 30, 60, 120, 300, 600),
)
PIPELINE_CONTROLLER_RECONCILE_ERRORS_TOTAL = Counter(
    "loom_pipeline_controller_reconcile_errors_total",
    "Exhausted Pipeline reconcile failures",
    labelnames=("reason",),
)

PIPELINE_LIVE_PREVIEW_FRAMES_TOTAL = Counter(
    "loom_pipeline_live_preview_frames_total",
    "Bounded Stage 1 preview frame publish outcomes",
    labelnames=("result", "reason"),
)
PIPELINE_LIVE_PREVIEW_BYTES_TOTAL = Counter(
    "loom_pipeline_live_preview_bytes_total",
    "Accepted Stage 1 preview JPEG bytes",
)
PIPELINE_LIVE_PREVIEW_ACTIVE_GENERATIONS = Gauge(
    "loom_pipeline_live_preview_active_generations",
    "Active ephemeral Stage 1 preview generations",
)
PIPELINE_LIVE_PREVIEW_LAST_FRAME_AGE_SECONDS = Gauge(
    "loom_pipeline_live_preview_last_frame_age_seconds",
    "Age of the oldest latest frame among active Stage 1 previews",
)
PIPELINE_LIVE_PREVIEW_PURGES_TOTAL = Counter(
    "loom_pipeline_live_preview_purges_total",
    "Ephemeral Stage 1 preview purge outcomes",
    labelnames=("reason",),
)
PIPELINE_STAGE_QUEUE_AGE_SECONDS = Gauge(
    "loom_pipeline_stage_queue_age_seconds",
    "Maximum Pipeline StageRun queue age",
    labelnames=("state", "resource_class"),
)
PIPELINE_STAGE_DEADLINE_OVERRUN_SECONDS = Gauge(
    "loom_pipeline_stage_deadline_overrun_seconds",
    "Maximum Pipeline Attempt deadline overrun",
    labelnames=("resource_class",),
)
PIPELINE_CHECKPOINT_OLDEST_AGE_SECONDS = Gauge(
    "loom_pipeline_checkpoint_oldest_age_seconds",
    "Oldest active Pipeline checkpoint age",
    labelnames=("resource_class",),
)
PIPELINE_ARTIFACT_COMMIT_FAILURES_TOTAL = Counter(
    "loom_pipeline_artifact_commit_failures_total",
    "Returned Pipeline Artifact commit failures",
    labelnames=("commit_kind", "reason"),
)

# Live worker tokens flagged by the staleness audit. Labels:
#   reason="unused_30d"  — last_seen_at (or issued_at, if never used)
#                          is older than 30 days
#   reason="aged_90d"    — issued_at is older than 90 days (rotation
#                          overdue per SOC2-equivalent service-credential
#                          cadence)
# Counts only non-revoked, non-expired worker-type tokens. An operator-
# facing alert fires when either count is > 0 for 1 hour; the metric is
# a SOFT signal (no auto-revocation — revoking a live worker pool's
# token 401s in-flight claims).
WORKER_TOKENS_STALE_COUNT = Gauge(
    "loom_worker_tokens_stale_count",
    "Live worker tokens flagged by the staleness audit",
    labelnames=("reason",),
)

SLURM_WORKER_DESIRED_SLOTS = Gauge(
    "loom_slurm_worker_desired_slots",
    "Desired Slurm worker slots by environment and pool",
    labelnames=("environment", "pool_name"),
)

SLURM_WORKER_ACTIVE_SLOTS = Gauge(
    "loom_slurm_worker_active_slots",
    "Running Slurm worker slots by environment and pool",
    labelnames=("environment", "pool_name"),
)

SLURM_WORKER_PENDING_SLOTS = Gauge(
    "loom_slurm_worker_pending_slots",
    "Pending Slurm worker slots by environment and pool",
    labelnames=("environment", "pool_name"),
)

SLURM_WORKER_STALE_SLOTS = Gauge(
    "loom_slurm_worker_stale_slots",
    "Stale Slurm worker slots by environment and pool",
    labelnames=("environment", "pool_name"),
)

SLURM_WORKER_RUNNING_JOBS = Gauge(
    "loom_slurm_worker_running_jobs",
    "Running Slurm worker jobs by environment and pool",
    labelnames=("environment", "pool_name"),
)

SLURM_WORKER_PENDING_JOBS = Gauge(
    "loom_slurm_worker_pending_jobs",
    "Pending Slurm worker jobs by environment and pool",
    labelnames=("environment", "pool_name"),
)

SLURM_WORKER_STALE_JOBS = Gauge(
    "loom_slurm_worker_stale_jobs",
    "Stale Slurm worker jobs by environment and pool",
    labelnames=("environment", "pool_name"),
)

SLURM_WORKER_FAILED_SUBMISSIONS = Gauge(
    "loom_slurm_worker_failed_submissions",
    "Failed Slurm worker submissions by environment and pool",
    labelnames=("environment", "pool_name"),
)

SLURM_WORKER_CANCELLED_PENDING_JOBS = Gauge(
    "loom_slurm_worker_cancelled_pending_jobs",
    "Cancelled pending Slurm worker jobs by environment and pool",
    labelnames=("environment", "pool_name"),
)

SLURM_WORKER_IDLE_EXITS = Gauge(
    "loom_slurm_worker_idle_exits",
    "Slurm workers that intentionally exited after idle timeout by pool",
    labelnames=("environment", "pool_name"),
)
