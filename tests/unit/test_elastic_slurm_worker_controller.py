from __future__ import annotations

import pytest

from loom_control_plane.elastic_slurm_worker_controller import (
    ElasticSlurmWorkerControllerConfig,
    SlurmWorkerCapacitySnapshot,
    SlurmWorkerControllerDecision,
    build_controller_config,
    build_sbatch_request,
    compute_controller_decision,
)


def _config(**overrides: object) -> ElasticSlurmWorkerControllerConfig:
    values: dict[str, object] = {
        "environment": "production",
        "pool_name": "oldlab",
        "allowed_nodes": ("oldlab-1", "oldlab-2", "oldlab-3", "oldlab-4", "oldlab-5"),
        "env_file": "/secure/.env.remote-worker",
        "repo_dir": "/opt/loom",
        "partition": "",
        "time_limit": "7-00:00:00",
        "requested_cpus": 12,
        "requested_memory_mib": 58000,
        "requested_concurrency": 6,
        "max_jobs": 5,
        "pending_job_cap": 2,
        "min_queued_trials": 1,
        "stale_after_seconds": 300,
        "sbatch_path": "sbatch",
        "squeue_path": "squeue",
        "sacct_path": "sacct",
        "scancel_path": "scancel",
        "command_timeout_seconds": 20.0,
    }
    values.update(overrides)
    return ElasticSlurmWorkerControllerConfig(**values)  # type: ignore[arg-type]


def test_decision_submits_missing_capacity_without_reusing_active_nodes() -> None:
    snapshot = SlurmWorkerCapacitySnapshot(
        queued_trials=20,
        running_trials=3,
        pending_jobs=1,
        running_jobs=1,
        active_slots=6,
        pending_slots=6,
        active_nodes={"oldlab-1", "oldlab-2"},
        cancellable_pending_job_ids=(),
        active_job_ids=("101", "102"),
    )

    decision = compute_controller_decision(_config(), snapshot)

    assert decision == SlurmWorkerControllerDecision(
        submit_nodes=("oldlab-3", "oldlab-4"),
        cancel_job_ids=(),
        reason="queued_backlog",
    )


def test_decision_respects_pending_cap_before_submitting_more() -> None:
    snapshot = SlurmWorkerCapacitySnapshot(
        queued_trials=36,
        running_trials=0,
        pending_jobs=2,
        running_jobs=0,
        active_slots=0,
        pending_slots=12,
        active_nodes={"oldlab-1", "oldlab-2"},
        cancellable_pending_job_ids=("201", "202"),
        active_job_ids=("201", "202"),
    )

    decision = compute_controller_decision(_config(), snapshot)

    assert decision == SlurmWorkerControllerDecision(
        submit_nodes=(),
        cancel_job_ids=(),
        reason="pending_cap_reached",
    )


def test_decision_cancels_pending_jobs_when_queue_drains() -> None:
    snapshot = SlurmWorkerCapacitySnapshot(
        queued_trials=0,
        running_trials=12,
        pending_jobs=2,
        running_jobs=1,
        active_slots=6,
        pending_slots=12,
        active_nodes={"oldlab-1", "oldlab-2", "oldlab-3"},
        cancellable_pending_job_ids=("301", "302"),
        active_job_ids=("301", "302", "303"),
    )

    decision = compute_controller_decision(_config(), snapshot)

    assert decision == SlurmWorkerControllerDecision(
        submit_nodes=(),
        cancel_job_ids=("301", "302"),
        reason="queue_drained",
    )


def test_build_sbatch_request_uses_environment_specific_worker_settings() -> None:
    request = build_sbatch_request(_config(partition="cpu"), node="oldlab-4")

    assert request.args == (
        "sbatch",
        "--parsable",
        "--job-name=loom-worker-oldlab-4",
        "--nodelist=oldlab-4",
        "--exclusive",
        "--time=7-00:00:00",
        "--partition=cpu",
        "--cpus-per-task=12",
        "--mem=58000M",
        "--export=ALL,LOOM_WORKER_MAX_CONCURRENT=6,LOOM_WORKER_POOL_NAME=oldlab,LOOM_REMOTE_WORKER_ENV_FILE=/secure/.env.remote-worker,LOOM_REMOTE_WORKER_REPO_DIR=/opt/loom",
    )
    assert "docker compose --env-file \"$LOOM_REMOTE_WORKER_ENV_FILE\"" in request.stdin
    assert "cd \"$LOOM_REMOTE_WORKER_REPO_DIR\"" in request.stdin


def test_build_sbatch_request_can_disable_exclusive_node_allocation() -> None:
    request = build_sbatch_request(_config(exclusive=False), node="oldlab-4")

    assert "--exclusive" not in request.args


def test_build_controller_config_rejects_enabled_missing_required_settings() -> None:
    with pytest.raises(ValueError, match="allowed nodes"):
        build_controller_config(
            enabled=True,
            environment="production",
            pool_name="oldlab",
            allowed_nodes_csv="",
            env_file="/secure/.env.remote-worker",
            repo_dir="/opt/loom",
            partition="",
            time_limit="7-00:00:00",
            requested_cpus=12,
            requested_memory_mib=58000,
            requested_concurrency=6,
            max_jobs=5,
            pending_job_cap=2,
            min_queued_trials=1,
            stale_after_seconds=300,
            sbatch_path="sbatch",
            squeue_path="squeue",
            sacct_path="sacct",
            scancel_path="scancel",
            command_timeout_seconds=20.0,
        )


def test_build_controller_config_parses_allowed_nodes_and_caps() -> None:
    config = build_controller_config(
        enabled=True,
        environment="staging",
        pool_name="oldlab",
        allowed_nodes_csv=" oldlab-1,oldlab-2 ,, oldlab-3 ",
        env_file="/secure/staging.env",
        repo_dir="/srv/loom",
        partition="",
        time_limit="2:00:00",
        requested_cpus=12,
        requested_memory_mib=58000,
        requested_concurrency=6,
        max_jobs=3,
        pending_job_cap=1,
        min_queued_trials=1,
        stale_after_seconds=300,
        sbatch_path="sbatch",
        squeue_path="squeue",
        sacct_path="sacct",
        scancel_path="scancel",
        command_timeout_seconds=20.0,
    )

    assert config.allowed_nodes == ("oldlab-1", "oldlab-2", "oldlab-3")
    assert config.max_jobs == 3
