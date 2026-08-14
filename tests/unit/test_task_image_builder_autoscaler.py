from __future__ import annotations

from dataclasses import replace

import pytest

from loom_control_plane.task_image_builder_autoscaler import (
    TaskImageBuilderPoolConfig,
    build_task_image_builder_sbatch_request,
)


def _config() -> TaskImageBuilderPoolConfig:
    return TaskImageBuilderPoolConfig(
        environment="staging",
        pool_name="task-image-builder-gb10",
        slurm_cluster_id="gb10",
        cpu_arch="arm64",
        allowed_nodes=("gb10-1", "gb10-2"),
        env_file="/shared/loom/builder.env",
        repo_dir="/shared/loom/repo",
        partition="gb10",
        time_limit="04:00:00",
        requested_cpus=16,
        requested_memory_mib=65536,
        requested_concurrency=1,
        max_jobs=2,
        pending_job_cap=2,
        idle_exit_after_seconds=120,
        sbatch_path="sbatch",
        squeue_path="squeue",
        sacct_path="sacct",
        scancel_path="scancel",
        command_timeout_seconds=20.0,
        exclusive=True,
        slurm_account="loom-staging",
        slurm_qos="loom-builder",
    )


def test_builder_pool_requires_exclusive_single_build_allocations() -> None:
    with pytest.raises(ValueError, match="exclusive"):
        replace(_config(), exclusive=False)
    with pytest.raises(ValueError, match="concurrency"):
        replace(_config(), requested_concurrency=2)


def test_builder_sbatch_is_exclusive_and_runs_only_builder_entrypoint() -> None:
    request = build_task_image_builder_sbatch_request(_config(), node="gb10-1")

    assert "--exclusive" in request.args
    assert "--nodes=1" in request.args
    assert "--ntasks=1" in request.args
    assert "--nodelist=gb10-1" in request.args
    assert "--account=loom-staging" in request.args
    assert "--qos=loom-builder" in request.args
    assert "LOOM_WORKER_MAX_CONCURRENT=1" in request.args[-1]
    assert "LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS=120" in request.args[-1]
    assert "docker compose" in request.stdin
    assert "run --rm --no-deps worker python -m loom_worker.task_image_builder" in request.stdin
    assert (
        "docker compose"
        not in request.stdin.split(
            "run --rm --no-deps worker python -m loom_worker.task_image_builder"
        )[1]
    )
