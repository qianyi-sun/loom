from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from loom_control_plane import task_image_builder_autoscaler as autoscaler
from loom_control_plane.task_image_builder_autoscaler import (
    SubprocessTaskImageBuilderSlurmRunner,
    TaskImageBuilderPoolConfig,
    build_task_image_builder_sbatch_request,
    build_task_image_builder_sbatch_test_request,
)


def _config() -> TaskImageBuilderPoolConfig:
    return TaskImageBuilderPoolConfig(
        environment="staging",
        pool_name="task-image-builder-gb10",
        slurm_cluster_id="gb10",
        cpu_arch="arm64",
        allowed_nodes=("gb10-1", "gb10-2"),
        env_file="/shared/loom/builder.env",
        env_template_file="/shared/loom/trial-worker.env",
        builder_token_file="/shared/loom/builder-token",
        repo_dir="/shared/loom/repo",
        partition="gb10",
        time_limit="04:00:00",
        requested_cpus=16,
        requested_memory_mib=65536,
        requested_concurrency=1,
        max_jobs=2,
        pending_job_cap=2,
        idle_exit_after_seconds=120,
        failure_backoff_seconds=300,
        sbatch_path="sbatch",
        squeue_path="squeue",
        sacct_path="sacct",
        scancel_path="scancel",
        command_timeout_seconds=20.0,
        exclusive=True,
        slurm_account="loom-staging",
        slurm_qos="loom-builder",
        slurm_reservation="loom-builder-exclusive",
        job_output_dir="/shared/loom/job-output",
        registry_docker_config_dir="/secure/loom/task-image-builder-docker",
    )


def test_builder_pool_requires_exclusive_single_build_allocations() -> None:
    with pytest.raises(ValueError, match="exclusive"):
        replace(_config(), exclusive=False)
    with pytest.raises(ValueError, match="concurrency"):
        replace(_config(), requested_concurrency=2)


def test_builder_pool_bounds_jobs_to_declared_nodes() -> None:
    with pytest.raises(ValueError, match="max_jobs"):
        replace(_config(), max_jobs=3)
    with pytest.raises(ValueError, match="pending_job_cap"):
        replace(_config(), pending_job_cap=3)


@pytest.mark.parametrize("seconds", [0, 3601])
def test_builder_pool_bounds_failed_allocation_backoff(seconds: int) -> None:
    with pytest.raises(ValueError, match="backoff"):
        replace(_config(), failure_backoff_seconds=seconds)


@pytest.mark.parametrize(
    ("field", "message"),
    [
        ("slurm_account", "slurm_account"),
        ("slurm_qos", "slurm_qos"),
        ("job_output_dir", "job_output_dir"),
    ],
)
def test_builder_pool_requires_explicit_slurm_and_output_authority(
    field: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        replace(_config(), **{field: ""})


def test_builder_sbatch_is_exclusive_and_runs_only_builder_entrypoint() -> None:
    request = build_task_image_builder_sbatch_request(_config(), node="gb10-1")

    assert "--exclusive" in request.args
    assert "--nodes=1" in request.args
    assert "--ntasks=1" in request.args
    assert "--nodelist=gb10-1" in request.args
    assert "--account=loom-staging" in request.args
    assert "--qos=loom-builder" in request.args
    assert "LOOM_WORKER_MAX_CONCURRENT=1" in request.args[-1]
    assert "LOOM_WORKER_TASK_IMAGE_BUILDER_IDLE_EXIT_SECONDS=120" in request.args[-1]
    assert (
        "LOOM_TASK_IMAGE_BUILDER_DOCKER_CONFIG_DIR=/secure/loom/task-image-builder-docker"
        in request.args[-1]
    )
    assert "LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS" not in request.args[-1]
    assert "docker compose" in request.stdin
    assert "export HOME=" not in request.stdin
    assert "worker python -m loom_worker.task_image_builder" in request.stdin
    assert (
        '--volume "$LOOM_TASK_IMAGE_BUILDER_DOCKER_CONFIG_DIR:'
        '/run/loom/task-image-builder-docker:ro"' in request.stdin
    )
    assert "--env DOCKER_CONFIG=/run/loom/task-image-builder-docker" in request.stdin
    assert (
        "docker compose"
        not in request.stdin.split("worker python -m loom_worker.task_image_builder")[1]
    )


def test_builder_sbatch_test_request_cannot_submit() -> None:
    live = build_task_image_builder_sbatch_request(_config(), node="gb10-1")
    tested = build_task_image_builder_sbatch_test_request(_config(), node="gb10-1")

    assert tested.args[0] == live.args[0]
    assert tested.args[1] == "--test-only"
    assert "--parsable" not in tested.args
    assert tuple(item for item in live.args[1:] if item != "--parsable") == tested.args[2:]
    assert tested.stdin == live.stdin


async def test_subprocess_runner_validates_without_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[str, ...], str]] = []

    async def run(
        args: tuple[str, ...],
        *,
        stdin: str | None = None,
        timeout: float,
    ) -> SimpleNamespace:
        calls.append((args, stdin or ""))
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(autoscaler, "_run_command", run)
    runner = SubprocessTaskImageBuilderSlurmRunner(_config())

    await runner.validate_builder_request(node="gb10-1", config=_config())

    assert len(calls) == 1
    assert calls[0][0][1] == "--test-only"
    assert "--parsable" not in calls[0][0]


async def test_builder_job_query_falls_back_to_sacct_for_terminal_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[tuple[str, ...]] = []

    async def run_command(
        args: tuple[str, ...],
        **_kwargs: object,
    ) -> SimpleNamespace:
        commands.append(args)
        if args[0] == "squeue":
            raise RuntimeError("squeue: Invalid job id specified")
        return SimpleNamespace(
            stdout="31619|COMPLETED|gb10-1|None\n",
            stderr="",
        )

    monkeypatch.setattr(autoscaler, "_run_command", run_command)
    runner = SubprocessTaskImageBuilderSlurmRunner(_config())

    observations = await runner.query_jobs(("31619",))

    assert [(row.job_id, row.slurm_state) for row in observations] == [
        ("31619", "COMPLETED"),
    ]
    assert [command[0] for command in commands] == ["squeue", "sacct"]


async def test_builder_job_query_keeps_controller_errors_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run_command(*_args: object, **_kwargs: object) -> SimpleNamespace:
        raise RuntimeError("squeue: Unable to contact slurm controller")

    monkeypatch.setattr(autoscaler, "_run_command", run_command)
    runner = SubprocessTaskImageBuilderSlurmRunner(_config())

    with pytest.raises(RuntimeError, match="Unable to contact"):
        await runner.query_jobs(("31619",))


async def test_builder_running_job_cancellation_is_not_pending_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[tuple[str, ...]] = []

    async def run_command(
        args: tuple[str, ...],
        **_kwargs: object,
    ) -> SimpleNamespace:
        commands.append(args)
        return SimpleNamespace(stdout="", stderr="")

    monkeypatch.setattr(autoscaler, "_run_command", run_command)
    runner = SubprocessTaskImageBuilderSlurmRunner(_config())

    await runner.cancel_job("31619")

    assert commands == [("scancel", "31619")]
