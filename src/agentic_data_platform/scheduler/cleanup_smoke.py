from __future__ import annotations

import argparse
import json
import os
import queue
import socket
import subprocess
import sys
import threading
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone

from sqlalchemy import Engine, select

from agentic_data_platform.domain.run_records import (
    BenchmarkTaskInstance,
    EvaluatorConfig,
    JudgeConfig,
    ModelConfig,
    ModelMode,
    RunnerConfig,
    RunnerKind,
    RunRecord,
    SandboxBackend,
)
from agentic_data_platform.persistence import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.models import RunAttemptRow, RunRow
from agentic_data_platform.persistence.repositories import IdentityRepository, ProjectRepository, RunRepository
from agentic_data_platform.sandbox.docker_terminal import (
    CommandRunner,
    DockerOwnedContainerCleaner,
    SubprocessCommandRunner,
    docker_owned_container_labels,
)
from agentic_data_platform.scheduler.service import RunScheduler
from agentic_data_platform.service.config import ServiceSettings, load_service_settings


@dataclass(frozen=True)
class SchedulerDockerCleanupSmokeResult:
    run_id: str
    container_id: str
    docker_cleanup_count: int
    docker_cleanup_error_count: int
    removed_container_ids: list[str]
    cleanup_event_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "container_id": self.container_id,
            "docker_cleanup_count": self.docker_cleanup_count,
            "docker_cleanup_error_count": self.docker_cleanup_error_count,
            "removed_container_ids": list(self.removed_container_ids),
            "cleanup_event_count": self.cleanup_event_count,
        }


@dataclass(frozen=True)
class SchedulerParentDeathCleanupSmokeResult:
    run_id: str
    container_id: str
    parent_process_returncode: int | None
    parent_process_was_terminated: bool
    docker_cleanup_count: int
    docker_cleanup_error_count: int
    removed_container_ids: list[str]
    cleanup_event_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "container_id": self.container_id,
            "parent_process_returncode": self.parent_process_returncode,
            "parent_process_was_terminated": self.parent_process_was_terminated,
            "docker_cleanup_count": self.docker_cleanup_count,
            "docker_cleanup_error_count": self.docker_cleanup_error_count,
            "removed_container_ids": list(self.removed_container_ids),
            "cleanup_event_count": self.cleanup_event_count,
        }


def run_scheduler_docker_cleanup_smoke(
    *,
    engine: Engine,
    scheduler_id: str,
    run_id: str,
    runner: CommandRunner | None = None,
    smoke_image: str | None = None,
    stale_active_seconds: int = 60,
    timeout_seconds: int = 30,
) -> SchedulerDockerCleanupSmokeResult:
    _require_non_empty("scheduler_id", scheduler_id)
    _require_non_empty("run_id", run_id)
    if stale_active_seconds <= 0:
        raise ValueError("stale_active_seconds must be positive")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    command_runner = runner or SubprocessCommandRunner()
    image = smoke_image or _current_container_image(command_runner, timeout_seconds=timeout_seconds)
    _seed_stale_active_run(engine=engine, run_id=run_id, stale_active_seconds=stale_active_seconds)
    container_id = _start_labeled_smoke_container(
        runner=command_runner,
        run_id=run_id,
        image=image,
        timeout_seconds=timeout_seconds,
    )

    settings = replace(
        load_service_settings(),
        scheduler_docker_cleanup_enabled=True,
        scheduler_docker_cleanup_timeout_seconds=timeout_seconds,
        scheduler_stale_active_heartbeat_timeout_seconds=stale_active_seconds,
    )
    scheduler = RunScheduler(
        engine=engine,
        scheduler_id=scheduler_id,
        settings=settings,
        docker_container_cleaner=DockerOwnedContainerCleaner(
            runner=command_runner,
            timeout_seconds=timeout_seconds,
        ),
    )

    result = scheduler.recover_once(request_id=f"{run_id}-cleanup-smoke")
    removed_container_ids = _removed_container_ids(result.docker_cleanup_runs, run_id=run_id)
    cleanup_event_count = _cleanup_event_count(engine=engine, run_id=run_id)
    if not _container_id_was_removed(container_id=container_id, removed_container_ids=removed_container_ids):
        _cleanup_container_best_effort(runner=command_runner, container_id=container_id, timeout_seconds=timeout_seconds)
        raise RuntimeError(
            f"scheduler Docker cleanup smoke did not remove container {container_id}; "
            f"removed={removed_container_ids!r}"
        )
    if cleanup_event_count < 1:
        raise RuntimeError(f"scheduler Docker cleanup smoke did not record sandbox cleanup event for {run_id}")

    return SchedulerDockerCleanupSmokeResult(
        run_id=run_id,
        container_id=container_id,
        docker_cleanup_count=result.docker_cleanup_count,
        docker_cleanup_error_count=result.docker_cleanup_error_count,
        removed_container_ids=removed_container_ids,
        cleanup_event_count=cleanup_event_count,
    )


def run_scheduler_parent_death_cleanup_smoke(
    *,
    engine: Engine,
    scheduler_id: str,
    run_id: str,
    runner: CommandRunner | None = None,
    smoke_image: str | None = None,
    stale_active_seconds: int = 60,
    timeout_seconds: int = 30,
) -> SchedulerParentDeathCleanupSmokeResult:
    _require_non_empty("scheduler_id", scheduler_id)
    _require_non_empty("run_id", run_id)
    if stale_active_seconds <= 0:
        raise ValueError("stale_active_seconds must be positive")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    command_runner = runner or SubprocessCommandRunner()
    image = smoke_image or _current_container_image(command_runner, timeout_seconds=timeout_seconds)
    _seed_stale_active_run(engine=engine, run_id=run_id, stale_active_seconds=stale_active_seconds)
    container_id = ""
    try:
        container_id, parent_returncode = _start_labeled_container_from_killed_parent(
            runner=command_runner,
            run_id=run_id,
            image=image,
            timeout_seconds=timeout_seconds,
        )

        settings = replace(
            load_service_settings(),
            scheduler_docker_cleanup_enabled=True,
            scheduler_docker_cleanup_timeout_seconds=timeout_seconds,
            scheduler_stale_active_heartbeat_timeout_seconds=stale_active_seconds,
        )
        scheduler = RunScheduler(
            engine=engine,
            scheduler_id=scheduler_id,
            settings=settings,
            docker_container_cleaner=DockerOwnedContainerCleaner(
                runner=command_runner,
                timeout_seconds=timeout_seconds,
            ),
        )

        result = scheduler.recover_once(request_id=f"{run_id}-parent-death-cleanup-smoke")
        removed_container_ids = _removed_container_ids(result.docker_cleanup_runs, run_id=run_id)
        cleanup_event_count = _cleanup_event_count(engine=engine, run_id=run_id)
        if not _container_id_was_removed(container_id=container_id, removed_container_ids=removed_container_ids):
            raise RuntimeError(
                f"scheduler parent-death cleanup smoke did not remove container {container_id}; "
                f"removed={removed_container_ids!r}"
            )
        if cleanup_event_count < 1:
            raise RuntimeError(
                f"scheduler parent-death cleanup smoke did not record sandbox cleanup event for {run_id}"
            )

        return SchedulerParentDeathCleanupSmokeResult(
            run_id=run_id,
            container_id=container_id,
            parent_process_returncode=parent_returncode,
            parent_process_was_terminated=parent_returncode is not None and parent_returncode < 0,
            docker_cleanup_count=result.docker_cleanup_count,
            docker_cleanup_error_count=result.docker_cleanup_error_count,
            removed_container_ids=removed_container_ids,
            cleanup_event_count=cleanup_event_count,
        )
    except Exception:
        if container_id:
            _cleanup_container_best_effort(
                runner=command_runner,
                container_id=container_id,
                timeout_seconds=timeout_seconds,
            )
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the scheduler Docker cleanup smoke check.")
    parser.add_argument("--mode", choices=["synthetic", "parent-death"], default="synthetic")
    parser.add_argument("--scheduler-id", default="scheduler-docker-cleanup-smoke")
    parser.add_argument(
        "--run-id",
        default=f"scheduler_cleanup_smoke_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}",
    )
    parser.add_argument("--smoke-image", default=os.environ.get("SCHEDULER_DOCKER_CLEANUP_SMOKE_IMAGE"))
    parser.add_argument("--stale-active-seconds", type=int, default=60)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    args = parser.parse_args(argv)

    settings = load_service_settings()
    engine = create_database_engine(settings.database_url)
    try:
        upgrade_database(engine)
        if args.mode == "parent-death":
            result = run_scheduler_parent_death_cleanup_smoke(
                engine=engine,
                scheduler_id=args.scheduler_id,
                run_id=args.run_id,
                smoke_image=args.smoke_image,
                stale_active_seconds=args.stale_active_seconds,
                timeout_seconds=args.timeout_seconds,
            )
        else:
            result = run_scheduler_docker_cleanup_smoke(
                engine=engine,
                scheduler_id=args.scheduler_id,
                run_id=args.run_id,
                smoke_image=args.smoke_image,
                stale_active_seconds=args.stale_active_seconds,
                timeout_seconds=args.timeout_seconds,
            )
        print(json.dumps(result.to_dict(), sort_keys=True), flush=True)
        return 0
    finally:
        engine.dispose()


def _seed_stale_active_run(*, engine: Engine, run_id: str, stale_active_seconds: int) -> None:
    stale_at = datetime.now(timezone.utc) - timedelta(seconds=stale_active_seconds + 30)
    with session_scope(engine) as session:
        IdentityRepository(session).create_team(team_id="scheduler-smoke", name="Scheduler Smoke")
        ProjectRepository(session).create_project(
            project_id="scheduler-cleanup-smoke",
            name="Scheduler Cleanup Smoke",
            owner_team_id="scheduler-smoke",
            description="Synthetic scheduler Docker cleanup smoke project",
        )
        repository = RunRepository(session)
        repository.create_run(_smoke_run(run_id=run_id), request_id=f"{run_id}-cleanup-smoke-create")
        repository.claim_queued_run(
            run_id,
            worker_id="scheduler-cleanup-smoke-worker",
            request_id=f"{run_id}-cleanup-smoke-claim",
        )
        row = session.get(RunRow, run_id)
        if row is not None:
            row.updated_at = stale_at
        attempt = session.scalar(select(RunAttemptRow).where(RunAttemptRow.run_id == run_id))
        if attempt is not None:
            metadata = dict(attempt.metadata_json or {})
            worker = dict(metadata.get("worker") or {})
            worker["last_heartbeat_at"] = stale_at.isoformat()
            metadata["worker"] = worker
            attempt.metadata_json = metadata


def _smoke_run(*, run_id: str) -> RunRecord:
    return RunRecord.create(
        run_id=run_id,
        project_id="scheduler-cleanup-smoke",
        owner_team="Scheduler Smoke",
        task=BenchmarkTaskInstance(
            benchmark_suite="SchedulerCleanupSmoke",
            benchmark_version="dev",
            task_family="docker-cleanup",
            instance_id=run_id,
            source_uri="internal://scheduler-cleanup-smoke",
            input_artifact_refs=[],
            required_artifacts=["trajectory", "workspace_snapshot", "evaluator_report"],
            metadata={"instruction": "Synthetic stale active run for scheduler Docker cleanup smoke."},
        ),
        model=ModelConfig(
            provider="smoke",
            model_name="noop",
            mode=ModelMode.API,
            prompt_template_version="scheduler-cleanup-smoke-v0",
        ),
        runner=RunnerConfig(
            kind=RunnerKind.CUSTOM_PIPELINE,
            sandbox_backend=SandboxBackend.DOCKER_TERMINAL,
            image="scheduler-cleanup-smoke",
            entrypoint=["/bin/sh", "-lc", "sleep 300"],
            internet_access=False,
            resource_limits={"cpu": 1, "memory_gib": 1, "timeout_seconds": 300},
        ),
        evaluator_configs=[
            EvaluatorConfig(
                evaluator_id="scheduler-cleanup-smoke",
                mode="llm_judge",
                judge=JudgeConfig(
                    provider="smoke",
                    model_name="noop",
                    rubric_version="scheduler-cleanup-smoke-v0",
                ),
            )
        ],
    )


def _start_labeled_smoke_container(
    *,
    runner: CommandRunner,
    run_id: str,
    image: str,
    timeout_seconds: int,
) -> str:
    labels = docker_owned_container_labels(run_id=run_id)
    args = ["docker", "run", "-d"]
    for key, value in labels.items():
        args.extend(["--label", f"{key}={value}"])
    args.extend([image, "/bin/sh", "-lc", "sleep 300"])
    process = runner.run(args, timeout=timeout_seconds)
    if process.returncode != 0:
        raise RuntimeError(f"docker cleanup smoke container start failed: {_coerce_output(process.stderr).strip()}")
    container_id = _coerce_output(process.stdout).strip()
    _require_non_empty("container_id", container_id)
    return container_id


def _start_labeled_container_from_killed_parent(
    *,
    runner: CommandRunner,
    run_id: str,
    image: str,
    timeout_seconds: int,
) -> tuple[str, int | None]:
    labels = docker_owned_container_labels(run_id=run_id)
    docker_args = ["docker", "run", "-d"]
    for key, value in labels.items():
        docker_args.extend(["--label", f"{key}={value}"])
    docker_args.extend([image, "/bin/sh", "-lc", "sleep 300"])

    start = getattr(runner, "start", None)
    if callable(start):
        parent_process = start(_parent_death_helper_args(docker_args, timeout_seconds=timeout_seconds))
    else:
        parent_process = SubprocessCommandRunner().start(
            _parent_death_helper_args(docker_args, timeout_seconds=timeout_seconds),
        )
    container_id = _read_parent_container_id(parent_process, timeout_seconds=timeout_seconds)
    parent_returncode = _terminate_parent_process(parent_process, timeout_seconds=timeout_seconds)
    _require_non_empty("container_id", container_id)
    return container_id, parent_returncode


def _parent_death_helper_args(docker_args: list[str], *, timeout_seconds: int) -> list[str]:
    return [
        sys.executable,
        "-u",
        "-c",
        _PARENT_DEATH_HELPER_CODE,
        json.dumps({"docker_args": docker_args, "timeout_seconds": timeout_seconds}),
    ]


def _current_container_image(runner: CommandRunner, *, timeout_seconds: int) -> str:
    current_container_id = os.environ.get("HOSTNAME") or socket.gethostname()
    process = runner.run(
        ["docker", "inspect", "--format", "{{.Config.Image}}", current_container_id],
        timeout=timeout_seconds,
    )
    if process.returncode != 0:
        raise RuntimeError(f"could not resolve current scheduler container image: {_coerce_output(process.stderr).strip()}")
    image = _coerce_output(process.stdout).strip()
    _require_non_empty("smoke_image", image)
    return image


def _removed_container_ids(cleanup_runs: list[dict[str, object]], *, run_id: str) -> list[str]:
    for item in cleanup_runs:
        if item.get("run_id") == run_id:
            return [str(container_id) for container_id in item.get("removed_container_ids", [])]
    return []


def _container_id_was_removed(*, container_id: str, removed_container_ids: list[str]) -> bool:
    for removed_container_id in removed_container_ids:
        if _container_ids_match(container_id, removed_container_id):
            return True
    return False


def _container_ids_match(expected_container_id: str, removed_container_id: str) -> bool:
    if expected_container_id == removed_container_id:
        return True
    expected = expected_container_id.strip()
    removed = removed_container_id.strip()
    if len(expected) < 12 or len(removed) < 12:
        return False
    return expected.startswith(removed) or removed.startswith(expected)


def _cleanup_event_count(*, engine: Engine, run_id: str) -> int:
    with session_scope(engine) as session:
        events = RunRepository(session).list_status_events(run_id)
    return sum(1 for event in events if event.event_type == "sandbox.container_cleanup")


def _cleanup_container_best_effort(*, runner: CommandRunner, container_id: str, timeout_seconds: int) -> None:
    try:
        runner.run(["docker", "rm", "-f", container_id], timeout=timeout_seconds)
    except (RuntimeError, subprocess.SubprocessError):
        return


def _read_parent_container_id(process: subprocess.Popen[str], *, timeout_seconds: int) -> str:
    if process.stdout is None:
        raise RuntimeError("parent-death helper did not expose stdout")

    line_queue: queue.Queue[str] = queue.Queue(maxsize=1)
    reader = threading.Thread(
        target=lambda: line_queue.put(process.stdout.readline()),
        name="parent-death-cleanup-smoke-stdout-reader",
        daemon=True,
    )
    reader.start()
    try:
        line = line_queue.get(timeout=timeout_seconds)
    except queue.Empty as exc:
        _terminate_parent_process(process, timeout_seconds=timeout_seconds)
        raise RuntimeError("parent-death helper did not report a container id before timeout") from exc

    container_id = line.strip()
    if not container_id:
        raise RuntimeError("parent-death helper reported an empty container id")
    return container_id


def _terminate_parent_process(process: subprocess.Popen[str], *, timeout_seconds: int) -> int | None:
    if process.poll() is None:
        process.terminate()
    try:
        return process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        return process.wait(timeout=timeout_seconds)


def _coerce_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _require_non_empty(name: str, value: str) -> None:
    if not value:
        raise ValueError(f"{name} must be a non-empty string")


_PARENT_DEATH_HELPER_CODE = r"""
import json
import subprocess
import sys
import time

payload = json.loads(sys.argv[1])
docker_args = payload["docker_args"]
timeout_seconds = payload["timeout_seconds"]
try:
    process = subprocess.run(
        docker_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout_seconds,
    )
except subprocess.TimeoutExpired as exc:
    sys.stderr.write(f"docker run timed out after {timeout_seconds} seconds\n")
    if exc.stderr:
        sys.stderr.write(str(exc.stderr))
    raise SystemExit(124)
if process.returncode != 0:
    sys.stderr.write(process.stderr)
    raise SystemExit(process.returncode)

container_id = process.stdout.strip()
if not container_id:
    sys.stderr.write("docker run did not return a container id\n")
    raise SystemExit(1)

print(container_id, flush=True)
while True:
    time.sleep(60)
"""


if __name__ == "__main__":
    raise SystemExit(main())
