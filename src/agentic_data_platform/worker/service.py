from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import Engine

from agentic_data_platform.artifacts.store import (
    ArtifactPersistence,
    LocalArtifactStore,
    build_s3_artifact_store,
)
from agentic_data_platform.domain.execution_events import RunEventType
from agentic_data_platform.domain.run_records import RunStatus
from agentic_data_platform.persistence import create_database_engine, session_scope
from agentic_data_platform.persistence.repositories import RunRepository
from agentic_data_platform.providers.config import DevProviderConfigRegistry
from agentic_data_platform.sandbox.docker_terminal import CommandRunner, SubprocessCommandRunner
from agentic_data_platform.service.config import ServiceSettings, load_service_settings
from agentic_data_platform.worker.executors import DockerTerminalWorkerExecutor, WorkerRunExecutor


@dataclass(frozen=True)
class WorkerRunResult:
    run_id: str
    status: str
    artifact_count: int
    turn_count: int
    evaluator_id: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "artifact_count": self.artifact_count,
            "turn_count": self.turn_count,
            "evaluator_id": self.evaluator_id,
        }


class RunWorker:
    def __init__(
        self,
        *,
        engine: Engine,
        worker_id: str,
        executor: WorkerRunExecutor,
        allow_legacy_queue_claim: bool = True,
        heartbeat_interval_seconds: int = 30,
    ) -> None:
        _require_non_empty("worker_id", worker_id)
        self.engine = engine
        self.worker_id = worker_id
        self.executor = executor
        self.allow_legacy_queue_claim = allow_legacy_queue_claim
        self.heartbeat_interval_seconds = heartbeat_interval_seconds

    def run_once(self, *, request_id: str | None = None) -> WorkerRunResult | None:
        with session_scope(self.engine) as session:
            repository = RunRepository(session)
            claimed = repository.claim_next_dispatched_run(worker_id=self.worker_id, request_id=request_id)
            if claimed is None and self.allow_legacy_queue_claim:
                claimed = repository.claim_next_queued_run(worker_id=self.worker_id, request_id=request_id)

        if claimed is None:
            return None

        try:
            with WorkerHeartbeat(
                engine=self.engine,
                run_id=claimed.run_id,
                worker_id=self.worker_id,
                interval_seconds=self.heartbeat_interval_seconds,
                request_id=request_id,
            ):
                completed = self.executor.execute(claimed)
        except Exception as exc:
            with session_scope(self.engine) as session:
                failed = RunRepository(session).transition_run(
                    claimed.run_id,
                    "failed",
                    event_type=RunEventType.WORKER_FAILED,
                    reason=str(exc),
                    request_id=request_id,
                )
            return _worker_result(failed)

        saved = _save_worker_result(
            self.engine,
            completed,
            worker_id=self.worker_id,
            request_id=request_id,
        )
        return _worker_result(saved)


class SubprocessRunWorker:
    def __init__(
        self,
        *,
        engine: Engine,
        worker_id: str,
        command_runner: CommandRunner | None = None,
        timeout_seconds: int = 7200,
        allow_legacy_queue_claim: bool = True,
        heartbeat_interval_seconds: int = 30,
        cancel_poll_interval_seconds: float = 5.0,
    ) -> None:
        _require_non_empty("worker_id", worker_id)
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if cancel_poll_interval_seconds <= 0:
            raise ValueError("cancel_poll_interval_seconds must be positive")
        self.engine = engine
        self.worker_id = worker_id
        self.command_runner = command_runner or SubprocessCommandRunner()
        self.timeout_seconds = timeout_seconds
        self.allow_legacy_queue_claim = allow_legacy_queue_claim
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.cancel_poll_interval_seconds = cancel_poll_interval_seconds

    def run_once(self, *, request_id: str | None = None) -> WorkerRunResult | None:
        with session_scope(self.engine) as session:
            repository = RunRepository(session)
            claimed = repository.claim_next_dispatched_run(worker_id=self.worker_id, request_id=request_id)
            if claimed is None and self.allow_legacy_queue_claim:
                claimed = repository.claim_next_queued_run(worker_id=self.worker_id, request_id=request_id)

        if claimed is None:
            return None

        args = _execution_child_args(
            run_id=claimed.run_id,
            worker_id=self.worker_id,
            request_id=request_id,
        )
        try:
            with WorkerHeartbeat(
                engine=self.engine,
                run_id=claimed.run_id,
                worker_id=self.worker_id,
                interval_seconds=self.heartbeat_interval_seconds,
                request_id=request_id,
            ):
                process = _run_child_command_with_cancel_monitor(
                    self.command_runner,
                    args,
                    timeout_seconds=self.timeout_seconds,
                    engine=self.engine,
                    run_id=claimed.run_id,
                    poll_interval_seconds=self.cancel_poll_interval_seconds,
                )
        except subprocess.TimeoutExpired:
            return _fail_active_subprocess_run(
                self.engine,
                run_id=claimed.run_id,
                reason=f"Worker subprocess timed out after {self.timeout_seconds} seconds",
                request_id=request_id,
            )
        if process.returncode != 0:
            return _fail_active_subprocess_run(
                self.engine,
                run_id=claimed.run_id,
                reason=f"Worker subprocess exited with code {process.returncode}",
                request_id=request_id,
            )

        with session_scope(self.engine) as session:
            completed = RunRepository(session).get_run(claimed.run_id)
        if completed.status not in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELED}:
            return _fail_active_subprocess_run(
                self.engine,
                run_id=claimed.run_id,
                reason="Worker subprocess exited without saving a terminal run result",
                request_id=request_id,
            )
        return _worker_result(completed)


def execute_claimed_run(
    *,
    engine: Engine,
    worker_id: str,
    run_id: str,
    executor: WorkerRunExecutor,
    request_id: str | None = None,
) -> WorkerRunResult:
    _require_non_empty("worker_id", worker_id)
    _require_non_empty("run_id", run_id)
    with session_scope(engine) as session:
        claimed = RunRepository(session).get_run(run_id)

    try:
        completed = executor.execute(claimed)
    except Exception as exc:
        with session_scope(engine) as session:
            failed = RunRepository(session).transition_run(
                run_id,
                "failed",
                event_type=RunEventType.WORKER_FAILED,
                reason=str(exc),
                request_id=request_id,
            )
        return _worker_result(failed)

    saved = _save_worker_result(
        engine,
        completed,
        worker_id=worker_id,
        request_id=request_id,
    )
    return _worker_result(saved)


def build_configured_worker(
    settings: ServiceSettings | None = None,
    *,
    worker_id: str = "worker-dev-1",
) -> RunWorker | SubprocessRunWorker:
    service_settings = settings or load_service_settings()
    if not service_settings.database_url:
        raise ValueError("DATABASE_URL is required for worker service")

    engine = create_database_engine(service_settings.database_url, pool_pre_ping=True)
    if service_settings.worker_subprocess_isolation_enabled:
        return SubprocessRunWorker(
            engine=engine,
            worker_id=worker_id,
            timeout_seconds=service_settings.worker_subprocess_timeout_seconds,
            allow_legacy_queue_claim=service_settings.worker_legacy_queue_claim_enabled,
            heartbeat_interval_seconds=service_settings.worker_heartbeat_interval_seconds,
            cancel_poll_interval_seconds=service_settings.worker_cancel_poll_interval_seconds,
        )
    executor = build_configured_executor(service_settings)
    return RunWorker(
        engine=engine,
        worker_id=worker_id,
        executor=executor,
        allow_legacy_queue_claim=service_settings.worker_legacy_queue_claim_enabled,
        heartbeat_interval_seconds=service_settings.worker_heartbeat_interval_seconds,
    )


def build_configured_executor(settings: ServiceSettings) -> DockerTerminalWorkerExecutor:
    store = build_worker_artifact_store(settings)
    store.ensure_bucket()
    return DockerTerminalWorkerExecutor(
        artifact_persistence=ArtifactPersistence(store),
        workspace_root=Path(settings.sandbox_workspace_root),
        host_workspace_root=(
            Path(settings.sandbox_host_workspace_root)
            if settings.sandbox_host_workspace_root
            else None
        ),
        provider_registry=DevProviderConfigRegistry.from_settings(settings),
    )


def run_worker_loop(
    worker: RunWorker | SubprocessRunWorker,
    *,
    poll_interval_seconds: float = 5.0,
) -> None:
    while True:
        result = worker.run_once()
        if result is not None:
            print(json.dumps(result.to_dict(), sort_keys=True), flush=True)
        time.sleep(poll_interval_seconds)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Agentic Data Platform evaluation worker.")
    parser.add_argument("--once", action="store_true", help="process at most one queued run and exit")
    parser.add_argument("--worker-id", default="worker-dev-1")
    parser.add_argument("--poll-interval-seconds", type=float, default=5.0)
    args = parser.parse_args(argv)

    worker = build_configured_worker(worker_id=args.worker_id)
    if args.once:
        result = worker.run_once()
        print(json.dumps(result.to_dict() if result is not None else {"claimed": False}, sort_keys=True))
        return 0

    run_worker_loop(worker, poll_interval_seconds=args.poll_interval_seconds)
    return 0


def _worker_result(run) -> WorkerRunResult:
    return WorkerRunResult(
        run_id=run.run_id,
        status=run.status.value,
        artifact_count=len(run.artifacts),
        turn_count=len(run.trajectory),
        evaluator_id=run.evaluator_result.evaluator_id if run.evaluator_result is not None else None,
    )


def _save_worker_result(
    engine: Engine,
    run,
    *,
    worker_id: str,
    request_id: str | None,
):
    with session_scope(engine) as session:
        repository = RunRepository(session)
        try:
            return repository.save_worker_result(
                run,
                worker_id=worker_id,
                request_id=request_id,
            )
        except ValueError:
            current = repository.get_run(run.run_id)
            if current.status in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELED}:
                return current
            raise


def _run_child_command_with_cancel_monitor(
    command_runner: CommandRunner,
    args: list[str],
    *,
    timeout_seconds: int,
    engine: Engine,
    run_id: str,
    poll_interval_seconds: float,
) -> subprocess.CompletedProcess[str]:
    start = getattr(command_runner, "start", None)
    if start is None:
        return command_runner.run(args, timeout=timeout_seconds)

    process = start(args)
    deadline = time.monotonic() + timeout_seconds
    while True:
        if _run_is_canceled(engine, run_id=run_id):
            stdout, stderr = _terminate_process(process)
            return subprocess.CompletedProcess(
                args=args,
                returncode=process.returncode if process.returncode is not None else -15,
                stdout=_coerce_process_output(stdout),
                stderr=_coerce_process_output(stderr),
            )

        returncode = process.poll()
        if returncode is not None:
            stdout, stderr = process.communicate()
            return subprocess.CompletedProcess(
                args=args,
                returncode=returncode,
                stdout=_coerce_process_output(stdout),
                stderr=_coerce_process_output(stderr),
            )

        if time.monotonic() >= deadline:
            stdout, stderr = _terminate_process(process)
            raise subprocess.TimeoutExpired(
                cmd=args,
                timeout=timeout_seconds,
                output=_coerce_process_output(stdout),
                stderr=_coerce_process_output(stderr),
            )

        time.sleep(min(poll_interval_seconds, max(deadline - time.monotonic(), 0.001)))


def _run_is_canceled(engine: Engine, *, run_id: str) -> bool:
    with session_scope(engine) as session:
        return RunRepository(session).get_run(run_id).status is RunStatus.CANCELED


def _terminate_process(process) -> tuple[str, str]:
    process.terminate()
    try:
        stdout, stderr = process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
    return _coerce_process_output(stdout), _coerce_process_output(stderr)


def _coerce_process_output(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


class WorkerHeartbeat:
    def __init__(
        self,
        *,
        engine: Engine,
        run_id: str,
        worker_id: str,
        interval_seconds: int,
        request_id: str | None,
    ) -> None:
        self.engine = engine
        self.run_id = run_id
        self.worker_id = worker_id
        self.interval_seconds = interval_seconds
        self.request_id = request_id
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> WorkerHeartbeat:
        self._record(propagate=True)
        if self.interval_seconds > 0:
            self._thread = threading.Thread(
                target=self._run,
                name=f"worker-heartbeat-{self.run_id}",
                daemon=True,
            )
            self._thread.start()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(min(self.interval_seconds, 1), 0.1))

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self._record(propagate=False)

    def _record(self, *, propagate: bool) -> None:
        try:
            with session_scope(self.engine) as session:
                RunRepository(session).record_worker_heartbeat(
                    self.run_id,
                    worker_id=self.worker_id,
                    status=RunStatus.RUNNING,
                    request_id=self.request_id,
                )
        except Exception as exc:
            if propagate:
                raise
            print(
                json.dumps(
                    {
                        "event": "worker_heartbeat_failed",
                        "run_id": self.run_id,
                        "worker_id": self.worker_id,
                        "error": str(exc),
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
                flush=True,
            )


def _execution_child_args(
    *,
    run_id: str,
    worker_id: str,
    request_id: str | None,
) -> list[str]:
    args = [
        sys.executable,
        "-m",
        "agentic_data_platform.worker.execution_child",
        "--run-id",
        run_id,
        "--worker-id",
        worker_id,
    ]
    if request_id:
        args.extend(["--request-id", request_id])
    return args


def _fail_active_subprocess_run(
    engine: Engine,
    *,
    run_id: str,
    reason: str,
    request_id: str | None,
) -> WorkerRunResult:
    with session_scope(engine) as session:
        repository = RunRepository(session)
        run = repository.get_run(run_id)
        if run.status in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELED}:
            return _worker_result(run)
        failed = repository.transition_run(
            run_id,
            "failed",
            event_type=RunEventType.WORKER_SUBPROCESS_FAILED,
            reason=reason,
            request_id=request_id,
        )
        failed.metadata["failure"] = {
            "category": "worker_subprocess_failed",
            "message": reason,
        }
        repository.save_run(failed)
    return _worker_result(failed)


def build_worker_artifact_store(settings: ServiceSettings):
    if (
        settings.object_storage_endpoint
        and settings.object_storage_bucket
        and settings.object_storage_access_key
        and settings.object_storage_secret_key
    ):
        return build_s3_artifact_store(
            endpoint_url=settings.object_storage_endpoint,
            bucket=settings.object_storage_bucket,
            access_key=settings.object_storage_access_key,
            secret_key=settings.object_storage_secret_key,
            region=settings.object_storage_region,
        )
    return LocalArtifactStore(Path(".runtime/artifacts"))


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


if __name__ == "__main__":
    raise SystemExit(main())
