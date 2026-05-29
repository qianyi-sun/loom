from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import Engine

from agentic_data_platform.artifacts.store import (
    ArtifactPersistence,
    LocalArtifactStore,
    build_s3_artifact_store,
)
from agentic_data_platform.persistence import create_database_engine, session_scope
from agentic_data_platform.persistence.repositories import RunRepository
from agentic_data_platform.providers.config import DevProviderConfigRegistry
from agentic_data_platform.service.config import ServiceSettings, load_service_settings
from agentic_data_platform.worker.executors import FixtureTerminalBenchmarkExecutor, WorkerRunExecutor


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
    ) -> None:
        _require_non_empty("worker_id", worker_id)
        self.engine = engine
        self.worker_id = worker_id
        self.executor = executor

    def run_once(self, *, request_id: str | None = None) -> WorkerRunResult | None:
        with session_scope(self.engine) as session:
            repository = RunRepository(session)
            claimed = repository.claim_next_queued_run(worker_id=self.worker_id, request_id=request_id)

        if claimed is None:
            return None

        try:
            completed = self.executor.execute(claimed)
        except Exception as exc:
            with session_scope(self.engine) as session:
                failed = RunRepository(session).transition_run(
                    claimed.run_id,
                    "failed",
                    event_type="run.worker_failed",
                    reason=str(exc),
                    request_id=request_id,
                )
            return _worker_result(failed)

        with session_scope(self.engine) as session:
            saved = RunRepository(session).save_worker_result(
                completed,
                worker_id=self.worker_id,
                request_id=request_id,
            )
        return _worker_result(saved)


def build_configured_worker(
    settings: ServiceSettings | None = None,
    *,
    worker_id: str = "worker-dev-1",
) -> RunWorker:
    service_settings = settings or load_service_settings()
    if not service_settings.database_url:
        raise ValueError("DATABASE_URL is required for worker service")

    engine = create_database_engine(service_settings.database_url, pool_pre_ping=True)
    store = build_worker_artifact_store(service_settings)
    store.ensure_bucket()
    return RunWorker(
        engine=engine,
        worker_id=worker_id,
        executor=FixtureTerminalBenchmarkExecutor(
            artifact_persistence=ArtifactPersistence(store),
            provider_registry=DevProviderConfigRegistry.from_settings(service_settings),
        ),
    )


def run_worker_loop(
    worker: RunWorker,
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
