from __future__ import annotations

import argparse
import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import Engine

from agentic_data_platform.domain.run_records import (
    BenchmarkTaskInstance,
    EvaluatorConfig,
    JudgeConfig,
    ModelConfig,
    ModelMode,
    RunnerConfig,
    RunnerKind,
    RunRecord,
    RunStatus,
    SandboxBackend,
)
from agentic_data_platform.persistence import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.repositories import IdentityRepository, ProjectRepository, RunRepository
from agentic_data_platform.scheduler.service import RunScheduler, SchedulerDispatchResult
from agentic_data_platform.service.config import ServiceSettings, load_service_settings


@dataclass(frozen=True)
class SchedulerRaceSmokeResult:
    project_id: str
    scheduler_count: int
    queued_run_count: int
    max_active_runs: int
    scheduler_results: list[dict[str, object]]
    dispatched_run_ids: list[str]
    status_counts: dict[str, int]
    cleanup_status_counts: dict[str, int]

    @property
    def total_dispatched_count(self) -> int:
        return len(self.dispatched_run_ids)

    def to_dict(self) -> dict[str, object]:
        return {
            "project_id": self.project_id,
            "scheduler_count": self.scheduler_count,
            "queued_run_count": self.queued_run_count,
            "max_active_runs": self.max_active_runs,
            "scheduler_results": list(self.scheduler_results),
            "dispatched_run_ids": list(self.dispatched_run_ids),
            "total_dispatched_count": self.total_dispatched_count,
            "status_counts": dict(self.status_counts),
            "cleanup_status_counts": dict(self.cleanup_status_counts),
        }


def run_scheduler_race_smoke(
    *,
    engine: Engine,
    run_id_prefix: str,
    scheduler_id_prefix: str = "scheduler-race-smoke",
    scheduler_count: int = 2,
    queued_run_count: int = 6,
    max_active_runs: int = 2,
    project_max_active_runs: int | None = None,
) -> SchedulerRaceSmokeResult:
    _require_positive("scheduler_count", scheduler_count)
    _require_positive("queued_run_count", queued_run_count)
    _require_positive("max_active_runs", max_active_runs)
    project_limit = project_max_active_runs or max_active_runs
    _require_positive("project_max_active_runs", project_limit)
    _require_non_empty("run_id_prefix", run_id_prefix)
    _require_non_empty("scheduler_id_prefix", scheduler_id_prefix)

    project_id = f"{run_id_prefix}_project"
    _seed_queued_runs(
        engine=engine,
        run_id_prefix=run_id_prefix,
        project_id=project_id,
        queued_run_count=queued_run_count,
    )
    settings = _race_smoke_settings(max_active_runs=max_active_runs, project_id=project_id, project_limit=project_limit)
    scheduler_results = _run_concurrent_dispatch(
        engine=engine,
        settings=settings,
        scheduler_id_prefix=scheduler_id_prefix,
        scheduler_count=scheduler_count,
        run_id_prefix=run_id_prefix,
    )
    dispatched_run_ids = [
        run_id
        for result in scheduler_results
        for run_id in result.get("dispatched_run_ids", [])
        if isinstance(run_id, str)
    ]
    status_counts = _status_counts(engine=engine, project_id=project_id)
    cleanup_status_counts: dict[str, int]
    try:
        if len(dispatched_run_ids) != len(set(dispatched_run_ids)):
            raise RuntimeError(f"scheduler race smoke dispatched duplicate run ids: {dispatched_run_ids!r}")
        if len(dispatched_run_ids) > max_active_runs:
            raise RuntimeError(
                f"scheduler race smoke over-dispatched {len(dispatched_run_ids)} runs above max_active_runs={max_active_runs}: "
                f"{dispatched_run_ids!r}"
            )
        if status_counts.get(RunStatus.DISPATCHED.value, 0) > max_active_runs:
            raise RuntimeError(
                f"scheduler race smoke left {status_counts.get(RunStatus.DISPATCHED.value, 0)} dispatched rows "
                f"above max_active_runs={max_active_runs}"
            )
    finally:
        cleanup_status_counts = _cancel_smoke_runs(engine=engine, project_id=project_id, run_id_prefix=run_id_prefix)

    return SchedulerRaceSmokeResult(
        project_id=project_id,
        scheduler_count=scheduler_count,
        queued_run_count=queued_run_count,
        max_active_runs=max_active_runs,
        scheduler_results=scheduler_results,
        dispatched_run_ids=dispatched_run_ids,
        status_counts=status_counts,
        cleanup_status_counts=cleanup_status_counts,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a scheduler multi-instance dispatch race smoke.")
    parser.add_argument("--scheduler-id-prefix", default="scheduler-race-smoke")
    parser.add_argument(
        "--run-id-prefix",
        default=f"scheduler_race_smoke_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}",
    )
    parser.add_argument("--scheduler-count", type=int, default=2)
    parser.add_argument("--queued-run-count", type=int, default=6)
    parser.add_argument("--max-active-runs", type=int, default=2)
    parser.add_argument("--project-max-active-runs", type=int)
    args = parser.parse_args(argv)

    settings = load_service_settings()
    engine = create_database_engine(settings.database_url)
    try:
        upgrade_database(engine)
        result = run_scheduler_race_smoke(
            engine=engine,
            run_id_prefix=args.run_id_prefix,
            scheduler_id_prefix=args.scheduler_id_prefix,
            scheduler_count=args.scheduler_count,
            queued_run_count=args.queued_run_count,
            max_active_runs=args.max_active_runs,
            project_max_active_runs=args.project_max_active_runs,
        )
        print(json.dumps(result.to_dict(), sort_keys=True), flush=True)
        return 0
    finally:
        engine.dispose()


def _seed_queued_runs(
    *,
    engine: Engine,
    run_id_prefix: str,
    project_id: str,
    queued_run_count: int,
) -> None:
    with session_scope(engine) as session:
        smoke_name = f"Scheduler Race Smoke {project_id}"
        IdentityRepository(session).create_team(team_id=project_id, name=smoke_name)
        ProjectRepository(session).create_project(
            project_id=project_id,
            name=smoke_name,
            owner_team_id=project_id,
            description="Synthetic scheduler multi-instance race smoke project",
        )
        repository = RunRepository(session)
        for index in range(queued_run_count):
            repository.create_run(
                _race_smoke_run(
                    run_id=f"{run_id_prefix}_{index}",
                    project_id=project_id,
                    task_instance_id=f"task-{index}",
                ),
                request_id=f"{run_id_prefix}-create-{index}",
            )


def _run_concurrent_dispatch(
    *,
    engine: Engine,
    settings: ServiceSettings,
    scheduler_id_prefix: str,
    scheduler_count: int,
    run_id_prefix: str,
) -> list[dict[str, object]]:
    barrier = threading.Barrier(scheduler_count)

    def dispatch(index: int) -> SchedulerDispatchResult:
        scheduler = RunScheduler(
            engine=engine,
            scheduler_id=f"{scheduler_id_prefix}-{index}",
            settings=settings,
        )
        barrier.wait(timeout=10)
        return scheduler.dispatch_once(request_id=f"{run_id_prefix}-dispatch-{index}")

    results: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=scheduler_count) as executor:
        futures = [executor.submit(dispatch, index) for index in range(scheduler_count)]
        for future in as_completed(futures):
            results.append(future.result().to_dict())
    return sorted(results, key=lambda item: str(item.get("scheduler_id", "")))


def _race_smoke_settings(*, max_active_runs: int, project_id: str, project_limit: int) -> ServiceSettings:
    return ServiceSettings(
        app_name="agentic-data-platform-scheduler-race-smoke",
        environment="smoke",
        database_url="",
        redis_url="",
        object_storage_endpoint="",
        object_storage_bucket="",
        object_storage_access_key="",
        object_storage_secret_key="",
        object_storage_region="us-east-1",
        scheduler_global_max_active_runs=max_active_runs,
        scheduler_backend_max_active_runs={"harbor-local-docker": max_active_runs},
        scheduler_project_max_active_runs={project_id: project_limit},
        scheduler_provider_max_active_runs={"race-smoke-provider": max_active_runs},
        scheduler_model_max_active_runs={"race-smoke-model": max_active_runs},
        scheduler_agent_max_active_runs={"race-smoke-agent": max_active_runs},
        scheduler_benchmark_max_active_runs={"race-smoke@dev": max_active_runs},
    )


def _status_counts(*, engine: Engine, project_id: str) -> dict[str, int]:
    counts = {status.value: 0 for status in RunStatus}
    with session_scope(engine) as session:
        for run in RunRepository(session).list_runs(project_id=project_id):
            counts[run.status.value] += 1
    return counts


def _cancel_smoke_runs(*, engine: Engine, project_id: str, run_id_prefix: str) -> dict[str, int]:
    cancelable_statuses = {
        RunStatus.QUEUED,
        RunStatus.DISPATCHED,
        RunStatus.PROVISIONING,
        RunStatus.RUNNING,
        RunStatus.EVALUATING,
    }
    with session_scope(engine) as session:
        repository = RunRepository(session)
        for run in repository.list_runs(project_id=project_id):
            if not run.run_id.startswith(f"{run_id_prefix}_"):
                continue
            if run.status in cancelable_statuses:
                repository.cancel_run(
                    run.run_id,
                    reason="Scheduler race smoke cleanup.",
                    request_id=f"{run_id_prefix}-cleanup",
                )
    return _status_counts(engine=engine, project_id=project_id)


def _race_smoke_run(*, run_id: str, project_id: str, task_instance_id: str) -> RunRecord:
    return RunRecord.create(
        run_id=run_id,
        project_id=project_id,
        owner_team="Scheduler Race Smoke",
        task=BenchmarkTaskInstance(
            benchmark_suite="SchedulerRaceSmoke",
            benchmark_version="race-smoke@dev",
            task_family="dispatch-race",
            instance_id=task_instance_id,
            source_uri="internal://scheduler-race-smoke",
            input_artifact_refs=[],
            required_artifacts=["trajectory", "workspace_snapshot", "evaluator_report"],
            metadata={"instruction": "Synthetic queued run for scheduler multi-instance race smoke."},
        ),
        model=ModelConfig(
            provider="race-smoke-provider",
            model_name="race-smoke-model",
            mode=ModelMode.API,
            prompt_template_version="scheduler-race-smoke-v0",
        ),
        runner=RunnerConfig(
            kind=RunnerKind.CUSTOM_PIPELINE,
            sandbox_backend=SandboxBackend.DOCKER_TERMINAL,
            image="scheduler-race-smoke",
            entrypoint=["/bin/sh", "-lc", "true"],
            internet_access=False,
            resource_limits={"cpu": 1, "memory_gib": 1, "timeout_seconds": 60},
            metadata={"harness_id": "harbor-local-docker", "agent_id": "race-smoke-agent"},
        ),
        evaluator_configs=[
            EvaluatorConfig(
                evaluator_id="scheduler-race-smoke",
                mode="llm_judge",
                judge=JudgeConfig(
                    provider="race-smoke-provider",
                    model_name="race-smoke-model",
                    rubric_version="scheduler-race-smoke-v0",
                ),
            )
        ],
    )


def _require_non_empty(name: str, value: str) -> None:
    if not value:
        raise ValueError(f"{name} must be a non-empty string")


def _require_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be positive")


if __name__ == "__main__":
    raise SystemExit(main())
