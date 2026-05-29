from __future__ import annotations

import argparse
import json
from uuid import uuid4

from sqlalchemy import Engine

from agentic_data_platform.artifacts.store import ArtifactPersistence
from agentic_data_platform.benchmarks.fixtures import load_fixture_catalogs
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
from agentic_data_platform.persistence.repositories import (
    BenchmarkCatalogRepository,
    IdentityRepository,
    ProjectRepository,
    RunRepository,
)
from agentic_data_platform.service.config import load_service_settings
from agentic_data_platform.worker.executors import FixtureTerminalBenchmarkExecutor
from agentic_data_platform.worker.service import RunWorker, build_worker_artifact_store


def run_worker_smoke(
    *,
    engine: Engine,
    artifact_persistence: ArtifactPersistence,
    run_id: str,
    worker_id: str = "worker-smoke",
) -> dict[str, object]:
    _seed_smoke_run(engine=engine, run_id=run_id)
    worker = RunWorker(
        engine=engine,
        worker_id=worker_id,
        executor=FixtureTerminalBenchmarkExecutor(artifact_persistence=artifact_persistence),
    )
    result = worker.run_once(request_id=f"{run_id}-request")
    if result is None:
        raise RuntimeError("worker smoke could not claim the seeded run")
    if result.status != "succeeded":
        raise RuntimeError(f"worker smoke failed with status: {result.status}")
    return result.to_dict()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Seed and execute one fixture run through the worker.")
    parser.add_argument("--run-id", default=f"worker_smoke_{uuid4().hex}")
    parser.add_argument("--worker-id", default="worker-smoke")
    args = parser.parse_args(argv)

    settings = load_service_settings()
    if not settings.database_url:
        raise ValueError("DATABASE_URL is required for worker smoke")

    engine = create_database_engine(settings.database_url, pool_pre_ping=True)
    store = build_worker_artifact_store(settings)
    store.ensure_bucket()
    result = run_worker_smoke(
        engine=engine,
        artifact_persistence=ArtifactPersistence(store),
        run_id=args.run_id,
        worker_id=args.worker_id,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


def _seed_smoke_run(*, engine: Engine, run_id: str) -> None:
    with session_scope(engine) as session:
        identities = IdentityRepository(session)
        identities.create_team(
            team_id="pilot-project",
            name="pilot group",
        )
        identities.create_user(
            user_id="worker-smoke",
            email="worker-smoke@example.com",
            display_name="Worker Smoke",
            team_id="pilot-project",
        )
        identities.create_user(
            user_id="[REDACTED_OWNER]",
            email="[REDACTED_OWNER]@example.com",
            display_name="[REDACTED_OWNER]",
            team_id="pilot-project",
        )
        projects = ProjectRepository(session)
        projects.create_project(
            project_id="worker-smoke",
            name="Worker Smoke",
            owner_team_id="pilot-project",
            created_by_user_id="worker-smoke",
            description="Deployment smoke project for queue and worker orchestration.",
        )
        projects.create_project(
            project_id="pilot-project",
            name="pilot group",
            owner_team_id="pilot-project",
            created_by_user_id="[REDACTED_OWNER]",
            description="Shared development project for authenticated API smoke checks.",
        )
        catalogs = BenchmarkCatalogRepository(session)
        for catalog in load_fixture_catalogs():
            catalogs.upsert_fixture_catalog(catalog)
        RunRepository(session).create_run(
            _smoke_run(run_id),
            created_by_user_id="worker-smoke",
            request_id=f"{run_id}-seed",
        )


def _smoke_run(run_id: str) -> RunRecord:
    return RunRecord.create(
        run_id=run_id,
        project_id="worker-smoke",
        owner_team="pilot group",
        task=BenchmarkTaskInstance(
            benchmark_suite="SkillLearnBench",
            benchmark_version="git:cxcscmu/SkillLearnBench@worker-smoke",
            task_family="spreadsheet-from-documents",
            instance_id="conference-expense-03",
            source_uri="https://github.com/cxcscmu/SkillLearnBench",
            input_artifact_refs=["s3://agentic-data-shared dev/benchmarks/skilllearnbench/input.tar.zst"],
            required_artifacts=["trajectory", "workspace_snapshot", "evaluator_report"],
            metadata={"instruction": "Read receipts and create receipts.xlsx."},
        ),
        model=ModelConfig(
            provider="mock-api",
            model_name="scripted-terminal-agent",
            mode=ModelMode.API,
            prompt_template_version="terminal-agent-v0",
        ),
        runner=RunnerConfig(
            kind=RunnerKind.ORIGINAL_BENCHMARK,
            sandbox_backend=SandboxBackend.DOCKER_TERMINAL,
            image="python:3.12-slim",
            entrypoint=["python", "-m", "skilllearnbench.runner"],
            internet_access=True,
            resource_limits={"cpu": 2, "memory_gib": 8, "timeout_seconds": 3600},
            metadata={"runner_contract": "skilllearnbench-original-wrapper-v0"},
        ),
        evaluator_configs=[
            EvaluatorConfig(
                evaluator_id="mock-judge-v0",
                mode="llm_judge",
                judge=JudgeConfig(
                    provider="mock",
                    model_name="deterministic-judge",
                    rubric_version="latent-skill-v0",
                ),
            )
        ],
        created_by_user_id="worker-smoke",
        metadata={"worker_fixture_commands": ["python solve.py"]},
    )


if __name__ == "__main__":
    raise SystemExit(main())
