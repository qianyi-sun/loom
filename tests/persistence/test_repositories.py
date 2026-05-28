import unittest
from datetime import datetime, timezone

from sqlalchemy import inspect

from agentic_data_platform.benchmarks.fixtures import load_fixture_catalog
from agentic_data_platform.domain.run_records import (
    ArtifactKind,
    ArtifactRef,
    BenchmarkTaskInstance,
    EvaluatorResult,
    JudgeConfig,
    ModelConfig,
    ModelMode,
    RunnerConfig,
    RunnerKind,
    RunRecord,
    RunStatus,
    SandboxBackend,
    TerminalTurn,
)
from agentic_data_platform.persistence.database import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.repositories import (
    AuditEventRepository,
    BenchmarkCatalogRepository,
    IdentityRepository,
    ProjectRepository,
    RunRepository,
)


class PersistenceRepositoryTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_database_engine("sqlite+pysqlite:///:memory:")
        upgrade_database(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_initial_migration_creates_core_backend_tables(self):
        tables = set(inspect(self.engine).get_table_names())

        self.assertTrue(
            {
                "users",
                "teams",
                "projects",
                "benchmark_suites",
                "task_families",
                "task_instances",
                "runs",
                "run_attempts",
                "artifacts",
                "evaluator_results",
                "audit_events",
            }.issubset(tables)
        )

    def test_project_identity_repositories_create_read_list_and_update(self):
        with session_scope(self.engine) as session:
            identities = IdentityRepository(session)
            projects = ProjectRepository(session)

            identities.create_team(
                team_id="pilot-project",
                name="pilot group",
            )
            identities.create_user(
                user_id="[REDACTED_OWNER]",
                email="[REDACTED_OWNER]@example.com",
                display_name="[REDACTED_OWNER]",
                team_id="pilot-project",
            )
            projects.create_project(
                project_id="latent-skill-pilot",
                name="Latent Skill Pilot",
                owner_team_id="pilot-project",
                created_by_user_id="[REDACTED_OWNER]",
                description="SkillFlow and SkillLearnBench pilot",
            )
            projects.update_project(
                project_id="latent-skill-pilot",
                description="Terminal benchmark pilot for pilot group",
            )

            project = projects.get_project("latent-skill-pilot")
            listed = projects.list_projects(owner_team_id="pilot-project")

        self.assertEqual(project.project_id, "latent-skill-pilot")
        self.assertEqual(project.owner_team_id, "pilot-project")
        self.assertEqual(project.created_by_user_id, "[REDACTED_OWNER]")
        self.assertEqual(project.description, "Terminal benchmark pilot for pilot group")
        self.assertEqual([item.project_id for item in listed], ["latent-skill-pilot"])

    def test_benchmark_catalog_repository_round_trips_fixture_catalog(self):
        catalog = load_fixture_catalog("SkillFlow")

        with session_scope(self.engine) as session:
            repository = BenchmarkCatalogRepository(session)

            repository.upsert_fixture_catalog(catalog)
            repository.upsert_fixture_catalog(catalog)
            loaded = repository.get_fixture_catalog(
                suite_name=catalog.suite_name,
                benchmark_version=catalog.benchmark_version,
            )
            listed = repository.list_task_instances(
                suite_name=catalog.suite_name,
                benchmark_version=catalog.benchmark_version,
            )

        self.assertEqual(loaded.suite_name, catalog.suite_name)
        self.assertEqual(loaded.benchmark_version, catalog.benchmark_version)
        self.assertEqual(loaded.source_version, catalog.source_version)
        self.assertEqual(len(loaded.task_instances()), len(catalog.task_instances()))
        self.assertEqual(
            loaded.to_task_spec(
                task_family="OCR-Data-Extraction",
                instance_id="task_family_invoice_images",
            ).runner_contract,
            "skillflow-original-wrapper-v0",
        )
        self.assertEqual(len(listed), len(catalog.task_instances()))

    def test_run_repository_round_trips_domain_run_record_and_related_rows(self):
        run = _completed_run()

        with session_scope(self.engine) as session:
            IdentityRepository(session).create_team(
                team_id="pilot-project",
                name="pilot group",
            )
            ProjectRepository(session).create_project(
                project_id="pilot-project",
                name="pilot group",
                owner_team_id="pilot-project",
            )
            runs = RunRepository(session)
            audits = AuditEventRepository(session)

            runs.save_run(run)
            audits.record_event(
                event_type="run.saved",
                project_id=run.project_id,
                run_id=run.run_id,
                payload={"status": run.status.value},
            )

            loaded = runs.get_run(run.run_id)
            listed = runs.list_runs(project_id=run.project_id)
            events = audits.list_events(run_id=run.run_id)

        self.assertEqual(loaded.to_dict(), run.to_dict())
        self.assertEqual([item.run_id for item in listed], [run.run_id])
        self.assertEqual(events[0].event_type, "run.saved")
        self.assertEqual(events[0].payload, {"status": "succeeded"})

    def test_run_repository_replaces_existing_run_snapshot(self):
        run = _completed_run()

        with session_scope(self.engine) as session:
            IdentityRepository(session).create_team(
                team_id="pilot-project",
                name="pilot group",
            )
            ProjectRepository(session).create_project(
                project_id="pilot-project",
                name="pilot group",
                owner_team_id="pilot-project",
            )
            runs = RunRepository(session)
            audits = AuditEventRepository(session)

            runs.save_run(run)
            audits.record_event(
                event_type="run.created",
                project_id=run.project_id,
                run_id=run.run_id,
                attempt_id="run_001:attempt:1",
                payload={"status": run.status.value},
            )
            run.metadata["reviewed"] = True
            run.artifacts[0] = ArtifactRef(
                artifact_id="run_001-trajectory",
                kind=ArtifactKind.TRAJECTORY,
                uri="minio://runs/run_001/trajectory-v2.jsonl",
                media_type="application/x-ndjson",
                sha256="4" * 64,
                size_bytes=768,
                metadata={"storage_key": "runs/run_001/trajectory-v2.jsonl"},
            )
            runs.save_run(run)
            loaded = runs.get_run(run.run_id)
            events = audits.list_events(run_id=run.run_id)

        self.assertEqual(loaded.metadata["reviewed"], True)
        self.assertEqual(loaded.artifacts[0].uri, "minio://runs/run_001/trajectory-v2.jsonl")
        self.assertEqual(len(loaded.artifacts), len(run.artifacts))
        self.assertEqual(events[0].event_type, "run.created")


def _completed_run() -> RunRecord:
    run = RunRecord.create(
        run_id="run_001",
        project_id="pilot-project",
        owner_team="pilot group",
        task=BenchmarkTaskInstance(
            benchmark_suite="SkillFlow",
            benchmark_version="hf:zhang-ziao/SkillFlow-Task@2026-05-28",
            task_family="OCR-Data-Extraction",
            instance_id="task_family_invoice_images",
            source_uri="https://huggingface.co/datasets/zhang-ziao/SkillFlow-Task",
            input_artifact_refs=["minio://benchmarks/skillflow/input.tar.zst"],
            required_artifacts=["trajectory", "workspace_snapshot", "evaluator_report"],
            metadata={"instruction": "Extract invoice fields."},
        ),
        model=ModelConfig(
            provider="openai",
            model_name="gpt-5",
            mode=ModelMode.API,
            prompt_template_version="terminal-agent-v0",
            model_version="2026-05-28",
        ),
        runner=RunnerConfig(
            kind=RunnerKind.ORIGINAL_BENCHMARK,
            sandbox_backend=SandboxBackend.DOCKER_TERMINAL,
            image="python:3.12-slim",
            entrypoint=["python", "-m", "agentic_data_platform.benchmark_wrappers.skillflow"],
            internet_access=True,
            resource_limits={"cpu": 2, "memory_gib": 8, "timeout_seconds": 3600},
            metadata={"runner_contract": "skillflow-original-wrapper-v0"},
        ),
        metadata={"benchmark_adapter": "SkillFlow"},
    )
    run.transition_to(RunStatus.PROVISIONING)
    run.transition_to(RunStatus.RUNNING)
    run.add_turn(
        TerminalTurn(
            turn_index=0,
            command="python solve.py",
            cwd="/workspace",
            started_at=datetime(2026, 5, 28, 12, 0, 0, tzinfo=timezone.utc),
            completed_at=datetime(2026, 5, 28, 12, 0, 2, tzinfo=timezone.utc),
            exit_code=0,
            stdout="created answer.xlsx\n",
            stderr="",
            changed_paths=["answer.xlsx"],
            model_call_id="call_001",
        )
    )
    run.transition_to(RunStatus.EVALUATING)
    run.attach_artifact(
        ArtifactRef(
            artifact_id="run_001-trajectory",
            kind=ArtifactKind.TRAJECTORY,
            uri="minio://runs/run_001/trajectory.jsonl",
            media_type="application/x-ndjson",
            sha256="1" * 64,
            size_bytes=512,
            metadata={"storage_key": "runs/run_001/trajectory.jsonl"},
        )
    )
    run.attach_artifact(
        ArtifactRef(
            artifact_id="run_001-workspace-snapshot",
            kind=ArtifactKind.WORKSPACE_SNAPSHOT,
            uri="minio://runs/run_001/workspace/snapshot.json",
            media_type="application/json",
            sha256="2" * 64,
            size_bytes=2048,
            metadata={"storage_key": "runs/run_001/workspace/snapshot.json"},
        )
    )
    run.attach_evaluator_result(
        EvaluatorResult(
            evaluator_id="llm-judge-v0",
            status="completed",
            score=0.91,
            metrics={"task_success": True},
            verbal_feedback="The extracted invoice workbook is correct.",
            judge=JudgeConfig(
                provider="openai",
                model_name="gpt-5",
                rubric_version="latent-skill-benchmark-2026-05-28",
            ),
            artifact_refs=["minio://runs/run_001/evaluation/report.json"],
        )
    )
    run.attach_artifact(
        ArtifactRef(
            artifact_id="run_001-llm-judge-v0-report",
            kind=ArtifactKind.EVALUATOR_REPORT,
            uri="minio://runs/run_001/evaluation/report.json",
            media_type="application/json",
            sha256="3" * 64,
            size_bytes=1024,
            metadata={"storage_key": "runs/run_001/evaluation/report.json"},
        )
    )
    run.transition_to(RunStatus.SUCCEEDED)
    return run
