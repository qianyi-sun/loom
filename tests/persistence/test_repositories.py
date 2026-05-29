import unittest
from datetime import datetime, timezone

from sqlalchemy import inspect

from agentic_data_platform.benchmarks.fixtures import load_fixture_catalog
from agentic_data_platform.domain.run_records import (
    ArtifactKind,
    ArtifactRef,
    BenchmarkTaskInstance,
    EvaluatorResult,
    EvaluatorConfig,
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
        inspector = inspect(self.engine)
        tables = set(inspector.get_table_names())

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
                "run_status_events",
                "artifacts",
                "evaluator_results",
                "audit_events",
            }.issubset(tables)
        )
        evaluator_columns = {column["name"]: column for column in inspector.get_columns("evaluator_results")}
        self.assertIn("mode", evaluator_columns)
        self.assertTrue(evaluator_columns["judge_provider"]["nullable"])
        self.assertTrue(evaluator_columns["judge_model_name"]["nullable"])
        self.assertTrue(evaluator_columns["judge_rubric_version"]["nullable"])

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

    def test_run_repository_records_create_cancel_and_retry_lifecycle(self):
        run = _queued_run(run_id="run_lifecycle_001")

        with session_scope(self.engine) as session:
            identities = IdentityRepository(session)
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
            ProjectRepository(session).create_project(
                project_id="pilot-project",
                name="pilot group",
                owner_team_id="pilot-project",
                created_by_user_id="[REDACTED_OWNER]",
            )
            runs = RunRepository(session)

            runs.create_run(run, created_by_user_id="[REDACTED_OWNER]", request_id="req-create-001")
            canceled = runs.cancel_run(
                run.run_id,
                reason="user requested cancellation",
                actor_user_id="[REDACTED_OWNER]",
                request_id="req-cancel-001",
            )
            retried = runs.retry_run(
                run.run_id,
                reason="retry after cancelled dry run",
                actor_user_id="[REDACTED_OWNER]",
                request_id="req-retry-001",
            )
            retried.transition_to(RunStatus.PROVISIONING)
            retried.transition_to(RunStatus.RUNNING)
            retried.add_turn(
                TerminalTurn(
                    turn_index=0,
                    command="python retry_solve.py",
                    cwd="/workspace",
                    started_at=datetime(2026, 5, 28, 13, 0, 0, tzinfo=timezone.utc),
                    completed_at=datetime(2026, 5, 28, 13, 0, 2, tzinfo=timezone.utc),
                    exit_code=0,
                    stdout="created retry.xlsx\n",
                    stderr="",
                    changed_paths=["retry.xlsx"],
                )
            )
            retried.transition_to(RunStatus.EVALUATING)
            retried.attach_artifact(
                ArtifactRef(
                    artifact_id="run_lifecycle_001-retry-trajectory",
                    kind=ArtifactKind.TRAJECTORY,
                    uri="minio://runs/run_lifecycle_001/retry/trajectory.jsonl",
                    media_type="application/x-ndjson",
                    sha256="5" * 64,
                    size_bytes=512,
                    metadata={"storage_key": "runs/run_lifecycle_001/retry/trajectory.jsonl"},
                )
            )
            retried.attach_evaluator_result(
                EvaluatorResult(
                    evaluator_id="llm-judge-v0",
                    status="completed",
                    score=0.88,
                    metrics={"task_success": True},
                    verbal_feedback="Retry output is correct.",
                    judge=JudgeConfig(
                        provider="openai",
                        model_name="gpt-5",
                        rubric_version="latent-skill-benchmark-2026-05-28",
                    ),
                    artifact_refs=["minio://runs/run_lifecycle_001/retry/evaluation/report.json"],
                )
            )
            retried.transition_to(RunStatus.SUCCEEDED)
            runs.save_run(retried)

            loaded = runs.get_run(run.run_id)
            listed = runs.list_runs(
                project_id="pilot-project",
                benchmark_suite="SkillLearnBench",
                task_family="spreadsheet-from-documents",
                task_instance_id="conference-expense-03",
                created_by_user_id="[REDACTED_OWNER]",
                created_after=datetime(2000, 1, 1, tzinfo=timezone.utc),
                created_before=datetime(2100, 1, 1, tzinfo=timezone.utc),
            )
            events = runs.list_status_events(run.run_id)

        self.assertEqual(canceled.status, RunStatus.CANCELED)
        self.assertEqual(retried.status, RunStatus.SUCCEEDED)
        self.assertEqual(loaded.status, RunStatus.SUCCEEDED)
        self.assertEqual(loaded.created_by_user_id, "[REDACTED_OWNER]")
        self.assertEqual(loaded.evaluator_configs[0].evaluator_id, "llm-judge-v0")
        self.assertEqual(loaded.trajectory[0].command, "python retry_solve.py")
        self.assertEqual(loaded.evaluator_result.score, 0.88)
        self.assertEqual([item.run_id for item in listed], [run.run_id])
        self.assertEqual([event.event_type for event in events], ["run.created", "run.canceled", "run.retried"])
        self.assertEqual(events[0].from_status, None)
        self.assertEqual(events[0].to_status, RunStatus.QUEUED)
        self.assertEqual(events[1].from_status, RunStatus.QUEUED)
        self.assertEqual(events[1].to_status, RunStatus.CANCELED)
        self.assertEqual(events[1].reason, "user requested cancellation")
        self.assertEqual(events[1].actor_user_id, "[REDACTED_OWNER]")
        self.assertEqual(events[1].request_id, "req-cancel-001")
        self.assertEqual(events[2].from_status, RunStatus.CANCELED)
        self.assertEqual(events[2].to_status, RunStatus.QUEUED)
        self.assertEqual(events[2].attempt_id, "run_lifecycle_001:attempt:2")

    def test_run_round_trip_preserves_multiple_evaluator_results_for_latest_attempt(self):
        run = _completed_run(run_id="run_multi_eval_001")
        run.status = RunStatus.EVALUATING
        run.evaluator_result = None
        run.evaluator_results.clear()
        run.attach_evaluator_result(
            EvaluatorResult(
                evaluator_id="harbor-verifier-v1",
                mode="harbor_verifier",
                status="completed",
                score=0.65,
                metrics={"reward": 0.65},
                verbal_feedback="",
                judge=None,
                artifact_refs=["minio://runs/run_multi_eval_001/evaluation/harbor-verifier/report.json"],
                metadata={"verifier_version": "harbor-2026-05-29"},
            )
        )
        run.attach_evaluator_result(
            EvaluatorResult(
                evaluator_id="llm-judge-v0",
                mode="llm_judge",
                status="completed",
                score=0.91,
                metrics={"task_success": True},
                verbal_feedback="The extracted invoice workbook is correct.",
                judge=JudgeConfig(
                    provider="openai",
                    model_name="gpt-5",
                    rubric_version="latent-skill-benchmark-2026-05-28",
                ),
                artifact_refs=["minio://runs/run_multi_eval_001/evaluation/llm-judge/report.json"],
            )
        )
        run.transition_to(RunStatus.SUCCEEDED)

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
            RunRepository(session).save_run(run)

        with session_scope(self.engine) as session:
            loaded = RunRepository(session).get_run("run_multi_eval_001")

        self.assertEqual([result.evaluator_id for result in loaded.evaluator_results], ["harbor-verifier-v1", "llm-judge-v0"])
        self.assertEqual(loaded.evaluator_result.evaluator_id, "llm-judge-v0")
        self.assertEqual(loaded.evaluator_results[0].mode, "harbor_verifier")
        self.assertIsNone(loaded.evaluator_results[0].judge)
        self.assertEqual(loaded.evaluator_results[0].metadata["verifier_version"], "harbor-2026-05-29")
        self.assertIsNotNone(loaded.evaluator_results[0].created_at)

    def test_run_repository_claims_next_queued_run_once_for_worker(self):
        run = _queued_run(run_id="run_claim_001")

        with session_scope(self.engine) as session:
            identities = IdentityRepository(session)
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
            ProjectRepository(session).create_project(
                project_id="pilot-project",
                name="pilot group",
                owner_team_id="pilot-project",
                created_by_user_id="[REDACTED_OWNER]",
            )
            runs = RunRepository(session)

            runs.create_run(run, created_by_user_id="[REDACTED_OWNER]", request_id="req-create-001")
            claimed = runs.claim_next_queued_run(worker_id="worker-a", request_id="req-claim-001")
            claimed_again = runs.claim_next_queued_run(worker_id="worker-b", request_id="req-claim-002")
            events = runs.list_status_events(run.run_id)

        self.assertIsNotNone(claimed)
        self.assertEqual(claimed.run_id, "run_claim_001")
        self.assertEqual(claimed.status, RunStatus.PROVISIONING)
        self.assertIsNone(claimed_again)
        self.assertEqual([event.event_type for event in events], ["run.created", "run.claimed"])
        self.assertEqual(events[1].from_status, RunStatus.QUEUED)
        self.assertEqual(events[1].to_status, RunStatus.PROVISIONING)
        self.assertEqual(events[1].request_id, "req-claim-001")
        self.assertEqual(events[1].metadata["worker_id"], "worker-a")

    def test_run_repository_does_not_overwrite_canceled_run_with_late_worker_result(self):
        run = _queued_run(run_id="run_cancel_after_claim_001")

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

            runs.create_run(run, request_id="req-create-001")
            claimed = runs.claim_next_queued_run(worker_id="worker-a", request_id="req-claim-001")
            canceled = runs.cancel_run(
                run.run_id,
                reason="operator canceled during provisioning",
                request_id="req-cancel-001",
            )
            claimed.transition_to(RunStatus.RUNNING)
            claimed.transition_to(RunStatus.EVALUATING)
            claimed.attach_evaluator_result(
                EvaluatorResult(
                    evaluator_id="llm-judge-v0",
                    status="completed",
                    score=0.91,
                    metrics={"task_success": True},
                    verbal_feedback="Late worker result should be ignored.",
                    judge=JudgeConfig(
                        provider="mock",
                        model_name="deterministic-judge",
                        rubric_version="latent-skill-v0",
                    ),
                    artifact_refs=[],
                )
            )
            claimed.transition_to(RunStatus.SUCCEEDED)
            saved = runs.save_worker_result(
                claimed,
                worker_id="worker-a",
                request_id="req-worker-finish-001",
            )
            events = runs.list_status_events(run.run_id)

        self.assertEqual(canceled.status, RunStatus.CANCELED)
        self.assertEqual(saved.status, RunStatus.CANCELED)
        self.assertEqual(saved.failure_reason, "operator canceled during provisioning")
        self.assertEqual([event.event_type for event in events], ["run.created", "run.claimed", "run.canceled"])

    def test_run_repository_rejects_invalid_lifecycle_transitions(self):
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

            runs.save_run(run)

            with self.assertRaisesRegex(ValueError, "Invalid run status transition"):
                runs.cancel_run(run.run_id, reason="too late")

            with self.assertRaisesRegex(ValueError, "can only retry failed or canceled runs"):
                runs.retry_run(run.run_id, reason="retry succeeded run")


def _completed_run(run_id: str = "run_001") -> RunRecord:
    run = RunRecord.create(
        run_id=run_id,
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
        evaluator_configs=[
            EvaluatorConfig(
                evaluator_id="llm-judge-v0",
                mode="llm_judge",
                judge=JudgeConfig(
                    provider="openai",
                    model_name="gpt-5",
                    rubric_version="latent-skill-benchmark-2026-05-28",
                ),
            )
        ],
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
            artifact_id=f"{run_id}-trajectory",
            kind=ArtifactKind.TRAJECTORY,
            uri=f"minio://runs/{run_id}/trajectory.jsonl",
            media_type="application/x-ndjson",
            sha256="1" * 64,
            size_bytes=512,
            metadata={"storage_key": f"runs/{run_id}/trajectory.jsonl"},
        )
    )
    run.attach_artifact(
        ArtifactRef(
            artifact_id=f"{run_id}-workspace-snapshot",
            kind=ArtifactKind.WORKSPACE_SNAPSHOT,
            uri=f"minio://runs/{run_id}/workspace/snapshot.json",
            media_type="application/json",
            sha256="2" * 64,
            size_bytes=2048,
            metadata={"storage_key": f"runs/{run_id}/workspace/snapshot.json"},
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
            artifact_refs=[f"minio://runs/{run_id}/evaluation/report.json"],
        )
    )
    run.attach_artifact(
        ArtifactRef(
            artifact_id=f"{run_id}-llm-judge-v0-report",
            kind=ArtifactKind.EVALUATOR_REPORT,
            uri=f"minio://runs/{run_id}/evaluation/report.json",
            media_type="application/json",
            sha256="3" * 64,
            size_bytes=1024,
            metadata={"storage_key": f"runs/{run_id}/evaluation/report.json"},
        )
    )
    run.transition_to(RunStatus.SUCCEEDED)
    return run


def _queued_run(*, run_id: str) -> RunRecord:
    return RunRecord.create(
        run_id=run_id,
        project_id="pilot-project",
        owner_team="pilot group",
        task=BenchmarkTaskInstance(
            benchmark_suite="SkillLearnBench",
            benchmark_version="git:cxcscmu/SkillLearnBench@abc123",
            task_family="spreadsheet-from-documents",
            instance_id="conference-expense-03",
            source_uri="https://github.com/cxcscmu/SkillLearnBench",
            input_artifact_refs=["minio://benchmarks/skilllearnbench/conference/input.tar.zst"],
            required_artifacts=["trajectory", "workspace_snapshot", "evaluator_report"],
        ),
        model=ModelConfig(
            provider="openai",
            model_name="gpt-5",
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
        ),
        evaluator_configs=[
            EvaluatorConfig(
                evaluator_id="llm-judge-v0",
                mode="llm_judge",
                judge=JudgeConfig(
                    provider="openai",
                    model_name="gpt-5",
                    rubric_version="latent-skill-benchmark-2026-05-28",
                ),
            )
        ],
    )
