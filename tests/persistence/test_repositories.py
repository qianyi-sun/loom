import unittest
from datetime import datetime, timedelta, timezone

from sqlalchemy import inspect, select

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
from agentic_data_platform.persistence.models import RunAttemptRow, RunRow
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

    def test_run_repository_stores_bounded_terminal_stream_previews(self):
        run = _completed_run(run_id="run_large_stream_001")
        large_stdout = "stdout-line\n" * 7000
        large_stderr = "stderr-line\n" * 7000
        original_turn = run.trajectory[0]
        run.trajectory[0] = TerminalTurn(
            turn_index=original_turn.turn_index,
            command=original_turn.command,
            cwd=original_turn.cwd,
            started_at=original_turn.started_at,
            completed_at=original_turn.completed_at,
            exit_code=original_turn.exit_code,
            stdout=large_stdout,
            stderr=large_stderr,
            changed_paths=list(original_turn.changed_paths),
            model_call_id=original_turn.model_call_id,
            metadata=dict(original_turn.metadata),
        )

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
            loaded = RunRepository(session).get_run(run.run_id)

        loaded_turn = loaded.trajectory[0]
        self.assertLess(len(loaded_turn.stdout.encode("utf-8")), len(large_stdout.encode("utf-8")))
        self.assertLess(len(loaded_turn.stderr.encode("utf-8")), len(large_stderr.encode("utf-8")))
        self.assertIn("truncated", loaded_turn.stdout)
        self.assertIn("truncated", loaded_turn.stderr)
        self.assertTrue(loaded_turn.metadata["stdout_truncated"])
        self.assertTrue(loaded_turn.metadata["stderr_truncated"])
        self.assertEqual(loaded_turn.metadata["stdout_original_bytes"], len(large_stdout.encode("utf-8")))
        self.assertEqual(loaded_turn.metadata["stderr_original_bytes"], len(large_stderr.encode("utf-8")))

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

    def test_retry_attempt_can_save_reused_artifact_ids(self):
        run = _queued_run(run_id="run_retry_artifacts_001")
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
            first = runs.claim_next_queued_run(worker_id="worker-1")
            first.transition_to(RunStatus.RUNNING)
            first.attach_artifact(_log_artifact("run_retry_artifacts_001-harbor-runner-report"))
            first.transition_to(RunStatus.FAILED)
            runs.save_worker_result(first, worker_id="worker-1")

            runs.retry_run(run.run_id, reason="retry after ingestion fix")
            second = runs.claim_next_queued_run(worker_id="worker-2")
            second.transition_to(RunStatus.RUNNING)
            second.attach_artifact(_log_artifact("run_retry_artifacts_001-harbor-runner-report"))
            second.transition_to(RunStatus.FAILED)
            runs.save_worker_result(second, worker_id="worker-2")

            loaded = runs.get_run(run.run_id)

        self.assertEqual(loaded.status, RunStatus.FAILED)
        self.assertEqual(len(loaded.artifacts), 1)
        self.assertEqual(
            loaded.artifacts[0].artifact_id,
            "run_retry_artifacts_001-harbor-runner-report:attempt:2",
        )

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
            attempt = session.scalar(select(RunAttemptRow).where(RunAttemptRow.run_id == run.run_id))

        self.assertIsNotNone(claimed)
        self.assertEqual(claimed.run_id, "run_claim_001")
        self.assertEqual(claimed.status, RunStatus.PROVISIONING)
        self.assertIsNone(claimed_again)
        self.assertEqual([event.event_type for event in events], ["run.created", "run.claimed"])
        self.assertEqual(events[1].from_status, RunStatus.QUEUED)
        self.assertEqual(events[1].to_status, RunStatus.PROVISIONING)
        self.assertEqual(events[1].request_id, "req-claim-001")
        self.assertEqual(events[1].metadata["worker_id"], "worker-a")
        self.assertEqual(attempt.metadata_json["worker"]["worker_id"], "worker-a")
        self.assertEqual(attempt.metadata_json["worker"]["heartbeat_status"], "provisioning")
        self.assertIn("claimed_at", attempt.metadata_json["worker"])
        self.assertIn("last_heartbeat_at", attempt.metadata_json["worker"])

    def test_run_repository_dispatches_queued_runs_with_global_capacity(self):
        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)

            for index in range(3):
                runs.create_run(_queued_run(run_id=f"run_dispatch_global_{index}"))

            first_batch = runs.dispatch_queued_runs(
                scheduler_id="scheduler-a",
                max_runs=2,
                request_id="req-dispatch-global-001",
            )
            second_batch = runs.dispatch_queued_runs(
                scheduler_id="scheduler-b",
                max_runs=2,
                request_id="req-dispatch-global-002",
            )
            events = runs.list_status_events("run_dispatch_global_0")
            statuses = {
                run.run_id: run.status
                for run in runs.list_runs(project_id="pilot-project")
                if run.run_id.startswith("run_dispatch_global_")
            }

        self.assertEqual([run.run_id for run in first_batch], ["run_dispatch_global_0", "run_dispatch_global_1"])
        self.assertEqual(second_batch, [])
        self.assertEqual(statuses["run_dispatch_global_0"], RunStatus.DISPATCHED)
        self.assertEqual(statuses["run_dispatch_global_1"], RunStatus.DISPATCHED)
        self.assertEqual(statuses["run_dispatch_global_2"], RunStatus.QUEUED)
        self.assertEqual([event.event_type for event in events], ["run.created", "run.dispatched"])
        self.assertEqual(events[1].from_status, RunStatus.QUEUED)
        self.assertEqual(events[1].to_status, RunStatus.DISPATCHED)
        self.assertEqual(events[1].request_id, "req-dispatch-global-001")
        self.assertEqual(events[1].metadata["scheduler_id"], "scheduler-a")

    def test_run_repository_dispatch_respects_backend_and_project_capacity(self):
        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            identities = IdentityRepository(session)
            identities.create_team(team_id="foundation-model", name="pilot group")
            ProjectRepository(session).create_project(
                project_id="foundation-model",
                name="Model Research",
                owner_team_id="foundation-model",
            )
            runs = RunRepository(session)

            latent_a = _queued_run(run_id="run_dispatch_latent_a")
            latent_a.runner.metadata["harness_id"] = "harbor-local-docker"
            latent_b = _queued_run(run_id="run_dispatch_latent_b")
            latent_b.runner.metadata["harness_id"] = "harbor-local-docker"
            foundation = _queued_run(run_id="run_dispatch_foundation")
            foundation.project_id = "foundation-model"
            foundation.owner_team = "pilot group"
            foundation.runner.metadata["harness_id"] = "harbor-local-docker"

            runs.create_run(latent_a)
            runs.create_run(latent_b)
            runs.create_run(foundation)

            dispatched = runs.dispatch_queued_runs(
                scheduler_id="scheduler-a",
                max_runs=3,
                backend_limits={"harbor-local-docker": 2},
                project_limits={"pilot-project": 1, "foundation-model": 1},
                request_id="req-dispatch-capacity-001",
            )
            statuses = {
                run.run_id: run.status
                for run in runs.list_runs()
                if run.run_id.startswith("run_dispatch_")
            }

        self.assertEqual([run.run_id for run in dispatched], ["run_dispatch_latent_a", "run_dispatch_foundation"])
        self.assertEqual(statuses["run_dispatch_latent_a"], RunStatus.DISPATCHED)
        self.assertEqual(statuses["run_dispatch_latent_b"], RunStatus.QUEUED)
        self.assertEqual(statuses["run_dispatch_foundation"], RunStatus.DISPATCHED)

    def test_run_repository_dispatch_skips_canceled_runs(self):
        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            canceled = _queued_run(run_id="run_dispatch_canceled")
            eligible = _queued_run(run_id="run_dispatch_eligible")

            runs.create_run(canceled)
            runs.create_run(eligible)
            runs.cancel_run(canceled.run_id, reason="user canceled before dispatch")
            dispatched = runs.dispatch_queued_runs(scheduler_id="scheduler-a", max_runs=2)

        self.assertEqual([run.run_id for run in dispatched], ["run_dispatch_eligible"])
        self.assertEqual(dispatched[0].status, RunStatus.DISPATCHED)

    def test_run_repository_requeues_stale_dispatched_runs(self):
        stale_before = datetime.now(timezone.utc) - timedelta(minutes=10)
        stale_timestamp = stale_before - timedelta(seconds=1)
        fresh_timestamp = stale_before + timedelta(seconds=1)

        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            for run_id in ("run_recover_stale", "run_recover_fresh", "run_recover_queued"):
                runs.create_run(_queued_run(run_id=run_id))
            runs.dispatch_queued_runs(scheduler_id="scheduler-a", max_runs=2)

            session.get(RunRow, "run_recover_stale").updated_at = stale_timestamp
            session.get(RunRow, "run_recover_fresh").updated_at = fresh_timestamp

            recovered = runs.requeue_stale_dispatched_runs(
                older_than=stale_before,
                scheduler_id="scheduler-a",
                max_runs=10,
                request_id="req-recover-dispatched-001",
            )
            statuses = {
                run.run_id: run.status
                for run in runs.list_runs(project_id="pilot-project")
                if run.run_id.startswith("run_recover_")
            }
            events = runs.list_status_events("run_recover_stale")

        self.assertEqual([run.run_id for run in recovered], ["run_recover_stale"])
        self.assertEqual(statuses["run_recover_stale"], RunStatus.QUEUED)
        self.assertEqual(statuses["run_recover_fresh"], RunStatus.DISPATCHED)
        self.assertEqual(statuses["run_recover_queued"], RunStatus.QUEUED)
        self.assertEqual(events[-1].event_type, "run.recovered")
        self.assertEqual(events[-1].from_status, RunStatus.DISPATCHED)
        self.assertEqual(events[-1].to_status, RunStatus.QUEUED)
        self.assertEqual(events[-1].request_id, "req-recover-dispatched-001")
        self.assertEqual(events[-1].reason, "stale dispatched run expired")
        self.assertEqual(events[-1].metadata["scheduler_id"], "scheduler-a")
        self.assertEqual(events[-1].metadata["recovery"], "stale_dispatched")
        self.assertEqual(events[-1].metadata["stale_before"], stale_before.isoformat())

    def test_run_repository_requeue_stale_dispatched_respects_batch_limit(self):
        stale_before = datetime.now(timezone.utc) - timedelta(minutes=10)
        stale_timestamp = stale_before - timedelta(seconds=1)

        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            for index in range(3):
                runs.create_run(_queued_run(run_id=f"run_recover_batch_{index}"))
            runs.dispatch_queued_runs(scheduler_id="scheduler-a", max_runs=3)
            for index in range(3):
                session.get(RunRow, f"run_recover_batch_{index}").updated_at = stale_timestamp

            recovered = runs.requeue_stale_dispatched_runs(
                older_than=stale_before,
                scheduler_id="scheduler-a",
                max_runs=2,
            )
            statuses = {
                run.run_id: run.status
                for run in runs.list_runs(project_id="pilot-project")
                if run.run_id.startswith("run_recover_batch_")
            }

        self.assertEqual([run.run_id for run in recovered], ["run_recover_batch_0", "run_recover_batch_1"])
        self.assertEqual(statuses["run_recover_batch_0"], RunStatus.QUEUED)
        self.assertEqual(statuses["run_recover_batch_1"], RunStatus.QUEUED)
        self.assertEqual(statuses["run_recover_batch_2"], RunStatus.DISPATCHED)

    def test_run_repository_records_worker_heartbeat_for_active_run(self):
        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            runs.create_run(_queued_run(run_id="run_heartbeat_active"))
            runs.claim_next_queued_run(worker_id="worker-a")

            updated = runs.record_worker_heartbeat(
                "run_heartbeat_active",
                worker_id="worker-a",
                status=RunStatus.RUNNING,
                request_id="req-heartbeat-001",
            )
            attempt = session.scalar(select(RunAttemptRow).where(RunAttemptRow.run_id == "run_heartbeat_active"))

        self.assertEqual(updated.status, RunStatus.PROVISIONING)
        self.assertEqual(attempt.metadata_json["worker"]["worker_id"], "worker-a")
        self.assertEqual(attempt.metadata_json["worker"]["heartbeat_status"], "running")
        self.assertIn("last_heartbeat_at", attempt.metadata_json["worker"])

    def test_run_repository_fails_stale_active_runs_with_expired_worker_heartbeat(self):
        stale_before = datetime.now(timezone.utc) - timedelta(minutes=10)
        stale_heartbeat = (stale_before - timedelta(seconds=1)).isoformat()
        fresh_heartbeat = (stale_before + timedelta(seconds=1)).isoformat()

        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            for run_id in ("run_active_stale", "run_active_fresh", "run_active_no_heartbeat"):
                runs.create_run(_queued_run(run_id=run_id))
                runs.claim_next_queued_run(worker_id=f"worker-{run_id}")

            _set_worker_last_heartbeat(session, run_id="run_active_stale", heartbeat_at=stale_heartbeat)
            _set_worker_last_heartbeat(session, run_id="run_active_fresh", heartbeat_at=fresh_heartbeat)
            session.scalar(
                select(RunAttemptRow).where(RunAttemptRow.run_id == "run_active_no_heartbeat")
            ).metadata_json = {}

            recovered = runs.fail_stale_active_runs_by_heartbeat(
                older_than=stale_before,
                scheduler_id="scheduler-a",
                max_runs=10,
                request_id="req-recover-active-001",
            )
            statuses = {
                run.run_id: run.status
                for run in runs.list_runs(project_id="pilot-project")
                if run.run_id.startswith("run_active_")
            }
            events = runs.list_status_events("run_active_stale")

        self.assertEqual([run.run_id for run in recovered], ["run_active_stale"])
        self.assertEqual(statuses["run_active_stale"], RunStatus.FAILED)
        self.assertEqual(statuses["run_active_fresh"], RunStatus.PROVISIONING)
        self.assertEqual(statuses["run_active_no_heartbeat"], RunStatus.PROVISIONING)
        self.assertEqual(events[-1].event_type, "run.recovered")
        self.assertEqual(events[-1].from_status, RunStatus.PROVISIONING)
        self.assertEqual(events[-1].to_status, RunStatus.FAILED)
        self.assertEqual(events[-1].reason, "stale worker heartbeat expired")
        self.assertEqual(events[-1].metadata["scheduler_id"], "scheduler-a")
        self.assertEqual(events[-1].metadata["recovery"], "stale_worker_heartbeat")
        self.assertEqual(events[-1].metadata["worker_id"], "worker-run_active_stale")
        self.assertEqual(events[-1].metadata["last_heartbeat_at"], stale_heartbeat)

    def test_run_repository_fails_stale_active_runs_respects_batch_limit(self):
        stale_before = datetime.now(timezone.utc) - timedelta(minutes=10)
        stale_heartbeat = (stale_before - timedelta(seconds=1)).isoformat()

        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            for index in range(3):
                run_id = f"run_active_batch_{index}"
                runs.create_run(_queued_run(run_id=run_id))
                runs.claim_next_queued_run(worker_id=f"worker-{index}")
                _set_worker_last_heartbeat(session, run_id=run_id, heartbeat_at=stale_heartbeat)

            recovered = runs.fail_stale_active_runs_by_heartbeat(
                older_than=stale_before,
                scheduler_id="scheduler-a",
                max_runs=2,
            )
            statuses = {
                run.run_id: run.status
                for run in runs.list_runs(project_id="pilot-project")
                if run.run_id.startswith("run_active_batch_")
            }

        self.assertEqual([run.run_id for run in recovered], ["run_active_batch_0", "run_active_batch_1"])
        self.assertEqual(statuses["run_active_batch_0"], RunStatus.FAILED)
        self.assertEqual(statuses["run_active_batch_1"], RunStatus.FAILED)
        self.assertEqual(statuses["run_active_batch_2"], RunStatus.PROVISIONING)

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


def _log_artifact(artifact_id: str) -> ArtifactRef:
    return ArtifactRef(
        artifact_id=artifact_id,
        kind=ArtifactKind.LOG,
        uri=f"minio://runs/{artifact_id}.json",
        media_type="application/json",
        sha256="4" * 64,
        size_bytes=256,
        metadata={"storage_key": f"runs/{artifact_id}.json"},
    )


def _seed_latent_project(session) -> None:
    identities = IdentityRepository(session)
    identities.create_team(
        team_id="pilot-project",
        name="pilot group",
    )
    ProjectRepository(session).create_project(
        project_id="pilot-project",
        name="pilot group",
        owner_team_id="pilot-project",
    )


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


def _set_worker_last_heartbeat(session, *, run_id: str, heartbeat_at: str) -> None:
    attempt = session.scalar(select(RunAttemptRow).where(RunAttemptRow.run_id == run_id))
    metadata = dict(attempt.metadata_json or {})
    worker = dict(metadata["worker"])
    worker["last_heartbeat_at"] = heartbeat_at
    metadata["worker"] = worker
    attempt.metadata_json = metadata
