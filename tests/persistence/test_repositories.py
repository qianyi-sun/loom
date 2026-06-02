import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from sqlalchemy import inspect, select

from agentic_data_platform.benchmarks.fixtures import load_fixture_catalog
from agentic_data_platform.domain.artifact_metadata import ArtifactUploadStatus
from agentic_data_platform.domain.artifact_metadata import (
    ArtifactChunkKind,
    ArtifactChunkMetadata,
)
from agentic_data_platform.domain.execution_metadata import (
    EXECUTION_ATTEMPT_METADATA_SCHEMA_VERSION,
    RunnerProcessStatus,
    SchedulerLeaseStatus,
)
from agentic_data_platform.domain.execution_events import RecoveryReasonCode, RunEventType
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
from agentic_data_platform.persistence.models import ArtifactRow, RunAttemptRow, RunDashboardProjectionRow, RunRow
from agentic_data_platform.persistence.repositories import (
    AuditEventRepository,
    BenchmarkCatalogRepository,
    DuplicateExecutionTaskError,
    IdentityRepository,
    ProjectRepository,
    RunRepository,
    StaleExecutionTaskError,
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
                "run_dashboard_projections",
                "artifacts",
                "artifact_chunks",
                "evaluator_results",
                "audit_events",
            }.issubset(tables)
        )
        artifact_chunk_columns = {column["name"]: column for column in inspector.get_columns("artifact_chunks")}
        self.assertIn("storage_key", artifact_chunk_columns)
        self.assertIn("upload_status", artifact_chunk_columns)
        self.assertIn("upload_error_reason", artifact_chunk_columns)
        artifact_chunk_indexes = {index["name"] for index in inspector.get_indexes("artifact_chunks")}
        self.assertIn("ix_artifact_chunks_run_attempt_kind_sequence", artifact_chunk_indexes)
        self.assertIn("ix_artifact_chunks_upload_status_created", artifact_chunk_indexes)
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

    def test_save_worker_result_refreshes_terminal_dashboard_projection(self):
        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            runs.create_run(_queued_run(run_id="run_projection_terminal"))
            claimed = runs.claim_next_queued_run(worker_id="worker-projection")
            claimed.transition_to(RunStatus.RUNNING)
            claimed.add_turn(
                TerminalTurn(
                    turn_index=0,
                    command="python solve.py",
                    cwd="/workspace",
                    started_at=datetime(2026, 5, 28, 13, 0, 0, tzinfo=timezone.utc),
                    completed_at=datetime(2026, 5, 28, 13, 0, 2, tzinfo=timezone.utc),
                    exit_code=0,
                    stdout="created answer.txt\n",
                    stderr="",
                    changed_paths=["answer.txt"],
                )
            )
            claimed.attach_artifact(_log_artifact("run_projection_terminal-trajectory"))
            claimed.transition_to(RunStatus.EVALUATING)
            claimed.attach_evaluator_result(
                EvaluatorResult(
                    evaluator_id="harbor-verifier-v1",
                    mode="harbor_verifier",
                    status="completed",
                    score=1.0,
                    metrics={"reward": 1.0},
                    verbal_feedback="Verifier accepted the final answer.",
                    judge=None,
                    artifact_refs=["minio://runs/run_projection_terminal/evaluation/verifier.json"],
                )
            )
            claimed.transition_to(RunStatus.SUCCEEDED)

            saved = runs.save_worker_result(
                claimed,
                worker_id="worker-projection",
                request_id="req-projection-terminal-001",
            )
            projection = session.get(RunDashboardProjectionRow, "run_projection_terminal")
            events = runs.list_status_events("run_projection_terminal")

        self.assertEqual(saved.status, RunStatus.SUCCEEDED)
        self.assertIsNotNone(projection)
        self.assertEqual(projection.status, RunStatus.SUCCEEDED.value)
        self.assertFalse(projection.dirty)
        self.assertEqual(projection.refresh_reason, "terminal_worker_result")
        self.assertEqual(projection.source_event_seq, events[-1].seq)
        self.assertEqual(projection.payload["status"], "succeeded")
        self.assertEqual(projection.payload["progress"]["turn_count"], 1)
        self.assertEqual(projection.payload["progress"]["artifact_count"], 1)
        self.assertEqual(projection.payload["evaluator"]["mode"], "harbor_verifier")

    def test_refresh_terminal_dashboard_projections_repairs_missing_and_respects_batch_limit(self):
        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            for index in range(2):
                runs.save_run(_completed_run(run_id=f"run_projection_repair_{index}"))

            refreshed = runs.refresh_terminal_dashboard_projections(
                scheduler_id="scheduler-projection",
                max_runs=1,
                request_id="req-projection-repair-001",
            )
            projection_0 = session.get(RunDashboardProjectionRow, "run_projection_repair_0")
            projection_1 = session.get(RunDashboardProjectionRow, "run_projection_repair_1")

        self.assertEqual([projection.run_id for projection in refreshed], ["run_projection_repair_0"])
        self.assertIsNotNone(projection_0)
        self.assertIsNone(projection_1)
        self.assertEqual(projection_0.status, RunStatus.SUCCEEDED.value)
        self.assertFalse(projection_0.dirty)
        self.assertEqual(projection_0.refresh_reason, "projection_recovery")

    def test_expire_stale_artifact_uploads_marks_pending_and_started_rows_with_recovery_events(self):
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=5)
        stale_timestamp = now - timedelta(minutes=10)

        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            runs.save_run(_completed_run(run_id="run_artifact_expiry"))

            pending = session.get(ArtifactRow, "run_artifact_expiry-trajectory")
            pending.metadata_json = {
                **dict(pending.metadata_json or {}),
                "upload_status": ArtifactUploadStatus.PENDING.value,
                "upload_started_at": stale_timestamp.isoformat(),
            }
            pending.created_at = stale_timestamp

            started = session.get(ArtifactRow, "run_artifact_expiry-workspace-snapshot")
            started.metadata_json = {
                **dict(started.metadata_json or {}),
                "upload_status": ArtifactUploadStatus.STARTED.value,
                "upload_started_at": stale_timestamp.isoformat(),
            }
            started.created_at = stale_timestamp + timedelta(seconds=1)

            completed = session.get(ArtifactRow, "run_artifact_expiry-llm-judge-v0-report")
            completed.metadata_json = {
                **dict(completed.metadata_json or {}),
                "upload_status": ArtifactUploadStatus.COMPLETED.value,
            }
            completed.created_at = stale_timestamp

            expired = runs.expire_stale_artifact_uploads(
                older_than=cutoff,
                scheduler_id="scheduler-artifact-expiry",
                max_artifacts=10,
                request_id="req-artifact-expiry-001",
            )
            events = runs.list_status_events("run_artifact_expiry")

        self.assertEqual(
            [item.artifact_id for item in expired],
            ["run_artifact_expiry-trajectory", "run_artifact_expiry-workspace-snapshot"],
        )
        self.assertEqual([item.previous_upload_status for item in expired], ["pending", "started"])

        with session_scope(self.engine) as session:
            pending = session.get(ArtifactRow, "run_artifact_expiry-trajectory")
            started = session.get(ArtifactRow, "run_artifact_expiry-workspace-snapshot")
            completed = session.get(ArtifactRow, "run_artifact_expiry-llm-judge-v0-report")

        self.assertEqual(pending.metadata_json["upload_status"], ArtifactUploadStatus.EXPIRED.value)
        self.assertEqual(started.metadata_json["upload_status"], ArtifactUploadStatus.EXPIRED.value)
        self.assertEqual(completed.metadata_json["upload_status"], ArtifactUploadStatus.COMPLETED.value)
        self.assertEqual(pending.metadata_json["upload_recovery"], "artifact_upload_expired")
        self.assertEqual(pending.metadata_json["upload_recovery_scheduler_id"], "scheduler-artifact-expiry")
        self.assertIn("upload_expired_at", pending.metadata_json)
        self.assertIn("upload_error_reason", pending.metadata_json)

        recovery_events = [event for event in events if event.event_type == RunEventType.RECOVERED.value]
        self.assertEqual([event.metadata["artifact_id"] for event in recovery_events], [
            "run_artifact_expiry-trajectory",
            "run_artifact_expiry-workspace-snapshot",
        ])
        self.assertEqual(recovery_events[0].metadata["recovery"], RecoveryReasonCode.ARTIFACT_UPLOAD_EXPIRED.value)
        self.assertEqual(recovery_events[0].from_status, RunStatus.SUCCEEDED)
        self.assertEqual(recovery_events[0].to_status, RunStatus.SUCCEEDED)
        artifact_events = [event for event in events if event.event_type == RunEventType.ARTIFACT_UPLOAD_EXPIRED.value]
        self.assertEqual([event.metadata["artifact_id"] for event in artifact_events], [
            "run_artifact_expiry-trajectory",
            "run_artifact_expiry-workspace-snapshot",
        ])
        self.assertEqual(artifact_events[0].metadata["previous_upload_status"], "pending")
        self.assertEqual(artifact_events[0].metadata["upload_status"], ArtifactUploadStatus.EXPIRED.value)
        self.assertEqual(artifact_events[0].metadata["scheduler_id"], "scheduler-artifact-expiry")
        self.assertEqual(artifact_events[0].metadata["recovery"], RecoveryReasonCode.ARTIFACT_UPLOAD_EXPIRED.value)
        self.assertEqual(artifact_events[0].from_status, RunStatus.SUCCEEDED)
        self.assertEqual(artifact_events[0].to_status, RunStatus.SUCCEEDED)

    def test_expire_stale_artifact_uploads_respects_batch_limit(self):
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=5)
        stale_timestamp = now - timedelta(minutes=10)

        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            runs.save_run(_completed_run(run_id="run_artifact_expiry_batch_0"))
            runs.save_run(_completed_run(run_id="run_artifact_expiry_batch_1"))
            for artifact_id in [
                "run_artifact_expiry_batch_0-trajectory",
                "run_artifact_expiry_batch_1-trajectory",
            ]:
                artifact = session.get(ArtifactRow, artifact_id)
                artifact.metadata_json = {
                    **dict(artifact.metadata_json or {}),
                    "upload_status": ArtifactUploadStatus.STARTED.value,
                    "upload_started_at": stale_timestamp.isoformat(),
                }
                artifact.created_at = stale_timestamp

            expired = runs.expire_stale_artifact_uploads(
                older_than=cutoff,
                scheduler_id="scheduler-artifact-expiry",
                max_artifacts=1,
            )

        self.assertEqual([item.artifact_id for item in expired], ["run_artifact_expiry_batch_0-trajectory"])
        with session_scope(self.engine) as session:
            expired_row = session.get(ArtifactRow, "run_artifact_expiry_batch_0-trajectory")
            still_started = session.get(ArtifactRow, "run_artifact_expiry_batch_1-trajectory")

        self.assertEqual(expired_row.metadata_json["upload_status"], ArtifactUploadStatus.EXPIRED.value)
        self.assertEqual(still_started.metadata_json["upload_status"], ArtifactUploadStatus.STARTED.value)

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

    def test_run_repository_records_ordered_artifact_log_chunk_indexes(self):
        run = _completed_run(run_id="run_log_chunks")
        now = datetime(2026, 6, 2, 12, 0, 0, tzinfo=timezone.utc)

        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            runs.save_run(run)
            runs.record_artifact_chunk(
                _artifact_chunk(
                    run_id="run_log_chunks",
                    attempt_id="run_log_chunks:attempt:1",
                    artifact_id="run_log_chunks-trajectory",
                    chunk_kind=ArtifactChunkKind.STDOUT,
                    chunk_sequence=1,
                    storage_key="runs/run_log_chunks/tasks/task/logs/stdout/000001.jsonl",
                    sha256="1" * 64,
                    size_bytes=64,
                    created_at=now,
                )
            )
            runs.record_artifact_chunk(
                _artifact_chunk(
                    run_id="run_log_chunks",
                    attempt_id="run_log_chunks:attempt:1",
                    artifact_id="run_log_chunks-trajectory",
                    chunk_kind=ArtifactChunkKind.STDOUT,
                    chunk_sequence=0,
                    storage_key="runs/run_log_chunks/tasks/task/logs/stdout/000000.jsonl",
                    sha256="2" * 64,
                    size_bytes=32,
                    created_at=now,
                )
            )
            runs.record_artifact_chunk(
                _artifact_chunk(
                    run_id="run_log_chunks",
                    attempt_id="run_log_chunks:attempt:1",
                    artifact_id="run_log_chunks-trajectory",
                    chunk_kind=ArtifactChunkKind.STDOUT,
                    chunk_sequence=1,
                    storage_key="runs/run_log_chunks/tasks/task/logs/stdout/000001.jsonl",
                    sha256="3" * 64,
                    size_bytes=128,
                    upload_status=ArtifactUploadStatus.FAILED,
                    upload_error_reason="object store write failed",
                    created_at=now + timedelta(seconds=1),
                    metadata={"retry_count": 1},
                )
            )
            runs.record_artifact_chunk(
                _artifact_chunk(
                    run_id="run_log_chunks",
                    attempt_id="run_log_chunks:attempt:1",
                    artifact_id="run_log_chunks-trajectory",
                    chunk_kind=ArtifactChunkKind.STDERR,
                    chunk_sequence=0,
                    storage_key="runs/run_log_chunks/tasks/task/logs/stderr/000000.jsonl",
                    sha256="4" * 64,
                    size_bytes=8,
                    created_at=now,
                )
            )
            runs.record_artifact_chunk(
                _artifact_chunk(
                    run_id="run_log_chunks",
                    attempt_id="run_log_chunks:attempt:1",
                    artifact_id="run_log_chunks-trajectory",
                    chunk_kind=ArtifactChunkKind.TRAJECTORY,
                    chunk_sequence=0,
                    storage_key="runs/run_log_chunks/tasks/task/trajectory/000000.jsonl",
                    sha256="5" * 64,
                    size_bytes=16,
                    created_at=now,
                )
            )

            stdout_chunks = runs.list_artifact_chunks(
                run_id="run_log_chunks",
                attempt_id="run_log_chunks:attempt:1",
                chunk_kind=ArtifactChunkKind.STDOUT,
            )
            stdout_after_zero = runs.list_artifact_chunks(
                run_id="run_log_chunks",
                chunk_kind=ArtifactChunkKind.STDOUT,
                after_sequence=0,
            )
            first_chunk = runs.list_artifact_chunks(
                run_id="run_log_chunks",
                chunk_kind=ArtifactChunkKind.STDOUT,
                limit=1,
            )
            events = runs.list_status_events("run_log_chunks")

        self.assertEqual([chunk.chunk_sequence for chunk in stdout_chunks], [0, 1])
        self.assertEqual([chunk.size_bytes for chunk in stdout_chunks], [32, 128])
        self.assertEqual(stdout_chunks[1].upload_status, ArtifactUploadStatus.FAILED)
        self.assertEqual(stdout_chunks[1].upload_error_reason, "object store write failed")
        self.assertEqual(stdout_chunks[1].metadata["retry_count"], 1)
        self.assertEqual([chunk.chunk_sequence for chunk in stdout_after_zero], [1])
        self.assertEqual([chunk.chunk_sequence for chunk in first_chunk], [0])
        log_events = [event for event in events if event.event_type == RunEventType.LOG_CHUNK_RECORDED.value]
        artifact_events = [
            event for event in events if event.event_type == RunEventType.ARTIFACT_CHUNK_RECORDED.value
        ]
        self.assertEqual(len(log_events), 4)
        self.assertEqual(len(artifact_events), 1)
        failed_log_event = next(event for event in log_events if event.metadata["upload_status"] == "failed")
        self.assertEqual(failed_log_event.from_status, RunStatus.SUCCEEDED)
        self.assertEqual(failed_log_event.to_status, RunStatus.SUCCEEDED)
        self.assertEqual(failed_log_event.metadata["artifact_id"], "run_log_chunks-trajectory")
        self.assertEqual(failed_log_event.metadata["chunk_kind"], ArtifactChunkKind.STDOUT.value)
        self.assertEqual(failed_log_event.metadata["chunk_sequence"], 1)
        self.assertEqual(failed_log_event.metadata["size_bytes"], 128)
        self.assertEqual(failed_log_event.metadata["sha256"], "3" * 64)
        self.assertEqual(failed_log_event.metadata["upload_error_reason"], "object store write failed")
        self.assertNotIn("stdout", failed_log_event.metadata)
        self.assertNotIn("stderr", failed_log_event.metadata)
        self.assertEqual(artifact_events[0].metadata["chunk_kind"], ArtifactChunkKind.TRAJECTORY.value)

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
        self.assertEqual(events[1].metadata["execution_task_id"], "run_claim_001:attempt:1")
        self.assertEqual(attempt.metadata_json["execution"]["schema_version"], EXECUTION_ATTEMPT_METADATA_SCHEMA_VERSION)
        runner = attempt.metadata_json["execution"]["runner"]
        self.assertEqual(runner["worker_id"], "worker-a")
        self.assertEqual(runner["process_status"], RunnerProcessStatus.CLAIMED.value)
        self.assertEqual(runner["heartbeat_status"], "provisioning")
        self.assertIn("claimed_at", runner)
        self.assertIn("last_heartbeat_at", runner)
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
            attempt = session.scalar(
                select(RunAttemptRow).where(RunAttemptRow.run_id == "run_dispatch_global_0")
            )
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
        self.assertEqual(events[1].metadata["execution_task_id"], "run_dispatch_global_0:attempt:1")
        self.assertEqual(attempt.metadata_json["execution"]["schema_version"], EXECUTION_ATTEMPT_METADATA_SCHEMA_VERSION)
        scheduler = attempt.metadata_json["execution"]["scheduler"]
        self.assertEqual(scheduler["scheduler_id"], "scheduler-a")
        self.assertEqual(scheduler["lease_status"], SchedulerLeaseStatus.DISPATCHED.value)
        self.assertEqual(scheduler["execution_task_id"], "run_dispatch_global_0:attempt:1")
        self.assertEqual(scheduler["backend_key"], "docker_terminal")
        self.assertEqual(scheduler["project_id"], "pilot-project")
        self.assertIn("dispatched_at", scheduler)

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

    def test_run_repository_dispatch_respects_provider_model_agent_and_benchmark_capacity(self):
        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)

            runs.create_run(
                _queued_capacity_run(
                    run_id="run_dispatch_capacity_a",
                    provider="openai",
                    model_name="gpt-5",
                    agent_id="codex",
                    benchmark_ref="terminal-bench@2.0",
                )
            )
            runs.create_run(
                _queued_capacity_run(
                    run_id="run_dispatch_capacity_b",
                    provider="openai",
                    model_name="gpt-5-mini",
                    agent_id="aider",
                    benchmark_ref="skillflow@2026-06-01",
                )
            )
            runs.create_run(
                _queued_capacity_run(
                    run_id="run_dispatch_capacity_c",
                    provider="anthropic",
                    model_name="gpt-5",
                    agent_id="codex",
                    benchmark_ref="skillflow@2026-06-01",
                )
            )
            runs.create_run(
                _queued_capacity_run(
                    run_id="run_dispatch_capacity_d",
                    provider="anthropic",
                    model_name="claude-sonnet-4",
                    agent_id="aider",
                    benchmark_ref="terminal-bench@2.0",
                )
            )
            runs.create_run(
                _queued_capacity_run(
                    run_id="run_dispatch_capacity_e",
                    provider="anthropic",
                    model_name="claude-sonnet-4",
                    agent_id="aider",
                    benchmark_ref="skillflow@2026-06-01",
                )
            )

            dispatched = runs.dispatch_queued_runs(
                scheduler_id="scheduler-a",
                max_runs=5,
                provider_limits={"openai": 1},
                model_limits={"gpt-5": 1},
                agent_limits={"codex": 1},
                benchmark_limits={"terminal-bench@2.0": 1},
                request_id="req-dispatch-expanded-capacity-001",
            )
            statuses = {
                run.run_id: run.status
                for run in runs.list_runs(project_id="pilot-project")
                if run.run_id.startswith("run_dispatch_capacity_")
            }
            events = runs.list_status_events("run_dispatch_capacity_a")
            attempt = session.scalar(
                select(RunAttemptRow).where(RunAttemptRow.run_id == "run_dispatch_capacity_a")
            )

        self.assertEqual(
            [run.run_id for run in dispatched],
            ["run_dispatch_capacity_a", "run_dispatch_capacity_e"],
        )
        self.assertEqual(statuses["run_dispatch_capacity_a"], RunStatus.DISPATCHED)
        self.assertEqual(statuses["run_dispatch_capacity_b"], RunStatus.QUEUED)
        self.assertEqual(statuses["run_dispatch_capacity_c"], RunStatus.QUEUED)
        self.assertEqual(statuses["run_dispatch_capacity_d"], RunStatus.QUEUED)
        self.assertEqual(statuses["run_dispatch_capacity_e"], RunStatus.DISPATCHED)
        scheduler = attempt.metadata_json["execution"]["scheduler"]
        self.assertEqual(scheduler["provider_key"], "openai")
        self.assertEqual(scheduler["model_key"], "gpt-5")
        self.assertEqual(scheduler["agent_key"], "codex")
        self.assertEqual(scheduler["benchmark_key"], "terminal-bench@2.0")
        self.assertEqual(events[1].metadata["provider_key"], "openai")
        self.assertEqual(events[1].metadata["model_key"], "gpt-5")
        self.assertEqual(events[1].metadata["agent_key"], "codex")
        self.assertEqual(events[1].metadata["benchmark_key"], "terminal-bench@2.0")

    def test_run_repository_records_capacity_blocked_reason_without_event_spam(self):
        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            runs.create_run(
                _queued_capacity_run(
                    run_id="run_dispatch_blocked_a",
                    provider="openai",
                    model_name="gpt-5",
                    agent_id="codex",
                    benchmark_ref="terminal-bench@2.0",
                )
            )
            runs.create_run(
                _queued_capacity_run(
                    run_id="run_dispatch_blocked_b",
                    provider="openai",
                    model_name="gpt-5-mini",
                    agent_id="aider",
                    benchmark_ref="skillflow@2026-06-01",
                )
            )

            result = runs.dispatch_queued_runs_with_diagnostics(
                scheduler_id="scheduler-a",
                max_runs=2,
                provider_limits={"openai": 1},
                request_id="req-dispatch-blocked-001",
            )
            runs.dispatch_queued_runs_with_diagnostics(
                scheduler_id="scheduler-a",
                max_runs=2,
                provider_limits={"openai": 1},
                request_id="req-dispatch-blocked-002",
            )
            blocked_events = [
                event
                for event in runs.list_status_events("run_dispatch_blocked_b")
                if event.event_type == RunEventType.SCHEDULER_CAPACITY_BLOCKED.value
            ]
            attempt = session.scalar(
                select(RunAttemptRow).where(RunAttemptRow.run_id == "run_dispatch_blocked_b")
            )
            current_blocks = runs.list_scheduler_capacity_blocks(project_ids=["pilot-project"])

        self.assertEqual([run.run_id for run in result.dispatched_runs], ["run_dispatch_blocked_a"])
        self.assertEqual([block.run_id for block in result.capacity_blocked_runs], ["run_dispatch_blocked_b"])
        self.assertEqual(result.capacity_blocked_runs[0].dimension, "provider")
        self.assertEqual(result.capacity_blocked_runs[0].key, "openai")
        self.assertEqual(result.capacity_blocked_runs[0].limit, 1)
        self.assertEqual(result.capacity_blocked_runs[0].active_count, 1)
        self.assertEqual(len(blocked_events), 1)
        self.assertEqual(blocked_events[0].from_status, RunStatus.QUEUED)
        self.assertEqual(blocked_events[0].to_status, RunStatus.QUEUED)
        self.assertEqual(blocked_events[0].metadata["dimension"], "provider")
        blocked = attempt.metadata_json["execution"]["scheduler"]["capacity_blocked"]
        self.assertEqual(blocked["dimension"], "provider")
        self.assertEqual(blocked["provider_key"], "openai")
        self.assertEqual([block.run_id for block in current_blocks], ["run_dispatch_blocked_b"])

    def test_run_repository_clears_capacity_blocked_metadata_when_run_dispatches(self):
        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            runs.create_run(
                _queued_capacity_run(
                    run_id="run_dispatch_unblocked_a",
                    provider="openai",
                    model_name="gpt-5",
                    agent_id="codex",
                    benchmark_ref="terminal-bench@2.0",
                )
            )
            runs.create_run(
                _queued_capacity_run(
                    run_id="run_dispatch_unblocked_b",
                    provider="openai",
                    model_name="gpt-5-mini",
                    agent_id="aider",
                    benchmark_ref="skillflow@2026-06-01",
                )
            )

            first_result = runs.dispatch_queued_runs_with_diagnostics(
                scheduler_id="scheduler-a",
                max_runs=2,
                provider_limits={"openai": 1},
            )
            second_result = runs.dispatch_queued_runs_with_diagnostics(
                scheduler_id="scheduler-a",
                max_runs=3,
                provider_limits={"openai": 2},
            )
            attempt = session.scalar(
                select(RunAttemptRow).where(RunAttemptRow.run_id == "run_dispatch_unblocked_b")
            )
            current_blocks = runs.list_scheduler_capacity_blocks(project_ids=["pilot-project"])

        self.assertEqual([block.run_id for block in first_result.capacity_blocked_runs], ["run_dispatch_unblocked_b"])
        self.assertEqual([run.run_id for run in second_result.dispatched_runs], ["run_dispatch_unblocked_b"])
        self.assertNotIn("capacity_blocked", attempt.metadata_json["execution"]["scheduler"])
        self.assertEqual(current_blocks, [])

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
        self.assertEqual(events[-1].event_type, RunEventType.RECOVERED.value)
        self.assertEqual(events[-1].from_status, RunStatus.DISPATCHED)
        self.assertEqual(events[-1].to_status, RunStatus.QUEUED)
        self.assertEqual(events[-1].request_id, "req-recover-dispatched-001")
        self.assertEqual(events[-1].reason, "stale dispatched run expired")
        self.assertEqual(events[-1].metadata["scheduler_id"], "scheduler-a")
        self.assertEqual(events[-1].metadata["execution_task_id"], "run_recover_stale:attempt:1")
        self.assertEqual(events[-1].metadata["recovery"], RecoveryReasonCode.STALE_DISPATCHED.value)
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
        runner = attempt.metadata_json["execution"]["runner"]
        self.assertEqual(runner["worker_id"], "worker-a")
        self.assertEqual(runner["process_status"], RunnerProcessStatus.HEARTBEATING.value)
        self.assertEqual(runner["heartbeat_status"], "running")
        self.assertIn("last_heartbeat_at", runner)
        self.assertEqual(attempt.metadata_json["worker"]["worker_id"], "worker-a")
        self.assertEqual(attempt.metadata_json["worker"]["heartbeat_status"], "running")
        self.assertIn("last_heartbeat_at", attempt.metadata_json["worker"])

    def test_run_repository_records_terminal_runner_process_metadata(self):
        run = _queued_run(run_id="run_worker_terminal_metadata")

        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            runs.create_run(run)
            claimed = runs.claim_next_queued_run(worker_id="worker-a")
            claimed.transition_to(RunStatus.RUNNING)
            claimed.attach_artifact(_log_artifact("run_worker_terminal_metadata-log"))
            claimed.transition_to(RunStatus.EVALUATING)
            claimed.transition_to(RunStatus.SUCCEEDED)
            runs.save_worker_result(claimed, worker_id="worker-a", request_id="req-save-worker-result")
            attempt = session.scalar(
                select(RunAttemptRow).where(RunAttemptRow.run_id == "run_worker_terminal_metadata")
            )
            events = runs.list_status_events("run_worker_terminal_metadata")

        runner = attempt.metadata_json["execution"]["runner"]
        self.assertEqual(runner["worker_id"], "worker-a")
        self.assertEqual(runner["process_status"], RunnerProcessStatus.COMPLETED.value)
        self.assertEqual(runner["heartbeat_status"], "succeeded")
        self.assertIn("completed_at", runner)
        self.assertEqual(events[-1].metadata["execution_task_id"], "run_worker_terminal_metadata:attempt:1")

    def test_save_worker_result_records_evaluator_event_when_previous_status_already_evaluating(self):
        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            runs.create_run(_queued_run(run_id="run_worker_existing_evaluating"))
            claimed = runs.claim_next_queued_run(worker_id="worker-a")
            attempt = session.scalar(
                select(RunAttemptRow).where(RunAttemptRow.run_id == "run_worker_existing_evaluating")
            )
            row = session.get(RunRow, "run_worker_existing_evaluating")

            row.status = RunStatus.RUNNING.value
            attempt.status = RunStatus.RUNNING.value
            runs._append_status_event(
                run_id="run_worker_existing_evaluating",
                attempt_id=attempt.attempt_id,
                event_type=RunEventType.STARTED,
                from_status=RunStatus.PROVISIONING,
                to_status=RunStatus.RUNNING,
                metadata={"worker_id": "worker-a", "execution_task_id": attempt.attempt_id},
            )
            row.status = RunStatus.EVALUATING.value
            attempt.status = RunStatus.EVALUATING.value
            runs._append_status_event(
                run_id="run_worker_existing_evaluating",
                attempt_id=attempt.attempt_id,
                event_type=RunEventType.EVALUATING,
                from_status=RunStatus.RUNNING,
                to_status=RunStatus.EVALUATING,
                metadata={"worker_id": "worker-a", "execution_task_id": attempt.attempt_id},
            )

            claimed.transition_to(RunStatus.RUNNING)
            claimed.attach_artifact(_log_artifact("run_worker_existing_evaluating-trajectory"))
            claimed.transition_to(RunStatus.EVALUATING)
            claimed.attach_evaluator_result(
                EvaluatorResult(
                    evaluator_id="harbor-verifier-v1",
                    mode="harbor_verifier",
                    status="completed",
                    score=1.0,
                    metrics={"reward": 1.0},
                    verbal_feedback="Do not persist full feedback in event metadata.",
                    judge=None,
                    artifact_refs=[
                        "file:///tmp/secret/evaluator/report.json",
                        "minio://runs/run_worker_existing_evaluating/evaluation/report.json?signature=secret#fragment",
                    ],
                )
            )
            claimed.transition_to(RunStatus.SUCCEEDED)

            runs.save_worker_result(
                claimed,
                worker_id="worker-a",
                request_id="req-existing-evaluating-result",
            )
            events = runs.list_status_events("run_worker_existing_evaluating")

        self.assertEqual(
            [event.event_type for event in events],
            [
                "run.created",
                "run.claimed",
                "run.started",
                "run.evaluating",
                "evaluator.completed",
                "run.succeeded",
            ],
        )
        evaluator_event = next(event for event in events if event.event_type == "evaluator.completed")
        self.assertEqual(evaluator_event.from_status, RunStatus.EVALUATING)
        self.assertEqual(evaluator_event.to_status, RunStatus.EVALUATING)
        self.assertEqual(evaluator_event.metadata["evaluator_id"], "harbor-verifier-v1")
        self.assertEqual(evaluator_event.metadata["artifact_refs"], [
            "report.json",
            "minio://runs/run_worker_existing_evaluating/evaluation/report.json",
        ])
        self.assertNotIn("verbal_feedback", evaluator_event.metadata)
        self.assertNotIn("metrics", evaluator_event.metadata)

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
        self.assertEqual(events[-1].event_type, RunEventType.RECOVERED.value)
        self.assertEqual(events[-1].from_status, RunStatus.PROVISIONING)
        self.assertEqual(events[-1].to_status, RunStatus.FAILED)
        self.assertEqual(events[-1].reason, "stale worker heartbeat expired")
        self.assertEqual(events[-1].metadata["scheduler_id"], "scheduler-a")
        self.assertEqual(events[-1].metadata["recovery"], RecoveryReasonCode.STALE_WORKER_HEARTBEAT.value)
        self.assertEqual(events[-1].metadata["execution_task_id"], "run_active_stale:attempt:1")
        self.assertEqual(events[-1].metadata["worker_id"], "worker-run_active_stale")
        self.assertEqual(events[-1].metadata["last_heartbeat_at"], stale_heartbeat)

    def test_run_repository_recovers_terminal_result_mismatch_before_heartbeat_failure(self):
        completed_at = datetime.now(timezone.utc) - timedelta(minutes=1)
        stale_before = datetime.now(timezone.utc) - timedelta(minutes=10)

        with session_scope(self.engine) as session:
            _seed_latent_project(session)
            runs = RunRepository(session)
            runs.create_run(_queued_run(run_id="run_terminal_mismatch"))
            runs.claim_next_queued_run(worker_id="worker-terminal-mismatch")
            attempt = session.scalar(select(RunAttemptRow).where(RunAttemptRow.run_id == "run_terminal_mismatch"))
            metadata = dict(attempt.metadata_json or {})
            execution = dict(metadata["execution"])
            runner = dict(execution["runner"])
            runner.update(
                {
                    "process_status": RunnerProcessStatus.COMPLETED.value,
                    "heartbeat_status": RunStatus.SUCCEEDED.value,
                    "completed_at": completed_at.isoformat(),
                    "return_code": 0,
                }
            )
            execution["runner"] = runner
            metadata["execution"] = execution
            attempt.metadata_json = metadata
            session.get(RunRow, "run_terminal_mismatch").updated_at = completed_at

            recovered = runs.recover_terminal_result_mismatches(
                scheduler_id="scheduler-a",
                max_runs=10,
                request_id="req-terminal-mismatch-001",
            )
            stale_failed = runs.fail_stale_active_runs_by_heartbeat(
                older_than=stale_before,
                scheduler_id="scheduler-a",
                max_runs=10,
                request_id="req-stale-active-after-mismatch-001",
            )
            run = runs.get_run("run_terminal_mismatch")
            attempt = session.scalar(select(RunAttemptRow).where(RunAttemptRow.run_id == "run_terminal_mismatch"))
            events = runs.list_status_events("run_terminal_mismatch")
            projection = session.get(RunDashboardProjectionRow, "run_terminal_mismatch")

        self.assertEqual([run.run_id for run in recovered], ["run_terminal_mismatch"])
        self.assertEqual(stale_failed, [])
        self.assertEqual(run.status, RunStatus.FAILED)
        self.assertEqual(run.failure_reason, "terminal worker result did not persist to run state")
        self.assertEqual(attempt.status, RunStatus.FAILED.value)
        self.assertEqual(attempt.metadata_json["execution"]["runner"]["process_status"], RunnerProcessStatus.FAILED.value)
        self.assertEqual(attempt.metadata_json["execution"]["runner"]["heartbeat_status"], RunStatus.PROVISIONING.value)
        self.assertEqual(events[-1].event_type, RunEventType.RECOVERED.value)
        self.assertEqual(events[-1].from_status, RunStatus.PROVISIONING)
        self.assertEqual(events[-1].to_status, RunStatus.FAILED)
        self.assertEqual(events[-1].metadata["recovery"], RecoveryReasonCode.TERMINAL_RESULT_MISMATCH.value)
        self.assertEqual(events[-1].metadata["scheduler_id"], "scheduler-a")
        self.assertEqual(events[-1].metadata["execution_task_id"], "run_terminal_mismatch:attempt:1")
        self.assertEqual(events[-1].metadata["worker_id"], "worker-terminal-mismatch")
        self.assertEqual(events[-1].metadata["runner_process_status"], RunnerProcessStatus.COMPLETED.value)
        self.assertEqual(events[-1].metadata["runner_heartbeat_status"], RunStatus.SUCCEEDED.value)
        self.assertIsNotNone(projection)
        self.assertEqual(projection.status, RunStatus.FAILED.value)
        self.assertEqual(projection.refresh_reason, "terminal_result_mismatch_recovery")

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

    def test_run_repository_rejects_stale_execution_task_result_after_retry(self):
        run = _queued_run(run_id="run_stale_execution_task_001")

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
            stale_execution_task_id = runs.current_execution_task_id(run.run_id)
            runs.cancel_run(
                run.run_id,
                reason="operator canceled stale attempt",
                request_id="req-cancel-001",
            )
            runs.retry_run(
                run.run_id,
                reason="retry after stale attempt",
                request_id="req-retry-001",
            )

            claimed.transition_to(RunStatus.RUNNING)
            claimed.transition_to(RunStatus.EVALUATING)
            claimed.attach_evaluator_result(
                EvaluatorResult(
                    evaluator_id="llm-judge-v0",
                    status="completed",
                    score=1.0,
                    metrics={"task_success": True},
                    verbal_feedback="Stale result must not overwrite attempt 2.",
                    judge=JudgeConfig(
                        provider="mock",
                        model_name="deterministic-judge",
                        rubric_version="latent-skill-v0",
                    ),
                    artifact_refs=[],
                )
            )
            claimed.transition_to(RunStatus.SUCCEEDED)

            with self.assertRaisesRegex(StaleExecutionTaskError, "stale execution task"):
                runs.save_worker_result(
                    claimed,
                    worker_id="worker-a",
                    execution_task_id=stale_execution_task_id,
                    request_id="req-worker-finish-001",
                )

            loaded = runs.get_run(run.run_id)
            current_execution_task_id = runs.current_execution_task_id(run.run_id)
            events = runs.list_status_events(run.run_id)

        self.assertEqual(loaded.status, RunStatus.QUEUED)
        self.assertEqual(current_execution_task_id, "run_stale_execution_task_001:attempt:2")
        self.assertEqual(
            [event.event_type for event in events],
            ["run.created", "run.claimed", "run.canceled", "run.retried"],
        )

    def test_run_repository_rejects_duplicate_execution_task_lock(self):
        run = _queued_run(run_id="run_duplicate_execution_task_001")

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
            runs.claim_next_queued_run(worker_id="worker-a", request_id="req-claim-001")
            execution_task_id = runs.current_execution_task_id(run.run_id)

            locked = runs.acquire_execution_task_lock(
                run.run_id,
                worker_id="worker-a",
                execution_task_id=execution_task_id,
                request_id="req-lock-001",
            )
            with self.assertRaisesRegex(DuplicateExecutionTaskError, "already executing"):
                runs.acquire_execution_task_lock(
                    run.run_id,
                    worker_id="worker-a",
                    execution_task_id=execution_task_id,
                    request_id="req-lock-duplicate-001",
                )

            attempt = session.scalar(select(RunAttemptRow).where(RunAttemptRow.run_id == run.run_id))
            runner = attempt.metadata_json["execution"]["runner"]

        self.assertEqual(locked.status, RunStatus.PROVISIONING)
        self.assertEqual(runner["execution_lock_id"], execution_task_id)
        self.assertEqual(runner["process_status"], RunnerProcessStatus.EXECUTING.value)
        self.assertIn("execution_lock_acquired_at", runner)

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


def _artifact_chunk(
    *,
    run_id: str,
    attempt_id: str,
    artifact_id: str,
    chunk_kind: ArtifactChunkKind,
    chunk_sequence: int,
    storage_key: str,
    sha256: str,
    size_bytes: int,
    created_at: datetime,
    upload_status: ArtifactUploadStatus = ArtifactUploadStatus.COMPLETED,
    upload_error_reason: str | None = None,
    metadata: dict | None = None,
) -> ArtifactChunkMetadata:
    return ArtifactChunkMetadata(
        run_id=run_id,
        attempt_id=attempt_id,
        artifact_id=artifact_id,
        chunk_kind=chunk_kind,
        chunk_sequence=chunk_sequence,
        storage_key=storage_key,
        media_type="application/x-ndjson",
        size_bytes=size_bytes,
        sha256=sha256,
        upload_status=upload_status,
        upload_error_reason=upload_error_reason,
        created_at=created_at,
        metadata=dict(metadata or {}),
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


def _queued_capacity_run(
    *,
    run_id: str,
    provider: str,
    model_name: str,
    agent_id: str,
    benchmark_ref: str,
) -> RunRecord:
    run = _queued_run(run_id=run_id)
    run.model = replace(run.model, provider=provider, model_name=model_name)
    run.metadata["harbor_run"] = {
        "agent": agent_id,
        "dataset_ref": benchmark_ref,
    }
    return run


def _set_worker_last_heartbeat(session, *, run_id: str, heartbeat_at: str) -> None:
    attempt = session.scalar(select(RunAttemptRow).where(RunAttemptRow.run_id == run_id))
    metadata = dict(attempt.metadata_json or {})
    worker = dict(metadata["worker"])
    worker["last_heartbeat_at"] = heartbeat_at
    metadata["worker"] = worker
    attempt.metadata_json = metadata
