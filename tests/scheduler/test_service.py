import io
import json
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from sqlalchemy import select
from sqlalchemy.pool import StaticPool

from agentic_data_platform.domain.run_records import RunStatus
from agentic_data_platform.persistence.database import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.models import RunAttemptRow, RunDashboardProjectionRow, RunRow
from agentic_data_platform.persistence.repositories import IdentityRepository, ProjectRepository, RunRepository
from agentic_data_platform.scheduler.service import RunScheduler, run_scheduler_loop
from agentic_data_platform.service.config import ServiceSettings
from tests.persistence.test_repositories import _completed_run, _queued_capacity_run, _queued_run


class SchedulerServiceTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_database_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        upgrade_database(self.engine)
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

    def tearDown(self):
        self.engine.dispose()

    def test_scheduler_dispatches_from_service_settings_capacity(self):
        with session_scope(self.engine) as session:
            runs = RunRepository(session)
            for index in range(3):
                run = _queued_run(run_id=f"run_scheduler_service_{index}")
                run.runner.metadata["harness_id"] = "harbor-local-docker"
                runs.create_run(run)

        scheduler = RunScheduler(
            engine=self.engine,
            scheduler_id="scheduler-test",
            settings=ServiceSettings(
                app_name="agentic-data-platform-test",
                environment="test",
                database_url="",
                redis_url="",
                object_storage_endpoint="",
                object_storage_bucket="",
                object_storage_access_key="",
                object_storage_secret_key="",
                object_storage_region="us-east-1",
                scheduler_global_max_active_runs=2,
                scheduler_backend_max_active_runs={"harbor-local-docker": 1},
            ),
        )

        result = scheduler.dispatch_once(request_id="req-scheduler-service-001")

        with session_scope(self.engine) as session:
            statuses = {
                run.run_id: run.status
                for run in RunRepository(session).list_runs(project_id="pilot-project")
                if run.run_id.startswith("run_scheduler_service_")
            }

        self.assertEqual(result.dispatched_run_ids, ["run_scheduler_service_0"])
        self.assertEqual(result.dispatched_count, 1)
        self.assertEqual(statuses["run_scheduler_service_0"], RunStatus.DISPATCHED)
        self.assertEqual(statuses["run_scheduler_service_1"], RunStatus.QUEUED)
        self.assertEqual(statuses["run_scheduler_service_2"], RunStatus.QUEUED)

    def test_scheduler_dispatches_from_expanded_service_settings_capacity(self):
        with session_scope(self.engine) as session:
            runs = RunRepository(session)
            runs.create_run(
                _queued_capacity_run(
                    run_id="run_scheduler_expanded_capacity_a",
                    provider="openai",
                    model_name="gpt-5",
                    agent_id="codex",
                    benchmark_ref="terminal-bench@2.0",
                )
            )
            runs.create_run(
                _queued_capacity_run(
                    run_id="run_scheduler_expanded_capacity_b",
                    provider="openai",
                    model_name="gpt-5-mini",
                    agent_id="aider",
                    benchmark_ref="skillflow@2026-06-01",
                )
            )

        scheduler = RunScheduler(
            engine=self.engine,
            scheduler_id="scheduler-test",
            settings=ServiceSettings(
                app_name="agentic-data-platform-test",
                environment="test",
                database_url="",
                redis_url="",
                object_storage_endpoint="",
                object_storage_bucket="",
                object_storage_access_key="",
                object_storage_secret_key="",
                object_storage_region="us-east-1",
                scheduler_global_max_active_runs=2,
                scheduler_provider_max_active_runs={"openai": 1},
                scheduler_model_max_active_runs={"gpt-5": 1},
                scheduler_agent_max_active_runs={"codex": 1},
                scheduler_benchmark_max_active_runs={"terminal-bench@2.0": 1},
            ),
        )

        result = scheduler.dispatch_once(request_id="req-scheduler-expanded-capacity-001")

        with session_scope(self.engine) as session:
            statuses = {
                run.run_id: run.status
                for run in RunRepository(session).list_runs(project_id="pilot-project")
                if run.run_id.startswith("run_scheduler_expanded_capacity_")
            }

        self.assertEqual(result.dispatched_run_ids, ["run_scheduler_expanded_capacity_a"])
        self.assertEqual(result.dispatched_count, 1)
        self.assertEqual(statuses["run_scheduler_expanded_capacity_a"], RunStatus.DISPATCHED)
        self.assertEqual(statuses["run_scheduler_expanded_capacity_b"], RunStatus.QUEUED)

    def test_scheduler_recovers_stale_dispatched_runs_from_service_settings(self):
        now = datetime.now(timezone.utc)
        stale_timestamp = now - timedelta(minutes=10)

        with session_scope(self.engine) as session:
            runs = RunRepository(session)
            for index in range(2):
                runs.create_run(_queued_run(run_id=f"run_scheduler_recover_{index}"))
            runs.dispatch_queued_runs(scheduler_id="scheduler-test", max_runs=2)
            for index in range(2):
                session.get(RunRow, f"run_scheduler_recover_{index}").updated_at = stale_timestamp

        scheduler = RunScheduler(
            engine=self.engine,
            scheduler_id="scheduler-test",
            settings=ServiceSettings(
                app_name="agentic-data-platform-test",
                environment="test",
                database_url="",
                redis_url="",
                object_storage_endpoint="",
                object_storage_bucket="",
                object_storage_access_key="",
                object_storage_secret_key="",
                object_storage_region="us-east-1",
                scheduler_stale_dispatched_timeout_seconds=300,
                scheduler_recovery_batch_size=1,
            ),
        )

        result = scheduler.recover_once(request_id="req-scheduler-recover-001")

        with session_scope(self.engine) as session:
            statuses = {
                run.run_id: run.status
                for run in RunRepository(session).list_runs(project_id="pilot-project")
                if run.run_id.startswith("run_scheduler_recover_")
            }

        self.assertEqual(result.requeued_run_ids, ["run_scheduler_recover_0"])
        self.assertEqual(result.failed_run_ids, [])
        self.assertEqual(result.requeued_count, 1)
        self.assertEqual(result.failed_count, 0)
        self.assertEqual(statuses["run_scheduler_recover_0"], RunStatus.QUEUED)
        self.assertEqual(statuses["run_scheduler_recover_1"], RunStatus.DISPATCHED)

    def test_scheduler_recovers_stale_active_worker_heartbeats_from_service_settings(self):
        now = datetime.now(timezone.utc)
        stale_heartbeat = (now - timedelta(minutes=10)).isoformat()

        with session_scope(self.engine) as session:
            runs = RunRepository(session)
            runs.create_run(_queued_run(run_id="run_scheduler_active_recover"))
            runs.claim_next_queued_run(worker_id="worker-stale")
            session.get(RunRow, "run_scheduler_active_recover").updated_at = now - timedelta(minutes=10)
            attempt = session.scalar(select(RunAttemptRow).where(RunAttemptRow.run_id == "run_scheduler_active_recover"))
            metadata = dict(attempt.metadata_json or {})
            worker = dict(metadata["worker"])
            worker["last_heartbeat_at"] = stale_heartbeat
            metadata["worker"] = worker
            attempt.metadata_json = metadata

        scheduler = RunScheduler(
            engine=self.engine,
            scheduler_id="scheduler-test",
            settings=ServiceSettings(
                app_name="agentic-data-platform-test",
                environment="test",
                database_url="",
                redis_url="",
                object_storage_endpoint="",
                object_storage_bucket="",
                object_storage_access_key="",
                object_storage_secret_key="",
                object_storage_region="us-east-1",
                scheduler_stale_active_heartbeat_timeout_seconds=300,
                scheduler_recovery_batch_size=10,
            ),
        )

        result = scheduler.recover_once(request_id="req-scheduler-active-recover-001")

        with session_scope(self.engine) as session:
            recovered = RunRepository(session).get_run("run_scheduler_active_recover")

        self.assertEqual(result.requeued_run_ids, [])
        self.assertEqual(result.failed_run_ids, ["run_scheduler_active_recover"])
        self.assertEqual(result.failed_count, 1)
        self.assertEqual(recovered.status, RunStatus.FAILED)
        self.assertEqual(result.projection_refreshed_run_ids, ["run_scheduler_active_recover"])

        with session_scope(self.engine) as session:
            projection = session.get(RunDashboardProjectionRow, "run_scheduler_active_recover")

        self.assertIsNotNone(projection)
        self.assertEqual(projection.status, RunStatus.FAILED.value)
        self.assertEqual(projection.refresh_reason, "terminal_worker_recovery")

    def test_scheduler_refreshes_dirty_terminal_projections_from_service_settings(self):
        with session_scope(self.engine) as session:
            RunRepository(session).save_run(_completed_run(run_id="run_scheduler_projection_refresh"))

        scheduler = RunScheduler(
            engine=self.engine,
            scheduler_id="scheduler-test",
            settings=ServiceSettings(
                app_name="agentic-data-platform-test",
                environment="test",
                database_url="",
                redis_url="",
                object_storage_endpoint="",
                object_storage_bucket="",
                object_storage_access_key="",
                object_storage_secret_key="",
                object_storage_region="us-east-1",
                scheduler_recovery_batch_size=1,
            ),
        )

        result = scheduler.recover_once(request_id="req-scheduler-projection-refresh-001")

        with session_scope(self.engine) as session:
            projection = session.get(RunDashboardProjectionRow, "run_scheduler_projection_refresh")

        self.assertEqual(result.requeued_run_ids, [])
        self.assertEqual(result.failed_run_ids, [])
        self.assertEqual(result.projection_refreshed_run_ids, ["run_scheduler_projection_refresh"])
        self.assertIsNotNone(projection)
        self.assertEqual(projection.status, RunStatus.SUCCEEDED.value)
        self.assertEqual(projection.refresh_reason, "projection_recovery")

    def test_scheduler_loop_logs_projection_only_recovery(self):
        with session_scope(self.engine) as session:
            RunRepository(session).save_run(_completed_run(run_id="run_scheduler_loop_projection_refresh"))

        scheduler = RunScheduler(
            engine=self.engine,
            scheduler_id="scheduler-test",
            settings=ServiceSettings(
                app_name="agentic-data-platform-test",
                environment="test",
                database_url="",
                redis_url="",
                object_storage_endpoint="",
                object_storage_bucket="",
                object_storage_access_key="",
                object_storage_secret_key="",
                object_storage_region="us-east-1",
                scheduler_recovery_batch_size=1,
            ),
        )

        output = io.StringIO()
        with patch("agentic_data_platform.scheduler.service.time.sleep", side_effect=StopIteration):
            with self.assertRaises(StopIteration):
                with redirect_stdout(output):
                    run_scheduler_loop(scheduler, poll_interval_seconds=0.0)

        lines = [line for line in output.getvalue().splitlines() if line.strip()]
        self.assertEqual(len(lines), 1)
        payload = json.loads(lines[0])
        self.assertEqual(payload["action"], "recover")
        self.assertEqual(payload["projection_refreshed_run_ids"], ["run_scheduler_loop_projection_refresh"])
        self.assertEqual(payload["requeued_run_ids"], [])
        self.assertEqual(payload["failed_run_ids"], [])


if __name__ == "__main__":
    unittest.main()
