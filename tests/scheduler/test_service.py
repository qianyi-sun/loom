import unittest
from datetime import datetime, timedelta, timezone

from sqlalchemy.pool import StaticPool

from agentic_data_platform.domain.run_records import RunStatus
from agentic_data_platform.persistence.database import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.models import RunRow
from agentic_data_platform.persistence.repositories import IdentityRepository, ProjectRepository, RunRepository
from agentic_data_platform.scheduler.service import RunScheduler
from agentic_data_platform.service.config import ServiceSettings
from tests.persistence.test_repositories import _queued_run


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
        self.assertEqual(result.requeued_count, 1)
        self.assertEqual(statuses["run_scheduler_recover_0"], RunStatus.QUEUED)
        self.assertEqual(statuses["run_scheduler_recover_1"], RunStatus.DISPATCHED)


if __name__ == "__main__":
    unittest.main()
