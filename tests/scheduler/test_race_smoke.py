from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from agentic_data_platform.domain.run_records import RunStatus
from agentic_data_platform.persistence import session_scope
from agentic_data_platform.persistence.database import create_database_engine
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.repositories import RunRepository
from agentic_data_platform.scheduler.race_smoke import run_scheduler_race_smoke


class SchedulerRaceSmokeTest(unittest.TestCase):
    def test_race_smoke_reports_capacity_safe_concurrent_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            database_path = Path(tmpdir) / "race-smoke.sqlite"
            engine = create_database_engine(
                f"sqlite+pysqlite:///{database_path}",
                connect_args={"check_same_thread": False},
            )
            try:
                upgrade_database(engine)

                result = run_scheduler_race_smoke(
                    engine=engine,
                    run_id_prefix="race_smoke_test",
                    scheduler_count=2,
                    queued_run_count=4,
                    max_active_runs=2,
                    project_max_active_runs=2,
                )

                with session_scope(engine) as session:
                    records = RunRepository(session).list_runs(project_id=result.project_id)
                final_status_counts = {status.value: 0 for status in RunStatus}
                for record in records:
                    final_status_counts[record.status.value] += 1
            finally:
                engine.dispose()

        self.assertEqual(result.scheduler_count, 2)
        self.assertEqual(result.queued_run_count, 4)
        self.assertEqual(result.max_active_runs, 2)
        self.assertLessEqual(result.total_dispatched_count, 2)
        self.assertEqual(len(result.dispatched_run_ids), len(set(result.dispatched_run_ids)))
        self.assertEqual(result.status_counts[RunStatus.DISPATCHED.value], 2)
        self.assertEqual(result.status_counts[RunStatus.QUEUED.value], 2)
        self.assertEqual(result.cleanup_status_counts[RunStatus.CANCELED.value], 4)
        self.assertEqual(final_status_counts[RunStatus.CANCELED.value], 4)
        self.assertEqual(final_status_counts[RunStatus.DISPATCHED.value], 0)
        self.assertEqual(final_status_counts[RunStatus.QUEUED.value], 0)


if __name__ == "__main__":
    unittest.main()
