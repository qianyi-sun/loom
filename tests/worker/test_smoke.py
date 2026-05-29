import tempfile
import unittest
from pathlib import Path

from sqlalchemy.pool import StaticPool

from agentic_data_platform.artifacts.store import ArtifactPersistence, LocalArtifactStore
from agentic_data_platform.persistence.database import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.repositories import IdentityRepository, ProjectRepository
from agentic_data_platform.worker.smoke import run_worker_smoke


class WorkerSmokeTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_database_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        upgrade_database(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_worker_smoke_seeds_and_completes_fixture_run(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            result = run_worker_smoke(
                engine=self.engine,
                artifact_persistence=ArtifactPersistence(LocalArtifactStore(Path(temp_dir))),
                run_id="worker_smoke_001",
                worker_id="worker-smoke-test",
            )

        self.assertEqual(result["run_id"], "worker_smoke_001")
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["turn_count"], 1)
        self.assertEqual(result["artifact_count"], 3)
        self.assertEqual(result["evaluator_id"], "mock-judge-v0")

        with session_scope(self.engine) as session:
            user = IdentityRepository(session).get_user("[REDACTED_OWNER]")
            project = ProjectRepository(session).get_project("pilot-project")

        self.assertEqual(user.team_id, "pilot-project")
        self.assertEqual(project.created_by_user_id, "[REDACTED_OWNER]")
