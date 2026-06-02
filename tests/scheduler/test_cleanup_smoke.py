from __future__ import annotations

import subprocess
import unittest

from agentic_data_platform.domain.execution_events import RecoveryReasonCode, RunEventType
from agentic_data_platform.domain.run_records import RunStatus
from agentic_data_platform.persistence import session_scope
from agentic_data_platform.persistence.database import create_database_engine
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.repositories import RunRepository
from agentic_data_platform.scheduler.cleanup_smoke import run_scheduler_docker_cleanup_smoke


class SchedulerDockerCleanupSmokeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_database_engine("sqlite+pysqlite:///:memory:")
        upgrade_database(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_smoke_creates_labeled_container_and_records_cleanup_evidence(self) -> None:
        runner = FakeDockerRunner()

        result = run_scheduler_docker_cleanup_smoke(
            engine=self.engine,
            scheduler_id="cleanup-smoke-scheduler",
            run_id="cleanup_smoke_test_run",
            runner=runner,
            smoke_image="python:3.12-slim",
            stale_active_seconds=60,
        )

        with session_scope(self.engine) as session:
            repository = RunRepository(session)
            run = repository.get_run("cleanup_smoke_test_run")
            events = repository.list_status_events("cleanup_smoke_test_run")

        self.assertEqual(run.status, RunStatus.FAILED)
        self.assertEqual(result.run_id, "cleanup_smoke_test_run")
        self.assertEqual(result.container_id, "container-cleanup-smoke")
        self.assertEqual(result.docker_cleanup_count, 1)
        self.assertEqual(result.removed_container_ids, ["container-cleanup-smoke"])
        self.assertEqual(result.cleanup_event_count, 1)
        docker_run = runner.commands[0]
        self.assertEqual(docker_run[:3], ["docker", "run", "-d"])
        self.assertIn("--label", docker_run)
        self.assertIn("com.agentic-data-platform.managed=true", docker_run)
        self.assertIn("com.agentic-data-platform.run_id=cleanup_smoke_test_run", docker_run)
        self.assertIn("com.agentic-data-platform.resource=sandbox-container", docker_run)
        cleanup_events = [event for event in events if event.event_type == RunEventType.SANDBOX_CONTAINER_CLEANUP.value]
        self.assertEqual(len(cleanup_events), 1)
        self.assertEqual(cleanup_events[0].metadata["recovery"], RecoveryReasonCode.DOCKER_CONTAINER_CLEANUP.value)
        self.assertEqual(cleanup_events[0].metadata["removed_container_ids"], ["container-cleanup-smoke"])


class FakeDockerRunner:
    def __init__(self) -> None:
        self.commands: list[list[str]] = []

    def run(
        self,
        args: list[str],
        *,
        timeout: int,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.commands.append(list(args))
        if args[:3] == ["docker", "run", "-d"]:
            return subprocess.CompletedProcess(args=args, returncode=0, stdout="container-cleanup-smoke\n", stderr="")
        if args[:3] == ["docker", "ps", "-aq"]:
            return subprocess.CompletedProcess(args=args, returncode=0, stdout="container-cleanup-smoke\n", stderr="")
        if args[:3] == ["docker", "rm", "-f"]:
            return subprocess.CompletedProcess(args=args, returncode=0, stdout="container-cleanup-smoke\n", stderr="")
        return subprocess.CompletedProcess(args=args, returncode=1, stdout="", stderr=f"unexpected command: {args}")


if __name__ == "__main__":
    unittest.main()
