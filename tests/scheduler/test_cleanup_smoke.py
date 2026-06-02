from __future__ import annotations

import subprocess
import unittest
from io import StringIO

from agentic_data_platform.domain.execution_events import RecoveryReasonCode, RunEventType
from agentic_data_platform.domain.run_records import RunStatus
from agentic_data_platform.persistence import session_scope
from agentic_data_platform.persistence.database import create_database_engine
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.repositories import RunRepository
from agentic_data_platform.scheduler.cleanup_smoke import (
    run_scheduler_docker_cleanup_smoke,
    run_scheduler_parent_death_cleanup_smoke,
)


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
        self.assertEqual(result.container_id, FakeDockerRunner.FULL_CONTAINER_ID)
        self.assertEqual(result.docker_cleanup_count, 1)
        self.assertEqual(result.removed_container_ids, [FakeDockerRunner.SHORT_CONTAINER_ID])
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
        self.assertEqual(cleanup_events[0].metadata["removed_container_ids"], [FakeDockerRunner.SHORT_CONTAINER_ID])

    def test_parent_death_smoke_kills_container_parent_and_records_cleanup_evidence(self) -> None:
        runner = FakeParentDeathDockerRunner()

        result = run_scheduler_parent_death_cleanup_smoke(
            engine=self.engine,
            scheduler_id="parent-death-cleanup-smoke-scheduler",
            run_id="parent_death_cleanup_smoke_test_run",
            runner=runner,
            smoke_image="python:3.12-slim",
            stale_active_seconds=60,
        )

        with session_scope(self.engine) as session:
            repository = RunRepository(session)
            run = repository.get_run("parent_death_cleanup_smoke_test_run")
            events = repository.list_status_events("parent_death_cleanup_smoke_test_run")

        self.assertEqual(run.status, RunStatus.FAILED)
        self.assertEqual(result.run_id, "parent_death_cleanup_smoke_test_run")
        self.assertEqual(result.container_id, FakeDockerRunner.FULL_CONTAINER_ID)
        self.assertEqual(result.parent_process_returncode, -15)
        self.assertEqual(result.parent_process_was_terminated, True)
        self.assertEqual(result.docker_cleanup_count, 1)
        self.assertEqual(result.removed_container_ids, [FakeDockerRunner.SHORT_CONTAINER_ID])
        self.assertEqual(result.cleanup_event_count, 1)
        self.assertEqual(runner.parent_process.terminated, True)
        self.assertEqual(runner.parent_process.killed, False)
        parent_start = runner.parent_start_args
        self.assertIn("-u", parent_start)
        self.assertIn("python:3.12-slim", " ".join(parent_start))
        cleanup_events = [event for event in events if event.event_type == RunEventType.SANDBOX_CONTAINER_CLEANUP.value]
        self.assertEqual(len(cleanup_events), 1)
        self.assertEqual(cleanup_events[0].metadata["recovery"], RecoveryReasonCode.DOCKER_CONTAINER_CLEANUP.value)
        self.assertEqual(cleanup_events[0].metadata["removed_container_ids"], [FakeDockerRunner.SHORT_CONTAINER_ID])


class FakeDockerRunner:
    FULL_CONTAINER_ID = "36d97bfaf5d59348bdbb33be4ab7b63a1aa356b3899e49c8bcf393b3d3cb9347"
    SHORT_CONTAINER_ID = FULL_CONTAINER_ID[:12]

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
            return subprocess.CompletedProcess(args=args, returncode=0, stdout=f"{self.FULL_CONTAINER_ID}\n", stderr="")
        if args[:3] == ["docker", "ps", "-aq"]:
            return subprocess.CompletedProcess(args=args, returncode=0, stdout=f"{self.SHORT_CONTAINER_ID}\n", stderr="")
        if args[:3] == ["docker", "rm", "-f"]:
            return subprocess.CompletedProcess(args=args, returncode=0, stdout=f"{self.SHORT_CONTAINER_ID}\n", stderr="")
        return subprocess.CompletedProcess(args=args, returncode=1, stdout="", stderr=f"unexpected command: {args}")


class FakeParentDeathDockerRunner(FakeDockerRunner):
    def __init__(self) -> None:
        super().__init__()
        self.parent_process = FakeParentProcess(f"{self.FULL_CONTAINER_ID}\n")
        self.parent_start_args: list[str] = []

    def start(
        self,
        args: list[str],
        *,
        env: dict[str, str] | None = None,
    ) -> "FakeParentProcess":
        self.parent_start_args = list(args)
        return self.parent_process


class FakeParentProcess:
    def __init__(self, stdout: str) -> None:
        self.stdout = StringIO(stdout)
        self.stderr = StringIO("")
        self.returncode: int | None = None
        self.terminated = False
        self.killed = False

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = -15

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    def wait(self, timeout: int | float | None = None) -> int:
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def poll(self) -> int | None:
        return self.returncode


if __name__ == "__main__":
    unittest.main()
