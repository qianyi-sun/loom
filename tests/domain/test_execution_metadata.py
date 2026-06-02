import unittest
from datetime import datetime, timezone

from agentic_data_platform.domain.execution_metadata import (
    EXECUTION_ATTEMPT_METADATA_SCHEMA_VERSION,
    RunnerProcessStatus,
    SchedulerCapacityBlock,
    SchedulerLeaseStatus,
    runner_process_metadata,
    scheduler_capacity_blocked_metadata,
    scheduler_lease_metadata,
)


class ExecutionMetadataContractTest(unittest.TestCase):
    def test_scheduler_lease_metadata_records_canonical_dispatch_contract(self):
        dispatched_at = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

        metadata = scheduler_lease_metadata(
            {},
            scheduler_id="scheduler-a",
            lease_status=SchedulerLeaseStatus.DISPATCHED,
            observed_at=dispatched_at,
            execution_task_id="run_001:attempt:1",
            backend_key="harbor-local-docker",
            project_id="pilot-project",
        )

        self.assertEqual(metadata["execution"]["schema_version"], EXECUTION_ATTEMPT_METADATA_SCHEMA_VERSION)
        self.assertEqual(metadata["execution"]["scheduler"]["scheduler_id"], "scheduler-a")
        self.assertEqual(metadata["execution"]["scheduler"]["lease_status"], "dispatched")
        self.assertEqual(metadata["execution"]["scheduler"]["execution_task_id"], "run_001:attempt:1")
        self.assertEqual(metadata["execution"]["scheduler"]["backend_key"], "harbor-local-docker")
        self.assertEqual(metadata["execution"]["scheduler"]["project_id"], "pilot-project")
        self.assertEqual(metadata["execution"]["scheduler"]["dispatched_at"], "2026-06-01T12:00:00Z")

    def test_runner_process_metadata_preserves_scheduler_and_records_heartbeat_contract(self):
        dispatched_at = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        claimed_at = datetime(2026, 6, 1, 12, 0, 5, tzinfo=timezone.utc)
        heartbeat_at = datetime(2026, 6, 1, 12, 0, 30, tzinfo=timezone.utc)
        completed_at = datetime(2026, 6, 1, 12, 1, 0, tzinfo=timezone.utc)
        metadata = scheduler_lease_metadata(
            {},
            scheduler_id="scheduler-a",
            lease_status="dispatched",
            observed_at=dispatched_at,
            execution_task_id="run_001:attempt:1",
            backend_key="harbor-local-docker",
            project_id="pilot-project",
        )

        metadata = runner_process_metadata(
            metadata,
            worker_id="worker-a",
            process_status=RunnerProcessStatus.COMPLETED,
            heartbeat_status="succeeded",
            observed_at=heartbeat_at,
            claimed_at=claimed_at,
            completed_at=completed_at,
            process_id=1234,
            return_code=0,
        )

        self.assertEqual(metadata["execution"]["scheduler"]["scheduler_id"], "scheduler-a")
        runner = metadata["execution"]["runner"]
        self.assertEqual(runner["worker_id"], "worker-a")
        self.assertEqual(runner["process_status"], "completed")
        self.assertEqual(runner["heartbeat_status"], "succeeded")
        self.assertEqual(runner["claimed_at"], "2026-06-01T12:00:05Z")
        self.assertEqual(runner["last_heartbeat_at"], "2026-06-01T12:00:30Z")
        self.assertEqual(runner["completed_at"], "2026-06-01T12:01:00Z")
        self.assertEqual(runner["process_id"], 1234)
        self.assertEqual(runner["return_code"], 0)

    def test_runner_process_metadata_preserves_execution_lock_during_heartbeat(self):
        lock_at = datetime(2026, 6, 1, 12, 0, 10, tzinfo=timezone.utc)
        heartbeat_at = datetime(2026, 6, 1, 12, 0, 30, tzinfo=timezone.utc)

        metadata = runner_process_metadata(
            {},
            worker_id="worker-a",
            process_status=RunnerProcessStatus.EXECUTING,
            heartbeat_status="provisioning",
            observed_at=lock_at,
            execution_lock_id="run_001:attempt:1",
            execution_lock_acquired_at=lock_at,
        )
        metadata = runner_process_metadata(
            metadata,
            worker_id="worker-a",
            process_status=RunnerProcessStatus.HEARTBEATING,
            heartbeat_status="running",
            observed_at=heartbeat_at,
        )

        runner = metadata["execution"]["runner"]
        self.assertEqual(runner["process_status"], "heartbeating")
        self.assertEqual(runner["execution_lock_id"], "run_001:attempt:1")
        self.assertEqual(runner["execution_lock_acquired_at"], "2026-06-01T12:00:10Z")

    def test_scheduler_capacity_blocked_metadata_records_current_blocker(self):
        blocked_at = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        block = SchedulerCapacityBlock(
            run_id="run_001",
            project_id="pilot-project",
            scheduler_id="scheduler-a",
            execution_task_id="run_001:attempt:1",
            dimension="provider",
            key="openai",
            active_count=1,
            limit=1,
            reason="provider capacity reached",
            observed_at=blocked_at,
            backend_key="harbor-local-docker",
            provider_key="openai",
            model_key="gpt-5",
            agent_key="codex",
            benchmark_key="terminal-bench@2.0",
        )

        metadata = scheduler_capacity_blocked_metadata({}, block=block)

        scheduler = metadata["execution"]["scheduler"]
        blocked = scheduler["capacity_blocked"]
        self.assertEqual(metadata["execution"]["schema_version"], EXECUTION_ATTEMPT_METADATA_SCHEMA_VERSION)
        self.assertEqual(blocked["dimension"], "provider")
        self.assertEqual(blocked["key"], "openai")
        self.assertEqual(blocked["active_count"], 1)
        self.assertEqual(blocked["limit"], 1)
        self.assertEqual(blocked["observed_at"], "2026-06-01T12:00:00Z")
        self.assertEqual(blocked["provider_key"], "openai")

    def test_execution_metadata_rejects_missing_identifiers(self):
        with self.assertRaisesRegex(ValueError, "scheduler_id must be a non-empty string"):
            scheduler_lease_metadata(
                {},
                scheduler_id="",
                lease_status="dispatched",
                observed_at=datetime.now(timezone.utc),
                execution_task_id="run_001:attempt:1",
                backend_key="harbor-local-docker",
                project_id="pilot-project",
            )

        with self.assertRaisesRegex(ValueError, "worker_id must be a non-empty string"):
            runner_process_metadata(
                {},
                worker_id="",
                process_status="claimed",
                heartbeat_status="provisioning",
                observed_at=datetime.now(timezone.utc),
            )


if __name__ == "__main__":
    unittest.main()
