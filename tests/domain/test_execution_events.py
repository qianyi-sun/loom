import unittest
from datetime import datetime, timezone

from agentic_data_platform.domain.execution_events import (
    RecoveryReasonCode,
    RunEventType,
    recovery_event_metadata,
)
from agentic_data_platform.domain.run_records import RunStatus, RunStatusEvent


class ExecutionEventContractTest(unittest.TestCase):
    def test_run_event_type_contract_covers_current_execution_boundaries(self):
        self.assertEqual(RunEventType.CREATED.value, "run.created")
        self.assertEqual(RunEventType.DISPATCHED.value, "run.dispatched")
        self.assertEqual(RunEventType.CLAIMED.value, "run.claimed")
        self.assertEqual(RunEventType.STARTED.value, "run.started")
        self.assertEqual(RunEventType.EVALUATING.value, "run.evaluating")
        self.assertEqual(RunEventType.SUCCEEDED.value, "run.succeeded")
        self.assertEqual(RunEventType.FAILED.value, "run.failed")
        self.assertEqual(RunEventType.CANCELED.value, "run.canceled")
        self.assertEqual(RunEventType.RETRIED.value, "run.retried")
        self.assertEqual(RunEventType.RECOVERED.value, "run.recovered")
        self.assertEqual(RunEventType.WORKER_FAILED.value, "run.worker_failed")
        self.assertEqual(RunEventType.WORKER_SUBPROCESS_FAILED.value, "run.worker_subprocess_failed")
        self.assertEqual(RunEventType.SCHEDULER_CAPACITY_BLOCKED.value, "scheduler.capacity_blocked")
        self.assertEqual(RunEventType.ARTIFACT_CHUNK_RECORDED.value, "artifact.chunk_recorded")
        self.assertEqual(RunEventType.ARTIFACT_UPLOAD_EXPIRED.value, "artifact.upload_expired")
        self.assertEqual(RunEventType.LOG_CHUNK_RECORDED.value, "log.chunk_recorded")
        self.assertEqual(RunEventType.SANDBOX_CONTAINER_CLEANUP.value, "sandbox.container_cleanup")
        self.assertEqual(RunEventType.EVALUATOR_COMPLETED.value, "evaluator.completed")
        self.assertEqual(RunEventType.EVALUATOR_FAILED.value, "evaluator.failed")
        self.assertEqual(RunEventType.PROJECTION_REFRESHED.value, "projection.refreshed")

    def test_recovery_reason_codes_cover_phase_one_reconciliation_paths(self):
        self.assertEqual(RecoveryReasonCode.STALE_DISPATCHED.value, "stale_dispatched")
        self.assertEqual(RecoveryReasonCode.STALE_WORKER_HEARTBEAT.value, "stale_worker_heartbeat")
        self.assertEqual(RecoveryReasonCode.TERMINAL_RESULT_MISMATCH.value, "terminal_result_mismatch")
        self.assertEqual(RecoveryReasonCode.CANCELED_RESOURCE_CLEANUP.value, "canceled_resource_cleanup")
        self.assertEqual(RecoveryReasonCode.DOCKER_CONTAINER_CLEANUP.value, "docker_container_cleanup")
        self.assertEqual(RecoveryReasonCode.ARTIFACT_UPLOAD_EXPIRED.value, "artifact_upload_expired")
        self.assertEqual(RecoveryReasonCode.PROJECTION_REFRESH_FAILED.value, "projection_refresh_failed")

    def test_recovery_event_metadata_uses_canonical_reason_code(self):
        metadata = recovery_event_metadata(
            RecoveryReasonCode.STALE_DISPATCHED,
            scheduler_id="scheduler-a",
            stale_before="2026-06-01T12:00:00+00:00",
            optional_none=None,
        )

        self.assertEqual(metadata["recovery"], "stale_dispatched")
        self.assertEqual(metadata["scheduler_id"], "scheduler-a")
        self.assertEqual(metadata["stale_before"], "2026-06-01T12:00:00+00:00")
        self.assertNotIn("optional_none", metadata)

    def test_run_status_event_stores_typed_event_as_string_value(self):
        event = RunStatusEvent(
            event_id="evt_001",
            seq=1,
            run_id="run_001",
            attempt_id="run_001:attempt:1",
            event_type=RunEventType.RECOVERED,
            from_status=RunStatus.DISPATCHED,
            to_status=RunStatus.QUEUED,
            created_at=datetime.now(timezone.utc),
        )

        self.assertEqual(event.event_type, "run.recovered")
        self.assertIs(type(event.event_type), str)


if __name__ == "__main__":
    unittest.main()
