import unittest
from datetime import datetime, timezone

from agentic_data_platform.domain.artifact_metadata import (
    ARTIFACT_OBJECT_METADATA_SCHEMA_VERSION,
    ArtifactChunkKind,
    ArtifactChunkMetadata,
    ArtifactContentType,
    ArtifactUploadStatus,
    artifact_content_type_value,
    finalize_stored_artifact_metadata,
)


class ArtifactMetadataContractTest(unittest.TestCase):
    def test_artifact_content_type_contract_covers_current_persistence_paths(self):
        self.assertEqual(ArtifactContentType.TRAJECTORY_JSONL.value, "trajectory_jsonl")
        self.assertEqual(ArtifactContentType.WORKSPACE_SNAPSHOT_MANIFEST.value, "workspace_snapshot_manifest")
        self.assertEqual(ArtifactContentType.EVALUATOR_REPORT.value, "evaluator_report")
        self.assertEqual(ArtifactContentType.HARBOR_JOBS_ARCHIVE.value, "harbor_jobs_archive")
        self.assertEqual(ArtifactContentType.HARBOR_RUNNER_REPORT.value, "harbor_runner_report")
        self.assertEqual(ArtifactContentType.HARBOR_INGESTION_DIAGNOSTICS.value, "harbor_ingestion_diagnostics")
        self.assertEqual(ArtifactContentType.LOCAL_FILE_ARTIFACT.value, "local_file_artifact")
        self.assertEqual(ArtifactContentType.ORIGINAL_WRAPPER_RESULT.value, "original_wrapper_result")
        self.assertEqual(ArtifactContentType.ORIGINAL_WRAPPER_ARTIFACT.value, "original_wrapper_artifact")
        self.assertEqual(ArtifactContentType.HARBOR_TASK_ARCHIVE.value, "harbor_task_archive")

    def test_upload_status_contract_covers_recovery_visible_states(self):
        self.assertEqual(ArtifactUploadStatus.PENDING.value, "pending")
        self.assertEqual(ArtifactUploadStatus.STARTED.value, "started")
        self.assertEqual(ArtifactUploadStatus.COMPLETED.value, "completed")
        self.assertEqual(ArtifactUploadStatus.FAILED.value, "failed")
        self.assertEqual(ArtifactUploadStatus.EXPIRED.value, "expired")

    def test_finalize_stored_artifact_metadata_adds_canonical_object_fields(self):
        metadata = finalize_stored_artifact_metadata(
            {
                "run_id": "run_001",
                "content_type": ArtifactContentType.TRAJECTORY_JSONL,
                "optional_none": None,
            },
            storage_key="runs/run_001/tasks/task/trajectory/trajectory.jsonl",
            size_bytes=12,
            sha256="1" * 64,
            storage_bucket="agentic-data-shared dev",
        )

        self.assertEqual(metadata["artifact_metadata_schema"], ARTIFACT_OBJECT_METADATA_SCHEMA_VERSION)
        self.assertEqual(metadata["upload_status"], ArtifactUploadStatus.COMPLETED.value)
        self.assertEqual(metadata["content_type"], ArtifactContentType.TRAJECTORY_JSONL.value)
        self.assertEqual(metadata["object_size_bytes"], 12)
        self.assertEqual(metadata["object_sha256"], "1" * 64)
        self.assertEqual(metadata["storage_bucket"], "agentic-data-shared dev")
        self.assertNotIn("optional_none", metadata)

    def test_artifact_chunk_metadata_serializes_safe_chunk_contract(self):
        chunk = ArtifactChunkMetadata(
            run_id="run_001",
            attempt_id="run_001:attempt:1",
            artifact_id="run_001-stdout",
            chunk_kind=ArtifactChunkKind.STDOUT,
            chunk_sequence=0,
            storage_key="runs/run_001/tasks/task/logs/stdout/000000.jsonl",
            media_type="application/x-ndjson",
            size_bytes=128,
            sha256="2" * 64,
            upload_status=ArtifactUploadStatus.COMPLETED,
            created_at=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
        )

        payload = chunk.to_dict()

        self.assertEqual(payload["schema_version"], "artifact-chunk-metadata-v1")
        self.assertEqual(payload["chunk_kind"], "stdout")
        self.assertEqual(payload["upload_status"], "completed")
        self.assertEqual(payload["chunk_sequence"], 0)

    def test_artifact_chunk_metadata_rejects_invalid_chunk_sequence(self):
        with self.assertRaisesRegex(ValueError, "chunk_sequence must be non-negative"):
            ArtifactChunkMetadata(
                run_id="run_001",
                attempt_id="run_001:attempt:1",
                artifact_id="run_001-stdout",
                chunk_kind=ArtifactChunkKind.STDOUT,
                chunk_sequence=-1,
                storage_key="runs/run_001/tasks/task/logs/stdout/000000.jsonl",
                media_type="application/x-ndjson",
                size_bytes=128,
                sha256="2" * 64,
                upload_status=ArtifactUploadStatus.COMPLETED,
                created_at=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
            )

    def test_artifact_content_type_value_rejects_blank_values(self):
        with self.assertRaisesRegex(ValueError, "content_type must be a non-empty string"):
            artifact_content_type_value("")


if __name__ == "__main__":
    unittest.main()
