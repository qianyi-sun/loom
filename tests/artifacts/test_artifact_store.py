import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from agentic_data_platform.artifacts.store import (
    ArtifactKeyFactory,
    Artifacpilot groupjectStore,
    ArtifactPersistence,
    LocalArtifactStore,
    S3ArtifactStore,
)
from agentic_data_platform.domain.artifact_metadata import (
    ARTIFACT_OBJECT_METADATA_SCHEMA_VERSION,
    ArtifactContentType,
    ArtifactUploadStatus,
)
from agentic_data_platform.domain.run_records import (
    ArtifactKind,
    EvaluatorResult,
    JudgeConfig,
    TerminalTurn,
)
from agentic_data_platform.sandbox.docker_terminal import WorkspaceFile, WorkspaceSnapshot


class ArtifactStoreTest(unittest.TestCase):
    def test_key_factory_places_artifacts_under_run_and_task(self):
        factory = ArtifactKeyFactory()

        self.assertEqual(
            factory.trajectory_key("run_001", "conference expense/03"),
            "runs/run_001/tasks/conference-expense-03/trajectory/trajectory.jsonl",
        )
        self.assertEqual(
            factory.workspace_snapshot_key("run_001", "conference expense/03"),
            "runs/run_001/tasks/conference-expense-03/workspace/snapshot.json",
        )
        self.assertEqual(
            factory.evaluator_report_key("run_001", "conference expense/03", "llm judge"),
            "runs/run_001/tasks/conference-expense-03/evaluation/llm-judge/report.json",
        )

    def test_local_store_rejects_unsafe_keys(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalArtifactStore(Path(temp_dir))

            with self.assertRaisesRegex(ValueError, "unsafe artifact key"):
                store.put_bytes("../secret.txt", b"secret", media_type="text/plain")

            with self.assertRaisesRegex(ValueError, "unsafe artifact key"):
                store.put_bytes("/absolute.txt", b"secret", media_type="text/plain")

    def test_local_store_implements_object_store_protocol(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalArtifactStore(Path(temp_dir))

            self.assertIsInstance(store, Artifacpilot groupjectStore)

    def test_object_store_boundary_returns_storage_metadata(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalArtifactStore(Path(temp_dir))

            stored = store.put_bytes(
                "runs/run_001/tasks/task_001/generated/answer.txt",
                b"answer\n",
                media_type="text/plain",
                metadata={"run_id": "run_001"},
            )

        self.assertEqual(stored.key, "runs/run_001/tasks/task_001/generated/answer.txt")
        self.assertTrue(stored.uri.startswith("file://"))
        self.assertEqual(stored.media_type, "text/plain")
        self.assertEqual(stored.size_bytes, 7)
        self.assertEqual(len(stored.sha256), 64)
        self.assertEqual(stored.metadata["run_id"], "run_001")
        self.assertEqual(stored.metadata["artifact_metadata_schema"], ARTIFACT_OBJECT_METADATA_SCHEMA_VERSION)
        self.assertEqual(stored.metadata["upload_status"], ArtifactUploadStatus.COMPLETED.value)
        self.assertEqual(stored.metadata["object_size_bytes"], 7)
        self.assertEqual(stored.metadata["object_sha256"], stored.sha256)

    def test_local_store_reads_bytes_and_returns_file_url_boundary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = LocalArtifactStore(Path(temp_dir))
            store.put_bytes("runs/run_001/output.txt", b"answer\n", media_type="text/plain")

            payload = store.get_bytes("runs/run_001/output.txt")
            url = store.presigned_get_url("runs/run_001/output.txt")

        self.assertEqual(payload, b"answer\n")
        self.assertTrue(url.startswith("file://"))

    def test_s3_store_puts_payload_and_returns_storage_metadata(self):
        client = FakeS3Client()
        store = S3ArtifactStore(bucket="agentic-data-shared dev", client=client)

        stored = store.put_bytes(
            "runs/run_001/tasks/task_001/generated/answer.txt",
            b"answer\n",
            media_type="text/plain",
            metadata={"run_id": "run_001", "turn_count": 1},
        )

        self.assertEqual(stored.key, "runs/run_001/tasks/task_001/generated/answer.txt")
        self.assertEqual(stored.uri, "s3://agentic-data-shared dev/runs/run_001/tasks/task_001/generated/answer.txt")
        self.assertEqual(stored.media_type, "text/plain")
        self.assertEqual(stored.size_bytes, 7)
        self.assertEqual(len(stored.sha256), 64)
        self.assertEqual(stored.metadata["storage_key"], "runs/run_001/tasks/task_001/generated/answer.txt")
        self.assertEqual(stored.metadata["storage_bucket"], "agentic-data-shared dev")
        self.assertEqual(stored.metadata["artifact_metadata_schema"], ARTIFACT_OBJECT_METADATA_SCHEMA_VERSION)
        self.assertEqual(stored.metadata["upload_status"], ArtifactUploadStatus.COMPLETED.value)
        self.assertEqual(stored.metadata["object_size_bytes"], 7)
        self.assertEqual(stored.metadata["object_sha256"], stored.sha256)
        self.assertEqual(client.put_object_calls[0]["Bucket"], "agentic-data-shared dev")
        self.assertEqual(client.put_object_calls[0]["Key"], "runs/run_001/tasks/task_001/generated/answer.txt")
        self.assertEqual(client.put_object_calls[0]["Body"], b"answer\n")
        self.assertEqual(client.put_object_calls[0]["ContentType"], "text/plain")
        self.assertEqual(client.put_object_calls[0]["Metadata"]["run_id"], "run_001")
        self.assertEqual(client.put_object_calls[0]["Metadata"]["turn_count"], "1")

    def test_s3_store_bootstraps_missing_bucket_and_downloads_payload(self):
        client = FakeS3Client(missing_bucket=True)
        store = S3ArtifactStore(bucket="agentic-data-shared dev", client=client)

        store.ensure_bucket()
        store.put_bytes("runs/run_001/output.txt", b"payload", media_type="text/plain")

        self.assertEqual(client.created_buckets, ["agentic-data-shared dev"])
        self.assertEqual(store.get_bytes("runs/run_001/output.txt"), b"payload")
        self.assertEqual(
            store.presigned_get_url("runs/run_001/output.txt", expires_in_seconds=60),
            "https://storage.example/agentic-data-shared dev/runs/run_001/output.txt?expires=60",
        )

    def test_persist_trajectory_writes_jsonl_and_returns_artifact_ref(self):
        turn = TerminalTurn(
            turn_index=0,
            command="python solve.py",
            cwd="/workspace",
            started_at=datetime(2026, 5, 28, 12, 0, 0, tzinfo=timezone.utc),
            completed_at=datetime(2026, 5, 28, 12, 0, 3, tzinfo=timezone.utc),
            exit_code=0,
            stdout="done\n",
            stderr="",
            changed_paths=["answer.txt"],
            model_call_id="call_001",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            persistence = ArtifactPersistence(LocalArtifactStore(Path(temp_dir)))

            ref = persistence.persist_trajectory(
                run_id="run_001",
                task_instance_id="conference-expense-03",
                turns=[turn],
            )

            payload_path = Path(temp_dir) / "runs/run_001/tasks/conference-expense-03/trajectory/trajectory.jsonl"
            payload = payload_path.read_text().splitlines()

        self.assertEqual(ref.kind, ArtifactKind.TRAJECTORY)
        self.assertEqual(ref.media_type, "application/x-ndjson")
        self.assertGreater(ref.size_bytes, 0)
        self.assertEqual(len(ref.sha256), 64)
        self.assertTrue(ref.uri.startswith("file://"))
        self.assertEqual(ref.metadata["run_id"], "run_001")
        self.assertEqual(ref.metadata["task_instance_id"], "conference-expense-03")
        self.assertEqual(ref.metadata["content_type"], ArtifactContentType.TRAJECTORY_JSONL.value)
        self.assertEqual(ref.metadata["upload_status"], ArtifactUploadStatus.COMPLETED.value)
        self.assertEqual(ref.metadata["storage_key"], "runs/run_001/tasks/conference-expense-03/trajectory/trajectory.jsonl")
        self.assertEqual(len(payload), 1)
        self.assertEqual(json.loads(payload[0])["command"], "python solve.py")

    def test_persist_workspace_snapshot_writes_manifest(self):
        snapshot = WorkspaceSnapshot(
            run_id="run_001",
            workspace_path="/tmp/workspaces/run_001",
            captured_at=datetime(2026, 5, 28, 12, 1, 0, tzinfo=timezone.utc),
            files=[
                WorkspaceFile(path="answer.txt", size_bytes=3, sha256="1" * 64),
                WorkspaceFile(path="nested/table.csv", size_bytes=12, sha256="2" * 64),
            ],
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            persistence = ArtifactPersistence(LocalArtifactStore(Path(temp_dir)))

            ref = persistence.persist_workspace_snapshot(
                run_id="run_001",
                task_instance_id="conference-expense-03",
                snapshot=snapshot,
            )

            payload_path = Path(temp_dir) / "runs/run_001/tasks/conference-expense-03/workspace/snapshot.json"
            payload = json.loads(payload_path.read_text())

        self.assertEqual(ref.kind, ArtifactKind.WORKSPACE_SNAPSHOT)
        self.assertEqual(ref.media_type, "application/json")
        self.assertEqual(ref.metadata["content_type"], ArtifactContentType.WORKSPACE_SNAPSHOT_MANIFEST.value)
        self.assertEqual(ref.metadata["upload_status"], ArtifactUploadStatus.COMPLETED.value)
        self.assertEqual(payload["run_id"], "run_001")
        self.assertEqual(payload["files"][0]["path"], "answer.txt")
        self.assertEqual(payload["files"][1]["sha256"], "2" * 64)

    def test_persist_evaluator_report_writes_json_and_metadata(self):
        result = EvaluatorResult(
            evaluator_id="llm-judge-v0",
            status="completed",
            score=0.9,
            metrics={"task_success": True},
            verbal_feedback="The final spreadsheet is correct.",
            judge=JudgeConfig(
                provider="openai",
                model_name="gpt-5",
                rubric_version="latent-skill-v0",
            ),
            artifact_refs=["file://previous/artifact"],
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            persistence = ArtifactPersistence(LocalArtifactStore(Path(temp_dir)))

            ref = persistence.persist_evaluator_report(
                run_id="run_001",
                task_instance_id="conference-expense-03",
                result=result,
            )

            payload_path = Path(temp_dir) / "runs/run_001/tasks/conference-expense-03/evaluation/llm-judge-v0/report.json"
            payload = json.loads(payload_path.read_text())

        self.assertEqual(ref.kind, ArtifactKind.EVALUATOR_REPORT)
        self.assertEqual(ref.metadata["content_type"], ArtifactContentType.EVALUATOR_REPORT.value)
        self.assertEqual(ref.metadata["upload_status"], ArtifactUploadStatus.COMPLETED.value)
        self.assertEqual(ref.metadata["evaluator_id"], "llm-judge-v0")
        self.assertEqual(payload["score"], 0.9)
        self.assertEqual(payload["judge"]["rubric_version"], "latent-skill-v0")
        self.assertIn("spreadsheet", payload["verbal_feedback"])


class FakeS3Client:
    def __init__(self, *, missing_bucket: bool = False) -> None:
        self.missing_bucket = missing_bucket
        self.created_buckets: list[str] = []
        self.put_object_calls: list[dict] = []
        self.objects: dict[tuple[str, str], bytes] = {}

    def head_bucket(self, *, Bucket: str) -> None:
        if self.missing_bucket and Bucket not in self.created_buckets:
            raise FakeS3ClientError({"Error": {"Code": "404", "Message": "Not Found"}})

    def create_bucket(self, *, Bucket: str) -> None:
        self.created_buckets.append(Bucket)

    def put_object(self, **kwargs) -> None:
        self.put_object_calls.append(kwargs)
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs["Body"]

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        return {"Body": FakeS3Body(self.objects[(Bucket, Key)])}

    def generate_presigned_url(self, ClientMethod: str, Params: dict, ExpiresIn: int) -> str:
        self.presign_client_method = ClientMethod
        return f"https://storage.example/{Params['Bucket']}/{Params['Key']}?expires={ExpiresIn}"


class FakeS3Body:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return self.payload


class FakeS3ClientError(Exception):
    def __init__(self, response: dict) -> None:
        super().__init__(response["Error"]["Message"])
        self.response = response
