import json
import tarfile
import tempfile
import unittest
from pathlib import Path

from agentic_data_platform.artifacts.store import ArtifactPersistence, LocalArtifactStore
from agentic_data_platform.domain.run_records import ArtifactKind
from agentic_data_platform.harbor.ingestion import HarborResultIngestor


class HarborResultIngestorTest(unittest.TestCase):
    def test_ingests_single_trial_jobs_directory_into_platform_records(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            jobs_dir = _write_harbor_job_fixture(temp_path / "jobs")
            store = LocalArtifactStore(temp_path / "object-store")
            ingestor = HarborResultIngestor(artifact_persistence=ArtifactPersistence(store))

            result = ingestor.ingest(
                run_id="run_harbor_001",
                task_instance_id="terminal-bench-hello",
                jobs_dir=jobs_dir,
            )

            self.assertEqual(result.job_name, "job-001")
            self.assertEqual(result.trial_name, "trial-hello")
            self.assertEqual(len(result.turns), 2)
            self.assertEqual(result.turns[0].command, "sed -n '1,120p' instructions.txt")
            self.assertEqual(result.turns[1].stdout, "42\n")
            self.assertEqual(result.evaluator_results[0].evaluator_id, "harbor-verifier")
            self.assertEqual(result.evaluator_results[0].mode, "harbor_verifier")
            self.assertEqual(result.evaluator_results[0].score, 1.0)
            self.assertEqual(result.evaluator_results[0].metrics["reward"], 1.0)
            self.assertEqual(result.evaluator_results[0].metadata["verifier_version"], "harbor-test-v1")
            self.assertIsNone(result.evaluator_results[0].judge)

            raw_jobs_artifact = next(artifact for artifact in result.artifacts if artifact.kind is ArtifactKind.LOG)
            trajectory_artifact = next(artifact for artifact in result.artifacts if artifact.kind is ArtifactKind.TRAJECTORY)
            generated_artifact = next(artifact for artifact in result.artifacts if artifact.kind is ArtifactKind.GENERATED_FILE)
            evaluator_artifact = next(artifact for artifact in result.artifacts if artifact.kind is ArtifactKind.EVALUATOR_REPORT)

            self.assertEqual(raw_jobs_artifact.metadata["content_type"], "harbor_jobs_archive")
            self.assertEqual(trajectory_artifact.metadata["turn_count"], 2)
            self.assertEqual(generated_artifact.uri, "harbor-artifact://job-001/trial-hello/artifacts/answer.txt")
            self.assertEqual(generated_artifact.metadata["destination"], "artifacts/answer.txt")
            self.assertIn(evaluator_artifact.uri, result.evaluator_results[0].artifact_refs)

            archive_payload = store.get_bytes(raw_jobs_artifact.metadata["storage_key"])
            archive_path = temp_path / "raw-harbor-jobs.tar.gz"
            archive_path.write_bytes(archive_payload)
            with tarfile.open(archive_path, "r:gz") as archive:
                names = set(archive.getnames())

            self.assertIn("job-001/result.json", names)
            self.assertIn("job-001/trial-hello/verifier/reward.txt", names)
            self.assertNotIn(str(temp_path), json.dumps(result.to_dict()))

    def test_rejects_ambiguous_jobs_directory_without_trial_name(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            jobs_dir = _write_harbor_job_fixture(temp_path / "jobs")
            _write_trial(jobs_dir / "job-001" / "trial-extra", reward=0.0)
            ingestor = HarborResultIngestor(artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "store")))

            with self.assertRaisesRegex(ValueError, "multiple Harbor trials"):
                ingestor.ingest(
                    run_id="run_harbor_002",
                    task_instance_id="terminal-bench-hello",
                    jobs_dir=jobs_dir,
                )

    def test_ingests_verifier_rewards_from_trial_result_when_reward_files_are_absent(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            jobs_dir = _write_harbor_job_fixture(temp_path / "jobs")
            trial_dir = jobs_dir / "job-001" / "trial-hello"
            (trial_dir / "verifier" / "reward.txt").unlink()
            trial_result_path = trial_dir / "result.json"
            trial_result = json.loads(trial_result_path.read_text(encoding="utf-8"))
            trial_result["verifier_result"] = {"rewards": {"reward": 1.0, "smoke_metric": 0.75}}
            trial_result_path.write_text(json.dumps(trial_result), encoding="utf-8")
            ingestor = HarborResultIngestor(
                artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "store"))
            )

            result = ingestor.ingest(
                run_id="run_harbor_003",
                task_instance_id="terminal-bench-hello",
                jobs_dir=jobs_dir,
            )

            self.assertEqual(result.evaluator_results[0].score, 1.0)
            self.assertEqual(result.evaluator_results[0].metrics["reward"], 1.0)
            self.assertEqual(result.evaluator_results[0].metrics["smoke_metric"], 0.75)


def _write_harbor_job_fixture(root: Path) -> Path:
    job_dir = root / "job-001"
    job_dir.mkdir(parents=True)
    (job_dir / "config.json").write_text(
        json.dumps({"dataset": "terminal-bench/terminal-bench-2", "agent": "oracle", "model": "gpt-5"}),
        encoding="utf-8",
    )
    (job_dir / "result.json").write_text(
        json.dumps({"status": "completed", "n_trials": 1, "accuracy": 1.0}),
        encoding="utf-8",
    )
    _write_trial(job_dir / "trial-hello", reward=1.0)
    return root


def _write_trial(trial_dir: Path, *, reward: float) -> None:
    trial_dir.mkdir(parents=True)
    (trial_dir / "config.json").write_text(
        json.dumps({"task": "hello-world", "verifier_version": "harbor-test-v1"}),
        encoding="utf-8",
    )
    (trial_dir / "result.json").write_text(
        json.dumps({"status": "completed", "duration_seconds": 12.5}),
        encoding="utf-8",
    )
    agent_dir = trial_dir / "agent"
    agent_dir.mkdir()
    (agent_dir / "trajectory.json").write_text(
        json.dumps(
            [
                {
                    "command": "sed -n '1,120p' instructions.txt",
                    "cwd": "/workspace",
                    "started_at": "2026-05-29T12:00:00Z",
                    "completed_at": "2026-05-29T12:00:01Z",
                    "exit_code": 0,
                    "stdout": "Find the answer.\n",
                    "stderr": "",
                    "changed_paths": [],
                },
                {
                    "command": "python solve.py",
                    "cwd": "/workspace",
                    "started_at": "2026-05-29T12:00:02Z",
                    "completed_at": "2026-05-29T12:00:03Z",
                    "exit_code": 0,
                    "stdout": "42\n",
                    "stderr": "",
                    "changed_paths": ["answer.txt"],
                },
            ]
        ),
        encoding="utf-8",
    )
    verifier_dir = trial_dir / "verifier"
    verifier_dir.mkdir()
    (verifier_dir / "reward.txt").write_text(f"{reward}\n", encoding="utf-8")
    (verifier_dir / "test-stdout.txt").write_text("passed\n", encoding="utf-8")
    (verifier_dir / "test-stderr.txt").write_text("", encoding="utf-8")
    artifacts_dir = trial_dir / "artifacts"
    artifacts_dir.mkdir()
    (artifacts_dir / "answer.txt").write_text("42\n", encoding="utf-8")
    (artifacts_dir / "manifest.json").write_text(
        json.dumps(
            [
                {
                    "source": "/logs/artifacts/answer.txt",
                    "destination": "artifacts/answer.txt",
                    "type": "file",
                    "status": "ok",
                }
            ]
        ),
        encoding="utf-8",
    )
