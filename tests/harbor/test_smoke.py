import json
import subprocess
import tomllib
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from agentic_data_platform.harbor.smoke import (
    HarborCliSmokeConfig,
    _write_smoke_task,
    run_harbor_cli_smoke,
)


class HarborCliSmokeTest(unittest.TestCase):
    def test_generated_task_metadata_uses_harbor_schema_author_shape(self):
        with TemporaryDirectory() as temp_dir:
            task_dir = Path(temp_dir) / "task"
            _write_smoke_task(task_dir)

            task_config = tomllib.loads((task_dir / "task.toml").read_text(encoding="utf-8"))

            authors = task_config["task"]["authors"]
            self.assertTrue(authors)
            self.assertTrue(all(isinstance(author, dict) for author in authors))
            self.assertEqual(authors[0]["name"], "CARIN Research Center")

    def test_creates_task_runs_harbor_and_ingests_output(self):
        with TemporaryDirectory() as temp_dir:
            workspace_root = Path(temp_dir) / "workspace"
            command_runner = FakeHarborCommandRunner()

            result = run_harbor_cli_smoke(
                HarborCliSmokeConfig(
                    workspace_root=workspace_root,
                    run_id="harbor_smoke_unit",
                    model_name="smoke/noop",
                    timeout_seconds=30,
                ),
                command_runner=command_runner,
            )

            self.assertEqual(result["run_id"], "harbor_smoke_unit")
            self.assertEqual(result["status"], "succeeded")
            self.assertEqual(result["score"], 1.0)
            self.assertEqual(result["turn_count"], 1)
            self.assertGreaterEqual(result["artifact_count"], 3)
            task_dir = workspace_root / "harbor_smoke_unit" / "task"
            self.assertTrue((task_dir / "task.toml").is_file())
            self.assertTrue((task_dir / "solution" / "solve.sh").is_file())
            self.assertEqual(command_runner.calls[0]["args"][0:2], ["harbor", "run"])
            self.assertIn("--agent", command_runner.calls[0]["args"])
            self.assertIn("oracle", command_runner.calls[0]["args"])
            self.assertIn("--yes", command_runner.calls[0]["args"])


class FakeHarborCommandRunner:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def run(self, args: list[str], *, timeout: int) -> subprocess.CompletedProcess[str]:
        self.calls.append({"args": args, "timeout": timeout})
        jobs_dir = Path(args[args.index("--jobs-dir") + 1])
        _write_harbor_job_fixture(jobs_dir)
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="harbor smoke ok\n", stderr="")


def _write_harbor_job_fixture(root: Path) -> None:
    trial_dir = root / "job-smoke" / "trial-smoke"
    (trial_dir / "agent").mkdir(parents=True)
    (trial_dir / "verifier").mkdir(parents=True)
    (root / "job-smoke" / "config.json").write_text(json.dumps({"job": "smoke"}), encoding="utf-8")
    (root / "job-smoke" / "result.json").write_text(json.dumps({"status": "completed"}), encoding="utf-8")
    (trial_dir / "config.json").write_text(json.dumps({"task": "smoke"}), encoding="utf-8")
    (trial_dir / "result.json").write_text(json.dumps({"status": "completed"}), encoding="utf-8")
    (trial_dir / "agent" / "trajectory.json").write_text(
        json.dumps(
            [
                {
                    "command": "bash /solution/solve.sh",
                    "cwd": "/app",
                    "started_at": "2026-05-29T12:00:00Z",
                    "completed_at": "2026-05-29T12:00:01Z",
                    "exit_code": 0,
                    "stdout": "ok\n",
                    "stderr": "",
                    "changed_paths": ["smoke-output.txt"],
                    "model_call_id": "oracle-smoke",
                }
            ]
        ),
        encoding="utf-8",
    )
    (trial_dir / "verifier" / "reward.txt").write_text("1\n", encoding="utf-8")
