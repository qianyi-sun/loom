import subprocess
import tempfile
import unittest
from pathlib import Path

from agentic_data_platform.harbor.runner import HarborRunSpec, HarborRunnerBackend


class HarborRunnerBackendTest(unittest.TestCase):
    def test_builds_dataset_command_and_captures_jobs_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            jobs_dir = Path(temp_dir) / "jobs"
            command_runner = FakeCommandRunner(stdout="harbor ok\n")
            spec = HarborRunSpec(
                run_id="run_harbor_runner_001",
                task_instance_id="terminal-bench-hello",
                dataset_ref="terminal-bench/terminal-bench-2",
                agent="default",
                model_name="gpt-5-mini",
                jobs_dir=jobs_dir,
                timeout_seconds=45,
                extra_args=["--max-tasks", "1"],
            )

            result = HarborRunnerBackend(command_runner=command_runner).run(spec)

            self.assertEqual(command_runner.calls[0]["timeout"], 45)
            self.assertEqual(
                result.command,
                [
                    "harbor",
                    "run",
                    "-d",
                    "terminal-bench/terminal-bench-2",
                    "--agent",
                    "default",
                    "--models",
                    "gpt-5-mini",
                    "--sandbox",
                    "docker",
                    "--jobs-dir",
                    str(jobs_dir),
                    "--max-tasks",
                    "1",
                ],
            )
            self.assertEqual(result.exit_code, 0)
            self.assertEqual(result.stdout, "harbor ok\n")
            self.assertEqual(result.jobs_dir, jobs_dir)
            self.assertGreaterEqual(result.duration_seconds, 0)
            self.assertTrue(jobs_dir.is_dir())

    def test_requires_exactly_one_dataset_or_task_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "exactly one"):
                HarborRunSpec(
                    run_id="run_harbor_runner_002",
                    task_instance_id="terminal-bench-hello",
                    model_name="gpt-5-mini",
                    jobs_dir=Path(temp_dir) / "jobs",
                )


class FakeCommandRunner:
    def __init__(self, *, returncode: int = 0, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.calls: list[dict[str, object]] = []

    def run(self, args: list[str], *, timeout: int) -> subprocess.CompletedProcess[str]:
        self.calls.append({"args": args, "timeout": timeout})
        return subprocess.CompletedProcess(args=args, returncode=self.returncode, stdout=self.stdout, stderr=self.stderr)
