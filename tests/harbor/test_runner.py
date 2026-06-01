import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from agentic_data_platform.harbor.runner import HarborCliRunnerBackend, HarborRunSpec, HarborRunnerBackend


class HarborRunnerBackendTest(unittest.TestCase):
    def test_runner_backend_alias_preserves_cli_runner_compatibility(self):
        self.assertIs(HarborRunnerBackend, HarborCliRunnerBackend)

    def test_run_spec_defaults_to_cli_backend_and_accepts_native_selector(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cli_spec = HarborRunSpec(
                run_id="run_harbor_runner_backend_cli",
                task_instance_id="local-task",
                task_path=Path(temp_dir) / "task",
                model_name="gpt-5",
                jobs_dir=Path(temp_dir) / "jobs-cli",
            )
            native_spec = HarborRunSpec(
                run_id="run_harbor_runner_backend_native",
                task_instance_id="local-task",
                task_path=Path(temp_dir) / "task",
                model_name="gpt-5",
                jobs_dir=Path(temp_dir) / "jobs-native",
                backend="native",
            )

            self.assertEqual(cli_spec.backend, "cli")
            self.assertEqual(native_spec.backend, "native")

    def test_rejects_unknown_backend_selector(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "backend must be one of"):
                HarborRunSpec(
                    run_id="run_harbor_runner_backend_bad",
                    task_instance_id="local-task",
                    task_path=Path(temp_dir) / "task",
                    model_name="gpt-5",
                    jobs_dir=Path(temp_dir) / "jobs",
                    backend="shell",
                )

    def test_cli_runner_rejects_native_backend_specs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(ValueError, "HarborCliRunnerBackend only supports backend='cli'"):
                HarborCliRunnerBackend(command_runner=FakeCommandRunner(stdout="harbor ok\n")).run(
                    HarborRunSpec(
                        run_id="run_harbor_runner_native_on_cli",
                        task_instance_id="local-task",
                        task_path=Path(temp_dir) / "task",
                        model_name="gpt-5",
                        jobs_dir=Path(temp_dir) / "jobs",
                        backend="native",
                    )
                )

    def test_builds_dataset_command_and_captures_jobs_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            jobs_dir = Path(temp_dir) / "jobs"
            command_runner = FakeCommandRunner(stdout="harbor ok\n")
            spec = HarborRunSpec(
                run_id="run_harbor_runner_001",
                task_instance_id="terminal-bench-hello",
                dataset_ref="terminal-bench/terminal-bench-2",
                agent="claude-code",
                model_name="gpt-5-mini",
                jobs_dir=jobs_dir,
                timeout_seconds=45,
                extra_args=["--n-tasks", "1"],
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
                    "claude-code",
                    "--model",
                    "gpt-5-mini",
                    "--env",
                    "docker",
                    "--jobs-dir",
                    str(jobs_dir),
                    "--yes",
                    "--n-tasks",
                    "1",
                ],
            )
            self.assertEqual(result.exit_code, 0)
            self.assertEqual(result.stdout, "harbor ok\n")
            self.assertEqual(result.jobs_dir, jobs_dir)
            self.assertGreaterEqual(result.duration_seconds, 0)
            self.assertTrue(jobs_dir.is_dir())

    def test_cli_runner_records_job_directory_created_by_current_run(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            jobs_dir = Path(temp_dir) / "jobs"
            _write_minimal_harbor_job(jobs_dir / "job-001")
            command_runner = FakeCommandRunner(stdout="harbor ok\n", created_job_name="job-002")
            spec = HarborRunSpec(
                run_id="run_harbor_runner_current_job",
                task_instance_id="terminal-bench-hello",
                dataset_ref="terminal-bench/terminal-bench-2",
                agent="codex",
                model_name="gpt-5-mini",
                jobs_dir=jobs_dir,
            )

            result = HarborRunnerBackend(command_runner=command_runner).run(spec)

            self.assertEqual(result.jobs_dir, jobs_dir)
            self.assertEqual(result.job_dir, jobs_dir / "job-002")
            self.assertEqual(result.to_report()["job_dir"], "job-002")

    def test_builds_task_path_command_with_custom_environment_and_env_args(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeCommandRunner(stdout="harbor ok\n")
            spec = HarborRunSpec(
                run_id="run_harbor_runner_003",
                task_instance_id="local-task",
                task_path=temp_path / "task",
                agent="codex",
                model_name="gpt-5",
                environment="docker",
                jobs_dir=temp_path / "jobs",
                agent_env=["OPENAI_BASE_URL=https://models.example/v1"],
                verifier_env=["EVALUATOR_MODE=smoke"],
            )

            result = HarborRunnerBackend(command_runner=command_runner).run(spec)

            self.assertEqual(
                result.command,
                [
                    "harbor",
                    "run",
                    "-p",
                    str(temp_path / "task"),
                    "--agent",
                    "codex",
                    "--model",
                    "gpt-5",
                    "--env",
                    "docker",
                    "--jobs-dir",
                    str(temp_path / "jobs"),
                    "--yes",
                    "--agent-env",
                    "OPENAI_BASE_URL=https://models.example/v1",
                    "--verifier-env",
                    "EVALUATOR_MODE=smoke",
                ],
            )

    def test_runner_report_redacts_environment_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            result = HarborRunnerBackend(command_runner=FakeCommandRunner(stdout="harbor ok\n")).run(
                HarborRunSpec(
                    run_id="run_harbor_runner_004",
                    task_instance_id="local-task",
                    task_path=Path(temp_dir) / "task",
                    agent="codex",
                    model_name="gpt-5",
                    jobs_dir=Path(temp_dir) / "jobs",
                    agent_env=["OPENAI_API_KEY=secret-key"],
                    verifier_env=["EVALUATOR_API_KEY=secret-evaluator-key"],
                )
            )

            report = result.to_report()

            self.assertIn("--agent-env", report["command"])
            self.assertIn("OPENAI_API_KEY=[redacted]", report["command"])
            self.assertIn("EVALUATOR_API_KEY=[redacted]", report["command"])
            self.assertNotIn("secret-key", json_string(report))
            self.assertNotIn("secret-evaluator-key", json_string(report))

    def test_runner_report_records_cli_backend(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            result = HarborCliRunnerBackend(command_runner=FakeCommandRunner(stdout="harbor ok\n")).run(
                HarborRunSpec(
                    run_id="run_harbor_runner_backend_report",
                    task_instance_id="local-task",
                    task_path=Path(temp_dir) / "task",
                    model_name="gpt-5",
                    jobs_dir=Path(temp_dir) / "jobs",
                )
            )

            self.assertEqual(result.to_report()["backend"], "cli")

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
    def __init__(
        self,
        *,
        returncode: int = 0,
        stdout: str = "",
        stderr: str = "",
        created_job_name: str | None = None,
    ) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.created_job_name = created_job_name
        self.calls: list[dict[str, object]] = []

    def run(
        self,
        args: list[str],
        *,
        timeout: int,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append({"args": args, "timeout": timeout, "env": env})
        if self.created_job_name is not None:
            jobs_dir = Path(args[args.index("--jobs-dir") + 1])
            _write_minimal_harbor_job(jobs_dir / self.created_job_name)
        return subprocess.CompletedProcess(args=args, returncode=self.returncode, stdout=self.stdout, stderr=self.stderr)


def _write_minimal_harbor_job(job_dir: Path) -> None:
    job_dir.mkdir(parents=True, exist_ok=True)
    (job_dir / "config.json").write_text(json.dumps({"agent": "codex"}), encoding="utf-8")
    (job_dir / "result.json").write_text(json.dumps({"status": "completed"}), encoding="utf-8")


def json_string(value) -> str:
    return json.dumps(value, sort_keys=True)
